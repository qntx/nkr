//! Deployed-gate measurements against a running relay (ws:// or
//! wss://): the Workers acceptance protocol's M3–M8 harness modes.
//!
//! - `rtt`: one connection, n sequential `REQ`→`EOSE`→`CLOSE`
//!   round-trips; publishes the client RTT to the object.
//! - `ingest`: events paced over C connections; measures accepted
//!   rate and `OK` latency.
//! - `wake`: S live subscribers idle through a hibernation, then a
//!   fresh publish must reach every restored subscription exactly
//!   once; reports warm/cold scan latency per round.
//! - `subbudget`: large-filter REQs until the subscription byte
//!   budget refuses one.
//! - `scan`: one stored-scan filter, n times; reports EOSE latency
//!   and whether the socket survives (the Free CPU-ceiling signal).
//!
//! Every mode prints one final JSON line on stdout; progress logs go
//! to stderr.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::{Semaphore, watch};
use yawc::{Frame, OpCode, TcpWebSocket};

use crate::{
    HANDSHAKE_CONCURRENCY, READY_TIMEOUT, author, connect, percentile, sign_event, subscribe,
    unix_now,
};

/// How long an ingest `OK` may take before it counts as a timeout.
const OK_TIMEOUT: Duration = Duration::from_secs(10);
/// How long wake deliveries may lag after the last publish.
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(30);
/// The relay's inbound WebSocket payload cap the subbudget REQs
/// must stay under (native `max_websocket_payload_size`).
const MAX_PAYLOAD: usize = 128 * 1024;

fn runid() -> String {
    format!(
        "{:x}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    )
}

fn log(msg: &str) {
    eprintln!("[{}] {msg}", unix_now());
}

/// The close code of a `Close` frame, when the payload carries one.
fn close_code(frame: &Frame) -> Option<u16> {
    let payload = frame.payload();
    (payload.len() >= 2).then(|| u16::from_be_bytes([payload[0], payload[1]]))
}

/// Read frames until the relay answers `sub_id`: `EOSE` counts as
/// accepted, `CLOSED` returns its text; anything else keeps reading.
/// `None` when the socket died first.
async fn req_outcome(
    ws: &mut TcpWebSocket,
    sub_id: &str,
    filter: &Value,
) -> Option<Result<u64, String>> {
    let req = json!(["REQ", sub_id, filter]).to_string();
    if let Err(e) = ws.send(Frame::text(req)).await {
        return Some(Err(e.to_string()));
    }
    let start = Instant::now();
    let eose = json!(["EOSE", sub_id]).to_string();
    while let Some(frame) = ws.next().await {
        if frame.opcode() != OpCode::Text {
            continue;
        }
        let text = frame.as_str();
        if text == eose {
            return Some(Ok(start.elapsed().as_micros() as u64));
        }
        if text.starts_with("[\"CLOSED\"") {
            return Some(Err(text.to_owned()));
        }
    }
    None
}

/// `rtt`: n sequential `REQ`→`EOSE`→`CLOSE` round-trips on one
/// connection against a scan that always finishes empty.
pub(crate) async fn rtt(url: url::Url, n: usize) {
    let mut ws = connect(&url, false).await.expect("connect");
    let mut micros = Vec::with_capacity(n);
    let probe = json!({"kinds": [65535], "limit": 0});
    for i in 0..n {
        let sub = format!("rtt{i}");
        match req_outcome(&mut ws, &sub, &probe).await {
            Some(Ok(us)) => micros.push(u32::try_from(us).unwrap_or(u32::MAX)),
            outcome => {
                eprintln!("rtt {i} failed: {outcome:?}");
                break;
            }
        }
        let _ = ws.send(Frame::text(format!("[\"CLOSE\",\"{sub}\"]"))).await;
    }
    micros.sort_unstable();
    println!(
        "{}",
        json!({
            "scenario": "rtt",
            "n": micros.len(),
            "p50_ms": percentile(&micros, 50.0),
            "min_ms": micros.first().map_or(f64::NAN, |v| f64::from(*v) / 1000.0),
            "max_ms": micros.last().map_or(f64::NAN, |v| f64::from(*v) / 1000.0),
        })
    );
}

/// The OK reason's bucket: the prefix before `:` (`duplicate`,
/// `invalid`, `rate-limited`, `blocked`, …).
fn ok_prefix(frame_text: &str) -> String {
    serde_json::from_str::<Value>(frame_text).ok().map_or_else(
        || "unparsed".to_owned(),
        |v| {
            v.get(3)
                .and_then(Value::as_str)
                .and_then(|r| r.split(':').next())
                .unwrap_or("unknown")
                .to_owned()
        },
    )
}

/// `ingest`: `events` kind-1 notes spread round-robin over `conns`,
/// each conn pacing one EVENT per `1 / rate_per_conn` seconds and
/// awaiting its `OK` (`OK_TIMEOUT` each). The `t` tag carries the
/// run id so a foreign relay's traffic cannot alias this run.
pub(crate) async fn ingest(url: url::Url, conns: usize, events: usize, rate_per_conn: f64) {
    let tag = format!("gate-{}", runid());
    log(&format!(
        "ingest runid {tag}: {events} events over {conns} conns at {rate_per_conn}/s"
    ));
    let mut tasks = Vec::with_capacity(conns);
    for j in 0..conns {
        let (url, tag) = (url.clone(), tag.clone());
        // Round-robin shares so the total is exactly `events`.
        let mine = events / conns + usize::from(j < events % conns);
        tasks.push(tokio::spawn(async move {
            ingest_conn(url, tag, j, mine, rate_per_conn).await
        }));
    }
    let wall = Instant::now();
    let (mut accepted, mut timeouts, mut errors) = (0usize, 0usize, 0usize);
    let mut rejected: HashMap<String, usize> = HashMap::new();
    let mut ok_us = Vec::new();
    for task in tasks {
        let outcome = task.await.unwrap_or_default();
        accepted += outcome.accepted;
        timeouts += outcome.timeouts;
        errors += outcome.errors;
        for (k, v) in outcome.rejected {
            *rejected.entry(k).or_insert(0) += v;
        }
        ok_us.extend(outcome.ok_us);
    }
    ok_us.sort_unstable();
    let wall_secs = wall.elapsed().as_secs_f64();
    println!(
        "{}",
        json!({
            "scenario": "ingest",
            "runid": tag,
            "events": events,
            "conns": conns,
            "rate_per_conn": rate_per_conn,
            "accepted": accepted,
            "rejected": rejected,
            "timeouts": timeouts,
            "errors": errors,
            "wall_secs": wall_secs,
            "accepted_per_sec": accepted as f64 / wall_secs,
            "ok_p50_ms": percentile(&ok_us, 50.0),
            "ok_p90_ms": percentile(&ok_us, 90.0),
            "ok_p99_ms": percentile(&ok_us, 99.0),
            "ok_max_ms": ok_us.last().map_or(f64::NAN, |v| f64::from(*v) / 1000.0),
        })
    );
}

/// One ingest connection: `count` events paced at `rate` per second,
/// each awaiting its `OK` for `OK_TIMEOUT`.
async fn ingest_conn(
    url: url::Url,
    tag: String,
    index: usize,
    count: usize,
    rate: f64,
) -> IngestOutcome {
    let mut outcome = IngestOutcome::default();
    let Ok(mut ws) = connect(&url, false).await else {
        outcome.errors += 1;
        return outcome;
    };
    let author = author(100_000 + index);
    let tags = json!([["t", tag], ["p", author.pk_hex]]);
    let start = Instant::now();
    for i in 0..count {
        let (id, event) = sign_event(&author, unix_now(), 1, &tags, &format!("ingest {i}"));
        let target = start + Duration::from_secs_f64(i as f64 / rate);
        tokio::time::sleep_until(target.into()).await;
        let sent = Instant::now();
        if ws
            .send(Frame::text(format!("[\"EVENT\",{event}]")))
            .await
            .is_err()
        {
            outcome.errors += 1;
            break;
        }
        match await_ok(&mut ws, &id).await {
            Some(text) if text.contains(",true,") => {
                outcome.accepted += 1;
                outcome
                    .ok_us
                    .push(u32::try_from(sent.elapsed().as_micros()).unwrap_or(u32::MAX));
            }
            Some(text) => *outcome.rejected.entry(ok_prefix(&text)).or_insert(0) += 1,
            None => outcome.timeouts += 1,
        }
    }
    outcome
}

/// The next `OK` frame for event `id`; `None` on timeout, close, or a
/// dead socket — `Err`-flavoured close reasons surface as empty.
async fn await_ok(ws: &mut TcpWebSocket, id: &str) -> Option<String> {
    let want = format!("\"{id}\"");
    let frame = tokio::time::timeout(OK_TIMEOUT, async {
        while let Some(frame) = ws.next().await {
            if frame.opcode() == OpCode::Text
                && frame.as_str().starts_with("[\"OK\"")
                && frame.as_str().contains(&want)
            {
                return Some(frame.as_str().to_owned());
            }
        }
        Some(String::new())
    })
    .await
    .ok()??;
    if frame.is_empty() { None } else { Some(frame) }
}

#[derive(Default)]
struct IngestOutcome {
    accepted: usize,
    rejected: HashMap<String, usize>,
    timeouts: usize,
    errors: usize,
    ok_us: Vec<u32>,
}

/// A wake subscriber's shared delivery accounting: `delivered[gi]`
/// counts the subscribers that received global event `gi`, `dups`
/// second-and-later receives, `lats` per-delivery `(gi, µs)`.
struct WakeShared {
    index: HashMap<String, usize>,
    sent: Vec<AtomicU64>,
    delivered: Vec<AtomicUsize>,
    dups: AtomicUsize,
    lats: std::sync::Mutex<Vec<(usize, u32)>>,
    closed: std::sync::Mutex<Vec<Option<u16>>>,
}

/// One wake subscriber: after EOSE it only reads — the idle window
/// must see zero client traffic, and yawc sends no pings of its own.
async fn wake_subscriber(
    mut ws: TcpWebSocket,
    shared: Arc<WakeShared>,
    base: Instant,
    mut stop: watch::Receiver<bool>,
) {
    let mut seen = vec![false; shared.sent.len()];
    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else {
                    shared.closed.lock().expect("mutex").push(None);
                    return;
                };
                if frame.opcode() == OpCode::Close {
                    shared.closed.lock().expect("mutex").push(close_code(&frame));
                    return;
                }
                if frame.opcode() != OpCode::Text {
                    continue;
                }
                let text = frame.as_str();
                if !text.starts_with("[\"EVENT\"") {
                    continue;
                }
                let Some(pos) = text.find("\"id\":\"") else {
                    continue;
                };
                let Some(id) = text.get(pos + 6..pos + 70) else {
                    continue;
                };
                let Some(&gi) = shared.index.get(id) else {
                    continue;
                };
                if seen[gi] {
                    shared.dups.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                seen[gi] = true;
                shared.delivered[gi].fetch_add(1, Ordering::Relaxed);
                let sent_at = shared.sent[gi].load(Ordering::Acquire);
                if sent_at != 0 {
                    let us = base.elapsed().as_nanos() as u64 - (sent_at - 1);
                    shared
                        .lats
                        .lock()
                        .expect("mutex")
                        .push((gi, u32::try_from(us / 1000).unwrap_or(u32::MAX)));
                }
            }
            _ = stop.changed() => return,
        }
    }
}

/// The NIP-42 AUTH event answering a relay challenge (kind 22242,
/// `relay` + `challenge` tags, empty content).
fn auth_event(author: &crate::Author, relay: &str, challenge: &str) -> (String, String) {
    let tags = json!([["relay", relay], ["challenge", challenge]]);
    sign_event(author, unix_now(), 22242, &tags, "")
}

/// Drive one connection through the lazy NIP-42 flow: restricted
/// REQ → `CLOSED auth-required:` + `AUTH` challenge → `AUTH` → re-REQ
/// → `EOSE`. Returns the live socket with its restricted sub open.
async fn authed_connection(url: &url::Url, author: &crate::Author) -> Result<TcpWebSocket, String> {
    let mut ws = connect(url, false).await?;
    let filter = json!({"kinds": [4], "#p": [author.pk_hex], "limit": 0});
    ws.send(Frame::text(json!(["REQ", "a", filter]).to_string()))
        .await
        .map_err(|e| e.to_string())?;
    // The challenge arrives lazily with the refusal.
    let mut challenge = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while challenge.is_none() && Instant::now() < deadline {
        let Some(frame) = tokio::time::timeout(Duration::from_secs(15), ws.next())
            .await
            .map_err(|_| "timed out waiting for the AUTH challenge".to_owned())?
        else {
            return Err("socket closed before the AUTH challenge".to_owned());
        };
        if frame.opcode() == OpCode::Text
            && let Some(v) = serde_json::from_str::<Value>(frame.as_str()).ok()
            && v.get(0) == Some(&json!("AUTH"))
        {
            challenge = v.get(1).and_then(Value::as_str).map(str::to_owned);
        }
    }
    let challenge = challenge.ok_or("no AUTH challenge before the deadline")?;
    let (id, event) = auth_event(author, url.as_str().trim_end_matches('/'), &challenge);
    ws.send(Frame::text(format!("[\"AUTH\",{event}]")))
        .await
        .map_err(|e| e.to_string())?;
    // The AUTH verdict OK must be accepted before the re-REQ.
    let want = format!("\"{id}\"");
    let accepted = tokio::time::timeout(OK_TIMEOUT, async {
        while let Some(frame) = ws.next().await {
            if frame.opcode() == OpCode::Text
                && frame.as_str().starts_with("[\"OK\"")
                && frame.as_str().contains(&want)
            {
                return frame.as_str().contains(",true,");
            }
        }
        false
    })
    .await
    .map_err(|_| "timed out waiting for the AUTH OK".to_owned())?;
    if !accepted {
        return Err("AUTH was refused".to_owned());
    }
    subscribe(&mut ws, "a", &filter).await?;
    Ok(ws)
}

/// Whether event `id` reaches `ws` within `DELIVERY_TIMEOUT`.
async fn await_event(ws: &mut TcpWebSocket, id: &str) -> bool {
    let want = format!("\"{id}\"");
    tokio::time::timeout(DELIVERY_TIMEOUT, async {
        while let Some(frame) = ws.next().await {
            if frame.opcode() == OpCode::Text
                && frame.as_str().starts_with("[\"EVENT\"")
                && frame.as_str().contains(&want)
            {
                return true;
            }
        }
        false
    })
    .await
    .unwrap_or(false)
}

/// `wake`: S subscribers with `limit:0` live filters tagged
/// `wake-<runid>` idle through a hibernation; per round a warm and a
/// cold probe measure scan latency around `idle_secs` of silence,
/// then a fresh publish must reach every restored sub exactly once.
#[allow(clippy::too_many_lines)]
pub(crate) async fn wake(
    url: url::Url,
    subs: usize,
    idle_secs: u64,
    rounds: usize,
    events: usize,
    auth: bool,
) {
    let tag = format!("wake-{}", runid());
    log(&format!(
        "wake runid {tag}: {subs} subs, {rounds} rounds of {idle_secs}s idle"
    ));
    let now = unix_now();
    let auth_key = author(900_001);
    let round_key = |r: usize| author(910_000 + r);

    // Pre-sign every event of every round: `gi = r * events + i`.
    let total = rounds * events;
    let mut shared = WakeShared {
        index: HashMap::with_capacity(total),
        sent: (0..total).map(|_| AtomicU64::new(0)).collect(),
        delivered: (0..total).map(|_| AtomicUsize::new(0)).collect(),
        dups: AtomicUsize::new(0),
        lats: std::sync::Mutex::new(Vec::new()),
        closed: std::sync::Mutex::new(Vec::new()),
    };
    let mut messages = Vec::with_capacity(total);
    for gi in 0..total {
        let r = gi / events;
        let (id, event) = sign_event(
            &round_key(r),
            now,
            1,
            &json!([["t", tag]]),
            &format!("wake {gi}"),
        );
        shared.index.insert(id, gi);
        messages.push(event);
    }
    let shared = Arc::new(shared);
    let base = Instant::now();

    // Subscribers: one REQ each, delivering after EOSE only.
    let (stop_tx, stop_rx) = watch::channel(false);
    let ready = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(HANDSHAKE_CONCURRENCY));
    let filter = json!({"kinds": [1], "#t": [tag], "limit": 0});
    let mut tasks = Vec::with_capacity(subs);
    for j in 0..subs {
        let (url, filter, shared, ready, failed, gate, stop) = (
            url.clone(),
            filter.clone(),
            Arc::clone(&shared),
            Arc::clone(&ready),
            Arc::clone(&failed),
            Arc::clone(&gate),
            stop_rx.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let permit = gate.acquire_owned().await.expect("semaphore open");
            let mut ws = match connect(&url, false).await {
                Ok(ws) => ws,
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    log(&format!("sub {j} connect failed: {e}"));
                    return;
                }
            };
            if let Err(e) = subscribe(&mut ws, &format!("w{j}"), &filter).await {
                failed.fetch_add(1, Ordering::Relaxed);
                log(&format!("sub {j} subscribe failed: {e}"));
                return;
            }
            drop(permit);
            ready.fetch_add(1, Ordering::Relaxed);
            wake_subscriber(ws, shared, base, stop).await;
        }));
    }
    let deadline = Instant::now() + READY_TIMEOUT;
    while ready.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < subs {
        assert!(Instant::now() < deadline, "subscribers not ready in time");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    log(&format!(
        "subscribers ready: {} ({} failed)",
        ready.load(Ordering::Relaxed),
        failed.load(Ordering::Relaxed)
    ));

    // The probe and the optional authed connection sit beside the
    // subscribers; both survive the idle window.
    let mut probe = connect(&url, false).await.expect("probe connects");
    let mut authed = if auth {
        match authed_connection(&url, &auth_key).await {
            Ok(ws) => {
                // The authed sub is watched by a subscriber-style task
                // for the per-round kind-4 events published to it.
                Some(ws)
            }
            Err(e) => {
                log(&format!("auth setup failed: {e}"));
                None
            }
        }
    } else {
        None
    };

    let probe_filter = json!({"kinds": [65535], "limit": 0});
    let mut rounds_json = Vec::with_capacity(rounds);
    for r in 0..rounds {
        // (a) warm: the object is live right now.
        let warm = req_outcome(&mut probe, "probe", &probe_filter).await;
        let warm_ms = warm
            .and_then(|r| r.ok())
            .map_or(f64::NAN, |us| us as f64 / 1000.0);

        // (b) idle: nothing is sent on any connection — subscribers
        // only read, yawc never pings on its own.
        tokio::time::sleep(Duration::from_secs(idle_secs)).await;

        // (c) cold: this REQ is the message that wakes the object.
        let cold = req_outcome(&mut probe, "probe", &probe_filter).await;
        let cold_ms = cold
            .and_then(|r| r.ok())
            .map_or(f64::NAN, |us| us as f64 / 1000.0);
        log(&format!(
            "round {r}: warm {warm_ms:.1} ms, cold {cold_ms:.1} ms"
        ));

        // (d) a fresh publisher sends this round's events.
        let closed_before = shared.closed.lock().expect("mutex").len();
        let mut publisher = connect(&url, false).await.expect("publisher connects");
        for i in 0..events {
            let gi = r * events + i;
            shared.sent[gi].store(base.elapsed().as_nanos() as u64 + 1, Ordering::Release);
            publisher
                .send(Frame::text(format!("[\"EVENT\",{}]", messages[gi])))
                .await
                .expect("publish send");
        }
        drop(publisher);

        // --auth: one kind-4 event p-tagged to the authed key must
        // reach the restored restricted subscription.
        let mut authed_got = false;
        if let Some(ws) = authed.as_mut() {
            let (auth_id, auth_ev) = sign_event(
                &round_key(r),
                now,
                4,
                &json!([["p", auth_key.pk_hex], ["t", tag]]),
                "wake dm",
            );
            let mut w = connect(&url, false).await.expect("dm publisher connects");
            let _ = w.send(Frame::text(format!("[\"EVENT\",{auth_ev}]"))).await;
            drop(w);
            authed_got = await_event(ws, &auth_id).await;
        }

        // Wait for the drain: every live subscriber got every event.
        let target = ready.load(Ordering::Relaxed);
        let drain = Instant::now() + DELIVERY_TIMEOUT;
        loop {
            let got_all = (0..events)
                .all(|i| shared.delivered[r * events + i].load(Ordering::Relaxed) >= target);
            if got_all || Instant::now() > drain {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let delivered: usize = (0..events)
            .map(|i| shared.delivered[r * events + i].load(Ordering::Relaxed))
            .sum();
        let expected = target * events;
        let mut lats: Vec<u32> = shared
            .lats
            .lock()
            .expect("mutex")
            .iter()
            .filter(|(gi, _)| *gi / events == r)
            .map(|(_, us)| *us)
            .collect();
        lats.sort_unstable();
        let closed: Vec<Option<u16>> = shared
            .closed
            .lock()
            .expect("mutex")
            .drain(closed_before..)
            .collect();
        rounds_json.push(json!({
            "round": r,
            "warm_ms": warm_ms,
            "cold_ms": cold_ms,
            "expected": expected,
            "delivered": delivered,
            "missing": expected.saturating_sub(delivered),
            "duplicates": shared.dups.swap(0, Ordering::Relaxed),
            "delivery_p50_ms": percentile(&lats, 50.0),
            "delivery_p99_ms": percentile(&lats, 99.0),
            "closed": closed.len(),
            "close_codes": closed,
            "authed_received": authed_got,
        }));
    }
    stop_tx.send_replace(true);
    for task in tasks {
        let _ = task.await;
    }
    println!(
        "{}",
        json!({
            "scenario": "wake",
            "runid": tag,
            "subs": subs,
            "subs_failed": failed.load(Ordering::Relaxed),
            "idle_secs": idle_secs,
            "events_per_round": events,
            "auth": auth,
            "rounds": rounds_json,
        })
    );
}

/// The wire size of one `--authors`-sized `subbudget` REQ.
fn subbudget_req_bytes(authors: usize) -> usize {
    // `["REQ","b9999999",{"authors":["<64 hex>"×N,],\"limit\":0}]`.
    40 + authors * 67
}

/// `subbudget`: REQs of `--authors`-sized author lists until the
/// relay refuses; each is paced under the default REQ rate limit.
pub(crate) async fn subbudget(url: url::Url, authors: usize) {
    let bytes = subbudget_req_bytes(authors);
    assert!(
        bytes < MAX_PAYLOAD,
        "REQ wire size {bytes} exceeds the {MAX_PAYLOAD} payload cap"
    );
    let mut ws = connect(&url, false).await.expect("connect");
    let pks: Vec<String> = (0..authors).map(|i| author(2_000_000 + i).pk_hex).collect();
    let filter = json!({"authors": pks, "limit": 0});
    let frame = |sub: &str| json!(["REQ", sub, filter]).to_string();
    assert!(frame("b0").len() < MAX_PAYLOAD);

    let mut accepted = 0usize;
    let mut terminal = String::from("exhausted sub ids");
    for i in 0..1000 {
        let outcome = req_outcome(&mut ws, &format!("b{i}"), &filter).await;
        match outcome {
            Some(Ok(_)) => accepted += 1,
            Some(Err(text)) => {
                terminal = text;
                break;
            }
            None => {
                terminal = "socket closed".to_owned();
                break;
            }
        }
        // Stay under the default 15 REQ/s bucket.
        tokio::time::sleep(Duration::from_millis(70)).await;
    }
    // A still-open socket answers a small REQ.
    let alive = matches!(
        req_outcome(&mut ws, "alive", &json!({"kinds": [65535], "limit": 0})).await,
        Some(Ok(_))
    );
    println!(
        "{}",
        json!({
            "scenario": "subbudget",
            "authors": authors,
            "req_bytes": frame("b0").len(),
            "accepted": accepted,
            "terminal": terminal,
            "socket_alive_after": alive,
        })
    );
}

/// `scan`: `n` times the same filter → EOSE latency and event count;
/// a socket close or error is the M8 CPU-ceiling signal.
pub(crate) async fn scan(url: url::Url, filter_json: &str, n: usize) {
    let filter: Value = serde_json::from_str(filter_json).expect("valid filter JSON");
    let mut ws = connect(&url, false).await.expect("connect");
    let mut runs = Vec::with_capacity(n);
    let mut terminal = Value::Null;
    for i in 0..n {
        let sub = format!("s{i}");
        let req = json!(["REQ", sub, filter]).to_string();
        let start = Instant::now();
        if ws.send(Frame::text(req)).await.is_err() {
            terminal = json!("send failed");
            break;
        }
        let mut events = 0usize;
        let mut done = None;
        let eose = json!(["EOSE", sub]).to_string();
        while let Some(frame) = ws.next().await {
            if frame.opcode() != OpCode::Text {
                continue;
            }
            let text = frame.as_str();
            if text.starts_with("[\"EVENT\"") {
                events += 1;
            } else if text == eose {
                done = Some(Ok(events));
                break;
            } else if text.starts_with("[\"CLOSED\"") {
                done = Some(Err(text.to_owned()));
                break;
            }
        }
        match done {
            Some(Ok(count)) => runs.push(json!({
                "eose_ms": start.elapsed().as_secs_f64() * 1000.0,
                "events": count,
            })),
            Some(Err(text)) => {
                terminal = json!(text);
                break;
            }
            None => {
                terminal = json!("socket closed");
                break;
            }
        }
        let _ = ws.send(Frame::text(format!("[\"CLOSE\",\"{sub}\"]"))).await;
    }
    let mut micros: Vec<u32> = runs
        .iter()
        .map(|r| (r["eose_ms"].as_f64().unwrap_or(0.0) * 1000.0) as u32)
        .collect();
    micros.sort_unstable();
    println!(
        "{}",
        json!({
            "scenario": "scan",
            "filter": filter,
            "runs": runs,
            "terminal": terminal,
            "eose_p50_ms": percentile(&micros, 50.0),
            "eose_max_ms": micros.last().map_or(f64::NAN, |v| f64::from(*v) / 1000.0),
        })
    );
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, reason = "tests may panic")]
mod tests {
    use super::*;

    #[test]
    fn ok_prefix_buckets_reasons() {
        assert_eq!(
            ok_prefix("[\"OK\",\"a\",false,\"blocked: subscription capacity exhausted\"]"),
            "blocked"
        );
        assert_eq!(
            ok_prefix("[\"OK\",\"a\",false,\"duplicate: x\"]"),
            "duplicate"
        );
        assert_eq!(ok_prefix("not json"), "unparsed");
    }

    #[test]
    fn subbudget_req_fits_the_payload_cap() {
        // 1900 authors × 67 B + frame overhead stays under 128 KiB.
        assert!(subbudget_req_bytes(1900) < MAX_PAYLOAD);
        assert!(subbudget_req_bytes(1900) > 120_000);
        let pks: Vec<String> = (0..1900).map(|i| author(2_000_000 + i).pk_hex).collect();
        let req = json!(["REQ", "b0", {"authors": pks, "limit": 0}]).to_string();
        assert!(req.len() < MAX_PAYLOAD);
    }

    #[test]
    fn close_code_reads_the_payload() {
        let frame = Frame::close(yawc::close::CloseCode::Normal, b"bye");
        assert_eq!(close_code(&frame), Some(1000));
        assert_eq!(close_code(&Frame::text("x")), None);
    }
}
