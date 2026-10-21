//! `soak`: a mixed long-running workload against one relay.
//!
//! Publishers write a deterministic kind mix and await every `OK`;
//! long-lived subscribers count deliveries; short-lived connections
//! churn through historical REQs; slow consumers hold a `REQ {}` they
//! never read. One JSON line per sample plus a final `end` line —
//! verdicts are left to whoever analyzes the JSONL.

use std::collections::{BTreeMap, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::watch;
use yawc::{Frame, OpCode, TcpWebSocket};

use crate::{Author, author, connect, proc_stats, sign_event, unix_now};

const AUTHORS: usize = 64;
const WORDS: [&str; 8] = [
    "nostr", "soak", "relay", "bench", "lmdb", "rust", "event", "zap",
];
const D_VALS: usize = 16;
/// A publisher gives up on an `OK` after this and reconnects.
const OK_TIMEOUT: Duration = Duration::from_secs(10);
/// A churn REQ that does not reach EOSE in time is a failure.
const CHURN_TIMEOUT: Duration = Duration::from_secs(10);
/// Slow consumers are dropped after this, whether or not the relay
/// already closed them.
const SLOW_HOLD: Duration = Duration::from_secs(15 * 60);

#[derive(Default)]
struct Counters {
    /// `OK` verdicts by `"<accepted>:<reason prefix>"`.
    ok: std::sync::Mutex<BTreeMap<String, u64>>,
    deliveries: AtomicU64,
    sub_reconnects: AtomicU64,
    churn_done: AtomicU64,
    churn_failed: AtomicU64,
    slow_spawned: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    fn ok(&self, accepted: bool, message: &str) {
        let prefix = message.split(':').next().unwrap_or("");
        self.ok
            .lock()
            .unwrap()
            .entry(format!("{accepted}:{prefix}"))
            .and_modify(|n| *n += 1)
            .or_insert(1);
    }

    fn snapshot(&self) -> Value {
        json!({
            "ok": self.ok.lock().unwrap().clone(),
            "deliveries": self.deliveries.load(Ordering::Relaxed),
            "sub_reconnects": self.sub_reconnects.load(Ordering::Relaxed),
            "churn_done": self.churn_done.load(Ordering::Relaxed),
            "churn_failed": self.churn_failed.load(Ordering::Relaxed),
            "slow_spawned": self.slow_spawned.load(Ordering::Relaxed),
            "errors": self.errors.load(Ordering::Relaxed),
        })
    }
}

/// `n % 100` slots of each non-kind-1 flavor, spread through the
/// cycle so the same signing key never emits two replaceables in one
/// second (a same-second kind-0 for one key is `duplicate:`-rejected).
const SLOTS_0: [u64; 10] = [5, 15, 25, 35, 45, 55, 65, 75, 85, 95];
const SLOTS_30023: [u64; 8] = [7, 11, 21, 36, 51, 66, 81, 96];
const SLOTS_20001: [u64; 7] = [3, 18, 33, 48, 63, 78, 93];
const SLOTS_5: [u64; 5] = [0, 20, 40, 60, 80];

/// A `created_at` for a replaceable event: `now`, but bumped past the
/// slot's previous value so a burst after an `OK` stall can never
/// produce a `Superseded` rejection (`duplicate:`).
fn replaceable_ca(slots: &mut BTreeMap<u64, u64>, slot: u64, now: u64) -> u64 {
    let ca = now.max(slots.get(&slot).map_or(0, |last| last + 1));
    slots.insert(slot, ca);
    ca
}

/// The event a publisher emits on counter `n`. `recent` holds this
/// publisher's last 100 acked kind-1 ids as `(key, id)` pairs so a
/// kind-5 always targets an event of the key that signs it.
/// `replaceable` remembers the last `created_at` per replaceable slot.
fn mix(
    n: u64,
    owned: &[Author],
    authors: &[String],
    recent: &VecDeque<(usize, String)>,
    replaceable: &mut BTreeMap<u64, u64>,
) -> (usize, u16, Value, String, u64) {
    let now = unix_now();
    let bucket = n % 100;
    let key = n as usize % owned.len();
    if SLOTS_5.contains(&bucket) && !recent.is_empty() {
        let (target_key, id) = &recent[n as usize % recent.len()];
        // Deletion requests expire too, so every stored event is either
        // expiring or replaceable and the live set stays bounded.
        let tags = json!([["e", id], ["expiration", (now + 600).to_string()]]);
        (*target_key, 5, tags, String::new(), now)
    } else if SLOTS_0.contains(&bucket) {
        let ca = replaceable_ca(replaceable, key as u64, now);
        (key, 0, json!([]), format!("profile {n}"), ca)
    } else if SLOTS_30023.contains(&bucket) {
        let d = n as usize % D_VALS;
        let ca = replaceable_ca(
            replaceable,
            1000 + key as u64 * D_VALS as u64 + d as u64,
            now,
        );
        (
            key,
            30023,
            json!([["d", format!("d{d}")]]),
            format!("article {n}"),
            ca,
        )
    } else if SLOTS_20001.contains(&bucket) {
        (key, 20001, json!([]), format!("ephemeral {n}"), now)
    } else {
        let tags = json!([
            ["t", WORDS[n as usize % WORDS.len()]],
            ["p", authors[(n as usize * 7 + 13) % AUTHORS]],
            ["expiration", (now + 600).to_string()],
        ]);
        (key, 1, tags, format!("soak note {n}"), now)
    }
}

/// Await the `OK` for `id`, skipping other frames; `None` when the
/// socket dies first.
async fn read_ok(ws: &mut TcpWebSocket, id: &str) -> Option<(bool, String)> {
    while let Some(frame) = ws.next().await {
        if frame.opcode() != OpCode::Text {
            continue;
        }
        let Ok(v) = serde_json::from_str::<Value>(frame.as_str()) else {
            continue;
        };
        if v.get(0).and_then(Value::as_str) == Some("OK")
            && v.get(1).and_then(Value::as_str) == Some(id)
        {
            let accepted = v.get(2).and_then(Value::as_bool).unwrap_or(false);
            let message = v.get(3).and_then(Value::as_str).unwrap_or("").to_owned();
            return Some((accepted, message));
        }
    }
    None
}

/// Keep the newest `capacity` accepted kind-1 event ids for `mix` to
/// reply to.
fn track_recent(recent: &mut VecDeque<(usize, String)>, keep: bool, key: usize, id: String) {
    if !keep {
        return;
    }
    if recent.len() == recent.capacity() {
        recent.pop_front();
    }
    recent.push_back((key, id));
}

/// One publisher: paces to `rate / publishers`, sends and awaits each
/// `OK`, reconnects after any error.
async fn publisher(
    url: url::Url,
    owned: Vec<Author>,
    authors: Arc<Vec<String>>,
    each_per_sec: f64,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let mut n = 0u64;
    let mut recent: VecDeque<(usize, String)> = VecDeque::with_capacity(100);
    let mut replaceable = BTreeMap::new();
    while !*stop.borrow() {
        let mut ws = match connect(&url, false).await {
            Ok(ws) => ws,
            Err(_) => {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        let interval = Duration::from_secs_f64(1.0 / each_per_sec);
        let mut next = Instant::now();
        loop {
            n += 1;
            let (key, kind, tags, content, created_at) =
                mix(n, &owned, &authors, &recent, &mut replaceable);
            let (id, event) = sign_event(&owned[key], created_at, kind, &tags, &content);
            next += interval;
            // A stall drops the missed sends instead of bursting them
            // later: the soak measures steady load.
            let now = Instant::now();
            if next < now {
                next = now;
            }
            tokio::select! {
                () = tokio::time::sleep_until(next.into()) => {}
                _ = stop.changed() => return,
            }
            if ws
                .send(Frame::text(format!("[\"EVENT\",{event}]")))
                .await
                .is_err()
            {
                counters.errors.fetch_add(1, Ordering::Relaxed);
                break;
            }
            let ok = tokio::time::timeout(OK_TIMEOUT, read_ok(&mut ws, &id)).await;
            match ok {
                Ok(Some((accepted, message))) => {
                    counters.ok(accepted, &message);
                    track_recent(&mut recent, accepted && kind == 1, key, id);
                }
                // OK timeout or dead socket: count and reconnect.
                Ok(None) | Err(_) => {
                    counters.errors.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
            if *stop.borrow() {
                return;
            }
        }
    }
}

/// One long-lived subscriber counting EVENT frames; reconnects when
/// the relay closes the socket.
async fn subscriber(
    url: url::Url,
    filter: Value,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let req = json!(["REQ", "soak", filter]).to_string();
    while !*stop.borrow() {
        let mut ws = match connect(&url, false).await {
            Ok(ws) => ws,
            Err(_) => {
                counters.sub_reconnects.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if ws.send(Frame::text(req.clone())).await.is_err() {
            counters.sub_reconnects.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        loop {
            tokio::select! {
                frame = ws.next() => {
                    let Some(frame) = frame else {
                        counters.sub_reconnects.fetch_add(1, Ordering::Relaxed);
                        break;
                    };
                    if frame.opcode() == OpCode::Text
                        && frame.as_str().starts_with("[\"EVENT\"")
                    {
                        counters.deliveries.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ = stop.changed() => return,
            }
        }
    }
}

/// One churn connection: REQ, read to EOSE, CLOSE, gone.
async fn churn_once(
    url: url::Url,
    index: usize,
    authors: Arc<Vec<String>>,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let filters = [
        json!({"kinds": [1], "limit": 100}),
        json!({"authors": [authors[index % AUTHORS]], "limit": 50}),
        json!({"#t": [WORDS[index % WORDS.len()]], "limit": 50}),
        json!({"kinds": [0]}),
    ];
    let filter = &filters[index % filters.len()];
    let result = async {
        let mut ws = connect(&url, false).await?;
        ws.send(Frame::text(json!(["REQ", "churn", filter]).to_string()))
            .await
            .map_err(|e| e.to_string())?;
        while let Some(frame) = ws.next().await {
            if frame.opcode() != OpCode::Text {
                continue;
            }
            let Ok(v) = serde_json::from_str::<Value>(frame.as_str()) else {
                continue;
            };
            match v.get(0).and_then(Value::as_str) {
                Some("EOSE") => {
                    ws.send(Frame::text(json!(["CLOSE", "churn"]).to_string()))
                        .await
                        .ok();
                    return Ok(());
                }
                Some("CLOSED") | Some("NOTICE") => return Err(frame.as_str().to_owned()),
                _ => {}
            }
        }
        Err("closed before EOSE".to_owned())
    };
    tokio::select! {
        // A detached task must still close its socket at stop so the
        // drain check can see zero connections.
        _ = stop.changed() => {}
        result = tokio::time::timeout(CHURN_TIMEOUT, result) => match result {
            Ok(Ok(())) => {
                counters.churn_done.fetch_add(1, Ordering::Relaxed);
            }
            Ok(Err(_)) | Err(_) => {
                counters.churn_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// Spawns `churn_rate` connections per second until stopped.
async fn churner(
    url: url::Url,
    rate_per_sec: f64,
    authors: Arc<Vec<String>>,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let mut index = 0usize;
    let mut next = Instant::now();
    loop {
        next += Duration::from_secs_f64(1.0 / rate_per_sec);
        tokio::select! {
            () = tokio::time::sleep_until(next.into()) => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
        tokio::spawn(churn_once(
            url.clone(),
            index,
            Arc::clone(&authors),
            Arc::clone(&counters),
            stop.clone(),
        ));
        index = index.wrapping_add(1);
    }
}

/// Every `every` seconds open a `REQ {}` consumer that never reads:
/// its socket buffers fill and the relay's write side stalls, which
/// is the point. The task holds the socket without polling it, so a
/// relay-side close just leaves a dead handle to drop at SLOW_HOLD.
async fn slow_spawner(
    url: url::Url,
    every: Duration,
    counters: Arc<Counters>,
    mut stop: watch::Receiver<bool>,
) {
    let mut ticker = tokio::time::interval(every);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            _ = stop.changed() => return,
        }
        if *stop.borrow() {
            return;
        }
        let url = url.clone();
        let counters = Arc::clone(&counters);
        let mut held = stop.clone();
        tokio::spawn(async move {
            let Ok(mut ws) = connect(&url, false).await else {
                return;
            };
            if ws
                .send(Frame::text(json!(["REQ", "slow", {}]).to_string()))
                .await
                .is_err()
            {
                return;
            }
            counters.slow_spawned.fetch_add(1, Ordering::Relaxed);
            // Keep the socket open and never read: incoming deliveries
            // back up in the kernel buffer and the relay's write queue.
            // Stop still closes it so the drain check sees zero conns.
            tokio::select! {
                () = tokio::time::sleep(SLOW_HOLD) => {}
                _ = held.changed() => {}
            }
            drop(ws);
        });
    }
}

/// `GET <url>` over a plain TcpStream; returns the body.
async fn http_get(url: &url::Url) -> Option<String> {
    let host = url.host_str()?;
    let port = url.port_or_known_default()?;
    let path = if url.path().is_empty() {
        "/"
    } else {
        url.path()
    };
    let mut stream = tokio::net::TcpStream::connect((host, port)).await.ok()?;
    let request = format!("GET {path} HTTP/1.0\r\nHost: {host}\r\n\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.ok()?;
    let text = String::from_utf8_lossy(&response);
    let pos = text.find("\r\n\r\n")?;
    Some(text[pos + 4..].to_owned())
}

/// The relay-side gauges/counters a sample needs.
struct RelayMetrics {
    /// `nosfry_connections`.
    conns: Option<u64>,
    /// `nosfry_connections_rejected_total`.
    rejected: Option<u64>,
    /// Summed `nosfry_rate_limited_total{…}`.
    limited: Option<u64>,
    /// `nosfry_outbound_pending_bytes`.
    outbound_pending: Option<u64>,
}

/// `nosfry_connections`, `nosfry_connections_rejected_total`, the
/// summed `nosfry_rate_limited_total{…}` and
/// `nosfry_outbound_pending_bytes` from the metrics body.
fn parse_metrics(body: &str) -> RelayMetrics {
    // Zero-valued counters are omitted from the exposition, so a
    // fetched body starts both totals at 0.
    let mut m = RelayMetrics {
        conns: None,
        rejected: Some(0),
        limited: Some(0),
        outbound_pending: None,
    };
    for line in body.lines() {
        let value = || line.rsplit(' ').next()?.parse::<u64>().ok();
        if line.starts_with("nosfry_connections ") {
            m.conns = value();
        } else if line.starts_with("nosfry_connections_rejected_total ") {
            m.rejected = value();
        } else if line.starts_with("nosfry_rate_limited_total") {
            if let Some(n) = value() {
                m.limited = m.limited.map(|total| total + n);
            }
        } else if line.starts_with("nosfry_outbound_pending_bytes ") {
            m.outbound_pending = value();
        }
    }
    m
}

/// Size of `<db-dir>/data.mdb` in bytes, if present.
fn db_bytes(db_dir: &Option<PathBuf>) -> Option<u64> {
    db_dir
        .as_ref()
        .and_then(|d| std::fs::metadata(d.join("data.mdb")).ok())
        .map(|m| m.len())
}

/// Everything the relay exposes in one sample line.
fn sample_line(
    t: u64,
    pid: Option<u32>,
    db_dir: &Option<PathBuf>,
    metrics: Option<&str>,
    counters: &Counters,
) -> Value {
    let m = metrics.map(parse_metrics);
    let mut line = counters.snapshot();
    let obj = line.as_object_mut().expect("snapshot is an object");
    obj.insert("type".into(), json!("sample"));
    obj.insert("t".into(), json!(t));
    let stats = pid.and_then(proc_stats);
    obj.insert(
        "rss_kib".into(),
        stats.as_ref().map_or(Value::Null, |s| json!(s.rss_kib)),
    );
    // Anonymous memory only: VmRSS also counts the LMDB map pages the
    // process touched, which are reclaimable page cache bounded by the
    // database size.
    obj.insert(
        "rss_anon_kib".into(),
        stats
            .as_ref()
            .and_then(|s| s.rss_anon_kib)
            .map_or(Value::Null, |n| json!(n)),
    );
    obj.insert(
        "db_bytes".into(),
        db_bytes(db_dir).map_or(Value::Null, |n| json!(n)),
    );
    obj.insert(
        "conns".into(),
        m.as_ref().map_or(Value::Null, |m| opt(m.conns)),
    );
    obj.insert(
        "rejected_total".into(),
        m.as_ref().map_or(Value::Null, |m| opt(m.rejected)),
    );
    obj.insert(
        "rate_limited_total".into(),
        m.as_ref().map_or(Value::Null, |m| opt(m.limited)),
    );
    obj.insert(
        "outbound_pending".into(),
        m.as_ref().map_or(Value::Null, |m| opt(m.outbound_pending)),
    );
    line
}

/// `Option<u64>` as a JSON number or null.
fn opt(value: Option<u64>) -> Value {
    value.map_or(Value::Null, |n| json!(n))
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run(
    url: url::Url,
    metrics_url: Option<url::Url>,
    pid: Option<u32>,
    db_dir: Option<PathBuf>,
    duration_secs: u64,
    sample_secs: u64,
    publishers: usize,
    rate: f64,
    subscribers: usize,
    churn_rate: f64,
    slow_every_secs: u64,
    label: String,
) {
    let counters = Arc::new(Counters::default());
    let authors = Arc::new(
        (0..AUTHORS)
            .map(|i| author(i).pk_hex)
            .collect::<Vec<String>>(),
    );
    let (stop_tx, stop_rx) = watch::channel(false);
    let start = Instant::now();
    let mut tasks = Vec::new();

    // Publisher i owns keys i, i+P, i+2P, …
    for i in 0..publishers.max(1) {
        let owned: Vec<Author> = (i..AUTHORS)
            .step_by(publishers.max(1))
            .map(author)
            .collect();
        tasks.push(tokio::spawn(publisher(
            url.clone(),
            owned,
            Arc::clone(&authors),
            rate / publishers.max(1) as f64,
            Arc::clone(&counters),
            stop_rx.clone(),
        )));
    }

    // Long-lived subscribers, each following 4 spread authors.
    for j in 0..subscribers {
        let filter = json!({
            "kinds": [1, 0, 30023, 20001],
            "authors": (0..4).map(|k| authors[(j + k * 16) % AUTHORS].clone()).collect::<Vec<_>>(),
        });
        tasks.push(tokio::spawn(subscriber(
            url.clone(),
            filter,
            Arc::clone(&counters),
            stop_rx.clone(),
        )));
    }

    tasks.push(tokio::spawn(churner(
        url.clone(),
        churn_rate,
        Arc::clone(&authors),
        Arc::clone(&counters),
        stop_rx.clone(),
    )));

    if slow_every_secs > 0 {
        tasks.push(tokio::spawn(slow_spawner(
            url.clone(),
            Duration::from_secs(slow_every_secs),
            Arc::clone(&counters),
            stop_rx.clone(),
        )));
    }

    // Sampling.
    let deadline = Duration::from_secs(duration_secs);
    let mut ticker = tokio::time::interval(Duration::from_secs(sample_secs.max(1)));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        let t = start.elapsed().as_secs();
        let body = match &metrics_url {
            Some(u) => http_get(u).await,
            None => None,
        };
        println!(
            "{}",
            sample_line(t, pid, &db_dir, body.as_deref(), &counters)
        );
        if start.elapsed() >= deadline {
            break;
        }
    }
    stop_tx.send_replace(true);
    for task in tasks {
        task.abort();
        let _ = task.await;
    }

    // Drain check: once every harness connection is closed the relay
    // must report zero queued outbound bytes — a non-zero gauge means
    // a closed connection still holds a charge.
    let mut drained = None;
    if let Some(u) = &metrics_url {
        let until = Instant::now() + Duration::from_secs(10);
        while Instant::now() < until {
            match http_get(u).await.as_deref().map(parse_metrics) {
                Some(m) if m.conns == Some(0) => {
                    drained = m.outbound_pending;
                    break;
                }
                Some(_) => tokio::time::sleep(Duration::from_millis(100)).await,
                None => break,
            }
        }
    }

    let mut end = counters.snapshot();
    let obj = end.as_object_mut().expect("snapshot is an object");
    obj.insert("type".into(), json!("end"));
    obj.insert("outbound_pending_after_close".into(), opt(drained));
    obj.insert("label".into(), json!(label));
    obj.insert("t".into(), json!(start.elapsed().as_secs()));
    let stats = pid.and_then(proc_stats);
    obj.insert(
        "rss_kib".into(),
        stats.as_ref().map_or(Value::Null, |s| json!(s.rss_kib)),
    );
    // Anonymous memory only: VmRSS also counts the LMDB map pages the
    // process touched, which are reclaimable page cache bounded by the
    // database size.
    obj.insert(
        "rss_anon_kib".into(),
        stats
            .as_ref()
            .and_then(|s| s.rss_anon_kib)
            .map_or(Value::Null, |n| json!(n)),
    );
    obj.insert(
        "db_bytes".into(),
        db_bytes(&db_dir).map_or(Value::Null, |n| json!(n)),
    );
    println!("{end}");
}
