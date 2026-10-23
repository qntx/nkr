//! nosfry-bench: complementary relay benchmarks that relayBench does not cover.
//!
//! - `fanout`: S live subscribers, one publisher at a fixed rate; measures
//!   publish-to-delivery latency for every (subscriber, event) pair and checks
//!   that every expected delivery arrives exactly once.
//! - `conns`: C idle subscribed connections; measures the relay's resident
//!   memory per connection and the CPU spent establishing them.
//! - `rtt`, `ingest`, `wake`, `subbudget`, `scan`: deployed-relay
//!   acceptance-gate modes (see `gate.rs`).
//!
//! Every result is printed as one JSON line on stdout.

use std::collections::HashMap;
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

mod gate;
mod soak;

use clap::{Parser, Subcommand, ValueEnum};
use futures::{SinkExt, StreamExt};
use secp256k1::{Keypair, schnorr};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::{Semaphore, watch};
use yawc::{CompressionLevel, Frame, OpCode, Options, TcpWebSocket, WebSocket};

/// Concurrent WebSocket handshakes while building up subscribers.
const HANDSHAKE_CONCURRENCY: usize = 128;
/// How long to wait for every subscriber to reach EOSE.
const READY_TIMEOUT: Duration = Duration::from_secs(120);
/// How long to wait for straggling deliveries after the last publish.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    Fanout {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 1000)]
        subs: usize,
        #[arg(long, default_value_t = 1000)]
        events: usize,
        /// Publish rate in events per second.
        #[arg(long, default_value_t = 200.0)]
        rate: f64,
        #[arg(long, value_enum, default_value_t = Mode::Broadcast)]
        mode: Mode,
        /// Distinct authors; selective subscriber j follows author j % authors.
        #[arg(long, default_value_t = 100)]
        authors: usize,
        #[arg(long)]
        deflate: bool,
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long, default_value = "")]
        label: String,
    },
    Conns {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 5000)]
        conns: usize,
        #[arg(long)]
        deflate: bool,
        /// Relay process id for RSS/CPU sampling; skipped when absent
        /// (e.g. a remote or deployed relay).
        #[arg(long)]
        pid: Option<u32>,
        #[arg(long, default_value_t = 5)]
        hold_secs: u64,
        #[arg(long, default_value = "")]
        label: String,
    },
    /// Mixed long-running workload; samples the relay as JSONL.
    Soak {
        #[arg(long)]
        url: String,
        /// `http://host:port/metrics`; skipped when absent.
        #[arg(long)]
        metrics_url: Option<String>,
        #[arg(long)]
        pid: Option<u32>,
        /// Relay db directory; `<db-dir>/data.mdb` is sized per sample.
        #[arg(long)]
        db_dir: Option<std::path::PathBuf>,
        #[arg(long)]
        duration_secs: u64,
        #[arg(long, default_value_t = 60)]
        sample_secs: u64,
        #[arg(long, default_value_t = 8)]
        publishers: usize,
        /// Total publish rate in events per second across publishers.
        #[arg(long, default_value_t = 200.0)]
        rate: f64,
        #[arg(long, default_value_t = 200)]
        subscribers: usize,
        /// Short-lived connections opened per second.
        #[arg(long, default_value_t = 10.0)]
        churn_rate: f64,
        /// Spawn one never-reading `REQ {}` consumer this often; 0 off.
        #[arg(long, default_value_t = 60)]
        slow_every_secs: u64,
        /// Start one negentropy session this often; 0 off.
        #[arg(long, default_value_t = 10)]
        neg_every_secs: u64,
        /// Run one vanish round this often; 0 off.
        #[arg(long, default_value_t = 60)]
        vanish_every_secs: u64,
        #[arg(long, default_value = "")]
        label: String,
        /// Negotiate permessage-deflate on every connection.
        #[arg(long)]
        deflate: bool,
    },
    /// One connection, n sequential REQ→EOSE round-trips.
    Rtt {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 20)]
        n: usize,
    },
    /// Events paced over connections; OK latency and acceptance rate.
    Ingest {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 30)]
        conns: usize,
        #[arg(long, default_value_t = 3000)]
        events: usize,
        #[arg(long, default_value_t = 15.0)]
        rate_per_conn: f64,
    },
    /// Subscribers idle through a hibernation, then fanout delivery.
    Wake {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 1000)]
        subs: usize,
        #[arg(long, default_value_t = 180)]
        idle_secs: u64,
        #[arg(long, default_value_t = 3)]
        rounds: usize,
        #[arg(long, default_value_t = 10)]
        events: usize,
        /// Keep one NIP-42-authenticated restricted subscription alive.
        #[arg(long)]
        auth: bool,
    },
    /// Large-author-list REQs until the subscription byte budget refuses.
    Subbudget {
        #[arg(long)]
        url: String,
        #[arg(long, default_value_t = 1900)]
        authors: usize,
    },
    /// n stored scans of one filter; reports the CPU-ceiling signal.
    Scan {
        #[arg(long)]
        url: String,
        #[arg(long)]
        filter: String,
        #[arg(long, default_value_t = 3)]
        n: usize,
    },
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    /// Every subscriber follows `kinds:[1]`; each event goes to every subscriber.
    Broadcast,
    /// Subscriber j follows one author; exercises the relay's live index.
    Selective,
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(DIGITS[usize::from(byte >> 4)]));
        out.push(char::from(DIGITS[usize::from(byte & 0x0f)]));
    }
    out
}

struct Author {
    keypair: Keypair,
    pk_hex: String,
}

fn author(index: usize) -> Author {
    let mut secret = [0u8; 32];
    secret[..8].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    secret[31] = 0x5a;
    let keypair = Keypair::from_secret_bytes(secret).expect("secret is a valid scalar");
    let pk_hex = hex(&keypair.x_only_public_key().0.to_byte_array());
    Author { keypair, pk_hex }
}

/// Returns `(id_hex, event_json)` for a signed event.
fn sign_event(
    author: &Author,
    created_at: u64,
    kind: u16,
    tags: &Value,
    content: &str,
) -> (String, String) {
    let preimage = json!([0, author.pk_hex, created_at, kind, tags, content]).to_string();
    let id: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
    let sig = schnorr::sign_no_aux_rand(&id, &author.keypair);
    let id_hex = hex(&id);
    let event = json!({
        "id": id_hex,
        "pubkey": author.pk_hex,
        "created_at": created_at,
        "kind": kind,
        "tags": tags,
        "content": content,
        "sig": hex(&sig.to_byte_array()),
    });
    (id_hex, event.to_string())
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after epoch")
        .as_secs()
}

async fn connect(url: &url::Url, deflate: bool) -> Result<TcpWebSocket, String> {
    let mut options = Options::default()
        .with_max_payload_read(16 << 20)
        .with_no_delay();
    if deflate {
        options = options.with_compression_level(CompressionLevel::fast());
    }
    WebSocket::connect(url.clone())
        .with_options(options)
        .await
        .map_err(|e| e.to_string())
}

/// Send a REQ and read until its EOSE. Frames before EOSE are ignored.
async fn subscribe(ws: &mut TcpWebSocket, sub_id: &str, filter: &Value) -> Result<(), String> {
    let req = json!(["REQ", sub_id, filter]).to_string();
    ws.send(Frame::text(req)).await.map_err(|e| e.to_string())?;
    let eose = json!(["EOSE", sub_id]).to_string();
    while let Some(frame) = ws.next().await {
        if frame.opcode() == OpCode::Text {
            let text = frame.as_str();
            if text == eose {
                return Ok(());
            }
            if text.starts_with("[\"CLOSED\"") || text.starts_with("[\"NOTICE\"") {
                return Err(text.to_owned());
            }
        }
    }
    Err("connection closed before EOSE".to_owned())
}

/// Process memory and CPU at one instant.
#[derive(Clone, Copy)]
struct ProcStats {
    /// Resident set size in KiB (includes file-backed LMDB pages).
    rss_kib: u64,
    /// Anonymous resident memory in KiB (Linux only): heap, stacks and
    /// buffers, excluding the file-backed LMDB map.
    rss_anon_kib: Option<u64>,
    /// Cumulative user + system CPU seconds.
    cpu_secs: f64,
}

impl ProcStats {
    /// The memory figure to attribute to connections: anonymous RSS
    /// where available, total RSS otherwise.
    fn mem_kib(self) -> u64 {
        self.rss_anon_kib.unwrap_or(self.rss_kib)
    }
}

/// Memory and CPU of `pid`: `/proc` on Linux, `ps` elsewhere.
fn proc_stats(pid: u32) -> Option<ProcStats> {
    #[cfg(target_os = "linux")]
    {
        proc_stats_linux(pid)
    }
    #[cfg(not(target_os = "linux"))]
    {
        proc_stats_ps(pid).map(|(rss_kib, cpu_secs)| ProcStats {
            rss_kib,
            rss_anon_kib: None,
            cpu_secs,
        })
    }
}

#[cfg(target_os = "linux")]
fn proc_stats_linux(pid: u32) -> Option<ProcStats> {
    use std::sync::OnceLock;
    static CLK_TCK: OnceLock<f64> = OnceLock::new();
    let ticks_per_sec = *CLK_TCK.get_or_init(|| {
        Command::new("getconf")
            .arg("CLK_TCK")
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(100.0)
    });
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    // Fields after the parenthesized command name; utime/stime are fields 14/15 overall.
    let rest = stat.rsplit_once(')')?.1;
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let utime: f64 = fields.get(11)?.parse().ok()?;
    let stime: f64 = fields.get(12)?.parse().ok()?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let kib = |key: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .and_then(|v| v.split_whitespace().next())
            .and_then(|v| v.parse::<u64>().ok())
    };
    Some(ProcStats {
        rss_kib: kib("VmRSS:")?,
        rss_anon_kib: kib("RssAnon:"),
        cpu_secs: (utime + stime) / ticks_per_sec,
    })
}

/// Resident set size (KiB) and cumulative CPU seconds of `pid`, via `ps`.
#[cfg(not(target_os = "linux"))]
fn proc_stats_ps(pid: u32) -> Option<(u64, f64)> {
    let out = Command::new("ps")
        .args(["-o", "rss=,time=", "-p", &pid.to_string()])
        .output()
        .ok()?;
    let text = String::from_utf8(out.stdout).ok()?;
    let mut fields = text.split_whitespace();
    let rss = fields.next()?.parse().ok()?;
    let time = fields.next()?;
    // `[[hh:]mm:]ss.ff`, or `d-hh:mm:ss` on some systems.
    let (days, clock) = time.split_once('-').unwrap_or(("0", time));
    let mut seconds = days.parse::<f64>().ok()? * 86_400.0;
    let mut unit = 1.0;
    for part in clock.rsplit(':') {
        seconds += part.parse::<f64>().ok()? * unit;
        unit *= 60.0;
    }
    Some((rss, seconds))
}

fn percentile(sorted: &[u32], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let rank = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    f64::from(sorted[rank.min(sorted.len() - 1)]) / 1000.0
}

#[derive(Default)]
struct SubOutcome {
    latencies_us: Vec<u32>,
    received: usize,
    duplicates: usize,
    unknown: usize,
    error: Option<String>,
}

/// One live subscriber after EOSE: record the delivery latency of every
/// expected event until all arrived or `stop` fires.
async fn run_subscriber(
    mut ws: TcpWebSocket,
    expected: usize,
    index: Arc<HashMap<String, u32>>,
    sent: Arc<Vec<AtomicU64>>,
    base: Instant,
    done: Arc<AtomicUsize>,
    mut stop: watch::Receiver<bool>,
) -> SubOutcome {
    let mut outcome = SubOutcome::default();
    let mut seen = vec![false; sent.len()];
    if expected == 0 {
        done.fetch_add(1, Ordering::Relaxed);
    }
    loop {
        tokio::select! {
            frame = ws.next() => {
                let Some(frame) = frame else {
                    outcome.error = Some("connection closed".to_owned());
                    break;
                };
                if frame.opcode() != OpCode::Text {
                    continue;
                }
                let now = base.elapsed().as_nanos() as u64;
                let text = frame.as_str();
                if !text.starts_with("[\"EVENT\"") {
                    continue;
                }
                let Some(pos) = text.find("\"id\":\"") else {
                    outcome.unknown += 1;
                    continue;
                };
                let Some(id) = text.get(pos + 6..pos + 70) else {
                    outcome.unknown += 1;
                    continue;
                };
                let Some(&i) = index.get(id) else {
                    outcome.unknown += 1;
                    continue;
                };
                let i = i as usize;
                if seen[i] {
                    outcome.duplicates += 1;
                    continue;
                }
                seen[i] = true;
                let sent_at = sent[i].load(Ordering::Acquire);
                if sent_at == 0 {
                    outcome.unknown += 1;
                    continue;
                }
                let latency_us = now.saturating_sub(sent_at - 1) / 1000;
                outcome.latencies_us.push(u32::try_from(latency_us).unwrap_or(u32::MAX));
                outcome.received += 1;
                if outcome.received == expected {
                    done.fetch_add(1, Ordering::Relaxed);
                }
            }
            _ = stop.changed() => break,
        }
    }
    outcome
}

#[allow(clippy::too_many_arguments)]
async fn fanout(
    url: url::Url,
    subs: usize,
    events: usize,
    rate: f64,
    mode: Mode,
    authors_n: usize,
    deflate: bool,
    pid: Option<u32>,
    label: String,
) {
    let now = unix_now();
    let authors: Vec<Author> = (0..authors_n).map(author).collect();
    // A per-run `t` tag: on a public relay, foreign kind-1 traffic must
    // not count toward this run's deliveries.
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let tag = format!("fanout-{nonce:x}");
    let tags = json!([["t", tag]]);
    let mut index = HashMap::with_capacity(events);
    let mut messages = Vec::with_capacity(events);
    for i in 0..events {
        let (id, event) = sign_event(
            &authors[i % authors_n],
            now,
            1,
            &tags,
            &format!("fanout {nonce} {i}"),
        );
        index.insert(id, u32::try_from(i).expect("events fit in u32"));
        messages.push(format!("[\"EVENT\",{event}]"));
    }
    let index = Arc::new(index);
    let sent: Arc<Vec<AtomicU64>> = Arc::new((0..events).map(|_| AtomicU64::new(0)).collect());
    let base = Instant::now();

    // Per-subscriber expectation and filter.
    let per_author = |a: usize| events / authors_n + usize::from(a < events % authors_n);
    let plan: Vec<(usize, Value)> = (0..subs)
        .map(|j| match mode {
            Mode::Broadcast => (
                events,
                json!({"kinds": [1], "#t": [tag], "since": now}),
            ),
            Mode::Selective => {
                let a = j % authors_n;
                (
                    per_author(a),
                    json!({"kinds": [1], "#t": [tag], "authors": [authors[a].pk_hex], "since": now}),
                )
            }
        })
        .collect();
    let expected_total: usize = plan.iter().map(|(n, _)| n).sum();

    // Build up subscribers; each task signals readiness after EOSE.
    let (stop_tx, stop_rx) = watch::channel(false);
    let ready = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(HANDSHAKE_CONCURRENCY));
    let mut tasks = Vec::with_capacity(subs);
    for (j, (expected, filter)) in plan.into_iter().enumerate() {
        let (url, index, sent, done, stop, ready, failed, gate) = (
            url.clone(),
            Arc::clone(&index),
            Arc::clone(&sent),
            Arc::clone(&done),
            stop_rx.clone(),
            Arc::clone(&ready),
            Arc::clone(&failed),
            Arc::clone(&gate),
        );
        tasks.push(tokio::spawn(async move {
            let permit = gate.acquire_owned().await.expect("semaphore open");
            let mut ws = match connect(&url, deflate).await {
                Ok(ws) => ws,
                Err(e) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    return SubOutcome {
                        error: Some(e),
                        ..SubOutcome::default()
                    };
                }
            };
            if let Err(e) = subscribe(&mut ws, &format!("s{j}"), &filter).await {
                failed.fetch_add(1, Ordering::Relaxed);
                return SubOutcome {
                    error: Some(e),
                    ..SubOutcome::default()
                };
            }
            drop(permit);
            ready.fetch_add(1, Ordering::Relaxed);
            run_subscriber(ws, expected, index, sent, base, done, stop).await
        }));
    }
    let ready_deadline = Instant::now() + READY_TIMEOUT;
    while ready.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < subs {
        assert!(
            Instant::now() < ready_deadline,
            "subscribers not ready in time"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let subs_ready = ready.load(Ordering::Relaxed);
    let subs_failed = failed.load(Ordering::Relaxed);

    // Publisher.
    let publisher = connect(&url, deflate).await.expect("publisher connects");
    let (mut sink, mut stream) = publisher.split();
    let oks = tokio::spawn(async move {
        let (mut accepted, mut rejected) = (0usize, 0usize);
        while accepted + rejected < events {
            let Some(frame) = stream.next().await else {
                break;
            };
            if frame.opcode() != OpCode::Text {
                continue;
            }
            let text = frame.as_str();
            if !text.starts_with("[\"OK\"") {
                continue;
            }
            if text.contains(",true,") {
                accepted += 1;
            } else {
                rejected += 1;
            }
        }
        (accepted, rejected)
    });

    let before = pid.and_then(proc_stats);
    let publish_start = Instant::now();
    for (i, message) in messages.into_iter().enumerate() {
        let target = publish_start + Duration::from_secs_f64(i as f64 / rate);
        tokio::time::sleep_until(target.into()).await;
        sent[i].store(base.elapsed().as_nanos() as u64 + 1, Ordering::Release);
        sink.send(Frame::text(message)).await.expect("publish send");
    }
    let publish_secs = publish_start.elapsed().as_secs_f64();
    let drain_deadline = Instant::now() + DRAIN_TIMEOUT;
    while done.load(Ordering::Relaxed) < subs_ready && Instant::now() < drain_deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let elapsed_secs = publish_start.elapsed().as_secs_f64();
    let after = pid.and_then(proc_stats);
    let (accepted, rejected) = tokio::time::timeout(DRAIN_TIMEOUT, oks)
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or((0, 0));
    stop_tx.send_replace(true);

    let mut latencies = Vec::with_capacity(expected_total);
    let (mut received, mut duplicates, mut unknown, mut errors) = (0, 0, 0, 0);
    for task in tasks {
        let outcome = task.await.unwrap_or_default();
        received += outcome.received;
        duplicates += outcome.duplicates;
        unknown += outcome.unknown;
        errors += usize::from(outcome.error.is_some());
        latencies.extend(outcome.latencies_us);
    }
    latencies.sort_unstable();
    let result = json!({
        "scenario": "fanout",
        "label": label,
        "mode": format!("{mode:?}").to_lowercase(),
        "deflate": deflate,
        "subs": subs,
        "subs_ready": subs_ready,
        "subs_failed": subs_failed,
        "events": events,
        "rate": rate,
        "ok_accepted": accepted,
        "ok_rejected": rejected,
        "expected": expected_total,
        "delivered": received,
        "missing": expected_total.saturating_sub(received),
        "duplicates": duplicates,
        "unknown": unknown,
        "subscriber_errors": errors,
        "publish_secs": publish_secs,
        "elapsed_secs": elapsed_secs,
        "latency_p50_ms": percentile(&latencies, 50.0),
        "latency_p99_ms": percentile(&latencies, 99.0),
        "latency_ms": {
            "p50": percentile(&latencies, 50.0),
            "p90": percentile(&latencies, 90.0),
            "p99": percentile(&latencies, 99.0),
            "p999": percentile(&latencies, 99.9),
            "max": latencies.last().map_or(f64::NAN, |v| f64::from(*v) / 1000.0),
        },
        "relay_cpu_secs": match (before, after) { (Some(b), Some(a)) => json!(a.cpu_secs - b.cpu_secs), _ => Value::Null },
        "relay_rss_kib_after": after.map(|a| a.rss_kib),
        "relay_rss_anon_kib_after": after.and_then(|a| a.rss_anon_kib),
    });
    println!("{result}");
}

async fn conns(
    url: url::Url,
    n: usize,
    deflate: bool,
    pid: Option<u32>,
    hold_secs: u64,
    label: String,
) {
    let before = pid.and_then(proc_stats);
    let (stop_tx, stop_rx) = watch::channel(false);
    let ready = Arc::new(AtomicUsize::new(0));
    let failed = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let gate = Arc::new(Semaphore::new(HANDSHAKE_CONCURRENCY));
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(n);
    for j in 0..n {
        let (url, ready, failed, dropped, gate, mut stop) = (
            url.clone(),
            Arc::clone(&ready),
            Arc::clone(&failed),
            Arc::clone(&dropped),
            Arc::clone(&gate),
            stop_rx.clone(),
        );
        tasks.push(tokio::spawn(async move {
            let permit = gate.acquire_owned().await.expect("semaphore open");
            let mut ws = match connect(&url, deflate).await {
                Ok(ws) => ws,
                Err(_) => {
                    failed.fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            if subscribe(
                &mut ws,
                &format!("c{j}"),
                &json!({"kinds": [1], "limit": 1}),
            )
            .await
            .is_err()
            {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
            drop(permit);
            ready.fetch_add(1, Ordering::Relaxed);
            loop {
                tokio::select! {
                    frame = ws.next() => {
                        if frame.is_none() {
                            // A socket that ended before the hold was
                            // released dropped during the hold.
                            if !*stop.borrow() {
                                dropped.fetch_add(1, Ordering::Relaxed);
                            }
                            break;
                        }
                    },
                    _ = stop.changed() => break,
                }
            }
        }));
    }
    let deadline = Instant::now() + READY_TIMEOUT;
    while ready.load(Ordering::Relaxed) + failed.load(Ordering::Relaxed) < n {
        assert!(Instant::now() < deadline, "connections not ready in time");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let setup_secs = start.elapsed().as_secs_f64();
    tokio::time::sleep(Duration::from_secs(hold_secs)).await;
    let held = pid.and_then(proc_stats);
    let opened = ready.load(Ordering::Relaxed);
    stop_tx.send_replace(true);
    for task in tasks {
        let _ = task.await;
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    let released = pid.and_then(proc_stats);
    let mem = |a: Option<ProcStats>| a.map(ProcStats::mem_kib);
    let result = json!({
        "scenario": "conns",
        "label": label,
        "deflate": deflate,
        "requested": n,
        "accepted": opened,
        "failed": failed.load(Ordering::Relaxed),
        "dropped_during_hold": dropped.load(Ordering::Relaxed),
        "setup_secs": setup_secs,
        "rss_kib_before": before.map(|b| b.rss_kib),
        "rss_kib_held": held.map(|h| h.rss_kib),
        "rss_kib_after_close": released.map(|r| r.rss_kib),
        "rss_anon_kib_before": before.and_then(|b| b.rss_anon_kib),
        "rss_anon_kib_held": held.and_then(|h| h.rss_anon_kib),
        "rss_anon_kib_after_close": released.and_then(|r| r.rss_anon_kib),
        "mem_metric": match held {
            Some(h) if h.rss_anon_kib.is_some() => json!("RssAnon"),
            Some(_) => json!("RSS"),
            None => Value::Null,
        },
        "kib_per_conn": match (mem(before), mem(held)) {
            (Some(b), Some(h)) if opened > 0 => json!((h as f64 - b as f64) / opened as f64),
            _ => Value::Null,
        },
        "relay_cpu_secs_setup": match (before, held) {
            (Some(b), Some(h)) => json!(h.cpu_secs - b.cpu_secs),
            _ => Value::Null,
        },
    });
    println!("{result}");
}

#[tokio::main]
async fn main() {
    match Cli::parse().cmd {
        Cmd::Fanout {
            url,
            subs,
            events,
            rate,
            mode,
            authors,
            deflate,
            pid,
            label,
        } => {
            let url = url.parse().expect("valid ws url");
            fanout(
                url,
                subs,
                events,
                rate,
                mode,
                authors.max(1),
                deflate,
                pid,
                label,
            )
            .await;
        }
        Cmd::Conns {
            url,
            conns: n,
            deflate,
            pid,
            hold_secs,
            label,
        } => {
            let url = url.parse().expect("valid ws url");
            conns(url, n, deflate, pid, hold_secs, label).await;
        }
        Cmd::Rtt { url, n } => {
            let url = url.parse().expect("valid ws(s) url");
            gate::rtt(url, n).await;
        }
        Cmd::Ingest {
            url,
            conns,
            events,
            rate_per_conn,
        } => {
            let url = url.parse().expect("valid ws(s) url");
            gate::ingest(url, conns, events, rate_per_conn).await;
        }
        Cmd::Wake {
            url,
            subs,
            idle_secs,
            rounds,
            events,
            auth,
        } => {
            let url = url.parse().expect("valid ws(s) url");
            gate::wake(url, subs, idle_secs, rounds, events, auth).await;
        }
        Cmd::Subbudget { url, authors } => {
            let url = url.parse().expect("valid ws(s) url");
            gate::subbudget(url, authors).await;
        }
        Cmd::Scan { url, filter, n } => {
            let url = url.parse().expect("valid ws(s) url");
            gate::scan(url, &filter, n).await;
        }
        Cmd::Soak {
            url,
            metrics_url,
            pid,
            db_dir,
            duration_secs,
            sample_secs,
            publishers,
            rate,
            subscribers,
            churn_rate,
            slow_every_secs,
            neg_every_secs,
            vanish_every_secs,
            label,
            deflate,
        } => {
            let url = url.parse().expect("valid ws url");
            let metrics_url = metrics_url.map(|u| u.parse().expect("valid metrics url"));
            soak::run(
                url,
                metrics_url,
                pid,
                db_dir,
                duration_secs,
                sample_secs,
                publishers,
                rate,
                subscribers,
                churn_rate,
                slow_every_secs,
                neg_every_secs,
                vanish_every_secs,
                label,
                deflate,
            )
            .await;
        }
    }
}
