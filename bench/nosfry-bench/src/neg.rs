//! `neg`: NIP-77 set reconciliation against a known corpus.
//!
//! The relay already holds the first `--base` lines of `--corpus`
//! (`corpus-filter --regular` output). The client builds the same set,
//! drops a seeded `diff_pct/2` percent (the expected `need` set) and
//! adds the same share from `--holdout-corpus` (the expected `have`
//! set), then runs a full `NEG-OPEN`/`NEG-MSG` reconcile over the `{}`
//! filter. The result records wall time, rounds, wire accounting and
//! whether the returned id sets match the seeded expectation exactly.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::Instant;

use futures::{SinkExt, StreamExt};
use negentropy::{Id, Negentropy, NegentropyStorageVector};
use serde_json::{Value, json};
use yawc::{Frame, OpCode, TcpWebSocket};

use crate::corpus::keyed_rng;
use crate::sampler::{HarnessMeter, Sampler};
use crate::{connect, hex};

/// The negentropy subscription id used everywhere.
const SUB: &str = "neg";

/// Next text frame parsed as JSON; `None` when the socket dies.
pub(crate) async fn recv_json(ws: &mut TcpWebSocket) -> Option<Value> {
    while let Some(frame) = ws.next().await {
        if frame.opcode() != OpCode::Text {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(frame.as_str()) {
            return Some(v);
        }
    }
    None
}

/// `["NEG-OPEN",sub,filter,"<hex>"]`.
pub(crate) fn neg_open_wire(sub: &str, filter: &str, msg: &[u8]) -> String {
    format!("[\"NEG-OPEN\",\"{sub}\",{filter},\"{}\"]", hex(msg))
}

/// `["NEG-MSG",sub,"<hex>"]`.
pub(crate) fn neg_msg_wire(sub: &str, msg: &[u8]) -> String {
    format!("[\"NEG-MSG\",\"{sub}\",\"{}\"]", hex(msg))
}

/// Hex-decode a wire payload.
pub(crate) fn unhex(text: &str) -> Result<Vec<u8>, String> {
    if !text.len().is_multiple_of(2) {
        return Err("odd hex".to_owned());
    }
    (0..text.len() / 2)
        .map(|i| u8::from_str_radix(&text[2 * i..2 * i + 2], 16).map_err(|e| e.to_string()))
        .collect()
}

/// Read until a `NEG-MSG` or `NEG-ERR` for `sub` and return it.
pub(crate) async fn neg_recv(ws: &mut TcpWebSocket, sub: &str) -> Result<Value, String> {
    while let Some(v) = recv_json(ws).await {
        if v.get(1).and_then(Value::as_str) != Some(sub) {
            match v.get(0).and_then(Value::as_str) {
                Some("CLOSED") | Some("NOTICE") => return Err(v.to_string()),
                _ => continue,
            }
        }
        if matches!(
            v.get(0).and_then(Value::as_str),
            Some("NEG-MSG" | "NEG-ERR")
        ) {
            return Ok(v);
        }
    }
    Err("connection closed".to_owned())
}

/// The message of a `NEG-ERR` frame as an error.
pub(crate) fn neg_err(v: &Value) -> String {
    v.get(2)
        .and_then(Value::as_str)
        .unwrap_or("NEG-ERR")
        .to_owned()
}

/// `(created_at, id)` pairs parsed from the first `limit` lines of a
/// JSONL corpus; `limit` 0 reads all lines.
fn load_pairs(path: &Path, limit: usize) -> Result<Vec<(u64, [u8; 32])>, String> {
    let file = File::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let mut items = Vec::new();
    for line in BufReader::new(file).lines() {
        if limit != 0 && items.len() >= limit {
            break;
        }
        let line = line.map_err(|e| format!("read {}: {e}", path.display()))?;
        if line.is_empty() {
            continue;
        }
        let created_at = crate::corpus::u64_field(&line, "created_at")
            .ok_or_else(|| format!("{}: line without created_at", path.display()))?;
        let pos = line
            .find("\"id\":\"")
            .ok_or_else(|| format!("{}: line without id", path.display()))?;
        let hex_id = line
            .get(pos + 6..pos + 70)
            .ok_or_else(|| format!("{}: truncated id", path.display()))?;
        let bytes = unhex(hex_id)?;
        let id: [u8; 32] = bytes
            .try_into()
            .map_err(|_| format!("{}: short id", path.display()))?;
        items.push((created_at, id));
    }
    Ok(items)
}

/// Sent/received accounting: negentropy payload bytes (hex/2) and the
/// surrounding JSON frame bytes separately.
#[derive(Default)]
struct WireBytes {
    payload_sent: u64,
    payload_recv: u64,
    overhead_sent: u64,
    overhead_recv: u64,
}

impl WireBytes {
    /// `wire` is the whole JSON frame text; `payload` its hex-decoded
    /// negentropy message length.
    fn sent(&mut self, wire: &str, payload: usize) {
        self.payload_sent += payload as u64;
        self.overhead_sent += (wire.len() - payload * 2) as u64;
    }

    /// Same accounting for a received `NEG-MSG` frame text.
    fn received(&mut self, wire: &str, hex_payload: usize) {
        self.payload_recv += (hex_payload / 2) as u64;
        self.overhead_recv += (wire.len() - hex_payload) as u64;
    }
}

/// Reconcile `--base` corpus items against the relay, verify the
/// seeded `have`/`need` sets, and print one JSON result line.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn neg(
    url: url::Url,
    corpus: PathBuf,
    holdout_corpus: PathBuf,
    base: usize,
    diff_pct: f64,
    seed: u64,
    frame_size_limit: u64,
    pid: Option<u32>,
    label: String,
) -> Result<(), String> {
    let items = load_pairs(&corpus, base)?;
    if items.len() < base {
        return Err(format!(
            "corpus {} holds {} lines, fewer than --base {base}",
            corpus.display(),
            items.len()
        ));
    }
    let holdout = load_pairs(&holdout_corpus, 0)?;
    // diff_pct percent of N, half removed (relay-only → `need`), half
    // added from the holdout (client-only → `have`).
    let half = ((base as f64) * diff_pct / 200.0).round() as usize;
    if holdout.len() < half {
        return Err(format!(
            "holdout corpus {} holds {} lines, fewer than the {half} additions needed",
            holdout_corpus.display(),
            holdout.len()
        ));
    }
    let mut rng = keyed_rng(seed, b"neg", 0);
    let drop_idx: HashSet<usize> = rand::seq::index::sample(&mut rng, items.len(), half)
        .iter()
        .collect();
    let add_idx: Vec<usize> = rand::seq::index::sample(&mut rng, holdout.len(), half)
        .iter()
        .collect();

    let mut expected_need = HashSet::with_capacity(half);
    let mut storage = NegentropyStorageVector::with_capacity(items.len());
    for (i, &(created_at, id)) in items.iter().enumerate() {
        if drop_idx.contains(&i) {
            expected_need.insert(id);
            continue;
        }
        storage
            .insert(created_at, Id::from_byte_array(id))
            .map_err(|e| e.to_string())?;
    }
    let mut expected_have = HashSet::with_capacity(half);
    for i in add_idx {
        let (created_at, id) = holdout[i];
        storage
            .insert(created_at, Id::from_byte_array(id))
            .map_err(|e| e.to_string())?;
        expected_have.insert(id);
    }
    storage.seal().map_err(|e| e.to_string())?;

    let sampler = pid.map(Sampler::start);
    let meter = HarnessMeter::begin();
    let mut ws = connect(&url, false).await?;
    let mut sync = Negentropy::borrowed(&storage, frame_size_limit).map_err(|e| e.to_string())?;
    let init = sync.initiate().map_err(|e| e.to_string())?;

    let t0 = Instant::now();
    let mut bytes = WireBytes::default();
    let open = neg_open_wire(SUB, "{}", &init);
    bytes.sent(&open, init.len());
    ws.send(Frame::text(open))
        .await
        .map_err(|e| e.to_string())?;

    let (mut have, mut need) = (Vec::new(), Vec::new());
    let mut rounds = 0u64;
    let wall_ms = loop {
        let frame = ws
            .next()
            .await
            .ok_or_else(|| "connection closed during reconciliation".to_owned())?;
        if frame.opcode() != OpCode::Text {
            continue;
        }
        let text = frame.as_str();
        let Ok(v) = serde_json::from_str::<Value>(text) else {
            continue;
        };
        if v.get(1).and_then(Value::as_str) != Some(SUB) {
            match v.get(0).and_then(Value::as_str) {
                Some("CLOSED") | Some("NOTICE") => return Err(v.to_string()),
                _ => continue,
            }
        }
        match v.get(0).and_then(Value::as_str) {
            Some("NEG-ERR") => return Err(neg_err(&v)),
            Some("NEG-MSG") => {
                let payload_hex = v
                    .get(2)
                    .and_then(Value::as_str)
                    .ok_or_else(|| "NEG-MSG without payload".to_owned())?;
                bytes.received(text, payload_hex.len());
                rounds += 1;
                let payload = unhex(payload_hex)?;
                let next = sync
                    .reconcile_with_ids(&payload, &mut have, &mut need)
                    .map_err(|e| e.to_string())?;
                match next {
                    Some(msg) => {
                        let wire = neg_msg_wire(SUB, &msg);
                        bytes.sent(&wire, msg.len());
                        ws.send(Frame::text(wire)).await.map_err(|e| e.to_string())?;
                    }
                    // Final empty response: the measured phase ends.
                    None => break t0.elapsed().as_secs_f64() * 1000.0,
                }
            }
            _ => continue,
        }
    };
    let close = format!("[\"NEG-CLOSE\",\"{SUB}\"]");
    bytes.overhead_sent += close.len() as u64;
    ws.send(Frame::text(close))
        .await
        .map_err(|e| e.to_string())?;

    let have_set: HashSet<[u8; 32]> = have.iter().map(|id| **id).collect();
    let need_set: HashSet<[u8; 32]> = need.iter().map(|id| **id).collect();
    let correct = have_set == expected_have && need_set == expected_need;
    let report = json!({
        "scenario": "neg",
        "label": label,
        "harness_sha": env!("GIT_SHA"),
        "url": url.to_string(),
        "base": base,
        "diff_pct": diff_pct,
        "seed": seed,
        "frame_size_limit": frame_size_limit,
        "client_items": items.len() - expected_need.len() + expected_have.len(),
        "expected_have": expected_have.len(),
        "expected_need": expected_need.len(),
        "have": have_set.len(),
        "need": need_set.len(),
        "missing_have": expected_have.difference(&have_set).count(),
        "unexpected_have": have_set.difference(&expected_have).count(),
        "missing_need": expected_need.difference(&need_set).count(),
        "unexpected_need": need_set.difference(&expected_need).count(),
        "correct": correct,
        "wall_ms": wall_ms,
        "rounds": rounds,
        "payload_bytes_sent": bytes.payload_sent,
        "payload_bytes_received": bytes.payload_recv,
        "overhead_bytes_sent": bytes.overhead_sent,
        "overhead_bytes_received": bytes.overhead_recv,
        "sampler": sampler.map(Sampler::finish).unwrap_or(Value::Null),
        "harness": meter.finish(),
    });
    println!("{report}");
    Ok(())
}
