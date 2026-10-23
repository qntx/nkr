//! `gen`: the D2 synthetic-corpus generator.
//!
//! The corpus is reproducible byte for byte: event `i` is fully
//! determined by `(seed, i)`, so output does not depend on thread
//! count. Templates come from the D1 real corpus (Amethyst's startup
//! dataset); generated events keep the template's kind, tag skeleton
//! and content, with authors Zipf-distributed over generated keys and
//! `e`/`p`/`a` tag values rewired to generated identities. The first
//! `P` events form the e-pool: their own `e` tags degrade to `p` tags
//! so every later `e` reference points at a real stored event and
//! thread queries stay meaningful.
//!
//! Outputs in `--out`: `events.jsonl` (i in 0..N), `holdout.jsonl`
//! (i in N..N+H), `params.json` (the query driver's sidecar,
//! including `pool_ids`) and `fingerprints.txt` (sha256 of each
//! output).

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;

use rand::RngExt;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use secp256k1::Keypair;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{Author, hex, sign_event};

/// Events per worker chunk; the writer emits chunks in index order.
const CHUNK: usize = 10_000;
/// E-pool size: the first `P` generated ids are the reply targets.
const POOL: usize = 100_000;
/// Fixed corpus epoch so generated `created_at` is reproducible.
const T0: u64 = 1_700_000_000;
/// Two-year span the corpus is spread over.
const SPAN: u64 = 63_072_000;
/// Jitter added to each event's base `created_at`.
const JITTER: u64 = 600;
/// Finished chunks the writer may buffer before workers block.
const CHANNEL_CAP: usize = 8;
/// Sidecar sizes: popular / random authors, random ids, hashtags,
/// follow lists, addressable triples.
const N_POPULAR: usize = 2_000;
const N_RANDOM: usize = 2_000;
const N_IDS: usize = 5_000;
const N_TAGS: usize = 500;
const N_FOLLOWS: usize = 1_000;
const N_TRIPLES: usize = 1_000;

/// Everything `gen` needs, straight from the CLI.
pub(crate) struct GenOpts {
    /// D1 corpus (`.json` or `.json.gz`): a JSON array of events.
    pub(crate) template: PathBuf,
    /// N: events into `events.jsonl`.
    pub(crate) events: usize,
    /// H: extra events into `holdout.jsonl` (used as ingest input).
    pub(crate) holdout: usize,
    /// A: author keypairs.
    pub(crate) authors: usize,
    /// Corpus seed; every draw derives from it.
    pub(crate) seed: u64,
    /// Output directory.
    pub(crate) out: PathBuf,
    /// Worker count; output is identical at any setting.
    pub(crate) threads: usize,
}

/// A D1 event kept as a generation template: kind, tag skeleton and
/// content; id, pubkey, created_at and sig are regenerated.
struct Template {
    kind: u16,
    tags: Vec<Value>,
    content: String,
}

/// Templates plus the per-reason drop counters for the final JSON.
struct Templates {
    list: Vec<Template>,
    total: usize,
    dropped_ephemeral: usize,
    dropped_kind5: usize,
    dropped_kind62: usize,
    dropped_expiration: usize,
    dropped_oversize_tag: usize,
    malformed: usize,
    /// sha256 of the template file as given.
    fingerprint: String,
}

/// Per-worker generation statistics, merged once per worker.
#[derive(Default)]
struct Stats {
    /// Draw count per author index: event authorship only.
    author_draws: Vec<u64>,
    kinds: BTreeMap<u16, u64>,
    t_counts: BTreeMap<String, u64>,
}

/// One finished chunk handed to the writer.
struct ChunkOut {
    idx: usize,
    /// Serialized event lines, i order.
    lines: Vec<String>,
    /// Ids of pool events in this chunk (i < POOL only).
    pool_ids: Vec<String>,
    /// `(i, id)` hits for the sidecar's random-id picks.
    wanted: Vec<(u64, String)>,
    /// `(kind, pubkey, d)` of kind-30000..39999 events, i order.
    triples: Vec<(u16, String, String)>,
}

/// Shared generation context borrowed by every worker.
struct Ctx<'a> {
    templates: &'a [Template],
    authors: &'a [Author],
    zipf: &'a [f64],
    pool: &'a PoolSlot,
    total: usize,
    /// Indices whose ids the sidecar wants (drawn before generation).
    wanted: &'a BTreeSet<u64>,
}

/// The pool-id handoff: workers generating `i >= P` block until the
/// writer has consumed every pool chunk and published the ids here.
/// Pool chunks never wait on themselves — they are all claimed first
/// (the atomic index hands out `0..pool_chunks` before any other).
struct PoolSlot {
    slot: Mutex<Option<Arc<Vec<String>>>>,
    ready: Condvar,
}

impl PoolSlot {
    fn wait(&self) -> Arc<Vec<String>> {
        let mut guard = self.slot.lock().expect("pool mutex");
        loop {
            if let Some(ids) = &*guard {
                return Arc::clone(ids);
            }
            guard = self.ready.wait(guard).expect("pool condvar");
        }
    }

    fn set(&self, ids: Vec<String>) {
        *self.slot.lock().expect("pool mutex") = Some(Arc::new(ids));
        self.ready.notify_all();
    }
}

/// Domain-separated rng stream: `sha256(seed || domain || i)`.
fn keyed_rng(seed: u64, domain: &[u8], i: u64) -> ChaCha20Rng {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    h.update(domain);
    h.update(i.to_le_bytes());
    ChaCha20Rng::from_seed(h.finalize().into())
}

/// Keypair `i`: `sha256(seed || "key" || i)`, retried with a counter
/// suffix until the bytes form a valid secret key.
fn author_keypair(seed: u64, i: u64) -> Keypair {
    let mut h = Sha256::new();
    h.update(seed.to_le_bytes());
    h.update(b"key");
    h.update(i.to_le_bytes());
    let base: [u8; 32] = h.finalize().into();
    for counter in 0u64.. {
        let candidate = if counter == 0 {
            base
        } else {
            let mut h = Sha256::new();
            h.update(base);
            h.update(counter.to_le_bytes());
            h.finalize().into()
        };
        if let Ok(keypair) = Keypair::from_secret_bytes(candidate) {
            return keypair;
        }
    }
    unreachable!("the counter search always finds a key")
}

/// Cumulative table for Zipf s=1.0 over `0..a`: `cdf[k]` is the mass
/// of items `0..=k`; [`zipf_draw`] draws by inverse CDF.
fn zipf_table(a: usize) -> Vec<f64> {
    let mut cdf = Vec::with_capacity(a);
    let mut acc = 0.0;
    for j in 1..=a {
        acc += 1.0 / j as f64;
        cdf.push(acc);
    }
    for c in &mut cdf {
        *c /= acc;
    }
    cdf
}

/// One Zipf draw over `cdf` via inverse CDF.
fn zipf_draw(cdf: &[f64], rng: &mut ChaCha20Rng) -> usize {
    let u: f64 = rng.random();
    cdf.partition_point(|&c| c < u).min(cdf.len() - 1)
}

/// sha256 of a file's raw bytes (the template's identity as given).
fn file_sha256(path: &Path) -> Result<String, String> {
    let mut file = std::fs::File::open(path).map_err(|e| format!("{path:?}: {e}"))?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex(&h.finalize()))
}

/// A `gunzip -c` child kept alive for the lifetime of its reader.
struct Gunzip {
    child: std::process::Child,
    inner: BufReader<std::process::ChildStdout>,
}

impl Read for Gunzip {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

impl BufRead for Gunzip {
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.inner.fill_buf()
    }
    fn consume(&mut self, amt: usize) {
        self.inner.consume(amt);
    }
}

impl Drop for Gunzip {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Open `path` as a byte stream of its JSON content: through
/// `gunzip -c` when the file carries the gzip magic, directly
/// otherwise (the plain path keeps tests and unpacked inputs working).
fn template_reader(path: &Path) -> Result<Box<dyn BufRead + Send>, String> {
    let mut file = std::fs::File::open(path).map_err(|e| format!("{path:?}: {e}"))?;
    let mut magic = [0u8; 2];
    let n = file.read(&mut magic).map_err(|e| e.to_string())?;
    if n == 2 && magic == [0x1f, 0x8b] {
        let mut child = Command::new("gunzip")
            .arg("-c")
            .arg(path)
            .stdout(Stdio::piped())
            .spawn()
            .map_err(|e| format!("gunzip -c {path:?}: {e}"))?;
        let stdout = child.stdout.take().ok_or("gunzip stdout missing")?;
        Ok(Box::new(Gunzip {
            child,
            inner: BufReader::new(stdout),
        }))
    } else {
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(0))
            .map_err(|e| e.to_string())?;
        Ok(Box::new(BufReader::new(file)))
    }
}

/// Iterator over the top-level elements of a JSON array stream:
/// tracks string/escape state and bracket depth so `},{` inside
/// strings or nested values never splits an element.
struct ArrayElements<R> {
    r: R,
    cur: Vec<u8>,
    depth: u32,
    in_elem: bool,
    in_str: bool,
    esc: bool,
    done: bool,
}

impl<R: BufRead> ArrayElements<R> {
    fn new(r: R) -> Self {
        Self {
            r,
            cur: Vec::with_capacity(4096),
            depth: 0,
            in_elem: false,
            in_str: false,
            esc: false,
            done: false,
        }
    }
}

impl<R: BufRead> Iterator for ArrayElements<R> {
    type Item = std::io::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            let mut emit = false;
            let consumed = {
                let buf = match self.r.fill_buf() {
                    Ok(b) if b.is_empty() => {
                        self.done = true;
                        return None;
                    }
                    Ok(b) => b,
                    Err(e) => {
                        self.done = true;
                        return Some(Err(e));
                    }
                };
                let mut consumed = 0;
                for &b in buf {
                    consumed += 1;
                    if self.in_elem {
                        self.cur.push(b);
                        if self.in_str {
                            if self.esc {
                                self.esc = false;
                            } else if b == b'\\' {
                                self.esc = true;
                            } else if b == b'"' {
                                self.in_str = false;
                            }
                            continue;
                        }
                        match b {
                            b'"' => self.in_str = true,
                            b'{' | b'[' => self.depth += 1,
                            b'}' | b']' => {
                                self.depth -= 1;
                                if self.depth == 1 {
                                    emit = true;
                                    break;
                                }
                            }
                            _ => {}
                        }
                    } else {
                        match b {
                            b'[' => self.depth = 1,
                            b'{' if self.depth == 1 => {
                                self.in_elem = true;
                                self.depth = 2;
                                self.cur.clear();
                                self.cur.push(b);
                            }
                            b']' if self.depth == 1 => {
                                self.done = true;
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                consumed
            };
            self.r.consume(consumed);
            if emit {
                self.in_elem = false;
                return Some(Ok(std::mem::take(&mut self.cur)));
            }
        }
        None
    }
}

/// Parse and filter the template file. Dropped templates:
/// kind 20000-29999 (ephemeral), 5 (NIP-09 deletion) and 62 (NIP-62
/// request to vanish): with Zipf-drawn authors one popular author's
/// deletion or vanish would wipe a large, relay-dependent share of
/// the corpus. Templates with an `expiration` tag or a tag element
/// over 1024 bytes are also dropped: the former expire by ingest
/// time, the latter are rejected by nosfry's size limits, and both
/// would leave relay-dependent holes in the stored set.
fn load_templates(path: &Path) -> Result<Templates, String> {
    let fingerprint = file_sha256(path)?;
    let reader = template_reader(path)?;
    let mut out = Templates {
        list: Vec::new(),
        total: 0,
        dropped_ephemeral: 0,
        dropped_kind5: 0,
        dropped_kind62: 0,
        dropped_expiration: 0,
        dropped_oversize_tag: 0,
        malformed: 0,
        fingerprint,
    };
    for element in ArrayElements::new(reader) {
        let bytes = element.map_err(|e| e.to_string())?;
        out.total += 1;
        let v: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        let kind = v.get("kind").and_then(Value::as_u64).unwrap_or(u64::MAX);
        if (20_000..30_000).contains(&kind) {
            out.dropped_ephemeral += 1;
            continue;
        }
        if kind == 5 {
            out.dropped_kind5 += 1;
            continue;
        }
        if kind == 62 {
            out.dropped_kind62 += 1;
            continue;
        }
        let mut expired = false;
        let mut oversize = false;
        if let Some(tags) = v.get("tags").and_then(Value::as_array) {
            for tag in tags.iter().filter_map(Value::as_array) {
                if tag.first().and_then(Value::as_str) == Some("expiration") {
                    expired = true;
                }
                if tag
                    .iter()
                    .skip(1)
                    .any(|e| e.as_str().is_none_or(|s| s.len() > 1024))
                {
                    oversize = true;
                }
            }
        }
        if expired {
            out.dropped_expiration += 1;
            continue;
        }
        if oversize {
            out.dropped_oversize_tag += 1;
            continue;
        }
        let (Some(tags), Some(content)) = (
            v.get("tags").and_then(Value::as_array),
            v.get("content").and_then(Value::as_str),
        ) else {
            out.malformed += 1;
            continue;
        };
        if kind > u64::from(u16::MAX) {
            out.malformed += 1;
            continue;
        }
        out.list.push(Template {
            kind: kind as u16,
            tags: tags.clone(),
            content: content.to_owned(),
        });
    }
    if out.list.is_empty() {
        return Err("template file produced no usable events".to_owned());
    }
    Ok(out)
}

/// Generate the `a` author keypairs, partitioned over `threads`.
fn authors(seed: u64, a: usize, threads: usize) -> Vec<Author> {
    let mut out: Vec<Option<Author>> = (0..a).map(|_| None).collect();
    let threads = threads.max(1).min(a.max(1));
    let span = a.div_ceil(threads);
    std::thread::scope(|s| {
        for (w, slots) in out.chunks_mut(span).enumerate() {
            s.spawn(move || {
                for (j, slot) in slots.iter_mut().enumerate() {
                    let i = w * span + j;
                    let keypair = author_keypair(seed, i as u64);
                    let pk_hex = hex(&keypair.x_only_public_key().0.to_byte_array());
                    *slot = Some(Author { keypair, pk_hex });
                }
            });
        }
    });
    out.into_iter().map(Option::unwrap).collect()
}

/// Rewrite one template tag. `pool` is `None` for e-pool members
/// (their `e` tags degrade to `p` tags) and `Some` past the pool.
fn rewrite_tag(tag: &[Value], pool: Option<&[String]>, ctx: &Ctx, rng: &mut ChaCha20Rng) -> Value {
    let Some(name) = tag.first().and_then(Value::as_str) else {
        return Value::Array(tag.to_vec());
    };
    let mut out = tag.to_vec();
    match name {
        "e" if pool.is_none() => {
            out[0] = json!("p");
            if out.len() > 1 {
                out[1] = json!(ctx.authors[zipf_draw(ctx.zipf, rng)].pk_hex);
            }
        }
        "e" => {
            if out.len() > 1 && let Some(pool) = pool.filter(|p| !p.is_empty()) {
                out[1] = json!(pool[rng.random_range(0..pool.len())]);
            }
        }
        "p" => {
            if out.len() > 1 {
                out[1] = json!(ctx.authors[zipf_draw(ctx.zipf, rng)].pk_hex);
            }
        }
        "a" => {
            if let Some(v) = out.get(1).and_then(Value::as_str)
                && let Some((k, rest)) = v.split_once(':')
                && let Some((_, d)) = rest.split_once(':')
            {
                let pk = &ctx.authors[zipf_draw(ctx.zipf, rng)].pk_hex;
                out[1] = json!(format!("{k}:{pk}:{d}"));
            }
        }
        _ => {}
    }
    Value::Array(out)
}

/// Generate event `i`: template pick, Zipf author, spread
/// `created_at`, rewritten tags, BIP-340 signature.
fn gen_event(i: u64, pool: Option<&[String]>, ctx: &Ctx, seed: u64, stats: &mut Stats) -> Event {
    let mut rng = keyed_rng(seed, b"ev", i);
    let template = &ctx.templates[rng.random_range(0..ctx.templates.len())];
    let author_idx = zipf_draw(ctx.zipf, &mut rng);
    stats.author_draws[author_idx] += 1;
    let created_at = T0
        + (i as u128 * u128::from(SPAN) / ctx.total as u128) as u64
        + rng.random_range(0..JITTER);
    let tags: Vec<Value> = template
        .tags
        .iter()
        .filter_map(|t| t.as_array())
        .map(|t| rewrite_tag(t, pool, ctx, &mut rng))
        .collect();
    let (id, line) = sign_event(
        &ctx.authors[author_idx],
        created_at,
        template.kind,
        &Value::Array(tags.clone()),
        &template.content,
    );
    Event {
        id,
        line,
        kind: template.kind,
        pubkey: ctx.authors[author_idx].pk_hex.clone(),
        tags,
    }
}

struct Event {
    id: String,
    line: String,
    kind: u16,
    pubkey: String,
    tags: Vec<Value>,
}

/// One worker: draw chunk indices until exhausted and send finished
/// chunks to the writer; merge local stats once at the end.
fn worker(
    next: &AtomicUsize,
    chunks: usize,
    pool_chunks: usize,
    ctx: &Ctx,
    seed: u64,
    tx: &std::sync::mpsc::SyncSender<ChunkOut>,
    stats: &Mutex<Stats>,
) -> Result<(), String> {
    let mut local = Stats {
        author_draws: vec![0; ctx.authors.len()],
        ..Stats::default()
    };
    let pool_n = POOL.min(ctx.total);
    loop {
        let idx = next.fetch_add(1, Ordering::Relaxed);
        if idx >= chunks {
            break;
        }
        // Wait for the pool ids only when this chunk can reference
        // them; pool chunks themselves never wait (see PoolSlot).
        let pool = if idx >= pool_chunks {
            Some(ctx.pool.wait())
        } else {
            None
        };
        let start = idx * CHUNK;
        let end = (start + CHUNK).min(ctx.total);
        let mut out = ChunkOut {
            idx,
            lines: Vec::with_capacity(end - start),
            pool_ids: Vec::new(),
            wanted: Vec::new(),
            triples: Vec::new(),
        };
        for i in start..end {
            let i = i as u64;
            // The pool never references itself even once ids exist.
            let eff_pool = if (i as usize) < pool_n {
                None
            } else {
                pool.as_deref()
            };
            let ev = gen_event(i, eff_pool.map(Vec::as_slice), ctx, seed, &mut local);
            *local.kinds.entry(ev.kind).or_insert(0) += 1;
            for tag in &ev.tags {
                let name = tag.get(0).and_then(Value::as_str).unwrap_or("");
                match (name, tag.get(1).and_then(Value::as_str)) {
                    ("t", Some(v)) => *local.t_counts.entry(v.to_owned()).or_insert(0) += 1,
                    ("d", Some(d)) if (30_000..40_000).contains(&ev.kind) => {
                        out.triples.push((ev.kind, ev.pubkey.clone(), d.to_owned()));
                    }
                    _ => {}
                }
            }
            if (i as usize) < pool_n {
                out.pool_ids.push(ev.id.clone());
            }
            if ctx.wanted.contains(&i) {
                out.wanted.push((i, ev.id));
            }
            out.lines.push(ev.line);
        }
        tx.send(out).map_err(|e| format!("writer channel: {e}"))?;
    }
    let mut shared = stats.lock().expect("stats mutex");
    for (a, n) in local.author_draws.iter().enumerate() {
        shared.author_draws[a] += n;
    }
    for (k, n) in local.kinds {
        *shared.kinds.entry(k).or_insert(0) += n;
    }
    for (t, n) in local.t_counts {
        *shared.t_counts.entry(t).or_insert(0) += n;
    }
    Ok(())
}

struct WriterOut {
    events_sha: String,
    holdout_sha: String,
    pool_ids: Vec<String>,
    wanted: Vec<(u64, String)>,
    triples: Vec<(u16, String, String)>,
}

/// The single ordered writer: buffers out-of-order chunks, writes
/// lines to `events.jsonl` (`i < N`) or `holdout.jsonl`, hashes both,
/// and publishes the pool ids once the pool prefix is complete.
fn writer(
    rx: Receiver<ChunkOut>,
    chunks: usize,
    events: usize,
    pool_chunks: usize,
    pool: &PoolSlot,
    out: &Path,
) -> Result<WriterOut, String> {
    let mut events_file = BufWriter::with_capacity(
        1 << 20,
        std::fs::File::create(out.join("events.jsonl")).map_err(|e| e.to_string())?,
    );
    let mut holdout_file = BufWriter::with_capacity(
        1 << 20,
        std::fs::File::create(out.join("holdout.jsonl")).map_err(|e| e.to_string())?,
    );
    let (mut eh, mut hh) = (Sha256::new(), Sha256::new());
    let mut pool_ids = Vec::new();
    let mut wanted = Vec::new();
    let mut triples = Vec::new();
    let mut pending: BTreeMap<usize, ChunkOut> = BTreeMap::new();
    let mut expect = 0usize;
    while expect < chunks {
        let chunk = rx.recv().map_err(|_| "writer channel closed early")?;
        pending.insert(chunk.idx, chunk);
        while let Some(chunk) = pending.remove(&expect) {
            let base = chunk.idx * CHUNK;
            for (j, line) in chunk.lines.iter().enumerate() {
                let i = base + j;
                let (file, h) = if i < events {
                    (&mut events_file, &mut eh)
                } else {
                    (&mut holdout_file, &mut hh)
                };
                file.write_all(line.as_bytes())
                    .and_then(|()| file.write_all(b"\n"))
                    .map_err(|e| e.to_string())?;
                h.update(line.as_bytes());
                h.update(b"\n");
            }
            pool_ids.extend(chunk.pool_ids);
            wanted.extend(chunk.wanted);
            triples.extend(chunk.triples);
            expect += 1;
            if expect == pool_chunks {
                pool.set(pool_ids.clone());
            }
        }
    }
    events_file.flush().map_err(|e| e.to_string())?;
    holdout_file.flush().map_err(|e| e.to_string())?;
    Ok(WriterOut {
        events_sha: hex(&eh.finalize()),
        holdout_sha: hex(&hh.finalize()),
        pool_ids,
        wanted,
        triples,
    })
}

/// The `params.json` sidecar: every draw here uses `sidecar_rng`, so
/// it is identical across reruns of the same seed.
fn write_params(
    opts: &GenOpts,
    templates: &Templates,
    authors: &[Author],
    zipf: &[f64],
    stats: &Stats,
    writer: &WriterOut,
) -> Result<String, String> {
    let mut rng = keyed_rng(opts.seed, b"params", 0);
    // Top-N authors by draw count; ties break toward the lower index
    // so the list is independent of worker interleaving.
    let mut ranked: Vec<usize> = (0..authors.len()).collect();
    ranked.sort_by_key(|&a| std::cmp::Reverse(stats.author_draws[a]));
    let popular: Vec<&str> = ranked
        .iter()
        .take(N_POPULAR)
        .map(|&a| authors[a].pk_hex.as_str())
        .collect();
    let random_authors: Vec<&str> = (0..N_RANDOM)
        .map(|_| authors[rng.random_range(0..authors.len())].pk_hex.as_str())
        .collect();
    let mut ids: Vec<String> = writer.wanted.iter().map(|(_, id)| id.clone()).collect();
    ids.sort_unstable();
    let tag_t: Vec<&String> = {
        let mut t: Vec<(&String, &u64)> = stats.t_counts.iter().collect();
        t.sort_by(|a, b| b.1.cmp(a.1).then_with(|| a.0.cmp(b.0)));
        t.iter().take(N_TAGS).map(|(v, _)| *v).collect()
    };
    let follows: Vec<Vec<String>> = (0..N_FOLLOWS)
        .map(|_| {
            let len = rng.random_range(100..=1000usize).min(authors.len());
            (0..len)
                .map(|_| authors[zipf_draw(zipf, &mut rng)].pk_hex.clone())
                .collect()
        })
        .collect();
    let mut seen = HashSet::new();
    let addr: Vec<Value> = writer
        .triples
        .iter()
        .filter(|t| seen.insert((*t).clone()))
        .take(N_TRIPLES)
        .map(|(k, pk, d)| json!({"kind": k, "pubkey": pk, "d": d}))
        .collect();
    let params = json!({
        "seed": opts.seed,
        "events": opts.events,
        "holdout": opts.holdout,
        "authors": opts.authors,
        "templates": templates.list.len(),
        "template_sha256": templates.fingerprint,
        "authors_popular": popular,
        "authors_random": random_authors,
        "ids": ids,
        "tag_t": tag_t,
        "follows": follows,
        "created_at": [T0, T0 + SPAN + JITTER - 1],
        "kinds": stats.kinds,
        "addr": addr,
        "pool_ids": writer.pool_ids,
    });
    let path = opts.out.join("params.json");
    let mut file = BufWriter::new(std::fs::File::create(&path).map_err(|e| e.to_string())?);
    serde_json::to_writer(&mut file, &params).map_err(|e| e.to_string())?;
    file.flush().map_err(|e| e.to_string())?;
    let bytes = std::fs::read(&path).map_err(|e| e.to_string())?;
    Ok(hex(&Sha256::digest(&bytes)))
}

/// Generate the corpus. Returns the final JSON line's fields.
pub(crate) fn run(opts: &GenOpts) -> Result<Value, String> {
    let started_utc = crate::unix_now();
    let started = Instant::now();
    std::fs::create_dir_all(&opts.out).map_err(|e| format!("{:?}: {e}", opts.out))?;
    let templates = load_templates(&opts.template)?;
    eprintln!(
        "gen: {} templates ({} dropped: ephemeral {}, kind5 {}, kind62 {}, expiration {}, oversize-tag {}, malformed {})",
        templates.list.len(),
        templates.total - templates.list.len(),
        templates.dropped_ephemeral,
        templates.dropped_kind5,
        templates.dropped_kind62,
        templates.dropped_expiration,
        templates.dropped_oversize_tag,
        templates.malformed,
    );
    let threads = opts.threads.max(1);
    let authors = authors(opts.seed, opts.authors, threads);
    let zipf = zipf_table(opts.authors);
    let total = opts.events + opts.holdout;
    if total == 0 {
        return Err("--events + --holdout must be positive".to_owned());
    }
    let chunks = total.div_ceil(CHUNK);
    let pool_chunks = POOL.min(total).div_ceil(CHUNK);

    // The sidecar's random-id picks, drawn before generation so hits
    // are collected on the fly instead of storing every id.
    let wanted: BTreeSet<u64> = {
        let mut rng = keyed_rng(opts.seed, b"ids", 0);
        let n = N_IDS.min(opts.events);
        let mut set = BTreeSet::new();
        while set.len() < n {
            set.insert(rng.random_range(0..opts.events.max(1)) as u64);
        }
        set
    };

    let (tx, rx) = sync_channel::<ChunkOut>(CHANNEL_CAP);
    let pool = PoolSlot {
        slot: Mutex::new(None),
        ready: Condvar::new(),
    };
    let ctx = Ctx {
        templates: &templates.list,
        authors: &authors,
        zipf: &zipf,
        pool: &pool,
        total,
        wanted: &wanted,
    };
    let stats = Mutex::new(Stats {
        author_draws: vec![0; authors.len()],
        ..Stats::default()
    });
    let next = AtomicUsize::new(0);
    let writer_out = std::thread::scope(|s| -> Result<WriterOut, String> {
        let out = &opts.out;
        let writer = s.spawn(|| writer(rx, chunks, opts.events, pool_chunks, &pool, out));
        let mut handles = Vec::with_capacity(threads);
        for _ in 0..threads {
            let (next, ctx, stats) = (&next, &ctx, &stats);
            let tx = tx.clone();
            handles.push(s.spawn(move || {
                worker(next, chunks, pool_chunks, ctx, opts.seed, &tx, stats)
            }));
        }
        drop(tx);
        for h in handles {
            h.join().map_err(|_| "worker panicked")??;
        }
        writer.join().map_err(|_| "writer panicked")?
    })?;

    let stats = stats.into_inner().expect("stats mutex");
    let params_sha = write_params(opts, &templates, &authors, &zipf, &stats, &writer_out)?;

    let mut fingerprints = String::new();
    for (name, sha) in [
        ("events.jsonl", writer_out.events_sha.as_str()),
        ("holdout.jsonl", writer_out.holdout_sha.as_str()),
        ("params.json", params_sha.as_str()),
    ] {
        fingerprints.push_str(&format!("{sha}  {name}\n"));
    }
    std::fs::write(opts.out.join("fingerprints.txt"), &fingerprints)
        .map_err(|e| e.to_string())?;

    Ok(json!({
        "scenario": "gen",
        "seed": opts.seed,
        "started_utc": started_utc,
        "ended_utc": crate::unix_now(),
        "harness_sha": env!("GIT_SHA"),
        "events": opts.events,
        "holdout": opts.holdout,
        "authors": opts.authors,
        "threads": threads,
        "template_sha256": templates.fingerprint,
        "templates_total": templates.total,
        "templates_used": templates.list.len(),
        "dropped_ephemeral": templates.dropped_ephemeral,
        "dropped_kind5": templates.dropped_kind5,
        "dropped_kind62": templates.dropped_kind62,
        "dropped_expiration": templates.dropped_expiration,
        "dropped_oversize_tag": templates.dropped_oversize_tag,
        "dropped_malformed": templates.malformed,
        "out": {
            "events.jsonl": writer_out.events_sha,
            "holdout.jsonl": writer_out.holdout_sha,
            "params.json": params_sha,
        },
        "pool_ids": writer_out.pool_ids.len(),
        "wall_secs": started.elapsed().as_secs_f64(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small D1-shaped template corpus covering every tag rewrite
    /// path and every dropped kind.
    fn write_template(path: &Path) {
        let mut events = Vec::new();
        for i in 0..300 {
            let kind = [0u16, 1, 3, 6, 7, 9735, 30023][i % 7];
            let mut tags = Vec::new();
            match kind {
                1 | 6 => {
                    tags.push(json!(["e", "ab".repeat(32), "", "reply"]));
                    tags.push(json!(["p", "cd".repeat(32)]));
                    tags.push(json!(["t", format!("tag{}", i % 20)]));
                }
                30023 => {
                    tags.push(json!(["d", format!("article-{i}")]));
                    tags.push(json!(["a", "30023:".to_owned() + &"ef".repeat(32) + ":x"]));
                    tags.push(json!(["t", "longform"]));
                }
                _ => {}
            }
            events.push(json!({
                "id": "00".repeat(32),
                "pubkey": "11".repeat(32),
                "created_at": 1_700_000_000u64 + i as u64,
                "kind": kind,
                "tags": tags,
                "content": format!("template content {i}"),
                "sig": "22".repeat(64),
            }));
        }
        // Dropped: a deletion, a vanish request, an ephemeral, an
        // expiring event, an oversized tag value.
        for kind in [5u16, 62, 20001] {
            events.push(json!({
                "id": "33".repeat(32), "pubkey": "44".repeat(32),
                "created_at": 1, "kind": kind, "tags": [],
                "content": "", "sig": "55".repeat(64),
            }));
        }
        events.push(json!({
            "id": "66".repeat(32), "pubkey": "77".repeat(32),
            "created_at": 1, "kind": 1,
            "tags": [["expiration", "1700000000"]],
            "content": "", "sig": "88".repeat(64),
        }));
        events.push(json!({
            "id": "99".repeat(32), "pubkey": "aa".repeat(32),
            "created_at": 1, "kind": 1,
            "tags": [["imeta", "x".repeat(2000)]],
            "content": "", "sig": "bb".repeat(64),
        }));
        std::fs::write(path, Value::Array(events).to_string()).unwrap();
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("nosfry-bench-corpus-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn run_gen(dir: &Path, template: &Path, events: usize, holdout: usize, threads: usize) -> Value {
        let opts = GenOpts {
            template: template.to_owned(),
            events,
            holdout,
            authors: 200,
            seed: 42,
            out: dir.to_owned(),
            threads,
        };
        run(&opts).unwrap()
    }

    #[test]
    fn templates_drop_by_reason() {
        let dir = test_dir("drops");
        let template = dir.join("d1.json");
        write_template(&template);
        let t = load_templates(&template).unwrap();
        assert_eq!(t.list.len(), 300);
        assert_eq!(t.dropped_ephemeral, 1);
        assert_eq!(t.dropped_kind5, 1);
        assert_eq!(t.dropped_kind62, 1);
        assert_eq!(t.dropped_expiration, 1);
        assert_eq!(t.dropped_oversize_tag, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The spec's determinism guarantee: the first 1,000 lines are
    /// identical between 1 and 4 threads (and the whole fingerprint
    /// is too, since chunks only reorder internally).
    #[test]
    fn same_seed_same_output_across_threads() {
        let dir = test_dir("det");
        let template = dir.join("d1.json");
        write_template(&template);
        // Three chunks plus a partial one, so ordering really matters.
        let a = dir.join("a");
        let b = dir.join("b");
        run_gen(&a, &template, 25_000, 500, 1);
        run_gen(&b, &template, 25_000, 500, 4);
        for name in ["events.jsonl", "holdout.jsonl", "params.json"] {
            let fa = std::fs::read(a.join(name)).unwrap();
            let fb = std::fs::read(b.join(name)).unwrap();
            assert_eq!(fa, fb, "{name} differs between thread counts");
        }
        let events = std::fs::read_to_string(a.join("events.jsonl")).unwrap();
        assert_eq!(events.lines().count(), 25_000);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Every generated event must verify: id recomputation from the
    /// serialized fields and a BIP-340 signature check.
    #[test]
    fn generated_events_verify() {
        let dir = test_dir("verify");
        let template = dir.join("d1.json");
        write_template(&template);
        run_gen(&dir, &template, 500, 0, 2);
        let events = std::fs::read_to_string(dir.join("events.jsonl")).unwrap();
        assert_eq!(events.lines().count(), 500);
        for line in events.lines() {
            let ev: Value = serde_json::from_str(line).unwrap();
            let preimage = json!([
                0,
                ev["pubkey"],
                ev["created_at"],
                ev["kind"],
                ev["tags"],
                ev["content"]
            ])
            .to_string();
            let id: [u8; 32] = Sha256::digest(preimage.as_bytes()).into();
            assert_eq!(hex(&id), ev["id"].as_str().unwrap());
            let mut sig_bytes = [0u8; 64];
            let sig_hex = ev["sig"].as_str().unwrap();
            for i in 0..64 {
                sig_bytes[i] = u8::from_str_radix(&sig_hex[2 * i..2 * i + 2], 16).unwrap();
            }
            let sig = secp256k1::schnorr::Signature::from_byte_array(sig_bytes);
            let mut pk_bytes = [0u8; 32];
            let pk_hex = ev["pubkey"].as_str().unwrap();
            for i in 0..32 {
                pk_bytes[i] = u8::from_str_radix(&pk_hex[2 * i..2 * i + 2], 16).unwrap();
            }
            let pk = secp256k1::XOnlyPublicKey::from_byte_array(pk_bytes).unwrap();
            secp256k1::schnorr::verify(&sig, &id, &pk).unwrap();
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The e-pool contract: members (i < P) carry no `e` tags and
    /// every later `e` tag references a pool id. Exercises the
    /// writer-side pool handoff between chunks.
    #[test]
    fn pool_members_degrade_e_tags_and_later_events_reference_pool() {
        let dir = test_dir("pool");
        let template = dir.join("d1.json");
        write_template(&template);
        run_gen(&dir, &template, POOL + 2_000, 0, 4);
        let params: Value = serde_json::from_str(
            &std::fs::read_to_string(dir.join("params.json")).unwrap(),
        )
        .unwrap();
        let pool: HashSet<String> = params["pool_ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_owned())
            .collect();
        assert_eq!(pool.len(), POOL);
        let events = std::fs::read_to_string(dir.join("events.jsonl")).unwrap();
        for (i, line) in events.lines().enumerate() {
            let ev: Value = serde_json::from_str(line).unwrap();
            for tag in ev["tags"].as_array().unwrap() {
                if tag[0].as_str() == Some("e") {
                    assert!(i >= POOL, "e tag survived inside the pool at i={i}");
                    assert!(
                        pool.contains(tag[1].as_str().unwrap()),
                        "e tag at i={i} references a non-pool id"
                    );
                }
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The inverse-CDF Zipf draw: the top 1% of items must collect
    /// close to the theoretical `H(k)/H(A)` share.
    #[test]
    fn zipf_top_share_matches_theory() {
        let a = 1_000usize;
        let cdf = zipf_table(a);
        let mut rng = keyed_rng(7, b"test", 0);
        let draws = 200_000usize;
        let top = (0..draws)
            .filter(|_| zipf_draw(&cdf, &mut rng) < 10)
            .count();
        let harmonic = |n: usize| (1..=n).map(|j| 1.0 / j as f64).sum::<f64>();
        let expected = harmonic(10) / harmonic(a);
        let share = top as f64 / draws as f64;
        assert!(
            (share - expected).abs() < 0.02,
            "top-1% share {share:.4} vs theory {expected:.4}"
        );
    }
}
