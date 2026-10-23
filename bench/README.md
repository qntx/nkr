# Benchmark tooling

Reproducible head-to-head benchmarks of nosfry against strfry. Nothing here
is part of the cargo workspace: `bench/nosfry-bench` is excluded in the root
`Cargo.toml`.

## Harnesses

- **relayBench** (third party, from
  [amethyst](https://github.com/vitorpamplona/amethyst) `relayBench/` at
  commit `9847d0a`; driven by `linux/run-relaybench.sh`).
  - Replays a deterministic corpus: synthetic `--events N --seed S`, or a
    dump via `--corpus`.
  - Measures ingest throughput, `OK` latency, and receipt-to-queryable
    latency.
  - Runs 12 client-shaped queries, reporting time to first event, time to
    `EOSE`, and 8-connection throughput.
  - Cross-checks the result sets of all relays.
  - Its OkHttp client negotiates permessage-deflate.
- **nosfry-bench** (`nosfry-bench/`; driven by `linux/run-nosfry-bench.sh`).
  - `fanout` measures publish-to-delivery latency for every subscriber and
    event pair, with exact delivery accounting and relay CPU.
  - `conns` measures anonymous resident memory per idle subscribed
    connection.
  - `soak` runs a mixed long-running workload against one relay
    (driven by `linux/run-soak.sh`): paced publishers writing a
    deterministic kind mix and awaiting every `OK`, long-lived
    subscribers counting deliveries, short-lived connections churning
    historical REQs and COUNTs, periodically spawned `REQ {}`
    consumers that never read, negentropy sessions ending by
    `NEG-CLOSE`, disconnect and the idle reaper, and vanish rounds
    that publish then ask the relay to delete them. Every
    `--sample-secs` it prints one cumulative JSON line with relay
    RSS, `data.mdb` size, connection, rate-limit,
    `nosfry_outbound_pending_bytes`, `nosfry_sync_memory_bytes` and
    `nosfry_subscription_bytes` metrics, and the workload counters; a
    final `end` line (with `outbound_pending_after_close`,
    `sync_memory_after_close` and `subscription_bytes_after_close`,
    the gauge values once every harness connection is closed) and a
    `relay_exit` line close the JSONL.
  - `gen` builds the D2 synthetic corpus (`--template` a gzipped JSON
    array of real events such as the Amethyst startup dump). The
    corpus is reproducible byte for byte: event `i` is fully
    determined by `--seed` and `i` (`sha256(seed || "ev" || i)` seeds
    a `ChaCha20Rng`), so `--threads` never changes the output.
    Templates keep their kind, tag skeleton and content; events get
    fresh authors drawn Zipf (s = 1.0) over `--authors` generated
    keypairs, a `created_at` spread over a fixed two-year window, and
    BIP-340 signatures with zero auxiliary randomness. Template events
    of ephemeral kinds (20000-29999) are dropped, and so are kinds 5
    (NIP-09 deletion) and 62 (NIP-62 request to vanish): with
    Zipf-drawn authors one popular author's vanish would wipe a large
    share of the corpus and deletions would make stored counts
    relay-dependent.
  - The e-pool: the first `P = 100,000` generated ids are the reply
    pool. Pool members carry no `e` tags of their own (a template `e`
    tag degrades to a `p` tag with a Zipf-drawn author); every event
    past `P` rewrites its `e` tags to a uniformly drawn pool id, so
    `#e` thread queries fan out to a real, shared set of parents.
  - Outputs in `--out`: `events.jsonl` (the `--events` prefix),
    `holdout.jsonl` (`--holdout` tail, the ingest driver's input),
    `params.json` (query-driver sidecar: popular and random authors,
    random ids, top `t` values, follow lists, `created_at` range,
    kind counts, `(kind, pubkey, d)` triples and `pool_ids`) and
    `fingerprints.txt` (sha256 of each).
- **aggregate.py**: `python3 bench/aggregate.py <results-dir>` prints the
  median and [min, max] of every metric across repeated runs.
- **soak_analyze.py**: `python3 bench/soak_analyze.py <run.jsonl>
--warmup-secs N` applies the soak criteria (anonymous-memory slope,
  `data.mdb` plateau, connection gauge, zero errors) and exits non-zero
  on a failure. Use a warm-up of at least the 600 s event expiration;
  the 72 h gate run uses 6 h.

## Environment

The scripts run inside the Linux container built from `linux/Dockerfile`:
Ubuntu 24.04, JDK 21, Rust, and strfry's build dependencies. Relays are
pinned to cores 0-3 (`strfry-pinned.sh`, `nosfry-relay.sh`) and harnesses to
cores 4-9, so a 10-core machine is expected. Check `nproc` first; the
scripts do not.

## Setup

Build relayBench on the host (JDK 21), once:

```bash
git clone https://github.com/vitorpamplona/amethyst && cd amethyst
git checkout 9847d0a
./gradlew :relayBench:installDist
```

Start the container, mounting this repository and the amethyst checkout:

```bash
docker build -t nosfry-bench:linux bench/linux
docker volume create nosfry-bench-work
docker run -d --name nosfry-bench --ulimit nofile=65536:65536 \
    -v nosfry-bench-work:/bench/work \
    -v "$PWD/results:/bench/results" \
    -v "$PWD:/src/nosfry:ro" \
    -v /path/to/amethyst:/amethyst:ro \
    nosfry-bench:linux sleep infinity
```

Inside the container (`docker exec nosfry-bench bash -lc '...'`):

```bash
# strfry 1.1.3, unpatched
git clone https://github.com/hoytech/strfry /bench/strfry-1.1.3
cd /bench/strfry-1.1.3 && git checkout 1.1.3 && git submodule update --init
make setup-golpe && make -j10

# one nosfry binary per commit under test
mkdir -p /bench/bin && git clone /src/nosfry /bench/nosfry-src
cd /bench/nosfry-src && git checkout <sha> && cargo build --release -p nosfry
cp target/release/nosfry /bench/bin/<name>

# nosfry-bench
cp -r /src/nosfry/bench/nosfry-bench /bench/harness
cd /bench/harness && cargo build --release

# relayBench and its real-event corpus
cp -r /amethyst/relayBench/build/install/relaybench /bench/relaybench
mkdir -p /bench/corpus
cp /amethyst/quartz/src/commonTest/resources/nostr_vitor_startup_data.json.gz /bench/corpus/

# drivers, relay wrappers, and config variants
cp /src/nosfry/bench/linux/*.sh /bench/
cp -r /src/nosfry/bench/linux/variants /bench/variants
```

## Runs

```bash
/bench/run-relaybench.sh <name>...
/bench/run-nosfry-bench.sh /bench/results/nosfry-bench <reps> <name>...
# a name may be bin+variant: /bench/variants/<variant>.toml is appended
# to the generated config and the full label stays in every result
/bench/run-nosfry-bench.sh /bench/results/r6 3 nosfry-020 nosfry-main nosfry-main+sliding
DURATION=1800 SAMPLE=30 /bench/run-soak.sh <name> [label]
```

`run-soak.sh` writes `/bench/results/soak-<label>.jsonl` and the
relay's log lives at `/bench/work/soak-<label>/relay.log`. Defaults:
8 publishers at 200 events/s total, 200 subscribers, 10 churn
connections/s, one slow consumer per minute, one negentropy session
per 10 s, one vanish round per minute; `DURATION` and `SAMPLE`
(seconds between samples) plus every workload knob
(`PUBLISHERS`, `RATE`, `SUBSCRIBERS`, `CHURN_RATE`, `SLOW_EVERY`,
`NEG_EVERY`, `VANISH_EVERY`)
are environment-overridable. `DEFLATE=1` makes every harness
connection negotiate permessage-deflate; `PORT`, `RELAY_CPUS`, and
`HARNESS_CPUS` move the soak off the default port and cores — a soak
can share the box with a benchmark run this way.

Each `<name>` is a binary in `/bench/bin`; strfry always runs alongside.

- `run-relaybench.sh` runs relayBench 3 times on the synthetic corpus
  (100,000 events, seed 1) and twice on the real corpus. It rotates the
  order of the nosfry binaries between runs; strfry always runs first, a
  relayBench limitation.
- `run-nosfry-bench.sh` runs six scenarios: broadcast to 1,000 subscribers
  (plain and deflate), broadcast to 5,000, selective delivery to 2,000, and
  5,000 idle connections (plain and deflate). Each scenario starts against
  a fresh relay and database, and the relay order rotates on every rep.

`nosfry-relay.sh` disables the rate limits and the per-IP connection cap
in the generated config: the scenarios run thousands of subscribers from
one IP and flood ingest over a single connection, and the benchmark
measures the engine, not the limiter.

The durability comparison adds a measurement-only build, never shipped,
that opens LMDB with `EnvFlags::NO_META_SYNC` in `Store::open`. This
reproduces the `MDB_NOMETASYNC` strfry effectively runs with: rasgueadb
passes `MDB_CREATE`, the same bit 0x40000, to `mdb_env_open`.

## Output

- `run-relaybench.sh`: `<out>/<run>/<timestamp>/{report.md,results.json}` and
  `<out>/<run>.log`.
- `run-nosfry-bench.sh`: `<out>/results.jsonl` (one JSON object per scenario,
  relay, and rep) and `<out>/rep<N>-<relay>-<scenario>.log`.
- `python3 bench/aggregate.py results` summarizes both.
