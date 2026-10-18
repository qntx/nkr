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
- **aggregate.py**: `python3 bench/aggregate.py <results-dir>` prints the
  median and [min, max] of every metric across repeated runs.

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
cd /bench/nosfry-src && git checkout <sha> && cargo build --release -p nosfry-server
cp target/release/nosfry /bench/bin/<name>

# nosfry-bench
cp -r /src/nosfry/bench/nosfry-bench /bench/harness
cd /bench/harness && cargo build --release

# relayBench and its real-event corpus
cp -r /amethyst/relayBench/build/install/relaybench /bench/relaybench
mkdir -p /bench/corpus
cp /amethyst/quartz/src/commonTest/resources/nostr_vitor_startup_data.json.gz /bench/corpus/

# drivers and relay wrappers
cp /src/nosfry/bench/linux/*.sh /bench/
```

## Runs

```bash
/bench/run-relaybench.sh <name>...
/bench/run-nosfry-bench.sh /bench/results/nosfry-bench <reps> <name>...
```

Each `<name>` is a binary in `/bench/bin`; strfry always runs alongside.

- `run-relaybench.sh` runs relayBench 3 times on the synthetic corpus
  (100,000 events, seed 1) and twice on the real corpus. It rotates the
  order of the nosfry binaries between runs; strfry always runs first, a
  relayBench limitation.
- `run-nosfry-bench.sh` runs six scenarios: broadcast to 1,000 subscribers
  (plain and deflate), broadcast to 5,000, selective delivery to 2,000, and
  5,000 idle connections (plain and deflate). Each scenario starts against
  a fresh relay and database, and the relay order rotates on every rep.

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
