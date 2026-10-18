# Benchmark tooling

Reproduction assets for the relay benchmarks. Nothing here is part of the
cargo workspace (`bench/nkrbench` is excluded in the root `Cargo.toml`).

## Harnesses

- **relayBench** (`bench/linux/run-relaybench.sh`) — third-party harness from
  [amethyst](https://github.com/vitorpamplona/amethyst)
  (`relayBench/`, commit `9847d0a`). Boots each relay, replays a
  deterministic corpus (synthetic `--events/--seed`, or an NDJSON/gz dump via
  `--corpus`), then measures ingest throughput, receipt→queryable latency,
  and EOSE latency plus aggregate throughput for 12 derived query
  scenarios. Writes `report.md` + `results.json` per run.
- **nkrbench** (`bench/nkrbench/`, driven by `run-nkrbench.sh`) — live-traffic
  harness. `fanout` measures publish→delivery latency, loss, and relay
  CPU/RSS under N concurrent subscribers (broadcast or `--authors`-selective
  modes, optional `--deflate` compression). `conns` measures connection
  setup rate and per-connection memory. Writes one JSON object per run to
  `results.jsonl`.
- **aggregate.py** — `python3 bench/aggregate.py <results-dir>` prints the
  median and [min, max] across repeated runs for every metric in both
  harnesses' outputs.

## Linux run (primary)

CPU pinning: relays run on cores 0–3 (`taskset` inside
`strfry-pinned.sh`/`nkr-relay.sh`), the harnesses run on cores 4–9. A 10-core
container is expected; the scripts check nothing, so verify `nproc` first.

```bash
# image
docker build -t nkr-bench:linux bench/linux
docker volume create nkr-bench-work
mkdir -p results
docker run -d --name nkr-bench --ulimit nofile=65536:65536 \
    -v nkr-bench-work:/bench/work -v "$PWD/results:/bench/results" \
    -v "$PWD:/src/nkr:ro" -v /tmp/nkr-bench:/host:ro \
    nkr-bench:linux sleep infinity
```

Inside the container (`docker exec nkr-bench bash -lc '...'`):

```bash
# pristine strfry 1.1.3 (no patches needed on Linux)
git clone https://github.com/hoytech/strfry /bench/strfry-1.1.3
cd /bench/strfry-1.1.3 && git checkout 1.1.3 && git submodule update --init
make setup-golpe && make -j10

# nkr at the commits under test
git clone /src/nkr /bench/nkr-src && cd /bench/nkr-src
git checkout <sha> && cargo build --release -p nkr-server
cp target/release/nkr /bench/bin/<name>   # e.g. nkr-fixed, nkr-next

# nkrbench
cp -r /src/nkr/bench/nkrbench /bench/harness   # from the repo mount
(cd /bench/harness && cargo build --release)

# relayBench is built on the host, not in the container:
#   git clone https://github.com/vitorpamplona/amethyst && cd amethyst
#   git checkout 9847d0a
#   JAVA_HOME=<jdk21> ./gradlew :relayBench:installDist
# then copy relayBench/build/install/relaybench into the container as
# /bench/relaybench and the corpus quartz/src/commonTest/resources/
# nostr_vitor_startup_data.json.gz as /bench/corpus/.

cp /src/nkr/bench/linux/* /bench/         # driver + wrapper scripts
```

Runs:

```bash
docker exec nkr-bench bash -lc '/bench/run-relaybench.sh nkr-fixed nkr-next nkr-meta'
docker exec nkr-bench bash -lc '/bench/run-nkrbench.sh /bench/results/nkrbench 3 \
    2>&1 | tee /bench/results/nkrbench.log'
```

`run-relaybench.sh <nkr-binary-name>...` runs relayBench 5 times (3× synthetic
100k seed 1, 2× the real corpus) with every named binary from `/bench/bin`
side by side with strfry (always present via `--strfry-bin`), rotating the
order of the nkr binaries between runs. The published durability comparison
(docs/benchmarks.mdx, "Durability") adds `nkr-meta`: HEAD with one
measurement-only change in `Store::open`,
`unsafe { options.flags(EnvFlags::NO_META_SYNC) };`, which reproduces the
`MDB_NOMETASYNC` that strfry gets from passing `MDB_CREATE` to
`mdb_env_open`. That build is never shipped. `run-nkrbench.sh <out> <reps>`
rotates the relay order `strfry, nkr-fixed, nkr-next` each rep and runs
fanout-broadcast-1k, fanout-broadcast-1k-deflate, fanout-broadcast-5k,
fanout-selective-2k, conns-5k, conns-5k-deflate, each against a fresh relay
with an empty database.

Aggregate afterwards with `python3 bench/aggregate.py results/`.

## macOS scripts

`bench/macos/` holds the native driver and relay wrapper used on macOS
(same scenario list). macOS numbers are not used for conclusions: LMDB uses
`F_FULLFSYNC` on Apple filesystems and `ps` RSS is unreliable there. The
macOS scripts reference `/tmp/nkr-bench/...` paths; the Linux scripts
reference `/bench/...` inside the container.

## Output layout

- `run-relaybench.sh` → `<out>/<name>/<timestamp>/{report.md,results.json}` +
  `<out>/<name>.log`
- `run-nkrbench.sh` → `<out>/results.jsonl` + `<out>/rep<N>-<relay>-<scenario>.log`
  per relay/scenario/rep
