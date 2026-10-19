# Changelog

All notable changes to `nosfry` are documented in this file.

The format is based on [Keep a Changelog]; this project follows
[Semantic Versioning].

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- `[relay] nofiles` (default 1,000,000, `0` disables): the binary raises
  its soft open-file limit at startup, capped by the hard limit, so
  systemd/containerd's 1024 default no longer caps connection count.
  Failures warn and do not abort startup.
- NIP-09 event deletion: kind-5 `e` tags delete the author's own events
  by id and `a` tags delete the author's own replaceable/addressable
  events at an address (versions with `created_at` not newer than the
  deletion request). Republishing a deleted event or address is
  rejected with `blocked: deleted by its author`; deleting the kind 5
  itself lifts the block. Deleting a deletion request has no effect.
- NIP-40 event expiration: the first `expiration` tag is validated at
  ingest (missing, non-decimal, or sub-100 values are rejected) and an
  already-expired event is refused with `invalid: event has expired`.
  Stored events are hidden from every `REQ` at their timestamp and
  swept by the writer on a 10-second timer.
- `Store::delete` (delete by seq) and `Store::expire` (sweep expired
  events, capped per call) in `nosfry-lmdb`; `WriteOutcome::Deleted`
  reports a blocked republish.
- `nosfry delete [--dry-run] <filter-json>` deletes every stored event
  matching a `REQ` filter (including already-expired ones) and prints
  the count; `ImportStats` gains a `blocked` field for blocked imports.
- Multi-arch container image published to `ghcr.io/qntx/nosfry` on tag
  pushes: distroless non-root runtime, `NOSFRY_CONFIG` pointing at a
  bundled config with `/data` as the database volume.
- taplo formatting for every TOML file (`.taplo.toml`, aligned `=`),
  checked in CI.
- Administration commands on the `nosfry` binary: `export` writes every
  stored event as JSON lines ordered by `created_at` (with `--since`,
  `--until`, and `--reverse`), `import` verifies and stores event JSON
  lines from stdin in 10 000-event batches without a `created_at`
  time-window check (`--show-rejected` logs each rejected line), `scan`
  runs a `REQ` filter over the whole database in newest-first order
  (with `--count`), and `compact` copies the database to a compacted
  file. Read-only commands fail with `no database at <path>` when the
  database does not exist.
- `Store::export` (snapshot-consistent chronological streaming via
  `ix_time`) and `Store::compact_to` (compacted environment copy) in
  `nosfry-lmdb`.
- `nosfry::admin`, the library API behind the administration commands.

### Changed

- The LMDB format version is now 2 (three new lifecycle databases);
  databases created by 0.1.0 cannot be opened. Since 0.1.0 had no
  `export`, a 0.1.0 database must be re-imported from its source.
- `Store::write` returns `Written { outcomes, deleted }`, listing every
  seq removed inside the same transaction, instead of the bare outcome
  list.
- `Scan::new` takes a `now` argument (an event expiring at or before it
  is hidden and never counted) and its emit callback receives
  `(seq, json)`.
- `--config` is a global flag on the `nosfry` binary: it works before
  or after the subcommand, not just on `relay`.
- All logs go to stderr (previously stdout), keeping stdout clean for
  `export` and `scan` output.

## [0.1.0] - 2026-10-19

### Added

- `nosfry-core`, the sans-IO protocol core. It has no async, no I/O, no clock,
  and no randomness, uses `forbid(unsafe_code)`, and builds cleanly for
  `wasm32-unknown-unknown`.
  - `event`: a strict NIP-01 ingest pipeline.
    - Parsing borrows from the input, requires all seven fields, and
      rejects duplicate keys, non-lowercase hex, and malformed `e`/`p`
      tags.
    - `EventLimits` sets the size, tag, and `created_at` window limits,
      with strfry's defaults.
    - `verify` checks the id hash, then the canonical JSON size, then
      the BIP-340 signature.
    - The result is an immutable `Event` with canonical JSON, a packed
      record, and the replaceable/addressable `address_hash`.
  - `packed`: a strfry-style index record with the same field order and
    tag encoding, but not byte-compatible with strfry.
  - `filter`: a compiled `Filter` with allocation-free matching.
    - Duplicate keys are rejected.
    - Unknown keys, `search`, and non-letter `#` keys are answered with
      `unsupported:`.
    - Hex values are accepted in any case and normalized.
    - An empty list makes the filter impossible.
  - `message`: an `Arc`-backed `SubId` that accepts any string of 1 to 64
    characters, including JSON escapes. Also exact-arity client message
    parsing and relay frame builders.
  - `limits`: `Limits { event, req }`.
  - `session`: a per-connection state machine with subscription
    generations.
  - `monitor`: the live-subscription inverted index, with O(1) removal.
- `nosfry-lmdb`, a synchronous LMDB event store.
  - The on-disk format is versioned, and sequence numbers persist and are
    never reused.
  - Each batch is written in one transaction. Replaceable and addressable
    events are evicted by `supersedes`.
  - Commits are fully durable: LMDB's meta page is synced before the
    write returns.
  - `Scan` runs time-sliced, snapshot-bounded index scans. Each step keeps
    one live iterator per cursor and streams results per completed
    `created_at` group, with cross-filter deduplication.
- `nosfry`, the native relay crate and binary.
  - **Threads:** one writer (group commits), M monitor shards (live index,
    snapshot authority, and scan control for their connections), and N
    readers, with exactly-once historical and live delivery.
  - **`OK` frames** are pushed by the writer straight onto the
    connection's outbound queue, after the batch is queued on every
    shard, so a `REQ` sent after `OK` always snapshots the event. Scan
    results arrive in chunks, and the first frame of each scan is sent
    alone.
  - **Backpressure:**
    - every connection has an outbound byte budget, and scans pause
      above a high-water mark;
    - slow consumers are closed with 1008;
    - every socket write is bounded;
    - half-open connections are closed after two ping intervals.
  - **Sockets:** `TCP_NODELAY` on every accepted socket. Queued frames are
    batched into one flush. permessage-deflate runs at level 1, with an
    optional sliding window.
  - **HTTP:** NIP-11 is served from a document serialized once, with
    empty optional fields omitted. Also CORS preflight and
    `real_ip_header`.
  - **Shutdown:** connections are tracked and get a 10-second deadline;
    the writer commits its last batch before worker threads join.
  - **Configuration:** TOML with strfry's section layout and defaults.
    Unknown keys are rejected. `nosfry.example.toml` lists every key;
    `NOSFRY_CONFIG` selects the file.
- Benchmark tooling under `bench/`:
  - the `nosfry-bench` harness for live fan-out latency, memory per
    connection, and relay CPU;
  - a Linux container recipe with relay and harness CPU pinning;
  - relayBench and nosfry-bench drivers;
  - the `aggregate.py` summary script.

[Unreleased]: https://github.com/qntx/nosfry/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/qntx/nosfry/releases/tag/v0.1.0
