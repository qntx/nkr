# Changelog

All notable changes to `nosfry` are documented in this file.

The format is based on [Keep a Changelog]; this project follows
[Semantic Versioning].

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- `nosfry`, the sans-IO protocol core. It has no async, no I/O, no clock,
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
- `nosfry-server`, the native relay and its `nosfry` binary.
  - **Threads:** one writer (group commits), M monitor shards (live index,
    snapshot authority, and scan control for their connections), and N
    readers, with exactly-once historical and live delivery.
  - **`OK` frames** are pushed by the writer straight onto the
    connection's outbound queue. Scan results arrive in chunks, and the
    first frame of each scan is sent alone.
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
