# Changelog

All notable changes to `nosfry` are documented in this file.

The format is based on [Keep a Changelog]; this project follows
[Semantic Versioning].

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- `nosfry` — sans-IO protocol core (no async, no I/O, no clock, no
  randomness, `forbid(unsafe_code)`, `wasm32-unknown-unknown` clean):
  - `kind`: `Kind`/`KindClass` with NIP-01 range predicates.
  - `event`: strict borrowed `UnverifiedEvent::parse` /
    `check_limits` / `verify` pipeline (all seven fields required,
    duplicate keys rejected, lowercase hex only, strfry `e`/`p` tag
    rule; verify checks the SHA-256 NIP-01 preimage and the BIP-340
    schnorr signature), immutable `Event` with canonical JSON, packed record,
    and replaceable/addressable `address_hash`;
    `cmp_newest_first`/`supersedes` ordering helpers.
  - `packed`: strfry-style `PackedEvent`/`PackedRef` index record
    (single-letter tag names, first value only, ≤255-byte values, `e`/`p`
    stored raw, NIP-40 `expiration` in the header).
  - `filter`: strict compiled `Filter` (duplicate keys rejected, unknown
    keys/`search`/non-letter `#` keys `unsupported:` per strfry relay
    policy, hex accepted case-insensitively and normalized, empty lists
    make the filter impossible) with binary-search `matches` and
    `effective_limit`.
  - `message`: `SubId` (1..=64 chars, pre-escaped JSON form),
    `ClientMessage` parser with exact arities (EVENT/REQ/CLOSE/COUNT/
    AUTH/NEG-*), relay frame builders, `Reason` prefixes.
  - `limits`: strfry-default `Limits` config struct.
  - `session`: per-connection state machine emitting `Command`s
    (publish, subscribe generation tracking, unsubscribe, frames);
    `close_by_relay` removes a subscription only when the generation
    still matches.
  - `monitor`: live-subscription inverted index (`Monitors`) with
    single-field-per-filter indexing and stamp-deduped `matches`.
- Vendored `qntx/nostr` test vectors under `crates/nosfry/tests/vectors`
  with explicit `DIVERGENCES` lists; proptest differential suites
  (filter matching, packed round trip, monitor vs brute force);
  criterion bench `benches/event.rs`.
- `nosfry-lmdb` — synchronous LMDB event store (native only; no async, no
  internal threads; the server owns the writer thread and reader pool):
  - `Store::open` (format-versioned `meta`, persistent `next_seq`),
    `Store::write` (single transaction, arrival order; `Stored` /
    `Duplicate` / `Superseded` / `Ephemeral` outcomes with
    `supersedes`-based replaceable and addressable eviction), and
    `Store::snapshot`.
  - `Scan` — time-sliced, snapshot-bounded index scans: one index per
    filter (`ix_id` point lookups or descending-range k-way merges over
    `ix_pubkey_kind`/`ix_pubkey`/`ix_tag`/`ix_kind`/`ix_time`), resumable
    cursors, boundary-`created_at` tie collection, cross-filter
    deduplication.
  - Differential proptest against a naive model, snapshot/persistence/
    replaceable/duplicate tests, criterion bench `benches/store.rs`.
- `nosfry-server` — the `nosfry` relay executable (package `nosfry-server`,
  `[[bin]] nosfry`, `publish = false`):
  - Shared-nothing threads: one writer (group commits + batch
    broadcast), M monitor shards (live-subscription `Monitors` index,
    snapshot authority, pending buffers), N reader threads
    (time-sliced snapshot-bounded `Scan`s); exactly-once historical +
    live delivery via the shard-assigned snapshot.
  - Per-connection `Outbound` queue with an atomic pending-bytes
    budget; slow consumers are closed with 1008.
  - axum HTTP: yawc WebSocket upgrade (permessage-deflate per config),
    NIP-11 document, CORS preflight, `real_ip_header`.
  - `nosfry relay --config <path>` CLI (also `NOSFRY_CONFIG`), TOML config
    (`[db]`/`[events]`/`[relay]`, `deny_unknown_fields`, strfry
    defaults, `nosfry.example.toml`), tracing-subscriber,
    graceful SIGTERM/Ctrl-C shutdown (close code 1001, writer drains
    and commits, threads joined).
- Benchmarks — reproducible harnesses under `bench/` (standalone
  `nosfry-bench` crate, Linux container recipe with CPU pinning,
  `aggregate.py`).

### Changed

- `nosfry`: `SubId` is an `Arc`-backed handle — monitor hits, reader
  chunks, and outbound frames clone a refcount instead of two `Box<str>`
  allocations.
- `nosfry-server` and `nosfry-lmdb`: the shard `conns`/`pending` maps
  and the stored-scan dedup set use `foldhash::fast::RandomState`
  instead of SipHash.
- `nosfry-server`: `channels`, `connection`, and `http` are private
  modules; the public API is `config`, `Server`, `serve`, and
  `ServerError`.
- Renamed the project to nosfry (crates nosfry, nosfry-lmdb,
  nosfry-server; binary nosfry; NOSFRY_CONFIG; nosfry.toml)
- `nosfry-server`: use the system allocator; mimalloc raised memory per
  connection by 40-55% with no throughput gain in the Linux benchmark.
- `nosfry-server`: permessage-deflate runs at compression level 1
  (`CompressionLevel::fast`), matching strfry's `deflateInit2` level;
  query responses no longer pay level-6 compression.
- `nosfry`: `Limits` is now `{ event: EventLimits, req: ReqLimits }` —
  `EventLimits` gained the three rejection time-window fields and the
  new `ReqLimits` holds the REQ limits, so `nosfry-server` config maps
  `[events]` and `[relay.limits]` onto them verbatim with no
  duplicated defaults.
- `nosfry-lmdb`: `WriteOutcome` is no longer `#[non_exhaustive]` and the
  writer reply is `Option<WriteOutcome>` (`None` = the commit failed);
  `OK` mapping is an exhaustive match.
- `nosfry-server`: `ServerError` carries source errors
  (`toml::de::Error`, `std::io::Error`) instead of strings.
- `nosfry-server`: the writer pushes each `OK` straight onto the
  connection's outbound queue — one fewer task wake-up than the old
  writer → oneshot → connection task hop; outcome→message mapping is
  an exhaustive match in the writer itself.

### Fixed

- `nosfry`: subscription ids containing JSON escapes (`\uXXXX`,
  `\"`, ...) now parse for `REQ`/`CLOSE`/`COUNT`/`NEG-*` — a borrowed
  `&str` deserialize rejected any escape sequence even though NIP-01
  allows any string. The verb and the NEG hex payload still only accept
  literal strings.
- `nosfry`: `UnverifiedEvent::verify` checks the canonical-JSON size
  before BIP-340 verification, so an oversized event no longer pays
  for a signature check before rejection.

- `nosfry-server`: a stale `Activate` from a superseded subscription
  generation no longer flushes/activates the re-registered generation
  before its scan's EOSE — pending state is keyed by generation and
  mismatched activations are ignored.
- `nosfry-server`: relay-initiated subscription closes (pending-buffer
  overflow, scan errors) now emit a real `CLOSED` frame and tear the
  subscription down in the session, reader, and shard via the new
  `Out::Closed` / `Session::close_by_relay` path.
- `nosfry-server`: reader threads no longer busy-spin when every queued
  scan is paused above the outbound high-water mark — an all-paused
  pass blocks on `recv_timeout`.
- `nosfry-server`: live fan-out now carries one shared `Arc<Event>` per
  match (`Out::Event`) instead of a per-subscription `String`; the
  connection assembles the `EVENT` frame at write time.
- `nosfry-server`: half-open connections are closed with 1001 after two
  ping intervals without any inbound frame (pings and pongs count).
- `nosfry-server`: OK replies map `WriteOutcome` variants explicitly; an
  unknown future variant is rejected with `error: internal error`
  instead of being silently accepted.
- `nosfry-server`: accepted sockets now get `TCP_NODELAY`; outbound
  frames are batched (up to 64 frames or 256 KiB) into a single socket
  flush; every socket write carries a timeout (the ping interval, or
  1 s for close frames); connection tasks are tracked in a
  `tokio_util::task::TaskTracker` and shutdown waits on them with a
  10 s deadline before joining worker threads off the runtime.
- `nosfry`: `Monitors::remove`/`remove_connection` are O(1) per index
  entry — each `FilterSlot` stores its position in its index list and
  removal `swap_remove`s it (10k-sub churn on `kinds:[1]`: ~324 ms ->
  ~7 ms per 10k removes+inserts).
- `nosfry-server`: the REQ path lost a hop — connections register only
  with their monitor shard, which forwards `Start`/`Cancel`/
  `CancelConn` to the reader over one FIFO channel; `Start` carries
  the shard's authoritative `last_seq`, and a REQ followed by CLOSE
  can no longer start a wasted scan after its cancel.

### Removed

- `nosfry`: `Event::is_protected` (unused; NIP-70 will add what it
  needs when implemented).
- `nosfry`: `UnverifiedEvent` accessors `pubkey`, `tags`, `content`,
  and `sig` (nothing outside tests used them).

- `nosfry`: `Limits::max_filter_limit_count` (no consumer until COUNT
  exists) and `cmp_oldest_first` (unused outside tests).
- `nosfry-server`: the non-standard `max_filters` field of the NIP-11
  `limitation` object; `default_limit` was added instead.
