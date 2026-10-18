# Changelog

All notable changes to `nkr` are documented in this file.

The format is based on [Keep a Changelog]; this project follows
[Semantic Versioning].

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- `nkr` — sans-IO protocol core (no async, no I/O, no clock, no
  randomness, `forbid(unsafe_code)`, `wasm32-unknown-unknown` clean):
  - `kind`: `Kind`/`KindClass` with NIP-01 range predicates.
  - `event`: strict borrowed `UnverifiedEvent::parse` /
    `check_limits` / `verify` pipeline (all seven fields required,
    duplicate keys rejected, lowercase hex only, strfry `e`/`p` tag
    rule; verify checks the SHA-256 NIP-01 preimage and the BIP-340
    schnorr signature), immutable `Event` with canonical JSON, packed record,
    NIP-70 `is_protected`, and replaceable/addressable `address_hash`;
    `cmp_newest_first`/`supersedes` ordering helpers.
  - `packed`: strfry-compatible `PackedEvent`/`PackedRef` index record
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
- Vendored `qntx/nostr` test vectors under `crates/nkr/tests/vectors`
  with explicit `DIVERGENCES` lists; proptest differential suites
  (filter matching, packed round trip, monitor vs brute force);
  criterion bench `benches/event.rs`.
- `nkr-lmdb` — synchronous LMDB event store (native only; no async, no
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
- `nkr-server` — the `nkr` relay executable (package `nkr-server`,
  `[[bin]] nkr`, `publish = false`):
  - Shared-nothing threads: one writer (group commits + batch
    broadcast), M monitor shards (live-subscription `Monitors` index,
    snapshot authority, pending buffers), N reader threads
    (time-sliced snapshot-bounded `Scan`s); exactly-once historical +
    live delivery via the shard-assigned snapshot.
  - Per-connection `Outbound` queue with an atomic pending-bytes
    budget; slow consumers are closed with 1008.
  - axum HTTP: yawc WebSocket upgrade (permessage-deflate per config),
    NIP-11 document, CORS preflight, `real_ip_header`.
  - `nkr relay --config <path>` CLI (also `NKR_CONFIG`), TOML config
    (`[db]`/`[events]`/`[relay]`, `deny_unknown_fields`, strfry
    defaults, `nkr.example.toml`), tracing-subscriber, mimalloc,
    graceful SIGTERM/Ctrl-C shutdown (close code 1001, writer drains
    and commits, threads joined).

### Fixed

- `nkr-server`: a stale `Activate` from a superseded subscription
  generation no longer flushes/activates the re-registered generation
  before its scan's EOSE — pending state is keyed by generation and
  mismatched activations are ignored.
- `nkr-server`: relay-initiated subscription closes (pending-buffer
  overflow, scan errors) now emit a real `CLOSED` frame and tear the
  subscription down in the session, reader, and shard via the new
  `Out::Closed` / `Session::close_by_relay` path.
- `nkr-server`: reader threads no longer busy-spin when every queued
  scan is paused above the outbound high-water mark — an all-paused
  pass blocks on `recv_timeout`.
- `nkr-server`: live fan-out now carries one shared `Arc<Event>` per
  match (`Out::Event`) instead of a per-subscription `String`; the
  connection assembles the `EVENT` frame at write time.
- `nkr-server`: half-open connections are closed with 1001 after two
  ping intervals without any inbound frame (pings and pongs count).
- `nkr-server`: OK replies map `WriteOutcome` variants explicitly; an
  unknown future variant is rejected with `error: internal error`
  instead of being silently accepted.
