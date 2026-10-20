# Changelog

All notable changes to `nosfry` are documented in this file.

The format is based on [Keep a Changelog]; this project follows
[Semantic Versioning].

[Keep a Changelog]: https://keepachangelog.com/en/1.1.0/
[Semantic Versioning]: https://semver.org/spec/v2.0.0.html

## [Unreleased]

### Added

- NIP-45 `COUNT`: `["COUNT", <sub>, <filters>…]` counts the stored
  events the filters would return and answers with one
  `["COUNT",<sub>,{"count":n}]` frame. COUNTs share the REQ rate
  buckets, subscription slots, and filter validation; a filter's
  `limit` is ignored and restricted kinds are counted only for
  readers allowed to see them. `relay.limits.max_filter_limit_count`
  (default 1,000,000, `0` disables COUNT) caps the index entries one
  COUNT may examine; reaching it ends the scan early and the answer
  carries `"approximate": true` rather than strfry's `limited`.
  Single-tag-condition queries also get `"hll"`, the 256-register
  HyperLogLog sketch of the counted authors (NIP-45 offset rule).
  NIP-11 advertises 45 while COUNT is enabled.

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
- Per-connection token-bucket rate limits (`[relay.rate_limits]`,
  20 events/s and 20 REQs/s with a burst of 100 each; a `*_per_second`
  of 0 disables that bucket). Over-limit EVENTs are answered `OK false
rate-limited: slow down`, over-limit REQs `CLOSED rate-limited: slow
down`; CLOSE is never charged.
- Shared per-client-address rate limits (`ip_events_per_second`,
  `ip_events_burst`, `ip_reqs_per_second`, `ip_reqs_burst`; defaults
  100/s burst 500, a `*_per_second` of 0 disables the bucket). Every
  connection of one client address — IPv4 per address, IPv6 per /64 —
  draws from the same buckets, and a message is refused when either
  the connection bucket or the shared address bucket is empty, closing
  the `max_connections_per_ip × per-connection-rate` bypass.
- `[relay] max_pending_outbound_bytes_total` (default 1 GiB, `0`
  disables, restart-only): a process-wide budget on the sum of every
  connection's pending outbound bytes. Over budget, stored scans pause
  only on connections already more than one chunk behind; caught-up
  readers keep scanning and live deliveries are unaffected. New gauge
  `nosfry_outbound_pending_bytes` reports the shared total.
- `nosfry_store_errors_total{op}` counter (`write`, `expire`, `scan`)
  for failed store operations.
- Connection caps: `[relay] max_connections` (default 0 = unlimited)
  and `max_connections_per_ip` (default 128) refuse over-cap upgrades
  with `429 Too Many Requests`. IPv4 counts per address, IPv4-mapped
  IPv6 folds to IPv4, other IPv6 groups per /64 prefix.
- Optional Prometheus metrics: `[relay] metrics = true` serves
  unauthenticated `GET /metrics` on the relay port in the text
  exposition format — `nostr_client_messages_total{verb}`,
  `nostr_relay_messages_total{verb}`, `nostr_events_total{kind}`,
  `nosfry_connections`, `nosfry_connections_rejected_total`, and
  `nosfry_rate_limited_total{verb}`, all on fixed label sets from
  lock-free counters.
- NIP-42 authentication (`[relay.auth]`): with `service_url` set, the
  relay challenges a connection lazily — once, the first time it REQs
  a fully restricted filter or publishes a protected event — and a
  valid kind-22242 AUTH event authenticates its pubkey. Up to 8
  distinct pubkeys authenticate per connection (strfry allows one).
- NIP-70 protected events: a `["-"]` tag makes the event rejectable —
  `blocked: event marked as protected` when AUTH is disabled,
  `auth-required:` (with the challenge) when the connection has not
  authenticated, `restricted: must be published by the author` when
  authenticated as a different key. Kind-6/16 reposts whose content
  embeds `["-"]` are blocked outright.
- Restricted read kinds: `restricted_read_kinds` (default `[4, 1059]`)
  are hidden from unauthenticated clients and, with
  `restrict_read_to_involved = true`, from authenticated clients not
  involved as author or `p`-tagged recipient. A REQ whose filters are
  all restricted is refused (`auth-required:` / `restricted:`); mixed
  REQs subscribe and the per-subscription `ReadGate` hides restricted
  events in both stored scans and the live monitor — hidden events
  never consume `limit`.
- The write-policy plugin request gains `authed`, the hex pubkey the
  connection authenticated with (omitted when absent), matching
  strfry's plugin field.
- `[relay.write_policy]`: a strfry-compatible write-policy plugin
  (`plugin`, `timeout_seconds` = 10). Each verified event is sent to
  `/bin/sh -c <plugin>` as one JSON line carrying the raw event, the
  receive time, and the client's IP (`IP4`/`IP6`); the plugin answers
  `accept`, `reject`, or `shadowReject`. A plugin configured as a bare
  path reloads when its mtime changes; any failure rejects the event
  with `error: internal error` and restarts the process for the next
  event. `nosfry import` does not invoke the plugin.
- Configuration hot reload (unix): SIGHUP makes the binary re-read its
  config file and apply it live — a parse error is logged and the
  running configuration is kept. Session limits and connection options
  apply to new connections and new publishes; `max_connections` /
  `max_connections_per_ip` and the write-policy plugin take effect for
  every connection at once. Restart-only keys (`[db]`, `relay.bind`,
  `relay.port`, `relay.nofiles`, `relay.metrics`, `relay.threads`,
  `relay.query_timeslice_budget_microseconds`,
  `relay.limits.max_filter_limit`) keep their startup values and are
  logged as ignored.

### Changed

- `Session::on_text` takes the current unix time in **milliseconds**
  (previously seconds), needed for millisecond-accurate rate buckets,
  and returns `Received { verb, rate_limited }` — the recognized client
  verb even when the message fails validation. `Command::Send` is now
  `Send { verb, text }` carrying the relay frame type.
- `MessageError` variants carry the recognized `ClientVerb`
  (`MessageError::verb()`), and `FilterVerb` is superseded by
  `ClientVerb`.
- `client_ip` returns `IpAddr` (the peer is mandatory now) and
  `ConnectionLimiter::try_acquire` takes `IpAddr` instead of
  `Option<IpAddr>`; a missing `ConnectInfo` in the HTTP handler is an
  internal error (500).
- A write-policy `reject` with an empty or absent `msg` answers
  `blocked: rejected by write policy` instead of strfry's bare empty
  message, because NIP-01 expects a prefixed rejection reason.
- `nosfry::serve` takes a `watch::Receiver<Config>` reload channel; the
  binary sends each config loaded on SIGHUP. Server state readers now
  go through `watch::Receiver<Arc<Live>>` instead of `Arc<Config>` —
  `ConnectionLimiter`'s caps are atomics, and `client_ip` /
  `try_acquire` already take the mandatory `IpAddr`.
- The LMDB format version is now 2 (three new lifecycle databases);
  databases created by 0.1.0 cannot be opened. Since 0.1.0 had no
  `export`, a 0.1.0 database must be re-imported from its source.
- `Store::write` returns `Written { outcomes, deleted }`, listing every
  seq removed inside the same transaction, instead of the bare outcome
  list.
- `Scan::new` takes a `now` argument (an event expiring at or before it
  is hidden and never counted) and its emit callback receives
  `(seq, json)`.
- **BREAKING:** kinds 4 and 1059 are no longer served to
  unauthenticated clients by default (`restricted_read_kinds`); set
  `restricted_read_kinds = []` to keep serving them openly.
- **BREAKING (API):** `Session::new` takes the client address's
  shared `Arc<AddressBuckets>` as a third argument. `Outbound`
  (crate-internal) now carries a shared process-wide pending-byte
  counter.
- **BREAKING (API):** `Session::new` takes the per-connection
  challenge; `Command::Publish` is `{ event, authed }` and
  `Command::Subscribe` carries a `ReadGate`; `Scan::new` and
  `Monitors::insert` take the gate; `Limits` is no longer `Copy`
  (new `auth` field); `Config::limits` is now fallible
  (`ServerError::Auth` on an invalid `relay.auth.service_url`).
- `--config` is a global flag on the `nosfry` binary: it works before
  or after the subcommand, not just on `relay`.
- All logs go to stderr (previously stdout), keeping stdout clean for
  `export` and `scan` output.
- `ServerError::Serve` is renamed to `ServerError::Shutdown`: it now
  wraps the `JoinError` of the worker-shutdown task (it previously
  wrapped `std::io::Error`).

### Fixed

- A reader process killed while holding a read transaction left its
  slot in `lock.mdb` permanently, so LMDB kept that reader's pages
  pinned and `data.mdb` grew without bound. `Store::open` now runs
  `mdb_reader_check` on startup and the relay's 10-second expiry sweep
  reaps stale readers on every tick.
- `nosfry compact` could leave a plausible-looking partial database at
  the destination path when killed mid-copy. It now writes to
  `<dest>.tmp`, fsyncs it, publishes by hard-linking to the final
  name, and fsyncs the directory; a leftover `.tmp` file makes the
  next run fail instead of clobbering it.
- Protected events (`["-"]` tag) were accepted although NIP-70
  requires rejecting them by default; kind-6/16 reposts embedding a
  protected event are now blocked too.
- A refused re-REQ (over the filter/tag/subscription limits,
  rate-limited, or a filter parse error) for an already-open
  subscription id now closes the old subscription before the `CLOSED`,
  so it no longer keeps delivering under an id the client was told is
  closed.
- `Filter::intrinsic_limit` bounded an `authors` + addressable-`kinds`
  filter without a `#d` condition to `authors * kinds`, so
  `{"kinds":[30023],"authors":[pk]}` served one article of fifty. It
  now follows nostr-tools `getFilterLimit`: the `authors * kinds` bound
  applies only when every kind is replaceable, `authors * kinds * #d`
  only when every kind is addressable and `#d` is present, and mixed or
  `#d`-less filters are unbounded.

### Security

- `relay.compression.enabled` now defaults to `false`: yawc bounds the
  compressed frame size, not the inflated one, so a permessage-deflate
  message could decompress hundreds of times past
  `max_websocket_payload_size`. Enable it only for trusted clients.
- `real_ip_header` now trusts the _last_ comma-separated value parsed
  as an `IpAddr` (the entry written by the proxy in front), falling
  back to the socket peer on missing or unparsable values — the first
  value is client-controlled.
- The HTTP listener now enforces a 10-second request-header read
  deadline (`axum::serve` installed no timer, so slowloris connections
  were held indefinitely) via an explicit hyper HTTP/1 accept loop
  with graceful shutdown.
- Inbound pings now flush their pong under the normal write deadline:
  a stalled pong write ends the connection instead of letting yawc's
  `obligated_sends` queue grow.
- Reader scans alternate between new and resumed queues, so a sustained
  stream of new `REQ`s can no longer starve long-running scans.

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
