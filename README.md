<!-- markdownlint-disable MD033 MD041 -->

# nosfry

High-performance Nostr relay in Rust, for bare-metal servers and Cloudflare Workers.

Status: 0.1. The native relay serves NIP-01, NIP-09 (event deletion),
NIP-11, NIP-40 (event expiration), NIP-45 (`COUNT`), NIP-62 (request
to vanish), NIP-77 (negentropy sync), and NIP-86 (relay management);
the remaining strfry feature-parity layer (NIP-42, 70, plugins,
metrics) is in progress. Linux and macOS are supported.

- `nosfry-core` — sans-IO protocol core (`wasm32-unknown-unknown` clean)
- `nosfry-lmdb` — synchronous LMDB event store (native only)
- `nosfry` — the relay crate and binary: WebSocket NIP-01 + NIP-11 over
  the store, on a writer/shard/reader thread topology (native only)

## Deployment

Prebuilt binaries for Linux (x86_64, aarch64) and macOS (aarch64) are
attached to every [GitHub release](https://github.com/qntx/nosfry/releases);
`cargo install nosfry --locked` builds from source. The relay listens on
`127.0.0.1:7777` by default; every key in `nosfry.example.toml` carries
its default and a comment. `NOSFRY_CONFIG` points at the config file,
`RUST_LOG` controls tracing, `RUST_LOG_JSON=1` selects JSON logs, and
SIGINT/SIGTERM shut down gracefully.

At startup the relay raises its soft open-file limit to `relay.nofiles`
(default 1,000,000; `0` disables), capped by the hard limit — systemd
`LimitNOFILE=` and `docker --ulimit nofile=` are only needed to lift the
hard limit above its default (systemd/containerd: 524288).

`relay.compression.enabled` (permessage-deflate) is off by default:
yawc bounds the _compressed_ frame size, not the inflated one
([infinitefield/yawc#55]), so a compressed message can decompress far
past `max_websocket_payload_size` — enable it only for trusted
clients.

`relay.rate_limits` throttles each connection with EVENT and REQ token
buckets (default 20/s, burst 100); over-limit EVENTs get `OK false
rate-limited:`, over-limit REQs get `CLOSED rate-limited:`. A
`*_per_second` of 0 disables that bucket. The `ip_*` keys add a second
pair of buckets shared by every connection of one client address
(default 100/s, burst 500 each): a message is refused when either its
connection bucket or the shared address bucket is empty, closing the
"128 connections × 20 events/s" bypass. `relay.max_connections`
caps total WebSocket connections and `relay.max_connections_per_ip`
(default 128) caps them per client IP — IPv6 is grouped per /64; an
over-cap upgrade is refused with `429 Too Many Requests` before the
handshake. `relay.max_pending_outbound_bytes_total` (default 1 GiB,
0 disables, restart-only) bounds the sum of every connection's pending
outbound bytes: over budget, scans pause only on connections already
more than one chunk behind, while caught-up readers keep scanning.

`relay.metrics = true` serves `GET /metrics` in the Prometheus text
exposition format: `nostr_client_messages_total{verb}` counts inbound
messages by recognized verb, `nostr_relay_messages_total{verb}` counts
frames written to sockets, `nostr_events_total{kind}` counts verified
events, `nosfry_connections` reports the live WebSocket count,
`nosfry_outbound_pending_bytes` the process-wide pending outbound
bytes, `nosfry_connections_rejected_total` refused upgrades,
`nosfry_rate_limited_total{verb}` rate-limit refusals, and
`nosfry_store_errors_total{op}` failed store operations
(`write`/`expire`/`scan`). The endpoint is unauthenticated — keep it
behind a reverse proxy or firewall.

`[relay.write_policy]` runs a strfry-compatible write-policy plugin:
plugins written for strfry run unchanged. Every verified event is sent
to `/bin/sh -c <plugin>` as one JSON line
(`{"type":"new","event":...,"receivedAt":...,"sourceType":"IP4"|"IP6","sourceInfo":"<ip>"}`)
and the plugin's `accept`/`reject`/`shadowReject` answer decides
whether the event is stored and broadcast, refused with `OK false`, or
acknowledged but silently dropped. `timeout_seconds` (default 10)
bounds each answer; on timeout or a plugin error the event is rejected
with `error: internal error` and a fresh plugin process serves the next
event. A plugin configured as a bare path is reloaded automatically
when the file's mtime changes.

`[relay] service_url` (default empty) is the relay's canonical
`ws://`/`wss://` address: clients echo it in the AUTH event's `relay`
tag and name it in a NIP-62 request to vanish. When it is set, NIP-11
advertises NIP 42 and the relay challenges clients lazily — only when
they REQ a fully restricted filter or publish a protected event. A connection may
authenticate up to 8 distinct pubkeys. `restricted_read_kinds`
(default `[4, 1059]` — DMs and gift wraps) are served only to
authenticated readers; with `restrict_read_to_involved = true`
(default) only to pubkeys involved as author or `p`-tagged recipient.
A REQ whose filters all list only restricted kinds is refused
(`CLOSED auth-required:` when AUTH is enabled, `CLOSED restricted:`
otherwise); mixed filters still subscribe but hide restricted events,
and hidden events never consume `limit`. Without a `service_url`,
DMs are stored but never served. NIP-70 protected events (a `["-"]`
tag) are rejected unless AUTH is configured and the author is
authenticated; kind-6/16 reposts embedding a protected event are
always blocked. AUTH events (kind 22242) are never stored or
broadcast. The write-policy request gains `authed` — the hex pubkey
the connection authenticated with — after a successful AUTH.

A kind-62 event (NIP-62 request to vanish) whose `relay` tags name
this relay's `service_url` — or hold `ALL_RELAYS`, which applies even
without one — deletes every event the author published with
`created_at` at or before the request (all kinds except 62 itself,
which can never be deleted or replaced), plus kind-1059 gift wraps
`p`-tagged to the author in that window. Deletion runs as background
sweeps on the writer (10,000 events per step, persisting across
restarts), republishing a vanished event is refused with
`blocked: request to vanish`, and the request bypasses the
write-policy plugin as NIP-62 requires. A request naming only other
relays is stored with no local effect.

`["COUNT", <sub>, <filters>…]` (NIP-45) counts the stored events the
same filters would return and answers with one
`["COUNT",<sub>,{"count":n}]` frame, then frees the sub id. COUNTs
share the REQ rate buckets, subscription slots, and filter
validation, take the same snapshot a REQ would (an event is counted
as soon as its `OK` is sent), and run on the same time-sliced reader
threads. A filter's `limit` is ignored — the count is always the full
union size — and restricted kinds are only counted for readers
allowed to see them. `relay.limits.max_filter_limit_count` (default
1 000 000, `0` disables COUNT) caps the index entries one COUNT may
examine; reaching it ends the scan early and the answer carries
`"approximate": true` (where strfry would say `limited`). When the
request is exactly one filter with exactly one tag condition of one
value, the answer also carries `"hll"`, the 256-register HyperLogLog
sketch of the counted authors' pubkeys (the NIP-45 offset rule), for
client-side follower-count combining.

`["NEG-OPEN", <sub>, <filter>, "<msg>"]` (NIP-77) runs a negentropy
sync against the stored events one filter would return: the relay
scans them in record mode (no payload fetches — matching, limits,
ordering, dedup, read-gate, and expiration identical to a REQ),
seals the `(created_at, id)` records into a
`NegentropyStorageVector`, and answers the initial message and every
later `NEG-MSG` with `NEG-MSG` replies until `NEG-CLOSE`. Syncs live
in their own subscription namespace, share the REQ rate buckets and
the `max_subs_per_connection` cap, and are reaped after 60 s without
a `NEG-MSG` (`closed: idle timeout`). `relay.limits.max_sync_events`
(default 1 000 000, `0` disables NEG-OPEN) caps the records one sync
may hold — over it the relay answers `blocked: too many query
results` with the cap as a fourth `NEG-ERR` element — and
`relay.max_sync_memory_bytes` (default 1 GiB, `0` unlimited) is a
process-wide budget for sealed storages at 40 bytes per record, over
which opens answer `blocked: sync capacity exhausted`. Restricted
kinds are never leaked to a sync the reader may not read.
NIP-11 advertises 77 while sync is enabled.

`POST /` with `Content-Type: application/nostr+json+rpc` is the NIP-86
management API (enabled when `[relay.management] admins` is
non-empty). Every call must carry a NIP-98 `Authorization: Nostr
<base64(event)>` header — a kind-27235 event with `method` = `POST`,
a `payload` tag hashing the raw body, a `u` tag matching `service_url`
after host normalization, a fresh `created_at`, and an admin pubkey —
or the answer is 401. Implemented methods: `supportedmethods`,
`banpubkey`/`unbanpubkey`/`listbannedpubkeys`,
`allowpubkey`/`unallowpubkey`/`listallowedpubkeys`,
`banevent`/`unbanevent`/`listbannedevents`,
`allowkind`/`disallowkind`/`listallowedkinds`/`listdisallowedkinds`,
`blockip`/`unblockip`/`listblockedips`, and
`changerelayname`/`changerelaydescription`/`changerelayicon`. State
persists in the database (a restart keeps every ban), enforcement is
an in-memory snapshot: banned authors/events and disallowed kinds are
refused at publish with `blocked: …` (AUTH and relay-scoped NIP-62
requests are exempt — a banned author may still vanish), `banevent`
deletes the stored event in the same transaction, blocked IPs get 403
on new requests (established connections are not cut), and the
info-field changes take precedence in the NIP-11 document. `import`
bypasses moderation as an operator tool.

SIGHUP reloads the configuration file without restarting: the relay
re-reads it, logs an error and keeps the running configuration on a
parse failure, and applies everything except the restart-only keys —
`[db]`, `relay.bind`, `relay.port`, `relay.nofiles`, `relay.metrics`,
`relay.threads`, `relay.query_timeslice_budget_microseconds`,
`relay.limits.max_filter_limit`, `relay.limits.max_filter_limit_count`,
`relay.limits.max_sync_events`, `relay.max_sync_memory_bytes`, and
`relay.max_pending_outbound_bytes_total` — which are logged as
ignored. Session
limits and connection options (`events`, `relay.limits`,
`relay.rate_limits`, payload size, ping interval, outbound budget,
compression, `real_ip_header`, NIP-11, the write-policy plugin) apply
to new connections and new publishes; existing connections keep the
limits they started with, while `max_connections` /
`max_connections_per_ip` and the write-policy decision take effect for
every connection at once. `docker kill -s HUP nosfry` reloads inside a
container.

### systemd

```ini
[Unit]
Description=nosfry Nostr relay
After=network.target

[Service]
User=nosfry
WorkingDirectory=/var/lib/nosfry
ExecStart=/usr/local/bin/nosfry --config /etc/nosfry/nosfry.toml relay
ExecReload=/bin/kill -HUP $MAINPID
# The relay raises its own soft limit; this lifts the hard limit.
LimitNOFILE=1000000
TimeoutStopSec=20
Restart=on-failure

[Install]
WantedBy=multi-user.target
```

### Container image

Multi-arch images are published to `ghcr.io/qntx/nosfry` on every tag.
The database lives in `/data`; always mount a volume there.

```bash
docker run -d --name nosfry \
    -p 7777:7777 \
    -v nosfry-data:/data \
    --ulimit nofile=1000000:1000000 \
    --stop-timeout 20 \
    ghcr.io/qntx/nosfry:latest
```

For many-connection deployments, `--network host` avoids Docker's
bridge NAT. Run administration commands inside the relay container, so
they share its PID namespace (LMDB tracks readers by PID):

```bash
docker exec nosfry /nosfry export > dump.jsonl
docker exec -i nosfry /nosfry import < dump.jsonl
```

## Administration

The binary ships strfry-style administration commands. `--config` is a
global flag, so `nosfry --config nosfry.toml <command>` and
`nosfry <command> --config nosfry.toml` are equivalent. All commands
write logs to stderr; event output goes to stdout.

```bash
nosfry export [--since <unix>] [--until <unix>] [--reverse] > dump.jsonl
nosfry import [--show-rejected] < dump.jsonl
nosfry scan '{"kinds":[1],"limit":10}' [--count]
nosfry delete [--dry-run] '{"kinds":[1]}'
nosfry compact <dest-file>
```

- `export` writes every stored event as one JSON line, ordered by
  `created_at` (newest first with `--reverse`), over a consistent
  snapshot.
- `import` verifies each line's signature and stores verified events in
  batches; malformed, oversized, or invalid lines are rejected and
  counted. Unlike relay ingest there is no `created_at` time-window
  check, so archives of any age import cleanly. Events imported while
  the relay is running are stored and queryable but are not broadcast
  to live subscriptions.
- `scan` runs a `REQ` filter over the whole database in newest-first
  order; `--count` prints the match count only.
- `delete` removes every stored event matching a `REQ` filter and
  prints the deleted count; `--dry-run` prints the count without
  deleting. Already-expired events still match the filter.
- `compact` writes a compacted copy of the database to a new file and
  refuses to overwrite an existing one.

`export`, `scan`, `delete`, and `compact` fail with `no database at
<path>` when `<db.path>/data.mdb` does not exist; `import` creates the
database like `relay` does.

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <https://www.apache.org/licenses/LICENSE-2.0>)
- MIT License ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/licenses/MIT>)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in this project shall be dual-licensed as above, without any additional terms or conditions.

---

<div align="center">

A **[QuantX](https://qntx.org)** open-source project.

<a href="https://qntx.org"><img alt="QuantX" width="369" src="https://raw.githubusercontent.com/qntx/.github/main/profile/qntx.svg" /></a>

Code is law. We write both.

</div>
