<!-- markdownlint-disable MD033 MD041 -->

# nosfry

High-performance Nostr relay in Rust, for bare-metal servers and Cloudflare Workers.

Status: 0.1. The native relay serves NIP-01, NIP-09 (event deletion),
NIP-11, and NIP-40 (event expiration); the remaining strfry
feature-parity layer (NIP-42, 45, 70, 77, plugins, metrics) is in
progress. Linux and macOS are supported.

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
`*_per_second` of 0 disables that bucket. `relay.max_connections`
caps total WebSocket connections and `relay.max_connections_per_ip`
(default 128) caps them per client IP — IPv6 is grouped per /64; an
over-cap upgrade is refused with `429 Too Many Requests` before the
handshake.

`relay.metrics = true` serves `GET /metrics` in the Prometheus text
exposition format: `nostr_client_messages_total{verb}` counts inbound
messages by recognized verb, `nostr_relay_messages_total{verb}` counts
frames written to sockets, `nostr_events_total{kind}` counts verified
events, `nosfry_connections` reports the live WebSocket count,
`nosfry_connections_rejected_total` refused upgrades, and
`nosfry_rate_limited_total{verb}` rate-limit refusals. The endpoint is
unauthenticated — keep it behind a reverse proxy or firewall.

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

`[relay.auth]` implements NIP-42 authentication and the strfry-style
read restrictions that ride on it. `service_url` (default empty) is
the relay's canonical `ws://`/`wss://` address clients echo in the AUTH
event's `relay` tag; when it is set, NIP-11 advertises NIP 42 and the
relay challenges clients lazily — only when they REQ a fully
restricted filter or publish a protected event. A connection may
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

SIGHUP reloads the configuration file without restarting: the relay
re-reads it, logs an error and keeps the running configuration on a
parse failure, and applies everything except the restart-only keys —
`[db]`, `relay.bind`, `relay.port`, `relay.nofiles`, `relay.metrics`,
`relay.threads`, `relay.query_timeslice_budget_microseconds`, and
`relay.limits.max_filter_limit` — which are logged as ignored. Session
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
