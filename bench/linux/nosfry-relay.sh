#!/usr/bin/env bash
# nosfry pinned to the relay cores, configured like relayBench's strfry setup.
set -euo pipefail
bin="$1"; port="$2"; dir="$3"
mkdir -p "$dir/nosfry-db"
cat > "$dir/nosfry.toml" <<CONF
[db]
path = "$dir/nosfry-db"

[events]
max_event_size = 65536
max_num_tags = 2000
reject_events_older_than_seconds = 3155760000

[relay]
bind = "127.0.0.1"
port = $port
max_websocket_payload_size = 131072
# Benchmarks measure the engine, not the limiter: 1000+ subscribers
# connect from one IP and ingest floods a single connection.
max_connections_per_ip = 0

[relay.rate_limits]
events_per_second = 0
events_burst = 0
reqs_per_second = 0
reqs_burst = 0
CONF
exec taskset -c 0-3 "$bin" relay --config "$dir/nosfry.toml"
