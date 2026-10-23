#!/usr/bin/env bash
# nosfry pinned to the relay cores, configured like relayBench's strfry setup.
# Usage: nosfry-relay.sh <binary> <port> <work-dir> [variant-toml]
#   The optional variant file is appended to the generated nosfry.toml.
set -euo pipefail
bin="$1"; port="$2"; dir="$3"; variant="${4:-}"
if [ -n "$variant" ] && [ ! -f "$variant" ]; then
    echo "nosfry-relay.sh: variant config '$variant' not found" >&2
    exit 1
fi
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

# Serve restricted read kinds (DMs) like strfry does, so relayBench's
# cross-check compares the same result sets.
[relay.auth]
restricted_read_kinds = []

[relay.rate_limits]
events_per_second = 0
events_burst = 0
reqs_per_second = 0
reqs_burst = 0
# Same for the per-address buckets: every benchmark connection shares
# one IP and would otherwise hit the default 500-REQ burst.
ip_events_per_second = 0
ip_events_burst = 0
ip_reqs_per_second = 0
ip_reqs_burst = 0
CONF
# Deliberately no [relay.compression] table above: a variant file may
# add one, and a duplicate table is a TOML error.
if [ -n "$variant" ]; then
    cat "$variant" >> "$dir/nosfry.toml"
fi
exec taskset -c 0-3 "$bin" relay --config "$dir/nosfry.toml"
