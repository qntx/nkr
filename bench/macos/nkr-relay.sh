#!/usr/bin/env bash
set -euo pipefail
bin="$1"; port="$2"; dir="$3"
mkdir -p "$dir/nkr-db"
cat > "$dir/nkr.toml" <<EOF
[db]
path = "$dir/nkr-db"

[events]
max_event_size = 65536
max_num_tags = 2000
reject_events_older_than_seconds = 3155760000

[relay]
bind = "127.0.0.1"
port = $port
max_websocket_payload_size = 131072
EOF
exec "$bin" relay --config "$dir/nkr.toml"
