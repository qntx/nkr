#!/usr/bin/env bash
# nostr-rs-relay pinned to the relay cores with the round 7 parity
# config.
# Usage: nostr-rs-relay.sh <binary> <port> <dir> [variant-toml]
#   The optional variant file is appended to the generated TOML.
# Env: RELAY_CPUS (default 0-3), DB_DIR (default <dir>/db — an explicit
#      database directory the wrapper uses but never wipes).
set -euo pipefail
bin="$1"; port="$2"; dir="$3"; variant="${4:-}"
RELAY_CPUS="${RELAY_CPUS:-0-3}"
DB_DIR="${DB_DIR:-$dir/db}"
if [ -n "$variant" ] && [ ! -f "$variant" ]; then
    echo "nostr-rs-relay.sh: variant config '$variant' not found" >&2
    exit 1
fi
mkdir -p "$dir" "$DB_DIR"
cat > "$dir/config.toml" <<CONF
[network]
address = "127.0.0.1"
port = $port

[database]
data_directory = "$DB_DIR"

# [options] reject_future_seconds stays unset = off (0.10.0 allows any
# date by default; the shipped config's 1800 s would skew the corpus).
# No [authorization] pubkey_whitelist: every valid event is accepted.

[limits]
max_event_bytes = 65536
max_ws_message_bytes = 131072
max_ws_frame_bytes = 131072
CONF
if [ -n "$variant" ]; then
    cat "$variant" >> "$dir/config.toml"
fi
exec taskset -c "$RELAY_CPUS" "$bin" --db "$DB_DIR" --config "$dir/config.toml"
