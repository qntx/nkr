#!/usr/bin/env bash
# strfry pinned to the relay cores with the round 7 parity config.
# Usage: strfry.sh <binary> <port> <dir> [variant-conf]
#   The optional variant file is appended to the generated strfry.conf
#   in strfry's own config syntax.
# Env: RELAY_CPUS (default 0-3), DB_DIR (default <dir>/db — an explicit
#      database directory the wrapper uses but never wipes).
set -euo pipefail
bin="$1"; port="$2"; dir="$3"; variant="${4:-}"
RELAY_CPUS="${RELAY_CPUS:-0-3}"
DB_DIR="${DB_DIR:-$dir/db}"
if [ -n "$variant" ] && [ ! -f "$variant" ]; then
    echo "strfry.sh: variant config '$variant' not found" >&2
    exit 1
fi
mkdir -p "$dir" "$DB_DIR"
cat > "$dir/strfry.conf" <<EOF
db = "$DB_DIR"

dbParams {
    mapsize = 137438953472
}

events {
    maxEventSize = 65536
    maxNumTags = 2000
    # Parity with nosfry: the corpus's created_at window must never clip.
    rejectEventsNewerThanSeconds = 3155760000
    rejectEventsOlderThanSeconds = 3155760000
}

relay {
    bind = "127.0.0.1"
    port = $port
    # The container ulimit applies; 0 keeps strfry from setting its own.
    nofiles = 0
    maxWebsocketPayloadSize = 131072
    # Default compression (enabled, sliding window); a variant may
    # override compression.enabled in strfry syntax.
    negentropy {
        maxSyncEvents = 2000000
    }
}
EOF
if [ -n "$variant" ]; then
    cat "$variant" >> "$dir/strfry.conf"
fi
exec taskset -c "$RELAY_CPUS" "$bin" --config "$dir/strfry.conf" relay
