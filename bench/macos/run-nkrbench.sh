#!/usr/bin/env bash
# Complementary relay benchmarks (fan-out latency, idle connections).
# Usage: run-nkrbench.sh <out-dir> <relay-name>...
#   relay names: strfry | nkr-fixed | nkr-915020c
# Every scenario runs against a freshly started relay with an empty database.
set -euo pipefail

BENCH=/tmp/nkr-bench
HARNESS="$BENCH/harness/target/release/nkrbench"
OUT="$1"; shift
mkdir -p "$OUT"
ulimit -n 65536

PORT=17777

start_relay() {
    local name="$1" dir="$2"
    rm -rf "$dir"; mkdir -p "$dir"
    case "$name" in
        strfry)
            mkdir -p "$dir/strfry-db"
            cat > "$dir/strfry.conf" <<EOF
db = "$dir/strfry-db"
events {
    maxEventSize = 65536
    rejectEventsOlderThanSeconds = 3155760000
    maxNumTags = 2000
}
relay {
    bind = "127.0.0.1"
    port = $PORT
    nofiles = 0
    maxWebsocketPayloadSize = 131072
}
EOF
            "$BENCH/strfry-1.1.3/strfry" --config "$dir/strfry.conf" relay > "$dir/relay.log" 2>&1 &
            ;;
        nkr-*)
            "$BENCH/nkr-relay.sh" "$BENCH/bin/$name" "$PORT" "$dir" > "$dir/relay.log" 2>&1 &
            ;;
        *) echo "unknown relay $name" >&2; exit 1 ;;
    esac
    RELAY_PID=$!
    for _ in $(seq 1 200); do
        if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then return 0; fi
        sleep 0.05
    done
    echo "$name did not start" >&2; tail -20 "$dir/relay.log" >&2; exit 1
}

stop_relay() {
    kill -TERM "$RELAY_PID" 2>/dev/null || true
    for _ in $(seq 1 100); do
        kill -0 "$RELAY_PID" 2>/dev/null || return 0
        sleep 0.1
    done
    kill -KILL "$RELAY_PID" 2>/dev/null || true
    wait "$RELAY_PID" 2>/dev/null || true
}

# scenario-name | harness arguments (the URL and --pid are appended)
SCENARIOS=(
    "fanout-broadcast-1k|fanout --subs 1000 --events 2000 --rate 500 --mode broadcast"
    "fanout-broadcast-1k-deflate|fanout --subs 1000 --events 2000 --rate 500 --mode broadcast --deflate"
    "fanout-selective-2k|fanout --subs 2000 --events 4000 --rate 1000 --mode selective --authors 200"
    "fanout-broadcast-5k|fanout --subs 5000 --events 500 --rate 100 --mode broadcast"
    "conns-5k|conns --conns 5000 --hold-secs 5"
    "conns-5k-deflate|conns --conns 5000 --hold-secs 5 --deflate"
)

for relay in "$@"; do
    for entry in "${SCENARIOS[@]}"; do
        scenario="${entry%%|*}"
        args="${entry#*|}"
        dir="$BENCH/work/$relay-$scenario"
        start_relay "$relay" "$dir"
        sleep 1
        # Single-connection warm-up REQ before concurrent load (same for every relay).
        "$HARNESS" conns --conns 1 --hold-secs 0 --url "ws://127.0.0.1:$PORT" --pid "$RELAY_PID" > /dev/null
        echo "== $relay / $scenario" >&2
        # shellcheck disable=SC2086
        "$HARNESS" $args --url "ws://127.0.0.1:$PORT" --pid "$RELAY_PID" --label "$relay" \
            | tee -a "$OUT/results.jsonl"
        stop_relay
        cp "$dir/relay.log" "$OUT/$relay-$scenario.log" 2>/dev/null || true
    done
done
