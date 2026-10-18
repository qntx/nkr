#!/usr/bin/env bash
# Complementary relay benchmarks on Linux with CPU isolation:
# the relay runs on cores 0-3, the harness on cores 4-9.
# Usage: run-nkrbench.sh <out-dir> <reps>
# Every scenario runs against a freshly started relay with an empty
# database; the relay order rotates on every repetition.
set -euo pipefail

BENCH=/bench
HARNESS="$BENCH/harness/target/release/nkrbench"
OUT="$1"; REPS="$2"
mkdir -p "$OUT"
ulimit -n 65536

PORT=17777
RELAYS=(strfry nkr-fixed nkr-next)

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
            "$BENCH/strfry-pinned.sh" --config "$dir/strfry.conf" relay > "$dir/relay.log" 2>&1 &
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
        kill -0 "$RELAY_PID" 2>/dev/null || { wait "$RELAY_PID" 2>/dev/null || true; return 0; }
        sleep 0.1
    done
    kill -KILL "$RELAY_PID" 2>/dev/null || true
    wait "$RELAY_PID" 2>/dev/null || true
}

harness() {
    taskset -c 4-9 "$HARNESS" "$@"
}

# scenario-name | harness arguments (URL, --pid and --label are appended).
# Rates are chosen to stay below saturation of both the relay and the
# harness, so latencies measure the relay rather than queueing collapse.
SCENARIOS=(
    "fanout-broadcast-1k|fanout --subs 1000 --events 1000 --rate 100 --mode broadcast"
    "fanout-broadcast-1k-deflate|fanout --subs 1000 --events 1000 --rate 100 --mode broadcast --deflate"
    "fanout-broadcast-5k|fanout --subs 5000 --events 200 --rate 20 --mode broadcast"
    "fanout-selective-2k|fanout --subs 2000 --events 4000 --rate 500 --mode selective --authors 200"
    "conns-5k|conns --conns 5000 --hold-secs 10"
    "conns-5k-deflate|conns --conns 5000 --hold-secs 10 --deflate"
)

for rep in $(seq 1 "$REPS"); do
    # Rotate the relay order so no relay always runs first or last.
    shift_by=$(( (rep - 1) % ${#RELAYS[@]} ))
    order=("${RELAYS[@]:shift_by}" "${RELAYS[@]:0:shift_by}")
    for entry in "${SCENARIOS[@]}"; do
        scenario="${entry%%|*}"
        args="${entry#*|}"
        for relay in "${order[@]}"; do
            dir="$BENCH/work/$relay-$scenario"
            start_relay "$relay" "$dir"
            sleep 1
            # Single-connection warm-up REQ before concurrent load (same for every relay).
            harness conns --conns 1 --hold-secs 0 --url "ws://127.0.0.1:$PORT" --pid "$RELAY_PID" > /dev/null
            echo "== rep $rep / $relay / $scenario" >&2
            # shellcheck disable=SC2086  # args is a deliberate word list
            harness $args --url "ws://127.0.0.1:$PORT" --pid "$RELAY_PID" --label "$relay" \
                | python3 -c "import json,sys; d=json.loads(sys.stdin.read()); d['rep']=$rep; print(json.dumps(d))" \
                | tee -a "$OUT/results.jsonl"
            stop_relay
            cp "$dir/relay.log" "$OUT/rep$rep-$relay-$scenario.log" 2>/dev/null || true
        done
    done
done
