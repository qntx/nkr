#!/usr/bin/env bash
# Soak test: one relay under a mixed long-running workload.
# Usage: run-soak.sh <nosfry-binary-name> [label]
#   The name is a binary under /bench/bin; the label picks the output
#   file <out>/soak-<label>.jsonl (default "soak").
# Env: DURATION (default 1800 s), SAMPLE (default 60 s),
#      PUBLISHERS 8, RATE 200 events/s total, SUBSCRIBERS 200,
#      CHURN_RATE 10 conns/s, SLOW_EVERY 60 s, NEG_EVERY 10 s,
#      VANISH_EVERY 60 s, DEFLATE 0 (1 = every harness connection
#      negotiates permessage-deflate), PORT 17777,
#      RELAY_CPUS 0-3, HARNESS_CPUS 4-9 (a soak can share the box with
#      a benchmark run by moving to its own port and cores).
set -euo pipefail

BENCH=/bench
HARNESS="$BENCH/harness/target/release/nosfry-bench"
OUT="${SOAK_OUT:-$BENCH/results}"
BIN="$1"; LABEL="${2:-${SOAK_LABEL:-soak}}"
DURATION="${DURATION:-1800}"
SAMPLE="${SAMPLE:-60}"
PUBLISHERS="${PUBLISHERS:-8}"
RATE="${RATE:-200}"
SUBSCRIBERS="${SUBSCRIBERS:-200}"
CHURN_RATE="${CHURN_RATE:-10}"
SLOW_EVERY="${SLOW_EVERY:-60}"
NEG_EVERY="${NEG_EVERY:-10}"
VANISH_EVERY="${VANISH_EVERY:-60}"
DEFLATE="${DEFLATE:-0}"
PORT="${PORT:-17777}"
RELAY_CPUS="${RELAY_CPUS:-0-3}"
HARNESS_CPUS="${HARNESS_CPUS:-4-9}"
DIR="$BENCH/work/soak-$LABEL"

mkdir -p "$OUT"
ulimit -n 65536
rm -rf "$DIR"; mkdir -p "$DIR/nosfry-db"

# Same shape as nosfry-relay.sh's config plus `metrics = true` for the
# sampler; everything not listed stays at the default.
cat > "$DIR/nosfry.toml" <<CONF
[db]
path = "$DIR/nosfry-db"

[relay]
bind = "127.0.0.1"
port = $PORT
metrics = true
# The soak opens hundreds of connections from one IP.
max_connections_per_ip = 0

[relay.rate_limits]
events_per_second = 0
events_burst = 0
reqs_per_second = 0
reqs_burst = 0
# Per-address buckets would throttle the single-IP publishers;
# the soak measures growth, not throttling.
ip_events_per_second = 0
ip_events_burst = 0
ip_reqs_per_second = 0
ip_reqs_burst = 0
CONF

taskset -c "$RELAY_CPUS" "$BENCH/bin/$BIN" relay --config "$DIR/nosfry.toml" > "$DIR/relay.log" 2>&1 &
RELAY_PID=$!

stop_relay() {
    kill -TERM "$RELAY_PID" 2>/dev/null || true
    for _ in $(seq 1 300); do
        kill -0 "$RELAY_PID" 2>/dev/null || { wait "$RELAY_PID" 2>/dev/null || true; return 0; }
        sleep 0.1
    done
    kill -KILL "$RELAY_PID" 2>/dev/null || true
    wait "$RELAY_PID" 2>/dev/null || true
}
trap stop_relay EXIT

for _ in $(seq 1 200); do
    if nc -z 127.0.0.1 "$PORT" 2>/dev/null; then break; fi
    kill -0 "$RELAY_PID" 2>/dev/null || { echo "relay died" >&2; tail -20 "$DIR/relay.log" >&2; exit 1; }
    sleep 0.05
done
nc -z 127.0.0.1 "$PORT" 2>/dev/null || { echo "relay did not start" >&2; tail -20 "$DIR/relay.log" >&2; exit 1; }

echo "== soak $LABEL: $DURATION s, sampling every $SAMPLE s" >&2
DEFLATE_FLAG=()
[ "$DEFLATE" = "1" ] && DEFLATE_FLAG=(--deflate)
taskset -c "$HARNESS_CPUS" "$HARNESS" soak \
    --url "ws://127.0.0.1:$PORT" \
    --metrics-url "http://127.0.0.1:$PORT/metrics" \
    --pid "$RELAY_PID" \
    --db-dir "$DIR/nosfry-db" \
    --duration-secs "$DURATION" \
    --sample-secs "$SAMPLE" \
    --publishers "$PUBLISHERS" \
    --rate "$RATE" \
    --subscribers "$SUBSCRIBERS" \
    --churn-rate "$CHURN_RATE" \
    --slow-every-secs "$SLOW_EVERY" \
    --neg-every-secs "$NEG_EVERY" \
    --vanish-every-secs "$VANISH_EVERY" \
    --label "$LABEL" \
    "${DEFLATE_FLAG[@]}" \
    > "$OUT/soak-$LABEL.jsonl" \
    2> "$DIR/harness.log" || true

kill -TERM "$RELAY_PID" 2>/dev/null || true
STATUS=0
for _ in $(seq 1 300); do
    if ! kill -0 "$RELAY_PID" 2>/dev/null; then break; fi
    sleep 0.1
done
wait "$RELAY_PID" 2>/dev/null || STATUS=$?
trap - EXIT
python3 -c "import json; print(json.dumps({'type':'relay_exit','label':'$LABEL','exit_status':$STATUS}))" \
    >> "$OUT/soak-$LABEL.jsonl"
echo "== relay exited with status $STATUS" >&2
