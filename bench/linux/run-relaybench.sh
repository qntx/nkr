#!/usr/bin/env bash
# relayBench (third-party harness) on Linux: the JVM harness runs on cores
# 4-9; the relay wrappers re-pin every relay to cores 0-3.
set -euo pipefail
OUT=/bench/results
RB=/bench/relaybench/bin/relaybench
W=/bench/nkr-relay.sh
CORPUS=/bench/corpus/nostr_vitor_startup_data.json.gz
mkdir -p "$OUT" /bench/work/rb
ulimit -n 65536

run() {
    local name="$1" first="$2" second="$3"; shift 3
    taskset -c 4-9 "$RB" --strfry-bin /bench/strfry-pinned.sh \
        --relay "$first=$W /bench/bin/$first {port} {dir}" \
        --relay "$second=$W /bench/bin/$second {port} {dir}" \
        --no-sync --no-color --work /bench/work/rb --out "$OUT/$name" "$@" 2>&1 | tee "$OUT/$name.log"
}

run rb-synthetic-100k-r1 nkr-fixed nkr-next --events 100000 --seed 1
run rb-synthetic-100k-r2 nkr-next nkr-fixed --events 100000 --seed 1
run rb-synthetic-100k-r3 nkr-fixed nkr-next --events 100000 --seed 1
run rb-real-r1 nkr-next nkr-fixed --corpus "$CORPUS"
run rb-real-r2 nkr-fixed nkr-next --corpus "$CORPUS"
