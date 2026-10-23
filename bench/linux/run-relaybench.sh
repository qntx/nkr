#!/usr/bin/env bash
# relayBench (third-party harness) on Linux: the JVM harness runs on cores
# 4-9; the relay wrappers re-pin every relay to cores 0-3.
# Usage: run-relaybench.sh <nosfry-entry>...
#   Each entry is a binary under /bench/bin, or bin+variant to also
#   append /bench/variants/<variant>.toml to the generated config;
#   entries are compared side by side with pristine strfry in every run.
#   The order of the nosfry binaries rotates between runs; strfry always
#   runs first (a relayBench limitation).
set -euo pipefail
OUT=/bench/results
RB=/bench/relaybench/bin/relaybench
W=/bench/relays/nosfry.sh
CORPUS=/bench/corpus/nostr_vitor_startup_data.json.gz
mkdir -p "$OUT"
ulimit -n 65536

NAMES=("$@")
[ "${#NAMES[@]}" -gt 0 ] || { echo "usage: $0 <nosfry-binary-name>..." >&2; exit 1; }

run() {
    local name="$1" shift_by="$2"; shift 2
    local order=("${NAMES[@]:shift_by}" "${NAMES[@]:0:shift_by}")
    local args=()
    for relay in "${order[@]}"; do
        # bin or bin+variant; the variant is a config fragment under
        # /bench/variants appended by relays/nosfry.sh.
        local bin="${relay%%+*}" variant=""
        if [ "$relay" != "$bin" ]; then
            variant=" /bench/variants/${relay#*+}.toml"
        fi
        args+=(--relay "$relay=$W /bench/bin/$bin {port} {dir}$variant")
    done
    taskset -c 4-9 "$RB" --strfry-bin /bench/strfry-pinned.sh "${args[@]}" \
        --no-sync --no-color --work "/bench/work/rb-$name" --out "$OUT/$name" "$@" 2>&1 | tee "$OUT/$name.log"
}

for rep in 1 2 3; do
    run "rb-synthetic-100k-r$rep" $(( (rep - 1) % ${#NAMES[@]} )) --events 100000 --seed 1
done
for rep in 1 2; do
    run "rb-real-r$rep" $(( (rep - 1) % ${#NAMES[@]} )) --corpus "$CORPUS"
done
