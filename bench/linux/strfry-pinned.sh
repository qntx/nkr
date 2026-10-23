#!/usr/bin/env bash
# strfry pinned to the relay cores; relayBench generates strfry.conf
# itself and appends `--config <file> relay` to this command. The round
# 7 wrappers are relays/*.sh — this shim stays only for the relayBench
# call convention. RELAY_CPUS (default 0-3) matches the wrappers.
exec taskset -c "${RELAY_CPUS:-0-3}" /bench/strfry-1.1.3/strfry "$@"
