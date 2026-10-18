#!/usr/bin/env bash
# strfry pinned to the relay cores; relayBench appends `--config <file> relay`.
exec taskset -c 0-3 /bench/strfry-1.1.3/strfry "$@"
