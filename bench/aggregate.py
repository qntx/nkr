#!/usr/bin/env python3
"""Aggregate relayBench results.json files and nkrbench results.jsonl.

Usage: aggregate.py <results-root>
Prints the median and [min, max] across repeated runs for every metric.
"""
import glob
import json
import statistics
import sys
from collections import defaultdict

root = sys.argv[1]


def fmt(values, digits=1):
    values = [v for v in values if v is not None]
    if not values:
        return "n/a"
    med = statistics.median(values)
    if len(values) == 1:
        return f"{med:.{digits}f}"
    return f"{med:.{digits}f} [{min(values):.{digits}f}-{max(values):.{digits}f}]"


def relaybench(prefix):
    files = sorted(glob.glob(f"{root}/{prefix}-r*/*/results.json"))
    if not files:
        return
    runs = [json.load(open(f)) for f in files]
    print(f"\n## relayBench {prefix}: {len(runs)} runs, corpus {runs[0]['corpus']['source']} "
          f"fingerprint {runs[0]['corpus']['fingerprint']}")
    fingerprints = {r["corpus"]["fingerprint"] for r in runs}
    assert len(fingerprints) == 1, f"corpus fingerprints differ: {fingerprints}"
    by_relay = defaultdict(list)
    for run in runs:
        for relay in run["relays"]:
            assert relay["error"] is None, f"{relay['name']} errored: {relay['error']}"
            by_relay[relay["name"]].append(relay)
    names = sorted(by_relay)
    print("\n| metric | " + " | ".join(names) + " |")
    print("|---|" + "---:|" * len(names))
    rows = [
        ("ingest throughput (ev/s)", lambda r: r["throughput"]["eventsPerSec"], 0),
        ("accepted", lambda r: r["throughput"]["accepted"], 0),
        ("OK ack p50 (ms)", lambda r: r["visibility"]["okLatency"]["p50"], 2),
        ("OK ack p99 (ms)", lambda r: r["visibility"]["okLatency"]["p99"], 2),
        ("receipt->queryable p50 (ms)", lambda r: r["visibility"]["visibleLatency"]["p50"], 2),
        ("receipt->queryable p99 (ms)", lambda r: r["visibility"]["visibleLatency"]["p99"], 2),
        ("storage (MiB)", lambda r: r["storageBytes"] / 2**20, 1),
    ]
    for label, getter, digits in rows:
        print(f"| {label} | " + " | ".join(fmt([getter(r) for r in by_relay[n]], digits) for n in names) + " |")
    scenarios = [q["scenario"] for q in by_relay[names[0]][0]["queries"]]
    for metric, key, digits in [
        ("EOSE p50 (ms)", lambda q: q["timeToEose"]["p50"], 2),
        ("first event p50 (ms)", lambda q: q["timeToFirst"]["p50"], 2),
        ("8-conn throughput (ev/s)", lambda q: q["concurrentEventsPerSec"], 0),
    ]:
        print(f"\n| query: {metric} | events | " + " | ".join(names) + " |")
        print("|---|---:|" + "---:|" * len(names))
        for scenario in scenarios:
            cells = []
            events = None
            for n in names:
                values = []
                for relay in by_relay[n]:
                    q = next(q for q in relay["queries"] if q["scenario"] == scenario)
                    events = q["eventsPerRound"]
                    values.append(key(q))
                cells.append(fmt(values, digits))
            print(f"| {scenario} | {events} | " + " | ".join(cells) + " |")


def nkrbench():
    files = sorted(glob.glob(f"{root}/nkrbench*/results.jsonl"))
    rows = [json.loads(line) for f in files for line in open(f) if line.strip()]
    if not rows:
        return
    groups = defaultdict(list)
    for row in rows:
        key = row["scenario"] + ("-deflate" if row.get("deflate") else "")
        if row["scenario"] == "fanout":
            key += f"-{row['mode']}-{row['subs']}subs-{row['events']}ev@{row['rate']:g}/s"
        else:
            key += f"-{row['requested']}"
        groups[(key, row["label"])].append(row)
    print(f"\n## nkrbench: {len(rows)} rows")
    for key in sorted({k for k, _ in groups}):
        print(f"\n### {key}")
        labels = sorted(label for k, label in groups if k == key)
        for label in labels:
            g = groups[(key, label)]
            if g[0]["scenario"] == "fanout":
                print(f"- {label} (n={len(g)}): "
                      f"p50 {fmt([r['latency_ms']['p50'] for r in g], 2)} ms, "
                      f"p99 {fmt([r['latency_ms']['p99'] for r in g], 2)} ms, "
                      f"max {fmt([r['latency_ms']['max'] for r in g], 2)} ms, "
                      f"missing {[r['missing'] for r in g]}, dup {[r['duplicates'] for r in g]}, "
                      f"ok {[r['ok_accepted'] for r in g]}, "
                      f"relay cpu {fmt([r['relay_cpu_secs'] for r in g], 2)} s")
            else:
                print(f"- {label} (n={len(g)}): {g[0].get('mem_metric', 'RSS')} "
                      f"{fmt([r['kib_per_conn'] for r in g], 1)} KiB/conn, "
                      f"opened {[r['opened'] for r in g]}, "
                      f"setup cpu {fmt([r['relay_cpu_secs_setup'] for r in g], 2)} s, "
                      f"setup {fmt([r['setup_secs'] for r in g], 2)} s")


relaybench("rb-synthetic-100k")
relaybench("rb-real")
nkrbench()
