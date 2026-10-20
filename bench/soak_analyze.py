#!/usr/bin/env python3
"""Verdict for a `nosfry-bench soak` JSONL run.

Usage: python3 bench/soak_analyze.py <run.jsonl> [--warmup-secs N]
       [--publishers P] [--subscribers S]

Criteria (fixed before any run was analyzed):
- memory: the least-squares slope of anonymous RSS (RssAnon; VmRSS also
  counts touched LMDB map pages, which are reclaimable page cache bounded
  by the database size) after warm-up, projected over 72 h, stays within
  max(64 MiB, 10% of the mean post-warm-up value);
- database: `data.mdb` in the last third of the run never exceeds the
  middle third's maximum by more than 5%;
- connections: the relay gauge never exceeds publishers + subscribers + 50
  and ends within publishers + subscribers + 20 (slow consumers and churn
  are reaped);
- errors: zero harness errors, zero failed churn queries, zero
  subscriber reconnects, every OK accepted, relay exit status 0.
"""

import argparse
import json
import sys

MIB = 1024


def slope(xs, ys):
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    den = sum((x - mx) ** 2 for x in xs)
    return 0.0 if den == 0 else sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("path")
    ap.add_argument("--warmup-secs", type=float, default=600)
    ap.add_argument("--publishers", type=int, default=8)
    ap.add_argument("--subscribers", type=int, default=200)
    args = ap.parse_args()

    samples, end, exit_status = [], None, None
    with open(args.path) as f:
        for line in f:
            line = line.strip()
            if not line.startswith("{"):
                continue
            rec = json.loads(line)
            kind = rec.get("type")
            if kind == "sample":
                samples.append(rec)
            elif kind == "end":
                end = rec
            elif kind == "relay_exit":
                exit_status = rec["exit_status"]

    post = [s for s in samples if s["t"] >= args.warmup_secs]
    if len(post) < 6:
        sys.exit(f"only {len(post)} post-warm-up samples; need at least 6")
    checks = []

    ts = [s["t"] for s in post]
    rss = [s["rss_anon_kib"] for s in post]
    total_rss = [s["rss_kib"] for s in post]
    mean_rss = sum(rss) / len(rss)
    rss_slope_h = slope(ts, rss) * 3600
    projected = rss_slope_h * 72
    budget = max(64 * MIB, 0.10 * mean_rss)
    checks.append((
        "memory",
        abs(projected) <= budget,
        f"anon mean {mean_rss / MIB:.1f} MiB, min {min(rss) / MIB:.1f}, max {max(rss) / MIB:.1f}, "
        f"total RSS max {max(total_rss) / MIB:.1f} MiB, "
        f"slope {rss_slope_h / MIB:+.2f} MiB/h, 72 h projection {projected / MIB:+.1f} MiB "
        f"(budget {budget / MIB:.1f} MiB)",
    ))

    third = len(post) // 3
    middle = post[third:2 * third]
    last = post[2 * third:]
    db_mid = max(s["db_bytes"] for s in middle)
    db_last = max(s["db_bytes"] for s in last)
    checks.append((
        "database",
        db_last <= db_mid * 1.05,
        f"middle-third max {db_mid / 2**20:.1f} MiB, last-third max {db_last / 2**20:.1f} MiB, "
        f"final {post[-1]['db_bytes'] / 2**20:.1f} MiB",
    ))

    base = args.publishers + args.subscribers
    conns = [s["conns"] for s in samples]
    final = samples[-1]["conns"]
    checks.append((
        "connections",
        max(conns) <= base + 50 and final <= base + 20,
        f"max {max(conns)}, final {final}, baseline {base}",
    ))

    counts = end or samples[-1]
    ok = counts.get("ok", {})
    rejected = {k: v for k, v in ok.items() if not k.startswith("true")}
    failures = {
        "errors": counts.get("errors", 0),
        "churn_failed": counts.get("churn_failed", 0),
        "sub_reconnects": counts.get("sub_reconnects", 0),
    }
    checks.append((
        "errors",
        not any(failures.values()) and not rejected and exit_status == 0,
        f"{failures}, rejected OKs {rejected or 'none'}, relay exit {exit_status}",
    ))

    print(f"run: {samples[-1]['t'] / 3600:.2f} h, {len(samples)} samples, {len(post)} after "
          f"{args.warmup_secs:.0f} s warm-up")
    print(f"load: {sum(ok.values())} OKs, {counts.get('deliveries', 0)} deliveries, "
          f"{counts.get('churn_done', 0)} churn queries, "
          f"{counts.get('slow_spawned', 0)} slow consumers")
    for name, passed, detail in checks:
        print(f"{'PASS' if passed else 'FAIL'} {name}: {detail}")
    sys.exit(0 if all(passed for _, passed, _ in checks) else 1)


if __name__ == "__main__":
    main()
