"""E-10: the tables in E-10-native-row-hash.md, from bench-results.jsonl and the E-06 runs.

python summarise.py DIR
"""

from __future__ import annotations

import ast
import json
import os
import statistics
import sys


def load(path):
    return [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]


def bench_tables(rows):
    by = {(r["mode"], r["table"], r["gen_rows"]): r for r in rows if "error" not in r}
    errors = [r for r in rows if "error" in r]
    modes = []
    for r in rows:
        if r["mode"] not in modes:
            modes.append(r["mode"])
    cols = [
        ("events", 1_000_000),
        ("detail", 1_000_000),
        ("events", 5_000_000),
        ("detail", 5_000_000),
    ]
    print("| mode | " + " | ".join(f"{t} {n // 10**6}M s (MB)" for t, n in cols) + " |")
    print("|---" * (len(cols) + 1) + "|")
    for m in modes:
        cells = []
        for t, n in cols:
            r = by.get((m, t, n))
            cells.append(f"{r['median_s']:.2f} ({r['peak_mb']:.0f})" if r else "")
        print(f"| {m} | " + " | ".join(cells) + " |")
    digests = {}
    for r in rows:
        if r.get("digest"):
            digests.setdefault((r["table"], r["gen_rows"]), set()).add(r["digest"])
    print()
    for k, v in digests.items():
        print(k, "distinct digests:", len(v), sorted(v)[0][:20])
    print("errors:", len(errors))


def e06(path):
    rows = load(path)
    out = {}
    for r in rows:
        key = (int(r["rows"]), r["variant"])
        out.setdefault(key, {"wall": [], "hash": [], "peak": [], "digests": set()})
        out[key]["wall"].append(float(r["wall_s"]))
        pk = r["peak_rss_mb"]
        pk = ast.literal_eval(pk) if isinstance(pk, str) else pk
        out[key]["peak"].append(pk["tree"])
        rec = r.get("record")
        if rec:
            rec = ast.literal_eval(rec) if isinstance(rec, str) else rec
            out[key]["hash"].append(rec.get("hash_seconds") or 0)
            for o in rec["outputs"] + rec["inputs"]:
                out[key]["digests"].add((o["name"], o["data_hash"][:20]))
    for k, v in sorted(out.items()):
        print(
            k,
            "wall",
            round(statistics.median(v["wall"]), 2),
            v["wall"],
            "hash",
            round(statistics.median(v["hash"]), 2) if v["hash"] else None,
            "peak",
            round(statistics.median(v["peak"])),
            sorted(v["digests"]),
        )
    return out


if __name__ == "__main__":
    d = sys.argv[1]
    bench_tables(load(os.path.join(d, "bench-results.jsonl")))
    for name in ("e06-devbox-base.jsonl", "e06-devbox-native.jsonl"):
        p = os.path.join(d, name)
        if os.path.exists(p):
            print("\n#", name)
            e06(p)
