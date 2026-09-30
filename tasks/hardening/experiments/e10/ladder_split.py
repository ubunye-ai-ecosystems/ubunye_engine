"""E-10: split the E-06 ladder's record hashing into input and output parts.

Reads the JSON lines of scale-ladder run 36673389425 (hardening/real-world 11823f9,
GitHub ubuntu-latest, 4 vCPU) and prints, per backend and size, the medians of the
plain, Ubunye and --lineage wall times, and of the record's hash_seconds for the
input (events) and the outputs (detail, summary).

    python ladder_split.py DIR
"""

from __future__ import annotations

import ast
import glob
import json
import os
import statistics
import sys


def med(xs):
    return round(statistics.median(xs), 2) if xs else None


def main(root):
    out = []
    for f in sorted(glob.glob(os.path.join(root, "*", "*.jsonl"))):
        rows = [json.loads(line) for line in open(f, encoding="utf-8")]
        backend, n = rows[0]["backend"], int(rows[0]["rows"])
        wall = {
            v: [float(r["wall_s"]) for r in rows if r["variant"] == v]
            for v in ("plain", "ubunye", "lineage")
        }
        ins, outs, tot = [], [], []
        for r in rows:
            if r["variant"] != "lineage" or not r.get("record"):
                continue
            rec = ast.literal_eval(r["record"]) if isinstance(r["record"], str) else r["record"]
            ins.append(sum(i.get("hash_seconds") or 0 for i in rec["inputs"]))
            outs.append(sum(o.get("hash_seconds") or 0 for o in rec["outputs"]))
            tot.append(rec.get("hash_seconds") or 0)
        row = {
            "backend": backend,
            "rows": n,
            "plain": med(wall["plain"]),
            "ubunye": med(wall["ubunye"]),
            "lineage": med(wall["lineage"]),
            "hash_in": med(ins),
            "hash_out": med(outs),
            "hash_total": med(tot),
        }
        row["ratio"] = round(row["lineage"] / row["plain"], 2)
        row["ratio_without_input_hash"] = round((row["lineage"] - row["hash_in"]) / row["plain"], 2)
        out.append(row)
        print(json.dumps(row))
    return out


if __name__ == "__main__":
    main(sys.argv[1])
