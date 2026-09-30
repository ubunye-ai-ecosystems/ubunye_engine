"""F-042: each step of the E-06 pandas transform on plain and on Arrow backed frames.

python e06_pandas_ops.py DATA_DIR
"""

import statistics
import sys
import time

import pandas as pd

from ubunye.adapters import pandas_io

data = sys.argv[1]


def t(label, fn, n=3):
    xs = []
    for _ in range(n):
        t0 = time.perf_counter()
        r = fn()
        xs.append(time.perf_counter() - t0)
    print(f"{label:<40} {statistics.median(xs):6.3f} s")
    return r


for kind, rd in (
    ("numpy (plain)", lambda p: pd.read_parquet(p)),
    ("arrow (ubunye)", lambda p: pandas_io.read_frame("parquet", p).native),
):
    ev = rd(f"{data}/events.parquet")
    rg = rd(f"{data}/regions.parquet")
    print(kind, dict(ev.dtypes.astype(str)))
    f = t(f"{kind} filter", lambda: ev[ev["qty"] > 0].copy())

    def derive():
        g = f.copy()
        g["revenue"] = g["amount"] * g["qty"]
        g["big"] = g["revenue"] > 500000
        return g

    g = t(f"{kind} derive", derive)
    d = t(f"{kind} merge", lambda: g.merge(rg, on="region", how="inner"))
    t(
        f"{kind} groupby",
        lambda: d.groupby(["region_name", "cat"], as_index=False).agg(
            n=("id", "size"), revenue=("revenue", "sum"), qty=("qty", "sum")
        ),
    )
