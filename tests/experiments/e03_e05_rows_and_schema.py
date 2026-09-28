"""E-03 (silent row loss) and E-05 (schema drift), on the pandas backend.

E-03: a join that drops the orders whose customer is unknown. Does the run record show
rows in and rows out, and can the task declare that no row may be lost?

E-05: after a good run, the source changes: a column added, dropped, renamed, or
retyped. For each: does the run fail loudly or write quietly, does the record show
the schema changed, and does `ubunye gate` against the good run catch it?

Usage: python tests/experiments/e03_e05_rows_and_schema.py <ubunye executable>
"""

from __future__ import annotations

import glob
import json
import os
import shutil
import subprocess
import sys

import pandas as pd

UBUNYE = sys.argv[1]
HERE = os.path.dirname(os.path.abspath(__file__))

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        orders, customers = sources["orders"], sources["customers"]
        joined = orders.merge(customers, on="customer_id", how="inner")
        joined["total"] = joined["qty"] * joined["price"]
        return {"enriched": joined}
"""


def config(root: str) -> str:
    root = root.replace("\\", "/")
    return f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    orders:
      format: s3
      path: "{root}/data/orders.parquet"
      file_format: parquet
    customers:
      format: s3
      path: "{root}/data/customers.parquet"
      file_format: parquet
  transform: {{}}
  outputs:
    enriched:
      format: s3
      path: "{root}/out/enriched"
      file_format: parquet
      mode: overwrite
"""


def orders(n: int = 1000) -> pd.DataFrame:
    # One order in ten has a customer the customers table does not know.
    return pd.DataFrame(
        {
            "order_id": range(n),
            "customer_id": [i % 100 if i % 10 else 999 for i in range(n)],
            "qty": [1 + i % 3 for i in range(n)],
            "price": [10.0 + i % 7 for i in range(n)],
        }
    )


def customers() -> pd.DataFrame:
    return pd.DataFrame({"customer_id": range(100), "city": ["jhb", "cpt"] * 50})


def run(work: str, *extra: str):
    cmd = [
        UBUNYE,
        "run",
        "-d",
        work,
        "-u",
        "uc",
        "-p",
        "pkg",
        "-t",
        "enrich",
        "--backend",
        "pandas",
        "--lineage",
        *extra,
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, (proc.stdout + proc.stderr)


def latest_record(work: str) -> dict:
    files = glob.glob(os.path.join(work, ".ubunye", "lineage", "uc", "pkg", "enrich", "*.json"))
    with open(max(files, key=os.path.getmtime), encoding="utf-8") as fh:
        return json.load(fh)


def steps(record: dict) -> dict:
    out = {}
    for side in ("inputs", "outputs"):
        for s in record.get(side) or []:
            out[f"{side[:-1]}:{s.get('name')}"] = {
                "rows": s.get("row_count"),
                "schema": (s.get("schema_hash") or "")[:19],
            }
    return out


def gate(work: str):
    proc = subprocess.run(
        [
            UBUNYE,
            "gate",
            "-d",
            work,
            "-u",
            "uc",
            "-p",
            "pkg",
            "-t",
            "enrich",
            "--baseline",
            "previous",
            "--candidate",
            "latest",
        ],
        capture_output=True,
        text=True,
    )
    lines = [ln for ln in (proc.stdout + proc.stderr).splitlines() if "[" in ln]
    return proc.returncode, lines[:8]


def main() -> None:
    work = os.path.join(HERE, "work_e03_e05")
    shutil.rmtree(work, ignore_errors=True)
    task = os.path.join(work, "uc", "pkg", "enrich")
    os.makedirs(task)
    os.makedirs(os.path.join(work, "data"))
    with open(os.path.join(task, "transformations.py"), "w") as fh:
        fh.write(TRANSFORM)
    with open(os.path.join(task, "config.yaml"), "w") as fh:
        fh.write(config(work))
    customers().to_parquet(os.path.join(work, "data", "customers.parquet"))

    print("== E-03: an inner join drops 100 of 1000 orders")
    orders().to_parquet(os.path.join(work, "data", "orders.parquet"))
    code, out = run(work)
    print("exit", code, "| record steps:", json.dumps(steps(latest_record(work))))

    print("\n== E-05: the orders source changes after a good run")
    drifts = {
        "column added": lambda d: d.assign(channel="web"),
        "column dropped (unused: price kept)": lambda d: d.drop(columns=["qty"]),
        "column renamed": lambda d: d.rename(columns={"price": "unit_price"}),
        "type changed (price to text)": lambda d: d.assign(price=d["price"].astype(str)),
        "type changed (qty int to float)": lambda d: d.assign(qty=d["qty"].astype(float)),
    }
    for label, change in drifts.items():
        orders().to_parquet(os.path.join(work, "data", "orders.parquet"))
        run(work)  # the good baseline run
        change(orders()).to_parquet(os.path.join(work, "data", "orders.parquet"))
        code, out = run(work)
        err = [ln for ln in out.splitlines() if "Error" in ln or "error" in ln][-1:] if code else []
        rec = steps(latest_record(work))
        gcode, glines = gate(work)
        print(f"\n-- {label}")
        print("   run exit", code, "|", (err[0].strip()[:160] if err else "wrote output"))
        print("   record:", json.dumps(rec))
        print("   gate exit", gcode)
        for ln in glines:
            print("     ", ln.strip()[:150])


if __name__ == "__main__":
    main()
