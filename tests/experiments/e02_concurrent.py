"""E-02: the same task and date, run twice at once.

Two runs of one task with the same variables start a moment apart (a manual rerun while
the scheduled run is still going; two schedulers). For an `overwrite` and an `append`
output: does the result equal one run's output, or does one run corrupt, lose, or
silently double the other? Does either run fail, and does it say why?

Usage: python tests/experiments/e02_concurrent.py <ubunye executable> [pairs]
"""

from __future__ import annotations

import glob
import os
import shutil
import subprocess
import sys
import time

import pandas as pd

UBUNYE = sys.argv[1]
PAIRS = int(sys.argv[2]) if len(sys.argv) > 2 else 10
HERE = os.path.dirname(os.path.abspath(__file__))
ROWS = 1_000_000

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Twice(Task):
    def transform(self, sources):
        df = sources["src"].copy()
        df["batch"] = self.config["CONFIG"]["transform"]["params"]["batch"]
        return {"snapshot": df, "events": df}
"""


def config(root: str) -> str:
    root = root.replace("\\", "/")
    return f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{root}/input.parquet"
      file_format: parquet
  transform:
    params:
      batch: "{{{{ dt }}}}"
  outputs:
    snapshot:
      format: s3
      path: "{root}/out/snapshot"
      file_format: parquet
      mode: overwrite
    events:
      format: s3
      path: "{root}/out/events"
      file_format: parquet
      mode: append
"""


def cmd(root: str) -> list:
    return [
        UBUNYE,
        "run",
        "-d",
        root,
        "-u",
        "uc",
        "-p",
        "pkg",
        "-t",
        "t",
        "--backend",
        "pandas",
        "-dt",
        "2",
    ]


def read(path: str):
    try:
        return pd.read_parquet(path), None
    except Exception as exc:  # noqa: BLE001 (report whatever reading fails with)
        return None, f"{type(exc).__name__}: {str(exc)[:120]}"


def main() -> None:
    work = os.path.join(HERE, "work_e02")
    shutil.rmtree(work, ignore_errors=True)
    base = os.path.join(work, "base")
    os.makedirs(os.path.join(base, "uc", "pkg", "t"))
    pd.DataFrame({"id": range(ROWS), "v": [i % 97 for i in range(ROWS)]}).to_parquet(
        os.path.join(base, "input.parquet")
    )
    with open(os.path.join(base, "uc", "pkg", "t", "transformations.py"), "w") as fh:
        fh.write(TRANSFORM)

    summary = {"snapshot ok": 0, "snapshot bad": 0, "events doubled": 0, "a run failed": 0}
    for n in range(PAIRS):
        root = os.path.join(work, f"pair{n:02d}")
        shutil.copytree(base, root)
        with open(os.path.join(root, "uc", "pkg", "t", "config.yaml"), "w") as fh:
            fh.write(config(root))
        # Pre-create the output once, so both runs overwrite an existing target.
        subprocess.run(cmd(root), capture_output=True)
        shutil.rmtree(os.path.join(root, "out", "events"), ignore_errors=True)

        a = subprocess.Popen(cmd(root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        time.sleep(0.05 * n)  # a different overlap each pair
        b = subprocess.Popen(cmd(root), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        out_a, out_b = a.communicate()[0], b.communicate()[0]
        codes = (a.returncode, b.returncode)

        snap, snap_err = read(os.path.join(root, "out", "snapshot"))
        events, ev_err = read(os.path.join(root, "out", "events"))
        snap_rows = None if snap is None else len(snap)
        ev_rows = None if events is None else len(events)
        debris = sorted(os.path.basename(p) for p in glob.glob(os.path.join(root, "out", ".*")))
        failed = [o for o, c in zip((out_a, out_b), codes) if c != 0]
        why = ""
        if failed:
            lines = [ln for ln in failed[0].splitlines() if "[ERROR]" in ln or "Error:" in ln]
            why = (lines[-1] if lines else failed[0].strip().splitlines()[-1])[:160]

        summary["snapshot ok" if snap_rows == ROWS and not snap_err else "snapshot bad"] += 1
        summary["events doubled"] += ev_rows == 2 * ROWS
        summary["a run failed"] += bool(failed)
        print(
            f"pair {n:02d} exits {codes} snapshot {snap_rows or snap_err} "
            f"events {ev_rows or ev_err} debris {debris} {why}"
        )
        shutil.rmtree(root, ignore_errors=True)
    print("\n", summary)


if __name__ == "__main__":
    main()
