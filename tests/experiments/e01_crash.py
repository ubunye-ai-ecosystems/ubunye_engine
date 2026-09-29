"""E-01: kill a run at a random moment, rerun it, compare with a clean run.

Hard kill (TerminateProcess on Windows, SIGKILL on Linux): what a power cut does to the
process. The operating system's own write cache survives it, so this tests the engine's
write protocol, not the disk.

Task: one input, two outputs: `snapshot` (overwrite) and `events` (append, one batch
per dt). Each trial starts from the same state (dt=1 already written once), kills a
dt=2 run, then reruns dt=2 to completion. Correct: snapshot equals a clean run; events
holds dt=1 once and dt=2 once; no run record claims success for a killed run.
"""

from __future__ import annotations

import glob
import json
import os
import random
import shutil
import subprocess
import sys
import time

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
UBUNYE = sys.argv[1]  # path to the ubunye executable under test
TRIALS = int(sys.argv[2]) if len(sys.argv) > 2 else 40
ROWS = 1_000_000
EVENTS_MODE = os.environ.get("EVENTS_MODE", "      mode: append\n")

TRANSFORM = """\
from ubunye.core.interfaces import Task


class Crash(Task):
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
""" + EVENTS_MODE


def make_base(base: str) -> None:
    os.makedirs(os.path.join(base, "uc", "pkg", "t"))
    pd.DataFrame(
        {"id": range(ROWS), "v": [i % 97 for i in range(ROWS)], "s": ["x" * 20] * ROWS}
    ).to_parquet(os.path.join(base, "input.parquet"))
    task = os.path.join(base, "uc", "pkg", "t")
    with open(os.path.join(task, "transformations.py"), "w") as fh:
        fh.write(TRANSFORM)


def cmd(root: str, dt: str) -> list:
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
        "--lineage",
        "-dt",
        dt,
    ]


def write_config(root: str) -> None:
    with open(os.path.join(root, "uc", "pkg", "t", "config.yaml"), "w") as fh:
        fh.write(config(root))


def run(root: str, dt: str, *, finished_ok: bool = False) -> float:
    """Run to completion. ``finished_ok``: a refusal because the killed run had in fact
    finished the batch (F-031) is the right answer, not a failure (returns -1)."""
    t0 = time.perf_counter()
    out = subprocess.run(cmd(root, dt), capture_output=True, text=True)
    if out.returncode != 0:
        if finished_ok and "already finished this batch" in out.stderr:
            return -1.0
        raise RuntimeError(out.stdout[-2000:] + out.stderr[-2000:])
    return time.perf_counter() - t0


def kill_run(root: str, dt: str, after: float) -> int:
    proc = subprocess.Popen(cmd(root, dt), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        proc.wait(timeout=after)
        return proc.returncode  # finished before the kill
    except subprocess.TimeoutExpired:
        if os.name == "nt":  # the whole tree: ubunye.exe launches python
            subprocess.run(["taskkill", "/F", "/T", "/PID", str(proc.pid)], capture_output=True)
        else:
            proc.kill()
        proc.wait()
        return -9


def read(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    return pd.read_parquet(path)


def records(root: str) -> list:
    out = []
    for f in glob.glob(os.path.join(root, ".ubunye", "lineage", "**", "*.json"), recursive=True):
        with open(f, encoding="utf-8") as fh:
            doc = json.load(fh)
        out.append((doc.get("status"), doc.get("variables", {}).get("dt")))
    return out


def debris(root: str) -> list:
    return sorted(os.path.basename(p) for p in glob.glob(os.path.join(root, "out", ".*")))


def main() -> None:
    work = os.path.join(HERE, "work")
    shutil.rmtree(work, ignore_errors=True)
    base = os.path.join(work, "base")
    make_base(base)
    write_config(base)
    t_clean = run(base, "1")  # the starting state: dt=1 written once
    state = os.path.join(work, "state")
    shutil.copytree(base, state)

    clean = os.path.join(work, "clean")
    shutil.copytree(state, clean)
    write_config(clean)
    t2 = run(clean, "2")
    want_snapshot = (
        read(os.path.join(clean, "out", "snapshot")).sort_values("id").reset_index(drop=True)
    )
    print(f"clean run: dt=1 {t_clean:.2f}s, dt=2 {t2:.2f}s")

    rng = random.Random(7)
    results = []
    for n in range(TRIALS):
        root = os.path.join(work, f"trial{n:02d}")
        shutil.copytree(state, root)
        write_config(root)
        after = rng.uniform(0.15 * t2, 1.05 * t2)
        code = kill_run(root, "2", after)
        after_kill = {
            "snapshot_rows": len(read(os.path.join(root, "out", "snapshot"))),
            "events_by_batch": read(os.path.join(root, "out", "events"))
            .get("batch", pd.Series(dtype=str))
            .value_counts()
            .to_dict(),
            "debris": debris(root),
            "records": records(root),
        }
        refused = run(root, "2", finished_ok=True) < 0
        snap = read(os.path.join(root, "out", "snapshot")).sort_values("id").reset_index(drop=True)
        events = read(os.path.join(root, "out", "events"))
        by_batch = events["batch"].value_counts().to_dict()
        result = {
            "trial": n,
            "kill_after_s": round(after, 2),
            "killed": code == -9,
            "rerun_refused_as_finished": refused,
            "after_kill": after_kill,
            "snapshot_ok": snap.equals(want_snapshot),
            "events_by_batch": by_batch,
            "events_ok": by_batch == {"1": ROWS, "2": ROWS},
            "debris_after_rerun": debris(root),
            "records_after_rerun": records(root),
        }
        results.append(result)
        flag = "OK " if result["snapshot_ok"] and result["events_ok"] else "BAD"
        print(flag, json.dumps(result, default=str)[:400])
        shutil.rmtree(root, ignore_errors=True)

    with open(os.path.join(HERE, "results.json"), "w") as fh:
        json.dump(results, fh, indent=1, default=str)
    killed = [r for r in results if r["killed"]]
    print("\ntrials", len(results), "killed", len(killed))
    print("snapshot wrong after rerun:", sum(not r["snapshot_ok"] for r in results))
    print("events wrong after rerun:", sum(not r["events_ok"] for r in results))
    print(
        "snapshot missing right after kill:",
        sum(r["after_kill"]["snapshot_rows"] == 0 for r in killed),
    )
    print("debris left after rerun:", sum(bool(r["debris_after_rerun"]) for r in results))
    print(
        "running records left:",
        sum(any(s == "running" for s, _ in r["records_after_rerun"]) for r in results),
    )


if __name__ == "__main__":
    main()
