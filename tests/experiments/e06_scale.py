"""E-06: the scale ladder. Ubunye against the same job written plain, at growing sizes.

    python e06_scale.py --backend pandas --rows 1000000 --repeats 3 --work DIR --out FILE

For one backend and one size: generate the data once (seeded, so every machine gets
the same rows), then run three variants ``--repeats`` times, interleaved, each in a
fresh process:

* ``plain``: ``e06_logic.py`` on its own, nothing of Ubunye imported;
* ``ubunye``: ``ubunye run`` of the same logic, no ``--lineage``;
* ``lineage``: the same with ``--lineage``.

Each run records its wall time, the peak memory of its process tree (the Python
process and, on Spark, the JVM, sampled every 50 ms), its exit, and on Spark the
jobs it ran from Spark's own event log (how long each took, the bytes it read, and
the bytes its tasks sent back to the driver). A ``--lineage`` run also gives its run
record: step timings and ``hash_seconds``. The outputs of every run are checked
(row counts), and the first run of each variant is hashed in full, so the three
variants are known to write the same rows. One JSON line per run goes to ``--out``.

``--summary FILE...`` turns result files into the median and spread table.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import shutil
import statistics
import subprocess
import sys
import sysconfig
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
USECASE, PACKAGE, TASK = "uc", "pkg", "job"
VARIANTS = ("plain", "ubunye", "lineage")
CHUNK = 1_000_000
# The console script a user runs (``python -m ubunye`` is the cloud entry point).
UBUNYE = shutil.which("ubunye", path=sysconfig.get_path("scripts")) or "ubunye"

TRANSFORM = """\
from ubunye.core.interfaces import Task

from e06_logic import {fn}


class Job(Task):
    def transform(self, sources):
        return {fn}(sources["events"], sources["regions"])
"""


def config(root: Path) -> str:
    r = root.as_posix()
    return f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    events:
      format: s3
      path: "{r}/data/events.parquet"
      file_format: parquet
    regions:
      format: s3
      path: "{r}/data/regions.parquet"
      file_format: parquet
  transform: {{}}
  outputs:
    detail:
      format: s3
      path: "{r}/out/detail"
      file_format: parquet
      mode: append
    summary:
      format: s3
      path: "{r}/out/summary"
      file_format: parquet
      mode: overwrite
"""


# --------------------------------------------------------------------------- #
# Data
# --------------------------------------------------------------------------- #


def generate(data: Path, rows: int) -> float:
    """``events`` (``rows`` rows, in 1M row groups) and ``regions`` (1,000 rows).

    Each million rows comes from its own seed, so a smaller size is a prefix of a
    larger one and any machine makes the same bytes of data.
    """
    import numpy as np
    import pyarrow as pa
    import pyarrow.parquet as pq

    t0 = time.perf_counter()
    data.mkdir(parents=True, exist_ok=True)
    cats = pa.array([f"cat_{i:02d}" for i in range(20)])
    schema = pa.schema(
        [
            ("id", pa.int64()),
            ("region", pa.int64()),
            ("cat", pa.string()),
            ("amount", pa.int64()),
            ("qty", pa.int64()),
        ]
    )
    with pq.ParquetWriter(data / "events.parquet", schema) as w:
        for start in range(0, rows, CHUNK):
            n = min(CHUNK, rows - start)
            rng = np.random.default_rng(606 + start // CHUNK)
            table = pa.table(
                {
                    "id": np.arange(start, start + n, dtype=np.int64),
                    "region": rng.integers(0, 1000, n, dtype=np.int64),
                    "cat": cats.take(pa.array(rng.integers(0, 20, n))),
                    "amount": rng.integers(1, 100_000, n, dtype=np.int64),
                    "qty": rng.integers(-2, 20, n, dtype=np.int64),
                },
                schema=schema,
            )
            w.write_table(table, row_group_size=CHUNK)
    regions = pa.table(
        {
            "region": np.arange(900, dtype=np.int64),
            "region_name": pa.array([f"region_{i:03d}" for i in range(900)]),
            "tier": np.arange(900, dtype=np.int64) % 3,
        }
    )
    pq.write_table(regions, data / "regions.parquet")
    return time.perf_counter() - t0


def make_task(root: Path, backend: str) -> None:
    task = root / USECASE / PACKAGE / TASK
    task.mkdir(parents=True, exist_ok=True)
    fn = "pandas_job" if backend == "pandas" else "spark_job"
    (task / "transformations.py").write_text(TRANSFORM.format(fn=fn))
    (task / "config.yaml").write_text(config(root))
    shutil.copy(HERE / "e06_logic.py", task / "e06_logic.py")


# --------------------------------------------------------------------------- #
# One run
# --------------------------------------------------------------------------- #


def command(variant: str, backend: str, root: Path) -> list:
    if variant == "plain":
        return [sys.executable, str(HERE / "e06_logic.py"), backend, str(root)]
    cmd = [UBUNYE, "run", "-d", str(root), "-u", USECASE]
    cmd += ["-p", PACKAGE, "-t", TASK, "--backend", backend, "-dt", "1"]
    return cmd + (["--lineage"] if variant == "lineage" else [])


def watch(cmd: list, env: dict, cwd: Path, timeout: float) -> dict:
    """Run ``cmd``; wall time and peak resident memory of its tree, by process kind."""
    import psutil

    t0 = time.perf_counter()
    proc = subprocess.Popen(
        cmd, env=env, cwd=cwd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    peak = {"python": 0, "java": 0, "tree": 0}
    import threading

    out: dict = {}

    def drain() -> None:
        out["stdout"], out["stderr"] = proc.communicate()

    reader = threading.Thread(target=drain, daemon=True)
    reader.start()
    try:
        top = psutil.Process(proc.pid)
    except psutil.Error:
        top = None
    timed_out = False
    while reader.is_alive():
        if top is not None:
            py = java = 0
            try:
                procs = [top] + top.children(recursive=True)
            except psutil.Error:
                procs = []
            for p in procs:
                try:
                    rss = p.memory_info().rss
                    name = p.name().lower()
                except psutil.Error:
                    continue
                if "java" in name:
                    java += rss
                else:
                    py += rss
            peak["python"] = max(peak["python"], py)
            peak["java"] = max(peak["java"], java)
            peak["tree"] = max(peak["tree"], py + java)
        if time.perf_counter() - t0 > timeout:
            timed_out = True
            for p in [top] + (top.children(recursive=True) if top else []):
                try:
                    p.kill()
                except Exception:
                    pass
            break
        reader.join(0.05)
    reader.join()
    wall = time.perf_counter() - t0
    return {
        "wall_s": round(wall, 3),
        "rc": proc.returncode,
        "timed_out": timed_out,
        "peak_rss_mb": {k: round(v / 2**20, 1) for k, v in peak.items()},
        "stdout_tail": (out.get("stdout") or "")[-1500:],
        "stderr_tail": (out.get("stderr") or "")[-3000:],
    }


def spark_env(env: dict, events: Path, driver_memory: str) -> dict:
    args = [
        "--master local[*]",
        f"--driver-memory {driver_memory}",
        "--conf spark.ui.enabled=false",
        "--conf spark.eventLog.enabled=true",
        "--conf spark.eventLog.compress=false",
        "--conf spark.eventLog.rolling.enabled=false",
        f"--conf spark.eventLog.dir={events.as_uri()}",
        "pyspark-shell",
    ]
    py = sys.executable
    return dict(
        env, PYSPARK_SUBMIT_ARGS=" ".join(args), PYSPARK_PYTHON=py, PYSPARK_DRIVER_PYTHON=py
    )


def short_site(site: str) -> str:
    head, _, where = site.partition(" at ")
    return f"{head} at {os.path.basename(where.replace(chr(92), '/'))}" if where else site


def event_log(events: Path) -> dict:
    """What Spark did, from its own event log: jobs, time, bytes read, bytes to driver."""
    files = [
        p
        for p in events.rglob("*")
        if p.is_file() and not p.name.startswith((".", "appstatus")) and p.suffix != ".crc"
    ]
    if not files:
        return {}
    stage_job: dict = {}
    jobs: dict = {}
    with open(files[0], encoding="utf-8") as fh:
        for line in fh:
            ev = json.loads(line)
            kind = ev.get("Event")
            if kind == "SparkListenerJobStart":
                jid = ev["Job ID"]
                props = ev.get("Properties") or {}
                jobs[jid] = {
                    "job": jid,
                    # "collect at /long/path/content_hash.py:75" -> "collect at content_hash.py:75"
                    "call_site": short_site(props.get("callSite.short", "")),
                    "start": ev.get("Submission Time"),
                    "tasks": 0,
                    "result_bytes": 0,
                    "read_bytes": 0,
                    "read_records": 0,
                }
                for sid in ev.get("Stage IDs", []):
                    stage_job[sid] = jid
            elif kind == "SparkListenerJobEnd":
                j = jobs.get(ev["Job ID"])
                if j is not None and j["start"] is not None:
                    j["ms"] = ev.get("Completion Time", 0) - j["start"]
            elif kind == "SparkListenerTaskEnd":
                j = jobs.get(stage_job.get(ev.get("Stage ID")))
                m = ev.get("Task Metrics") or {}
                if j is not None:
                    j["tasks"] += 1
                    j["result_bytes"] += int(m.get("Result Size", 0) or 0)
                    inp = m.get("Input Metrics") or {}
                    j["read_bytes"] += int(inp.get("Bytes Read", 0) or 0)
                    j["read_records"] += int(inp.get("Records Read", 0) or 0)
    listed = sorted(jobs.values(), key=lambda j: j["job"])
    for j in listed:
        j.pop("start", None)
    return {
        "jobs": listed,
        "n_jobs": len(listed),
        "result_bytes": sum(j["result_bytes"] for j in listed),
        "read_records": sum(j["read_records"] for j in listed),
        "max_job_result_bytes": max((j["result_bytes"] for j in listed), default=0),
    }


def run_record(root: Path) -> dict:
    found = glob.glob(str(root / ".ubunye" / "lineage" / "**" / "*.json"), recursive=True)
    records = []
    for path in found:
        try:
            with open(path, encoding="utf-8") as fh:
                rec = json.load(fh)
        except Exception:
            continue
        if isinstance(rec, dict) and "run_id" in rec:
            records.append(rec)
    if not records:
        return {}
    rec = records[-1]

    def step(s: dict) -> dict:
        keep = ("name", "row_count", "data_hash", "hash_seconds", "hash_reused_from")
        return {k: s.get(k) for k in keep + ("hash_error",) if s.get(k) is not None}

    return {
        "status": rec.get("status"),
        "duration_sec": rec.get("duration_sec"),
        "timings": rec.get("timings"),
        "inputs": [step(s) for s in rec.get("inputs") or []],
        "outputs": [step(s) for s in rec.get("outputs") or []],
        "hash_seconds": round(
            sum(
                float(s.get("hash_seconds") or 0)
                for s in (rec.get("inputs") or []) + (rec.get("outputs") or [])
            ),
            3,
        ),
    }


def check_outputs(root: Path, deep: bool) -> dict:
    """Row counts of both outputs; with ``deep``, the ``rows-v1`` hash of each too."""
    import pyarrow.dataset as ds

    got = {}
    for name in ("detail", "summary"):
        where = root / "out" / name
        if not where.exists():
            got[name] = {"rows": None}
            continue
        d = ds.dataset(str(where), format="parquet")
        got[name] = {"rows": d.count_rows()}
        if deep:
            from ubunye.lineage.content_hash import fingerprint_arrow

            t0 = time.perf_counter()
            got[name]["data_hash"] = fingerprint_arrow(d.to_table()).data_hash
            got[name]["check_s"] = round(time.perf_counter() - t0, 1)
    return got


# --------------------------------------------------------------------------- #
# The ladder for one backend and size
# --------------------------------------------------------------------------- #


def ladder(args: argparse.Namespace) -> None:
    work = Path(args.work).resolve()
    root = work / f"{args.backend}-{args.rows}"
    data = root / "data"
    if not (data / "events.parquet").exists():
        gen_s = generate(data, args.rows)
        print(f"generated {args.rows:,} rows in {gen_s:.1f} s", flush=True)
    make_task(root, args.backend)
    base_env = dict(os.environ, UBUNYE_TELEMETRY="0", PYTHONUNBUFFERED="1")
    base_env["PYTHONPATH"] = os.pathsep.join(
        [str(HERE)] + ([base_env["PYTHONPATH"]] if base_env.get("PYTHONPATH") else [])
    )
    variants = [v for v in VARIANTS if v in args.variants.split(",")]
    hashed = set()
    for rep in range(1, args.repeats + 1):
        for variant in variants:
            for stale in (root / "out", root / ".ubunye", root / "events"):
                shutil.rmtree(stale, ignore_errors=True)
            env = base_env
            if args.backend == "spark":
                (root / "events").mkdir()
                env = spark_env(base_env, root / "events", args.driver_memory)
            res = watch(command(variant, args.backend, root), env, root, args.timeout)
            row = {
                "backend": args.backend,
                "rows": args.rows,
                "variant": variant,
                "repeat": rep,
                "host": args.host or platform.node(),
                "python": platform.python_version(),
                **res,
            }
            if res["rc"] == 0:
                deep = variant not in hashed
                row["outputs"] = check_outputs(root, deep)
                hashed.add(variant)
            if variant == "lineage":
                row["record"] = run_record(root)
            if args.backend == "spark":
                row["spark"] = event_log(root / "events")
            with open(args.out, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(row) + "\n")
            print(
                f"{args.backend} {args.rows:>11,} {variant:<8} rep {rep}: rc {res['rc']} "
                f"{res['wall_s']:8.2f} s  peak {res['peak_rss_mb']}",
                flush=True,
            )
            if res["rc"] != 0:
                print(res["stderr_tail"][-1500:], flush=True)
    shutil.rmtree(root / "out", ignore_errors=True)


# --------------------------------------------------------------------------- #
# Summary
# --------------------------------------------------------------------------- #


def summary(paths: list) -> None:
    rows = []
    for path in paths:
        with open(path, encoding="utf-8") as fh:
            rows += [json.loads(line) for line in fh if line.strip()]
    groups: dict = {}
    for r in rows:
        groups.setdefault((r["backend"], r["rows"]), {}).setdefault(r["variant"], []).append(r)

    def med(xs):
        return statistics.median(xs) if xs else float("nan")

    print(
        "| backend | rows | plain s (min-max) | ubunye s (min-max) | lineage s (min-max) "
        "| ubunye / plain | lineage / plain | hash s (lineage) | peak MB plain / ubunye / "
        "lineage | same rows |"
    )
    print("|---|---|---|---|---|---|---|---|---|---|")
    for (backend, n), by in sorted(groups.items()):
        cells, meds, peaks = [], {}, []
        for v in VARIANTS:
            ok = [r for r in by.get(v, []) if r["rc"] == 0]
            bad = [r for r in by.get(v, []) if r["rc"] != 0]
            if not ok:
                cells.append(f"failed ({len(bad)} of {len(by.get(v, []))})")
                meds[v] = None
                peaks.append("n/a")
                continue
            w = [r["wall_s"] for r in ok]
            meds[v] = med(w)
            note = f" ({len(bad)} failed)" if bad else ""
            cells.append(f"{med(w):.2f} ({min(w):.2f}-{max(w):.2f}){note}")
            peaks.append(f"{med([r['peak_rss_mb']['tree'] for r in ok]):.0f}")
        ratio = []
        for v in ("ubunye", "lineage"):
            if meds.get(v) and meds.get("plain"):
                ratio.append(f"{meds[v] / meds['plain']:.2f}x")
            else:
                ratio.append("n/a")
        hs = [
            r["record"]["hash_seconds"]
            for r in by.get("lineage", [])
            if r["rc"] == 0 and r.get("record")
        ]
        hashes = {}
        for v in VARIANTS:
            for r in by.get(v, []):
                for name, o in (r.get("outputs") or {}).items():
                    if "data_hash" in o:
                        hashes.setdefault(name, set()).add(o["data_hash"])
        counts = {
            (name, o.get("rows"))
            for v in VARIANTS
            for r in by.get(v, [])
            for name, o in (r.get("outputs") or {}).items()
        }
        same = all(len(s) == 1 for s in hashes.values()) and len(counts) == 2
        print(
            f"| {backend} | {n:,} | {cells[0]} | {cells[1]} | {cells[2]} | {ratio[0]} | "
            f"{ratio[1]} | {med(hs):.2f} | {' / '.join(peaks)} | {'yes' if same else 'NO'} |"
        )


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--backend", choices=("pandas", "spark"))
    p.add_argument("--rows", type=int)
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--variants", default=",".join(VARIANTS))
    p.add_argument("--work", default="e06-work")
    p.add_argument("--out", default="e06-results.jsonl")
    p.add_argument("--host", default="")
    p.add_argument("--driver-memory", default="6g")
    p.add_argument("--timeout", type=float, default=3600)
    p.add_argument("--summary", nargs="*")
    args = p.parse_args()
    if args.summary:
        summary(args.summary)
    else:
        ladder(args)


if __name__ == "__main__":
    main()
