"""E-10 speed: the rows-v1 hash with each candidate, on the E-06 table shapes.

    python bench.py one MODE TABLE ROWS   one measurement in this process (JSON line)
    python bench.py all OUT.jsonl         every measurement, each in a fresh process

Tables (data generated as E-06 generates it, same seeds):
  events  5 columns (id, region, cat, amount, qty), the E-06 input
  detail  9 columns, the E-06 output: events with qty > 0 and region < 900, plus
          revenue, big, region_name, tier

Modes:
  lines            build every slice's canonical lines only (Arrow compute; no hash)
  hash:<c>         hash prebuilt lines, one call per 32,768 row slice (the engine's slices)
  hashall:<c>      hash prebuilt lines, one call for the whole table
  fp:ref1          fingerprint_arrow as it is, one process (UBUNYE_HASH_WORKERS=1)
  fp:ref4          fingerprint_arrow as it is, 4 helper processes (the default)
  fp:<c>           fingerprint_arrow, one process, _arrow_lanes replaced by candidate c
  thr:<c>:<k>      prototype: slices hashed on k threads in this process, candidate c
  help:c:<k>       the engine file with the C kernel appended, k helper processes

Every mode reports the first call (cold: imports, JIT, helper start) and the
median of the next 3 calls, plus the peak resident memory of the process tree.
Every fingerprint mode also returns its data hash, checked equal to fp:ref1's.
"""

from __future__ import annotations

import json
import os
import statistics
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
SLICE = 1 << 15
CHUNK = 1_000_000


def gen_events(rows):
    import numpy as np
    import pyarrow as pa

    cats = pa.array([f"cat_{i:02d}" for i in range(20)])
    parts = []
    for start in range(0, rows, CHUNK):
        n = min(CHUNK, rows - start)
        rng = np.random.default_rng(606 + start // CHUNK)
        parts.append(
            pa.table(
                {
                    "id": np.arange(start, start + n, dtype=np.int64),
                    "region": rng.integers(0, 1000, n, dtype=np.int64),
                    "cat": cats.take(pa.array(rng.integers(0, 20, n))),
                    "amount": rng.integers(1, 100_000, n, dtype=np.int64),
                    "qty": rng.integers(-2, 20, n, dtype=np.int64),
                }
            )
        )
    return pa.concat_tables(parts)


def gen_detail(rows):
    import numpy as np
    import pyarrow as pa
    import pyarrow.compute as pc

    ev = gen_events(rows)
    keep = pc.and_(pc.greater(ev["qty"], 0), pc.less(ev["region"], 900))
    ev = ev.filter(keep)
    revenue = pc.multiply(ev["amount"], ev["qty"])
    names = pa.array([f"region_{i:03d}" for i in range(900)])
    region = ev["region"].combine_chunks()
    return (
        ev.append_column("revenue", revenue)
        .append_column("big", pc.greater(revenue, 500000))
        .append_column("region_name", names.take(region))
        .append_column("tier", pa.array(np.asarray(region) % 3))
        .combine_chunks()
    )


class Peak:
    """Peak resident memory of this process and its children, sampled every 20 ms."""

    def __init__(self):
        import psutil

        self.p = psutil.Process()
        self.peak = 0
        self.stop = False
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()

    def sample(self):
        total = self.p.memory_info().rss
        for c in self.p.children(recursive=True):
            try:
                total += c.memory_info().rss
            except Exception:
                pass
        self.peak = max(self.peak, total)

    def run(self):
        while not self.stop:
            self.sample()
            time.sleep(0.02)

    def done(self):
        self.stop = True
        self.t.join()
        self.sample()
        return self.peak / 2**20


def lanes_fn(name):
    import candidates as C

    if name.startswith("duckdb"):
        threads = int(name.split("_")[1]) if "_" in name else 1
        return lambda lines: C.lanes_duckdb(lines, threads=threads)
    if name.startswith("numba_par"):
        k = int(name.split("_")[-1])
        return lambda lines: C.lanes_numba_parallel(lines, parts=k)
    if name == "c_plain":
        C.c_dll().rows_v1_set_mode(0)
        return C.lanes_c
    return C.CANDIDATES[name]


def one(mode, table_name, rows):
    import candidates as C

    table = (gen_events if table_name == "events" else gen_detail)(rows)
    kind = mode.split(":")
    peak = Peak()
    base_mb = peak.p.memory_info().rss / 2**20
    ch = C.load_content_hash()
    names = sorted(table.column_names)
    kinds = {f.name: ch.arrow_kind(f.type) for f in table.schema}
    digest = None
    os.environ["UBUNYE_HASH_WORKERS"] = "1"

    if kind[0] in ("lines", "hash", "hashall"):

        def build():
            return [ch._slice_lines(names, kinds, piece) for piece in ch._slices(table, names)]

        if kind[0] == "lines":
            call = build
        else:
            prebuilt = build()
            assert all(x is not None for x in prebuilt)
            fn = ch._arrow_lanes if kind[1] == "ref" else lanes_fn(kind[1])
            if kind[0] == "hashall":
                import pyarrow as pa

                whole = pa.chunked_array(prebuilt).combine_chunks()

                def call():
                    return fn(whole)

            else:

                def call():
                    a = b = 0
                    for lines in prebuilt:
                        x, y = fn(lines)
                        a += x
                        b += y
                    return a & C.MASK, b & C.MASK

    elif kind[0] == "fp":
        if kind[1] == "ref4":
            os.environ["UBUNYE_HASH_WORKERS"] = "4"
        elif kind[1] != "ref1":
            ch._arrow_lanes = lanes_fn(kind[1])

        def call():
            return ch.fingerprint_arrow(table).data_hash

    elif kind[0] == "thr":
        from concurrent.futures import ThreadPoolExecutor

        fn = ch._arrow_lanes if kind[1] == "ref" else lanes_fn(kind[1])
        ch._arrow_lanes = fn
        pool = ThreadPoolExecutor(int(kind[2]))
        schema = [(f.name, kinds[f.name]) for f in table.schema]

        def call():
            sums = list(
                pool.map(lambda p: ch._slice_lanes(names, kinds, p), ch._slices(table, names))
            )
            a = sum(s[0] for s in sums)
            b = sum(s[1] for s in sums)
            return ch.data_hash(schema, table.num_rows, (a, b))

    elif kind[0] == "help":
        import native_patch

        dst = os.path.join(HERE, "content_hash_native.py")
        native_patch.make(ch.__file__, dst)
        os.environ["E10_DLL"] = os.path.join(HERE, "rows_v1_lanes.dll")
        chn = C.load_content_hash(dst)
        os.environ["UBUNYE_HASH_WORKERS"] = kind[2]
        seen = []
        real = chn._parallel_lanes
        chn._parallel_lanes = lambda *a, **k: seen.append(real(*a, **k)) or seen[-1]

        def call():
            d = chn.fingerprint_arrow(table).data_hash
            assert seen and seen[-1] is not None, "helpers did not answer"
            return d

    else:
        raise SystemExit(f"unknown mode {mode}")

    times = []
    results = []
    for _ in range(4):
        t = time.perf_counter()
        results.append(call())
        times.append(time.perf_counter() - t)
    mb = peak.done()
    if kind[0] in ("fp", "thr", "help"):
        digest = results[0]
        assert len(set(results)) == 1
    elif kind[0] != "lines":
        assert len(set(results)) == 1
        digest = ch.data_hash(
            [(f.name, kinds[f.name]) for f in table.schema], table.num_rows, results[0]
        )
    return {
        "mode": mode,
        "table": table_name,
        "rows": table.num_rows,
        "gen_rows": rows,
        "cold_s": round(times[0], 3),
        "median_s": round(statistics.median(times[1:]), 3),
        "runs_s": [round(t, 3) for t in times],
        "us_per_row": round(statistics.median(times[1:]) / table.num_rows * 1e6, 3),
        "peak_mb": round(mb, 0),
        "base_mb": round(base_mb, 0),
        "digest": digest,
    }


MODES = [
    "lines",
    "hash:ref",
    "hash:c",
    "hash:c_plain",
    "hash:duckdb",
    "hash:duckdb_4",
    "hash:polars_hash",
    "hash:datafusion",
    "hash:numba",
    "hashall:c",
    "hashall:duckdb",
    "hashall:duckdb_4",
    "hashall:polars_hash",
    "hashall:datafusion",
    "hashall:numba",
    "hashall:numba_par_4",
    "fp:ref1",
    "fp:ref4",
    "fp:c",
    "fp:duckdb",
    "fp:polars_hash",
    "fp:datafusion",
    "fp:numba",
    "thr:ref:4",
    "thr:c:2",
    "thr:c:4",
    "thr:c:8",
    "help:c:2",
    "help:c:4",
]


def run_all(
    out,
    sizes=(
        (1_000_000, "events"),
        (1_000_000, "detail"),
        (5_000_000, "events"),
        (5_000_000, "detail"),
    ),
    modes=None,
):
    modes = modes or MODES
    for rows, table in sizes:
        for mode in modes:
            proc = subprocess.run(
                [sys.executable, __file__, "one", mode, table, str(rows)],
                capture_output=True,
                text=True,
                timeout=1800,
            )
            line = proc.stdout.strip().splitlines()[-1] if proc.stdout.strip() else ""
            if proc.returncode or not line.startswith("{"):
                line = json.dumps(
                    {"mode": mode, "table": table, "gen_rows": rows, "error": proc.stderr[-800:]}
                )
            print(line, flush=True)
            with open(out, "a", encoding="utf-8") as fh:
                fh.write(line + "\n")


if __name__ == "__main__":
    if sys.argv[1] == "one":
        print(json.dumps(one(sys.argv[2], sys.argv[3], int(sys.argv[4]))))
    elif sys.argv[1] == "all":
        modes = sys.argv[3].split(",") if len(sys.argv) > 3 else None
        sizes = None
        if len(sys.argv) > 4:
            sizes = [(int(s.split("@")[0]), s.split("@")[1]) for s in sys.argv[4].split(",")]
        run_all(
            sys.argv[2],
            sizes
            or (
                (1_000_000, "events"),
                (1_000_000, "detail"),
                (5_000_000, "events"),
                (5_000_000, "detail"),
            ),
            modes,
        )
