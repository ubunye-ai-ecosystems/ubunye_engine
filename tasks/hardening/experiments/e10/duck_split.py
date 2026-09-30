"""E-10: where DuckDB's time goes: sha256() alone against sha256() plus the lane parsing."""

import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bench  # noqa: E402
import candidates as C  # noqa: E402
import pyarrow as pa  # noqa: E402

ch = C.load_content_hash()
table = bench.gen_events(1_000_000)
names = sorted(table.column_names)
kinds = {f.name: ch.arrow_kind(f.type) for f in table.schema}
lines = pa.chunked_array(
    [ch._slice_lines(names, kinds, p) for p in ch._slices(table, names)]
).combine_chunks()
for threads in (1, 4, 8):
    con = C.duck_con(threads)
    e10_lines = pa.table({"l": lines})
    con.register("e10_lines", e10_lines)
    for label, sql in (
        ("scan only: sum(length(l))", "select sum(length(l)) from e10_lines"),
        ("sha256 only: count(sha256(l))", "select count(sha256(l)) from e10_lines"),
        ("sha256 + lanes (candidate)", C._DUCK_SQL),
    ):
        ts = []
        for _ in range(3):
            t = time.perf_counter()
            con.execute(sql).fetchall()
            ts.append(time.perf_counter() - t)
        print(f"threads={threads} {label:34s} {statistics.median(ts):.3f} s per 1M rows")
    con.unregister("e10_lines")
