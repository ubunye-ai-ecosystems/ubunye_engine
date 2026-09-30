"""Where does a Spark `--lineage` hash spend its time? (E-06, F-039)

On the E-06 data (``e06_scale.py`` makes it), with the input cached so reading is
out of the picture, time each part of ``fingerprint_spark``'s one aggregation, then
hash the ``detail`` output twice: as the lazy plan the recorder gets (computed again)
and cached (the rows already there).

    python e06_spark_hash_parts.py DATA_DIR [repeats]
"""

import statistics
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ubunye.adapters.spark.content_hash import _quoted, fingerprint_spark
from ubunye.lineage import content_hash as ch

sys.path.insert(0, str(Path(__file__).resolve().parents[4] / "tests" / "experiments"))
from e06_logic import spark_job  # noqa: E402

data = sys.argv[1]
repeats = int(sys.argv[2]) if len(sys.argv) > 2 else 3
spark = SparkSession.builder.config("spark.ui.enabled", "false").getOrCreate()
events = spark.read.parquet(f"{data}/events.parquet")
regions = spark.read.parquet(f"{data}/regions.parquet")
cached = events.persist()
rows = cached.count()


def timed(label, fn):
    times = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        times.append(time.perf_counter() - t0)
    print(f"{label:<52} {statistics.median(times):7.2f} s  ({min(times):.2f}-{max(times):.2f})")


def line(df):
    names = sorted(df.columns)
    return F.to_json(F.struct(*[F.col(_quoted(n)) for n in names]), ch.JSON_OPTIONS)


print(f"events: {rows:,} rows, cached")
timed("count only", lambda: cached.agg(F.count(F.lit(1))).collect())
timed("+ to_json line", lambda: cached.agg(F.sum(F.length(line(cached)))).collect())
timed(
    "+ sha2 of the line",
    lambda: cached.agg(F.sum(F.length(F.sha2(line(cached), 256)))).collect(),
)
timed("fingerprint_spark (+ conv to decimal, 2 sums)", lambda: fingerprint_spark(cached))

detail = spark_job(events, regions)["detail"]
timed("detail: fingerprint of the lazy plan (recomputed)", lambda: fingerprint_spark(detail))
detail_cached = detail.persist()
n = detail_cached.count()
timed(f"detail: fingerprint of cached rows ({n:,})", lambda: fingerprint_spark(detail_cached))
spark.stop()
