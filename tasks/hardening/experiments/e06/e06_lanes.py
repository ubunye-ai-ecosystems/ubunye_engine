"""F-041: what each part of the Spark content hash costs, and a cheaper exact sum.

On the E-06 ``events`` input, cached, time one aggregation over every row, adding
one part of ``fingerprint_spark`` at a time, then the same two lane totals from
four 32 bit half lanes summed as ``long``.

    python e06_lanes.py DATA_DIR
"""

import statistics
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ubunye.adapters.spark.content_hash import _quoted
from ubunye.lineage import content_hash as ch

spark = SparkSession.builder.config("spark.ui.enabled", "false").getOrCreate()
df = spark.read.parquet(f"{sys.argv[1]}/events.parquet").persist()
df.count()
line = F.to_json(F.struct(*[F.col(_quoted(n)) for n in sorted(df.columns)]), ch.JSON_OPTIONS)
d = F.sha2(line, 256)


def timed(label, agg):
    xs = []
    for _ in range(3):
        t0 = time.perf_counter()
        df.agg(*agg).collect()
        xs.append(time.perf_counter() - t0)
    print(f"{label:<60} {statistics.median(xs):6.2f} s")


def lane(start):
    return F.conv(F.substring(d, start, 16), 16, 10)


def half(start):
    return F.conv(F.substring(d, start, 8), 16, 10).cast("long")


timed("count only", [F.count(F.lit(1))])
timed("+ to_json line", [F.sum(F.length(line))])
timed("+ sha2 of the line", [F.sum(F.length(d))])
timed("+ two lanes conv to decimal text", [F.sum(F.length(lane(1))), F.sum(F.length(lane(17)))])
timed(
    "today: lanes cast to decimal(20,0), two decimal sums",
    [F.sum(lane(1).cast("decimal(20,0)")), F.sum(lane(17).cast("decimal(20,0)"))],
)
timed("four half lanes conv to long, four long sums", [F.sum(half(s)) for s in (1, 9, 17, 25)])

# The half lane totals give the lane totals exactly.
row = df.agg(
    F.sum(lane(1).cast("decimal(20,0)")).alias("a"),
    F.sum(lane(17).cast("decimal(20,0)")).alias("b"),
    *[F.sum(half(s)).alias(f"h{s}") for s in (1, 9, 17, 25)],
).collect()[0]
a = row["h1"] * 2**32 + row["h9"]
b = row["h17"] * 2**32 + row["h25"]
print("same lane totals:", int(row["a"]) == a and int(row["b"]) == b)
spark.stop()
