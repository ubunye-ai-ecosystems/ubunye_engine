"""F-041: four 32 bit half lanes summed as long, against today's decimal lanes.

Times one aggregation over N generated rows (cached), three ways, median of 3:
today's two decimal(20,0) lane sums, four ``sum`` of long half lanes, and four
``try_sum`` (null instead of an ANSI overflow error, the form the fix uses). Then
checks every way gives the same two lane totals modulo 2**64, which is all the
digest keeps (``content_hash.data_hash`` masks each sum).

    python e06_halflanes.py [ROWS]
"""

import statistics
import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from ubunye.adapters.spark.content_hash import _quoted
from ubunye.lineage import content_hash as ch

ROWS = int(sys.argv[1]) if len(sys.argv) > 1 else 2_000_000
MASK = (1 << 64) - 1

spark = (
    SparkSession.builder.config("spark.ui.enabled", "false")
    .config("spark.driver.memory", "2g")
    .getOrCreate()
)
df = (
    spark.range(ROWS)
    .selectExpr(
        "id",
        "id % 97 as shop",
        "cast(id * 1.5 as double) as amount",
        "concat('customer-', id % 10007) as customer",
        "timestamp'2026-01-01 00:00:00' + make_interval(0, 0, 0, 0, 0, 0, id % 86400) as ts",
    )
    .persist()
)
df.count()
line = F.to_json(F.struct(*[F.col(_quoted(n)) for n in sorted(df.columns)]), ch.JSON_OPTIONS)
d = F.sha2(line, 256)


def lane(start):
    return F.conv(F.substring(d, start, 16), 16, 10).cast("decimal(20,0)")


def half(start):
    return F.conv(F.substring(d, start, 8), 16, 10).cast("long")


def timed(label, agg):
    xs = []
    for _ in range(3):
        t0 = time.perf_counter()
        row = df.agg(*agg).collect()[0]
        xs.append(time.perf_counter() - t0)
    print(f"{label:<55} {statistics.median(xs):6.2f} s  (runs {', '.join(f'{x:.2f}' for x in xs)})")
    return row


today = timed("today: two decimal(20,0) lane sums", [F.sum(lane(1)), F.sum(lane(17))])
plain = timed("four long half lane sums", [F.sum(half(s)) for s in (1, 9, 17, 25)])
tried = timed(
    "four long half lane try_sum (the fix)",
    [F.call_function("try_sum", half(s)) for s in (1, 9, 17, 25)],
)


def totals(h):
    return ((h[0] << 32) + h[1]) & MASK, ((h[2] << 32) + h[3]) & MASK


want = (int(today[0]) & MASK, int(today[1]) & MASK)
print("rows:", ROWS)
print("long sums give the same totals:", totals([int(x) for x in plain]) == want)
print("try_sum gives the same totals:", totals([int(x) for x in tried]) == want)
spark.stop()
