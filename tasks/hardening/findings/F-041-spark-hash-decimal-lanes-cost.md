# F-041: On Spark, 40% of the content hash time is decimal arithmetic that long sums could do

**Status:** open
**Severity:** minor
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 7 (the core stays small)

## What happens
`fingerprint_spark` turns each row's SHA-256 into two 64 bit lanes with
`conv(substring(hex, ..., 16), 16, 10).cast("decimal(20,0)")` and sums them as
decimals. On the E-06 `events` input (5,000,000 rows, 5 columns, cached, dev box,
Spark 4.2 local, median of 3):

| aggregation over every row | seconds |
|---|---|
| count only | 0.16 |
| + `to_json` of the row | 0.79 |
| + `sha2` of that line | 1.11 |
| + two lanes `conv` to decimal text | 2.21 |
| **today:** lanes cast to `decimal(20,0)`, two decimal sums | **3.93** |
| four 32 bit half lanes, `conv` to `long`, four `long` sums | 2.32 |

The script checks the half lane sums give the same two lane totals (`same lane
totals: True`). An earlier run of the same parts: 4.52 s today against 2.28 s.

The decimal cast and the decimal sums take about 1.7 of the 3.9 seconds (40%). Four half
lane sums give the same two lane totals exactly (`a = hi * 2**32 + lo`, so
`sum(a) = sum(hi) * 2**32 + sum(lo)`), hence the same digest, in about 40% less time.

On GitHub `ubuntu-latest` the record's own `hash_seconds` for the `events` input is
about 1 to 2 s per million rows (10.3 s at 5,000,000 rows, 48 s at 50,000,000), the
largest part of a `--lineage` run on Spark (E-06, F-039).

## Repro
`python tasks/hardening/experiments/e06/e06_lanes.py DATA_DIR`, where `DATA_DIR`
holds the E-06 data (`e06_scale.py --backend spark --rows 5000000 --work W` makes it
in `W/spark-5000000/data`).

## Expected
The same `rows-v1` digest for less work. The trade to state if long sums are used:
a `long` sum of 32 bit values is exact up to about 2.1 billion rows per sum, where
the decimal sums are good to about 10**11 rows; a table beyond that needs the
decimal path (or a sum per partition, combined on the driver as Python integers).
Changing the digest definition itself (for example `xxhash64`) is not the fix: the
digest must stay equal to the pandas side's (ADR 006).
