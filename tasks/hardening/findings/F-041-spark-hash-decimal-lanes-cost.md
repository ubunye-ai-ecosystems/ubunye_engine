# F-041: On Spark, 40% of the content hash time is decimal arithmetic that long sums could do

**Status:** fixed on fix/f041-half-lanes
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

## Fix
`fingerprint_spark` sums four 32 bit half lanes as `long` (`_half_lane_sums`) and
rebuilds the two lane sums in Python (`lanes_from_halves`). The digest is the same
`rows-v1` digest: the Spark digest still equals the pandas and Arrow digests
(`tests/integration/test_content_hash_parity.py`), and a combine step that is off by
one bit fails that test.

Past about 2.1 billion rows a `long` sum overflows. With ANSI on (Spark 4's default)
Spark raises `ARITHMETIC_OVERFLOW`; only that error is caught, and the hash is taken
again with the old decimal sums (`_decimal_lane_sums`, same digest, pinned by
`test_the_decimal_fallback_gives_the_same_digest`). Any other error is raised, so a
failing job is not computed twice. With ANSI off the sums wrap modulo 2**64, which is
all the digest keeps (`data_hash` masks each sum), so the digest is still exact
(`test_sums_that_wrapped_in_a_spark_long_give_the_same_digest`).

Checked on live Spark 4.2 with a real `long` overflow (four rows of 2**62): ANSI on
raises `[ARITHMETIC_OVERFLOW]`, which `_long_overflow` recognises; ANSI off returns
0, the true sum modulo 2**64. Only that error is retried: a JVM `StackOverflowError`
or a failed task is raised, not hashed again.

`try_sum` (null instead of an error) was measured and rejected: it keeps about a third
of the saving.

## After
`tasks/hardening/experiments/e06/e06_halflanes.py 2000000`, dev box, Spark 4.2 local,
generated 5 column rows, cached, median of 3, every way giving the same lane totals:

| aggregation | seconds |
|---|---|
| before: two `decimal(20,0)` lane sums | 2.29 |
| **after: four `long` half lane sums** | **1.25** (45% less) |
| four `try_sum` half lane sums (rejected) | 1.62 |

On the E-06 scale ladder (GitHub `ubuntu-latest`, 4 vCPU, Spark 4, median of 3; before =
`hardening/real-world` at f569e0a, after = this fix; runs 36632756146 and 36632771300):

| Spark rows | record `hash_seconds`, before | after | `--lineage` / plain, before | after |
|---|---|---|---|---|
| 1,000,000 | 8.0 | **5.9** | 1.74x | **1.55x** |
| 5,000,000 | 16.7 | **12.5** | 2.52x | **2.03x** |
| 20,000,000 | 62.1 | **34.3** | 4.08x | **2.97x** |
| 50,000,000 | 125.1 | **83.5** | 4.80x | **3.54x** |

All 48 output digests are identical before and after. pandas is unchanged (its hash
does not use this code; its rows moved within run to run noise).

The E-06 target (`--lineage` within 1.5x of plain at 5M rows and above) is still not
met on Spark. What remains is mostly reading and hashing each input a second time
(F-046) and the `to_json` plus `sha2` per row that the digest definition requires.

