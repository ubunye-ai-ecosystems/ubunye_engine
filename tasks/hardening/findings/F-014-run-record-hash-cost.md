# F-014: `--lineage` makes a 1M-row run 8.5 times slower

**Status:** fixed on fix/hash-speed (2026-09-29)
**Severity:** major
**Source:** experiment E-01 (2026-09-28)
**Promise:** 7 (the core stays small) and the scale requirement (E-06)

## What happens
The same task, 1,000,000 rows, one input and two outputs, pandas backend, on the dev
box: 1.61 s without `--lineage`, 13.62 s with it (12.89 s with telemetry off). The run
record itself says the task took 0.70 s (read 0.35, transform 0.01, two writes of 0.16);
the rest is hashing inputs and outputs for the record, about 4 s per million rows, in
one Python process, after the writes. That time is not in the record, and it is the
window in which F-011 happens.

## Repro
`tests/experiments/timing.py` against the E-01 task.

## Expected
Hashing costs a small fraction of the run, is itself timed in the record, and does not
grow on one machine when the backend is distributed. Measure on Spark too (E-06).

## Evidence
Timings above. Earlier: 0.78 s per 60,000 rows (0.6.0), 2.4 times faster in 0.7.0.

## Fix (2026-09-29)
`fingerprint_arrow` no longer builds each line in Python. Per slice of 32,768 rows,
Arrow compute builds each column's `"name":value` text (integers by cast, booleans by
`if_else`, strings with the backslash and quote escaped by `replace_substring`, dates
by cast, doubles by cast plus the ".0" Java adds, microsecond timestamps by a cast of
the UTC wall time), joins the members into the line, and each line is hashed straight
from Arrow's buffer; numpy sums the lanes (uint64 wraps modulo 2**64, like the masked
Python sum). All in the calling thread. Values whose
Arrow text is not provably the canonical text go through the old Python path, value by
value: strings with a control character, doubles outside 0.001 to 10,000,000 and NaN
or infinity, timestamps outside years 1000 to 9999. Kinds with no vectorised path
(float32, decimal, binary, nanosecond timestamps, date64, lists, structs, maps) use
the Python path for the column. The recorder hashes a frame written to two outputs
once, and records `hash_seconds` for every input and output.

Two traps found on the way: `pc.binary_join_element_wise(..., null_handling="skip")`
(F-033) on pyarrow 25 drops the rows where every input is null (the array comes back
shorter), so nulls are joined as empty text instead; and Arrow's `strftime`, `year`
and time zone kernels take about 12 s per million values on Windows, so timestamps
are written by a plain cast.

**Digests unchanged.** Property tests (`tests/unit/lineage/test_content_hash_fast_path.py`)
hold the fast path to `fingerprint_rows` line by line and digest by digest, over every
kind incl. nulls, NaN, -0.0, infinities, control characters, quotes, backslashes,
dates from year 1, int64 and uint64 extremes, aware and naive timestamps, nested
types, and across slices. A one off three way check (new == old e4b7ea5 ==
reference) passed on 6,000 random tables; doubles checked value by value on
4,000,000 values (log uniform, random bit patterns, short decimals, integers);
timestamps, dates and strings on 300,000 each. Live Spark 4.2: C01 (bb08a7d7a9fd),
R1 food prices (f61e0f0544f5) and the Spark vs pandas hash parity tests pass.

**After (dev box, best of 3, 1,000,000 rows):**

| | old | new |
|---|---|---|
| `fingerprint_arrow`, E-01 shape (4 columns) | 2.56 to 2.61 s | 1.46 to 1.52 s |
| `fingerprint_arrow`, 8 mixed columns | 14.1 to 14.3 s | 2.79 to 2.86 s |
| E-01 task, no `--lineage` | 1.6 s | 1.6 s |
| E-01 task, `--lineage` | 10.8 to 13.4 s | 4.4 to 4.8 s |

Not under the 1 s per million target. What bounds it: one SHA-256 call per row
from Python, about 0.85 s per million short lines on this box, plus about 0.6 s of
Arrow compute to build the lines. A first version built the next slice on a helper
thread (0.95 s per million), but that only paid off with a lower
`sys.setswitchinterval`, a process wide setting a library must not change, so it was
removed in review. Going lower needs the hashing out of Python (a native kernel);
Spark already hashes where the data lives and is unchanged. Pandas to Arrow
conversion adds to the recorded `hash_seconds` (1.28 s for the 3 column input, 1.60 s
for the 4 column output, the second output 0 s as it is the same frame).

## Skeptic review (2026-09-29), fixed in a follow-up commit
- **Wrong hash from a reused id (proven, 3 of 40 on Spark).** The hash-once cache
  keyed on `id(getattr(frame, "native", frame))`. A Spark DataFrame with a column
  named `native` returns a fresh Column that is freed at once; its id was reused and a
  later output got another frame's fingerprint. Now only the pandas adapter is
  unwrapped, and the cache keeps the object and checks it is the same one. The
  skeptic's Spark repro gives 0 of 40 wrong; a unit test reproduces the id reuse
  (3 distinct hashes of 6 on the old code).
- **Invalid UTF-8 got a digest Spark does not give (proven).** The old path recorded
  `UnicodeDecodeError` and no digest; the fast path hashed Arrow's raw bytes. A string
  column is now validated first (`validate(full=True)`, about 7 ms per million
  strings) and invalid data goes to the Python path, so the old error comes back.
  E-01 shape hash after the check: 1.48 to 1.50 s per million rows.
- **Behaviour change, stated:** a timestamp column with a zone east of UTC near
  9999-12-31 used to raise `OverflowError` (no digest); it now gets the digest of the
  same instants in UTC.
- A step that reuses another step's hash records `hash_reused_from` and
  `hash_seconds` 0.
