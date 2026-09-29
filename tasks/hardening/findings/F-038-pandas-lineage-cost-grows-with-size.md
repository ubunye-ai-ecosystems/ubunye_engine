# F-038: On pandas, `--lineage` costs 5 to 11 times the job, and the share grows with size

**Status:** open
**Severity:** major
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 7 (the core stays small) and the scale requirement

## What happens
E-06 job (filter, derived columns, join, group by; one append and one overwrite
output), GitHub `ubuntu-latest` (4 vCPU, 16 GB), Python 3.12, median of 3 cold runs:

| rows | plain s | Ubunye s | `--lineage` s | `--lineage` / plain | record `hash_seconds` (events / detail) |
|---|---|---|---|---|---|
| 1,000,000 | 0.81 | 1.10 | 4.39 | 5.43x | 3.28 (1.54 / 1.70) |
| 5,000,000 | 1.88 | 2.36 | 18.22 | 9.67x | 15.87 (7.46 / 8.37) |
| 20,000,000 | 6.09 | 6.99 | 70.14 | 11.52x | 63.15 (29.69 / 33.07) |
| 50,000,000 | 16.09 | 16.73 | 171.78 | 10.67x | 155.48 (73.68 / 81.77) |

(E-06 runs 36583035813 and, for 50M, 36584681630.)

The job itself grows by about 0.3 s per million rows; the run record's hashing grows
by about 3.1 s per million input rows (every input and every output is hashed, every
row, in one Python thread). Once the fixed start up cost stops hiding it, the ratio
settles at about 11 times. `hash_seconds` in the record accounts for almost all of
the gap: at 5,000,000 rows the record says 15.9 s of hashing (events 7.5 s, detail
8.4 s) in an 18.2 s run whose steps took 1.9 s.

F-014 cut the hash from about 4 s to about 1.5 s per million rows of its 3 column
shape. On this runner it is 1.5 s per million rows for `events` (5 columns) and 2.1 s
for `detail` (9 columns, 0.78 rows per input row).
That fix was right, and not enough for scale.

## Repro
```
pip install -e . pandas pyarrow numpy psutil
python tests/experiments/e06_scale.py --backend pandas --rows 5000000 --repeats 3 \
  --work /tmp/e06 --out e06.jsonl
python tests/experiments/e06_scale.py --summary e06.jsonl
```
Or run `.github/workflows/scale-ladder.yml` (see E-06).

## Expected
Recording a run costs a bounded share of the run at every size. A bound the data
supports (E-06): `--lineage` within 1.5 times the plain job. That needs the per row
SHA-256 out of the one Python thread: a native kernel, or the slices hashed on
several cores. Not inherent: Spark hashes the same rows with the same digest in
parallel where they live.

## Not a fix to reach for
Sampling rows: the record promises every row (ADR 006). Turning off input hashing
(`hash_inputs=False` exists on the recorder) halves the cost but gives up the input
receipt; it is an option for a user, not the answer.
