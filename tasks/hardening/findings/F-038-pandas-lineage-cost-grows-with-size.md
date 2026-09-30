# F-038: On pandas, `--lineage` costs 5 to 11 times the job, and the share grows with size

**Status:** partly fixed on fix/f038-pandas-hash-speed (hash 2.8 times faster at 5M rows on 4 helpers; `--lineage` 2.2 times plain on the dev box, bound is 1.5)
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

## Profile (dev box, one process, E-06 `events` shape, 5 columns, 65 bytes a line)
| part | 1,000,000 rows | 5,000,000 rows |
|---|---|---|
| canonical lines (Arrow compute) | 0.85 s | 4.11 s |
| offsets out of Arrow's buffer | 0.03 s | 0.12 s |
| SHA-256, one call per row | 0.99 s | 4.67 s |
| join the digests | 0.07 s | 0.29 s |
| lane sums (numpy) | 0.01 s | 0.05 s |
| total | 1.94 s | 9.25 s |

Two halves: the lines (Arrow C++ kernels, which release the GIL) and the SHA-256
(one Python call per row: 0.76 s per million on a fixed 60 byte buffer, so the call
itself is the floor; slicing the buffer adds only 0.2 s). CPython's SHA-256 keeps the
GIL for inputs under 2 KB, so threads cannot share the hashing.

Options weighed:
- **A vectorised SHA-256 already installed.** None: pyarrow and numpy have no
  SHA-256 kernel, polars only has non cryptographic hashes. duckdb has `sha256()`,
  but it is not installed and would be a new dependency for one function.
- **Less Python per row.** Already done by F-014: each line is hashed straight from
  Arrow's buffer through a memoryview slice. What is left is the call.
- **Cores.** Chosen: the only way to move both halves without new code in C.

## Fix
`fingerprint_arrow` hands a table of 500,000 rows or more to helper processes
(`_parallel_lanes`). The rows are cut into one run per helper and streamed to it as
Arrow IPC over its stdin; the helper builds and hashes every line of its run with the
same functions, and writes back its two lane sums; the sums add up to the same
digest. The helper is a fresh `python -c` that loads `content_hash.py` by its path:
not the `ubunye` package (it starts in about 0.15 s, not 0.4 s), not
`multiprocessing` (which would import the caller's script again on Windows and
macOS), and with the working folder dropped from `sys.path` (a stray module there
cannot shadow the standard library). The caller's canonical kinds travel in the
stream's schema metadata and the helper checks it gets the same kinds from the
stream. Extension types stay in one process. Any failure (a helper that cannot
start, exits non zero, or says anything but two numbers) makes the caller hash the
whole table itself, so a digest never depends on the helpers. Invalid UTF-8 still
records its `UnicodeDecodeError` and no digest.

`UBUNYE_HASH_WORKERS` caps the helpers (default: the cores, at most 4; `1`, `0` or
anything not a number turns it off). A helper holds about 75 MB of its own memory
(100 MB resident) while it works; 8 helpers at 1,000,000 rows raised the run's peak
from 300 MB to 1.2 GB and were no faster, so the default stops at 4.

Each helper pays a fixed start of about 0.5 s (Python 0.15 s, then Arrow's first
cast, 0.26 s, which builds its cast table). That is why the threshold is 500,000
rows and each helper gets at least 125,000.

**Digests unchanged.** `tests/unit/lineage/test_content_hash_parallel.py` shrinks
the thresholds so small tables go through real helpers, and holds every digest to
the one process path (the code before this fix, unchanged, `UBUNYE_HASH_WORKERS=1`)
and to `fingerprint_rows`: 40 generated tables a run over every kind (nulls, NaN,
control characters, nested types, maps, zones, chunked), the E-06 shape plus a double
and a timestamp column at 200,000 rows, a golden table, and the fallbacks (a helper
that cannot start, one that exits non zero, invalid UTF-8). A mutated helper (lane
sum plus one) fails the property test.

## After (dev box: Windows 11, 8 cores / 16 threads, Python 3.13, pyarrow 25.0.1, other work running; median of 3)
`fingerprint_arrow` alone:

| table | helpers | 1,000,000 rows | 5,000,000 rows |
|---|---|---|---|
| E-06 `events`, 5 columns | 1 (before) | 1.61 s | 8.41 s |
| | 2 | | 5.09 s |
| | 4 (default) | 1.25 s | 3.57 s |
| | 8 | 1.17 s | 3.04 s |
| `detail` like, 9 columns | 1 (before) | 2.40 s | 12.07 s |
| | 2 | | 6.89 s |
| | 4 (default) | 1.40 s | 4.15 s |
| | 8 | 1.76 s | 3.45 s |

The E-06 job through `tests/experiments/e06_scale.py` (plain and Ubunye without
`--lineage` are unchanged by this fix):

| rows | plain s | Ubunye s | `--lineage` before s | `--lineage` after s | ratio before / after | record `hash_seconds` before / after | peak MB before / after |
|---|---|---|---|---|---|---|---|
| 1,000,000 | 1.39 | 2.06 | 5.82 | 5.33 | 4.19x / 3.83x | 3.54 / 3.02 | 300 / 818 |
| 5,000,000 | 4.73 | 4.59 | 21.75 | 10.53 | 4.60x / 2.23x | 17.20 / 6.04 | 986 / 1,236 |

At 5,000,000 rows the record's hashing went from 17.2 s (events 8.0, detail 9.1) to
6.0 s (events 2.9, detail 3.2). At 1,000,000 rows the gain is small: `detail`
(780,000 rows) and `events` each pay the helpers' start. This box's plain job is 2.5
times slower than GitHub's runner, so the ratio there must be measured again with
`.github/workflows/scale-ladder.yml`; scaling the runner's 15.9 s of hashing by this
box's 2.85 times gives about 5.6 s, so about 4 times plain on the runner.

## What remains to reach 1.5 times
Scaling on cores is sub linear here: 4 helpers give 2.4 to 2.9 times, 8 give 2.8 to
3.5 (each helper's line building slows as more run). At 4 vCPU the floor is about
0.5 microseconds a row, far above the about 0.05 the bound needs. Reaching 1.5 times
needs the per row work out of Python: a native kernel (for example a small optional
compiled extension, or duckdb's vectorised `sha256()` as an optional extra with a
parity test) that builds and hashes the lines in one call per slice. Other smaller
steps: keep helpers alive for the whole run (saves about 0.5 s per large table
after the first), and join each column's head and tail into the final line once
instead of per column (fewer copies of every line; not measured).
