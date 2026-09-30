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

## Skeptic review (2026-09-30), fixed in a follow-up commit
Digests were identical in every case the skeptic tried (t1: 30 awkward tables, 2, 3
and 7 helpers, no difference). It proved six process safety problems. Each now has a
test in `test_content_hash_parallel.py` that fails on 22f11fb and passes after
(bounded: a watchdog kills the helpers at 20 s). Before and after, with the
skeptic's scripts (`scratchpad/skeptic-f038`):

1. **No timeout, a pipe deadlock, Ctrl+C blocked.** `sys.executable` = `findstr`
   (it echoes stdin): the hash hung past 90 s (the parent wrote all of stdin before
   reading stdout). A helper that never reads (`fakeapp`): Ctrl+Break landed after
   23 s. Now each helper's stdin is written and its stdout drained on their own
   threads (at most 256 bytes kept), the parent waits in 50 ms steps, helpers past
   a deadline (60 s plus 20 microseconds a cell) are stopped, and Ctrl+C kills every
   helper before it is raised. After: `findstr` falls back in 0.7 s; Ctrl+Break with
   real helpers at 6,000,000 rows lands in 0.17 s, no helper left. Killing the venv
   launcher also ends its Python (checked: no process left).
2. **Frozen apps.** A PyInstaller style app reports itself as `sys.executable`;
   started with `-c` it runs its own main again. The skeptic's `frozenapp.py`
   (depth guard 2) on 22f11fb started itself 20 times: 4 copies at depth 1, 16 at
   depth 2, each printing tracebacks. Now no helpers when `sys.frozen`
   is set or the program's name is not `python*`/`pythonw*`/`pypy*` (or it is not
   a file), and every helper gets `UBUNYE_HASH_WORKERS=1`, `PYTHONWARNINGS=ignore`
   and no `PYTHONINSPECT`/`PYTHONSTARTUP`. After: `frozenapp.py` runs once
   (depth 0 only in its log); `PYTHONINSPECT=1` and `PYTHONIOENCODING=utf-16` no
   longer make the helpers fall back (the helper writes its answer as ASCII bytes).
3. **Noisy, leaky fallback.** `cmd.exe` as `sys.executable` printed 20 lines of
   `Exception ignored in: <_io.BufferedWriter>`; `PYTHONDEVMODE` showed unclosed
   pipes. Now every pipe is closed in `finally`, errors there are swallowed, and
   one debug line (`rows-v1: helpers failed (...); hashing here`, logger
   `ubunye.lineage.content_hash`) says why. After: 0 stderr lines in every t5 case
   that is not the probe's own output.
4. **Helpers ran the file on disk.** Replace `content_hash.py` after import (t7):
   the parallel digest changed with no error. Now the stream carries the SHA-256 of
   the caller's file (read at import) and the caller's pyarrow version; a helper
   that differs exits (codes 5 and 6) and the caller hashes itself. After: t7
   gives the serial digest.
5. **Resources.** `UBUNYE_HASH_WORKERS` was not held to the usable cores, and three
   hashes in threads started 12 helpers (24 processes, 1,477 MB).
   Now the cap is `min(setting or 4, usable cores)`, usable cores from
   `os.process_cpu_count()` (3.13), else the affinity, else `os.cpu_count()`,
   bounded by a cgroup v2 `cpu.max` quota; and a process wide budget of that size
   is shared by concurrent hashes (a hash that gets fewer than 2 helpers runs in
   its own process). After: t6 peaks at 8 processes (4 helpers) and 629 MB.
   Memory measured again: each helper is about 105 to 125 MB resident on this
   Windows venv (t2: 4 helpers add 470 MB to the parent's 200 MB); documented in
   `docs/deployment/anywhere.md` with advice for containers.
6. **Another pyarrow in the helper:** covered by 4.

Also found: CI's mypy failed on 22f11fb (an unused `type: ignore` that is only used
on Windows, and a `sum` over a list of optionals). Both gone.

Timings after the safety changes (same box, median of 3): `fingerprint_arrow`
5,000,000 rows, 4 helpers: `events` 2.98 s (was 3.57 s in the first run),
9 columns 4.03 s (was 4.15 s); 1,000,000 rows: 1.17 s and 1.37 s. E-06 at
5,000,000 rows: `--lineage` 11.04 s, plain 4.74 s (2.33x), record hashing 6.45 s:
the same as before within this box's noise.

## On the scale ladder (2026-09-30)
GitHub `ubuntu-latest`, median of 3, run 36673389425: `--lineage` / plain on pandas 6.35x at
5M rows (was 9.81x), 4.90x at 50M (was 9.67x); record `hash_seconds` 9.6 s at 5M (was
15.7), 71.9 s at 50M (was 156.1). The rest of the gap is tracked in F-039 and E-07.

