# E-10: Can a native per row SHA-256 give the same `rows-v1` digest fast enough to bring pandas `--lineage` near 1.5 times plain?

**Status:** answered (2026-09-30). The hash half: yes, 10 times faster, same digest. The whole: not alone. A native kernel frees the GIL, so threads can replace the helper processes, and the E-06 job with `--lineage` went from 2.17x to 1.42x plain at 5M rows on the dev box. On GitHub's 4 vCPU runner the estimate is about 3.4x (from 6.35x), because building the canonical lines, not hashing them, is now the cost.
**Why:** F-038 and F-039 left pandas `--lineage` at 6.35x plain at 5M rows on the ladder. The helper processes hit a floor of about 0.5 microseconds a row per core, set by one Python `sha256()` call per row.

## Question
Can a native per row SHA-256 give the identical `rows-v1` digest (ADR 006, unchanged)
much faster than today's pandas path, enough to bring pandas `--lineage` toward
1.5 times plain at 5M rows and above, without a new required dependency?

Also measured, from F-039: (1) keeping the helper processes alive across the tables
of a run; (2) skipping the input hash when a pinned Delta version names the input.

## Method
Only the per row SHA-256, the lane extraction and the sums were replaced. The
canonical lines are built by the engine's own `_slice_lines`, byte for byte as today.
Each candidate is a drop in for `content_hash._arrow_lanes(lines)`: an Arrow string
array of lines in, the two lane sums out. No engine code changed; everything is in
`tasks/hardening/experiments/e10/`.

Candidates:

| | candidate | what it is |
|---|---|---|
| e | hashlib (today) | `_arrow_lanes`: one `_sha2.sha256(memoryview slice)` call per row. The reference. |
| a | DuckDB 1.5.6 | `sha256(l)` over the lines, registered as an Arrow table; lanes parsed from the hex text in SQL |
| b | Polars 1.44.2 | no SHA-256 in Polars itself (only `hash`, not cryptographic). The `polars-hash` 0.9.3 plugin has `chash.sha2_256()` |
| c | C, 90 KB | `rows_v1_lanes.c`, built here with MSVC 2019 Build Tools (the only toolchain on the box; no gcc, clang, rustc or cargo). x86 SHA extensions when the CPU has them (checked at run time), else plain C. Loaded with ctypes, so the call releases the GIL. |
| d | pyarrow 25.0.1, numpy 2.4.4 | no SHA-256 kernel in either (pyarrow's `hash_*` functions are grouped aggregations) |
| f | DataFusion 54.0.0 | `sha256()` returns 32 bytes as binary; the lanes are read straight from its buffer |
| f | numba 0.67 | SHA-256 written in Python and compiled by numba (`nogil`, and a `prange` version) |
| f | others | PyPI has no `pyarrow-hashing`, `pyarrow-hash`, `arrow-hash`, `vectorized-hash`, `sha256-simd` or `pysha2`. Nothing else found with a vectorised SHA-256 over an Arrow string array. |

Dev box: Windows 11, AMD Ryzen 7 3700X (8 cores, 16 threads, has the SHA extensions),
16 GB, Python 3.13.6, pyarrow 25.0.1. Candidates were installed in a throwaway venv
(`T:\venvs\e07`, deleted at the end). Tables are the E-06 shapes, generated with E-06's
seeds: `events` (5 columns, the input) and `detail` (9 columns, the output: 0.78 rows per
input row). `bench.py` runs every measurement in a fresh process: the first call
(cold) and the median of the next 3, with peak resident memory of the process and its
children. Box noise was low (CPU about 4% before the run) but other sessions were
open; read single numbers as directions and the ratios as the result.

Scripts: `rows_v1_lanes.c` + `build.bat` (the kernel), `candidates.py`,
`correctness.py`, `bench.py`, `e10_native.py` (threads + kernel, the shape a fix could
take), `native_patch.py` (the engine file with the kernel appended, for helpers),
`keepalive.py` (option 1), `ladder_split.py` (option 2), `packages.py`,
`duck_split.py`, `summarise.py`. Raw numbers: `bench-results.jsonl`,
`e06-devbox-*.jsonl`, `keepalive.jsonl`, `ladder_split.jsonl`, `packages.json`,
`correctness.txt`, `duck_split.txt`.

## Correctness: every candidate gives the identical digest
`correctness.py` uses the engine's own property strategy
(`tests/unit/lineage/test_content_hash_fast_path.py::tables`: every kind, nulls, NaN,
control characters, unicode, lists, structs, maps, zones, chunked), 60 generated tables
(half with 3 row slices, so every table crosses slices), plus 6 fixed tables (the
awkward table, the golden table, an empty table, an all null row, lines of 0 to about
800 bytes, the E-06 shape at 200,000 rows). Each digest was held to the unchanged
`fingerprint_arrow` and to the row at a time reference `fingerprint_rows`.

66 tables, 200,829 rows, 992 slices through each candidate: **0 differences** for C
(SHA extensions), C (plain), DuckDB (1 and 4 threads), polars-hash, DataFusion, numba,
numba parallel, and the threaded prototype. The generated tables held nulls (63),
non ASCII text (47), nested types (36), maps (12), chunked columns (18). The C
SHA-256 also matched `hashlib` on 1,500 random inputs of 0 to 5,000 bytes, on both paths.
Every benchmark run below gave the same digest per table as today's code, and the
5M `events` and `detail` digests (`sha256:95a90973...`, `sha256:b412fee5...`) are the
ones the GitHub ladder recorded.

## Results

### The hash step alone (microseconds per row, lines already built)
One call per 32,768 row slice, as the engine calls it. "Whole" is one call for the table.

| candidate | events 1M | detail 1M | events 5M | detail 5M | whole table, 5M events |
|---|---|---|---|---|---|
| hashlib (today) | 1.18 | 1.46 | 1.17 | 1.43 | |
| **C, SHA extensions** | **0.10** | **0.13** | **0.10** | **0.13** | 0.07 |
| C, plain | 0.49 | 0.93 | 0.95 | 0.89 | |
| DuckDB, 1 thread | 1.58 | 1.94 | 2.68 | 1.86 | 1.46 |
| DuckDB, 4 threads | 1.62 | 1.90 | 1.56 | 1.97 | 1.42 |
| polars-hash | 0.46 | 0.47 | 0.45 | 0.46 | 0.28 |
| DataFusion | 0.08 | 0.11 | 0.08 | 0.09 | 0.03 (uses every core) |
| numba | 0.50 | 0.97 | 0.48 | 0.90 | 0.47; 0.13 on 4 threads |
| *building the lines (today, not replaced)* | *0.93* | *1.88* | *1.02* | *1.74* | |

DuckDB is slower than hashlib: its `sha256()` alone is 0.93 s per million lines and
does not use the SHA extensions, the hex parsing adds 0.47 s, and one in memory Arrow
array is scanned on one thread whatever `threads` says (`duck_split.txt`). numba pays
2 to 4 s of compiling in every new process (cold column in `bench-results.jsonl`).

### End to end `fingerprint_arrow` (seconds, median of 3; peak MB)

| | events 1M | detail 1M | events 5M | detail 5M (3.89M rows) |
|---|---|---|---|---|
| today, one process | 2.15 (150) | 2.61 (196) | 10.63 (304) | 12.04 (579) |
| **today, 4 helpers (default)** | **1.40 (566)** | **1.31 (743)** | **3.27 (752)** | **4.08 (1,181)** |
| C, one process | 1.19 (142) | 1.54 (196) | 5.18 (297) | 7.32 (574) |
| DataFusion, one process | 1.13 (482) | 1.63 (630) | 5.89 (1,833) | 7.20 (2,476) |
| polars-hash, one process | 1.62 (176) | 1.93 (226) | 7.13 (333) | 7.82 (608) |
| numba, one process | 1.47 (221) | 1.97 (272) | 7.46 (375) | 9.53 (647) |
| DuckDB, one process | 2.93 (169) | 2.99 (214) | 12.96 (324) | 14.25 (602) |
| hashlib on 4 threads | 1.44 (212) | 1.45 (345) | 6.67 (398) | 7.16 (607) |
| C on 4 helpers | 1.01 (558) | 1.21 (724) | 2.17 (749) | 2.30 (1,185) |
| C on 2 threads | 0.57 (168) | 0.76 (263) | 3.02 (342) | 3.83 (577) |
| **C on 4 threads** | **0.32 (192)** | **0.41 (321)** | **1.56 (403)** | **2.01 (587)** |
| C on 8 threads | 0.24 (246) | 0.26 (487) | 0.88 (465) | 1.34 (722) |

Three things follow.

1. **Once the hash is native, the lines are the cost.** In one process the C kernel
   halves the time (10.63 s to 5.18 s at 5M `events`); what is left is the Arrow
   compute that builds the lines, about 1.0 microsecond a row for 5 columns and 1.7
   for 9.
2. **The kernel's real gain is that it frees the GIL.** Arrow's kernels release it and
   ctypes releases it for the C call, so one process can hash slices on threads. With
   hashlib, threads gain nothing (6.67 s: the GIL). With the kernel, 4 threads are 2.1
   times faster than today's 4 helpers at 5M rows (3.57 s against 7.35 s for both
   tables), and use half the memory (peaks of 403 and 587 MB against 752 and 1,181 MB), with no process start,
   no IPC copy and none of F-038's process safety code.
3. **It composes with the helpers, but threads are better.** The kernel appended to
   the engine file (`native_patch.py`) runs in the helpers unchanged (same digest):
   2.17 s and 2.30 s at 5M, but each helper still pays about 0.5 s to start and the
   rows are copied through a pipe.

### The E-06 job on the dev box
`tests/experiments/e06_scale.py --backend pandas`, 3 repeats, `ubunye run` in a fresh
process. "Native" is the same `--lineage` run with `e10_native.install()` put in by
`sitecustomize.py` (4 threads, C kernel); nothing else differs. Every record's digests
equal the unpatched ones.

| rows | plain s | Ubunye s | `--lineage` today s | `--lineage` native s | ratio today | ratio native | record `hash_seconds` today / native | peak MB today / native |
|---|---|---|---|---|---|---|---|---|
| 1,000,000 | 1.44 | 2.02 | 4.84 | 2.56 | 3.36x | 1.78x | 2.69 / 0.54 | 813 / 399 |
| 5,000,000 | 4.95 | 4.46 | 10.75 | 7.03 | 2.17x | **1.42x** | 6.30 / 2.60 | 1,235 / 992 |

(The native runs ran after the others, not interleaved.)

### On the E-06 ladder (GitHub `ubuntu-latest`, 4 vCPU): an estimate, not a run
The ladder's pandas `--lineage` time is the Ubunye run plus the record's hashing,
within 0.1 s (run 36673389425: at 5M, 11.93 s = 2.32 + 9.60). The runner's plain job
is 2.6 times faster than the dev box's (1.88 s against 4.95 s at 5M), but its 4 vCPU
hash 1.5 times slower than the dev box's 4 helpers (9.60 s against 6.30 s). Scaling the
native hashing the same way (2.60 s times 1.5, about 4.0 s):

| rows | ratio on the ladder now | estimate, native on 4 threads | estimate, native and no input hash |
|---|---|---|---|
| 5,000,000 | 6.35x | about 3.4x | about 2.3x |
| 50,000,000 | 4.90x | about 2.6x | about 1.8x |

(50M: today's 71.9 s of hashing scaled by the same 0.42 as at 5M.)

The bound is not reached on the runner: 1.5 times plain at 5M leaves about 0.5 s of
hashing for about 9 million rows, 55 nanoseconds a row across 4 vCPU. The hash is now
about 0.1 microseconds a row per core; the lines are about 1.0 to 1.7. To get there
the lines themselves would have to be built natively too, 5 to 10 times faster than
Arrow compute builds them (today each column is cast to text, joined and searched in
several passes). Only a run of the ladder with the kernel built on Linux can confirm
the estimate.

### Option 1: helpers kept alive across the tables of a run
Prototype `keepalive.py`: 4 helpers started once, each answering one Arrow stream
after another. Hashing `events` then `detail`, median of 3:

| rows | fresh helpers per table (today) | kept alive (start included) | saved | one helper, start to first answer |
|---|---|---|---|---|
| 1,000,000 | 2.54 s | 1.52 s | 1.03 s | 0.48 s |
| 5,000,000 | 6.51 s | 5.52 s | 1.00 s | 0.46 s |

Same digests. About 1 s a run on this job, a fixed saving that does not grow with
size: at 5M on the ladder about 11.9 s to 10.9 s (6.35x to about 5.8x). It needs a
process pool with a life of its own (who stops it, what happens across threads and on
Ctrl+C), which F-038's review showed is where the risk is. With the native kernel on
threads there are no helpers to keep.

### Option 2: no input hash when a pinned Delta version names the input
From the ladder's records (`ladder_split.jsonl`, run 36673389425, medians), the input
(`events`) is about half the hashing:

| backend | rows | `--lineage` / plain now | input hash s | without the input hash |
|---|---|---|---|---|
| pandas | 5,000,000 | 6.35x | 4.59 of 9.60 | 3.90x |
| pandas | 50,000,000 | 4.90x | 34.29 of 71.92 | 3.00x |
| Spark | 5,000,000 | 1.92x | 6.76 of 12.81 | **1.52x** |
| Spark | 50,000,000 | 3.78x | 38.93 of 80.16 | 2.67x |

An estimate only: E-06 reads parquet, not Delta, so this skip would not apply to E-06
as it is run. It is the one option here that helps Spark (the native kernel is for
pandas and Arrow; Spark hashes with its own `to_json` and `sha2`). It gives up the input
receipt ADR 006 promises, so it can only be an opt in (F-039).

## Install, licence, platforms

| candidate | installed size | licence | wheels: Windows / Linux / macOS | Python 3.10 to 3.13 | composes with helpers / threads |
|---|---|---|---|---|---|
| C kernel | 0.09 MB (one DLL) | ours | must be built: none exists; this box has MSVC; gcc and clang on the runners | no Python API used, so one binary per platform serves every version | yes (measured) / yes (measured, the GIL is released) |
| DataFusion 54.0.0 | 134 MB | Apache 2.0 | win amd64 (no arm64) / manylinux x86_64 and aarch64 (no musl) / macOS x86_64 and arm64 | yes (abi3 from cp310) | would start its own thread pool in every helper; threads not measured |
| polars-hash 0.9.3 + polars | 203 MB | MIT (repository; the wheel carries no licence text) | all but win arm64 | yes (abi3) | not measured |
| numba 0.67 + llvmlite | 143 MB | BSD 2 clause, LLVM Apache 2.0 with exception | no macOS x86_64 wheel; no musl | yes | compiles again in every new process (2 to 4 s) |
| DuckDB 1.5.6 | 37 MB | MIT | all but musl | yes | slower than today; not worth it |

## Conclusion
- **Hash step:** yes. A native per row SHA-256 gives the identical digest (0
  differences over 66 tables and every benchmark table) and is about 10 times
  faster than one hashlib call per row: 0.10 to 0.13 microseconds a row with the SHA
  extensions, 0.5 to 0.9 in plain C. DataFusion is as fast but is 134 MB and uses a lot
  of memory. DuckDB is slower than today.
- **End to end:** not by itself. After the hash, building the lines costs 1.0 to
  1.7 microseconds a row. The large gain comes from threads, which the native call makes
  possible: on the dev box the E-06 job with `--lineage` went from 2.17x to 1.42x plain
  at 5M rows (under the 1.5x bound), and to 1.78x at 1M. On the 4 vCPU ladder runner the
  estimate is about 3.4x at 5M and 2.6x at 50M. That halves today's overhead, but it
  does not meet 1.5x.
- **Cheaper options:** keeping helpers alive saves about 1 s a run, and is not worth
  the process work if threads replace the helpers. Skipping the input hash for a pinned
  Delta read would roughly halve the hashing on both backends, and on Spark alone it
  reaches 1.52x at 5M. It changes ADR 006's promise, so it can only be an opt in.

## Recommendation
1. **Build the kernel as an optional extra, not a required dependency.** A tiny C
   library (`rows_v1_lanes`: SHA-256 plus lane sums over Arrow's offsets and data),
   shipped as its own small wheel (for example `ubunye-rowhash`) and pulled in by
   `pip install ubunye-engine[fast-hash]`. It uses no Python C API, so one binary per
   platform serves every Python; build it with cibuildwheel for win amd64 and arm64,
   manylinux x86_64 and aarch64, musllinux, macOS x86_64 and arm64. Add an ARMv8
   SHA-2 path next to the x86 one; without it ARM gets plain C (about 0.5 to 0.9
   microseconds a row, still free of the GIL, so still faster on threads than today).
2. **When the kernel loads, hash on threads in the calling process** (`e10_native.py`
   is the shape): 4 threads by default under the same `UBUNYE_HASH_WORKERS` cap. The
   helper processes stay as the path without the kernel, unchanged, so a digest never
   depends on whether the extra is installed. At load, check the kernel against
   known SHA-256 vectors and a known table digest; if it fails, do not use it.
3. **Keep `hashlib` as the reference.** The property test of the fast path must run
   with and without the kernel, and CI needs a job with the extra installed.
4. **Do not use DuckDB, numba or polars-hash for this.** DataFusion is the only fallback
   worth keeping in mind if building wheels is not wanted: as fast, but a 134 MB extra
   for one function and 3 to 4 times the memory.
5. **Measure the ladder, then decide on the next step.** Build the kernel with gcc in
   the scale ladder workflow and run pandas at 5M and 50M. If about 3.4x holds, the next
   cost is building the lines. That would need a native builder for the common kinds
   (integers, strings, booleans, dates, plain doubles), held byte for byte to
   `_members` by the same property test. It is a larger piece of C, and it is where
   `rows-v1` itself becomes the limit.
6. **Skip keeping helpers alive.** Offer the pinned Delta input skip only as an opt in
   (it is the lever for Spark), with the record saying the input was named by version,
   not hashed.

## Risks
- **A second SHA-256 to trust.** A wrong kernel means wrong digests with no error. The
  load time self check, the property test on both paths, and the fallback to hashlib
  (never to a guess) are what hold it. The code is small (about 150 lines) and
  checked against hashlib on every length up to 5,000 bytes on both paths.
- **A build matrix to maintain,** and wheels that must move in step with the engine.
  With no Python API, the binary does not change between Python versions.
- **Threads in a user's process.** Four threads that release the GIL do not change the
  user's code, but they do share its memory: peak memory went down in every measured
  case (5M: 1,235 MB to 992 MB for the E-06 run).
- **The dev box is not the runner.** 1.42x is a dev box number (8 cores, slow plain job);
  the runner estimate (about 3.4x) must be measured before any claim.
- **Nothing here helps Spark.** Spark hashes with `to_json` and `sha2` inside the JVM;
  its levers are F-041, the input skip, and hashing within the write's own pass.

## Not measured
- The ladder with the kernel (estimate only; needs a Linux build in the workflow).
- ARM machines, and macOS.
- DataFusion or polars-hash on threads (only the C kernel and hashlib were run on threads).
- Numba with `cache=True` (its compile cost across processes).
- The Delta input skip in a real run (E-06 reads parquet).
