# F-043: On Spark, each expectations pass computes the output again, and the write computes it once more

**Status:** open
**Severity:** minor for a cheap plan; major for a plan that is costly to compute
**Source:** scale-runner, experiment E-06 (2026-09-29)
**Promise:** 7 (the core stays small)

## What happens
`CONFIG.expectations` are checked before any write (right: nothing is written when
a `fail` rule breaks). On Spark each check is an aggregation over the lazy output,
so each is a new Spark job that reads the sources and redoes the transform; nothing
is persisted, and the write then computes the output once more.

E-06 job with these rules, all passing: `detail`: `not_null: id`, `unique: id`,
`between: {column: qty, min: 1}`, `row_count: {min: 1}`; `summary`: `not_null:
region_name`. That is three passes (the row rules of `detail` in one, `unique` in
another, `summary`'s rule in a third), each reading all of `events` again. From
Spark's event log: 18 jobs against 7, and `events` records read 5 times the input
against 2 times.

| rows | where, repeats | plain s | Ubunye s | Ubunye + expectations s | / plain | input records read, Ubunye / + expectations |
|---|---|---|---|---|---|---|
| 1,000,000 | dev box, 2 | 13.95 | 13.55 | 18.94 | 1.36x | 2,001,800 / 5,004,500 |
| 5,000,000 | dev box, 2 | 16.38 | 15.55 | 25.18 | 1.54x | 10,001,800 / 25,004,500 |
| 20,000,000 | GitHub `ubuntu-latest`, 3 | 19.60 | 19.21 | 28.77 | 1.47x | 40,001,800 / 100,004,500 |

(Medians. GitHub run 36584681630.) The JVM also peaks higher with the checks (5.0 GB
against 2.4 GB at 20M; local mode, so this is executor memory for the `unique`
aggregation, not the driver).

Nothing is pulled to the driver: each check collects one row of counts (Spark sent
154 kB of task results to the driver at 1M rows, all task metadata, flat in rows).

## Repro
`python tests/experiments/e06_scale.py --backend spark --rows 5000000 --variants plain,ubunye,expect`

## Expected
Checking costs one pass over the output, or less, and the write does not compute
the output again. Persisting a checked output (`MEMORY_AND_DISK`) for the checks and
the write would do both; the checks could also share one job (the `unique` count
and the row rules in one aggregation). The trade is executor memory or disk for
the persisted rows, which a user may want to switch off.
