# ADR 005: One transform for every engine: Narwhals, detected not declared

**Status:** accepted, 0.7.0

## Context

A task folder runs on Spark or on pandas (ADR 001 to 004), but only if its
transform does. The Titanic examples call the Spark API (`F.col`, `groupBy`,
`F.when`), so on pandas they fail on their first line. "The same folder runs
anywhere" was true of the config and not of the code.

Two ways to write a transform once were tried on the Titanic example's own
logic (clean, then survival by class and age group), on the real 891-row file,
each result compared with the Spark reference by the run record's content hash
(ADR 006), which covers every value and type:

| Candidate | Spark | pandas | Verdict |
|---|---|---|---|
| **Narwhals** (one dataframe API over pandas and PySpark) | same hash as the reference | same hash as the reference, once the sum is cast (below) | **ships** |
| **SQL**: Spark SQL on Spark, DuckDB on pandas | same hash | clean step same, aggregate different: `SUM(BIGINT)` is `DECIMAL(38,0)` in DuckDB, `bigint` in Spark | not shipped |

Narwhals keeps each engine's own rules, and two differences are known. A sum of
whole numbers is `bigint` on Spark and keeps the column's type on pandas
(`int32` for small numbers read from CSV), so the aggregate matched only once
`Survived` was cast to `Int64` (best before the group by: inside `agg` pandas
takes a slow path). And a value exactly halfway rounds half up on Spark and
half to even on pandas (0.125 to two places: 0.13 and 0.12); the Titanic rates
never land on a tie. Both are small and local, both are documented, and the
run record's hash, which covers every value and type, shows either one when it
is missed.

The SQL result is not a bug to fix but a property of SQL: two engines agree on
the language and differ on types. Every aggregate a user writes would need a
cast to be safe, and the engine could not promise parity. SQL returns in 0.7
with the DuckDB backend, where one engine runs it.

The spike also found a read bug: the pandas backend unescaped doubled quotes in
CSV values where Spark does not, so 53 Titanic names differed before any
transform ran. That was fixed first (a port of Spark's CSV parser), because a
portability check is worthless on inputs that already differ.

## Decision

1. **Narwhals is the supported way to write one transform for every engine.**
   The transform wraps what it gets and may return the Narwhals frame:

   ```python
   import narwhals as nw

   class SurvivalByGroup(Task):
       def transform(self, sources):
           people = nw.from_native(sources["titanic"])
           ...
           return {"summary": summary}  # a Narwhals frame is fine
   ```

   The engine unwraps any returned frame whose class offers `to_native()`
   (Narwhals does), so the caller, writers, hooks and lineage see the engine's
   own frames (ADR 004). The engine never imports Narwhals; you install it
   when you use it.

2. **Portability is detected, not declared.** There is no config field for it.
   `ubunye plan` reads `transformations.py` (without running it) and reports
   which dataframe API it is written for, from its imports: `pyspark`,
   `narwhals`, `pandas`, `polars`, or nothing detected. One Spark import is
   enough to say `pyspark`. Imports under `if TYPE_CHECKING:` and optional
   ones (inside a `try` that catches ImportError) do not count.

3. **A mismatch is a warning, not a problem.** `plan --backend pandas` on a
   transform written for pyspark warns, with the fix; so does a pandas
   transform on a Spark backend. It is a warning because imports are evidence,
   not proof: an import can be unused.

## Consequences

- A task can move between engines by writing its transform once, and the run
  records prove it: same data hash on both (tested end to end in
  `tests/integration/test_narwhals_transform_parity.py`). Where the engines
  type a result differently (a sum of whole numbers), the transform casts.
- `plan --json` carries `transform.frame_api` and `transform.frame_imports`,
  so an agent can choose a backend a task can actually run on.
- Existing Spark API and pandas transforms are untouched; they keep running on
  the engine they were written for.
- Only `transformations.py` itself is read; a helper module it imports is not.
