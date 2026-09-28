# The proving ground

Ubunye says a task runs the same on a laptop, on Spark, on Databricks and on the
clouds. The proving ground is how that stops being a claim: the same workload is run in
each place, and the run records (ADR 006) are compared, dimension by dimension. An
environment that did not run says so.

## The contract

For one workload, every environment is compared with a **reference** environment:

| Dimension | PASS means | Compared from the record |
|---|---|---|
| execute | the run succeeded | `status` |
| identity | it is the same workload | `code_hash`, and the same output names |
| inputs | every input held the same rows | each input's `data_hash`, where inputs were hashed |
| data | every output holds the same rows | each output's `data_hash` (`rows-v1`), same `hash_method` |
| schema | every output has the same columns and types | each output's `schema_hash` (canonical type names) |
| rows | every output has the same number of rows | each output's `row_count` |

`rows-v1` covers every row, ignores row and column order, writes timestamps as UTC
instants and tells null from NaN, so **data PASS is semantic equality of the rows and
their types**.

Verdicts: PASS, FAIL, PARTIAL (some of it could not be compared, such as a record
without a hash or hashed by an older method), NOT RECORDED, and for an environment that
did not execute, NOT RUN or UNSUPPORTED. An environment that was expected and left no
evidence is NOT RUN, never PASS.

**Not compared, on purpose:** the config hash (it covers paths, and a cloud reads
`s3://` where a laptop reads a folder), and the bytes of written files (Spark and pandas
write valid Parquet differently; byte equality is not a promise Ubunye makes).

## Commands

```bash
# after a run with --lineage, turn its record into an observation
ubunye prove observe --workload c01-portable-etl --env spark-local \
  -d pipelines -u proving -p c01 -t etl -o evidence

# a record from elsewhere (a cloud run's artifact)
ubunye prove observe --workload c01-portable-etl --env aws-glue --kind cloud \
  --provider aws --runtime "Glue 5.0" --record glue.json -o evidence

# an environment that did not run, and why
ubunye prove skip --workload c01-portable-etl --env azure --reason "no subscription" -o evidence

# compare everything with the reference; exit 1 if any executed environment disagrees
ubunye prove report evidence --workload c01-portable-etl --reference spark-local \
  --expect pandas-local,databricks,aws-glue,gcp-dataproc,azure --json m.json --md m.md
```

An observation is one JSON file, `evidence/<workload>/<environment>.json`: the run
record, the platform, the cost with where the figure came from (`actual`,
`provider_estimate`, `ubunye_estimate`, `local` or `unknown`; an estimate is never shown
as a bill), and a link to the run.

## Workloads

| Id | What it protects | Where |
|---|---|---|
| `c01-portable-etl` | a portable (Narwhals) join, filter, null group keys, integer money, timestamps cut to a day | `examples/proving/c01_portable_etl` |

What the proving ground has found is recorded as findings in `tasks/hardening/`; the
first was F-021 (a Spark session's day depended on the machine's time zone, ADR 007).
