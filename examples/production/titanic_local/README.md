# Titanic — Local Production Example

A production-grade reference pipeline that runs the Ubunye Engine locally with a
real Spark session, against the canonical Kaggle Titanic training set.

This example is one half of a portability demonstration: the same
`transformations.py` module is reused verbatim by
[`../titanic_databricks/`](../titanic_databricks/). Only the config and
deployment wrapper differ between the two runtimes.

---

## What it does

The pipeline reads the Titanic CSV, computes survival rate per passenger class,
and writes a Parquet dataset partitioned by `mode` and `dt`. Output columns:

| Column            | Type  | Description                                   |
|-------------------|-------|-----------------------------------------------|
| `Pclass`          | int   | Passenger class (1, 2, 3)                     |
| `passenger_count` | int   | Total passengers in the class                 |
| `survivors_count` | int   | Number who survived                           |
| `survival_rate`   | float | `survivors_count / passenger_count`, 4 d.p.   |

---

## Directory layout

```
titanic_local/
├── pipelines/                                # usecase_dir passed to the CLI
│   └── titanic/analytics/survival_by_class/
│       ├── config.yaml                       # pipeline config (dev/prod profiles)
│       └── transformations.py                # Task subclass + pure pandas/Spark helpers
├── data/                                     # gitignored; CSV fetched at runtime
├── expected_output/
│   └── survival_by_class.parquet             # golden output (committed)
├── scripts/
│   ├── fetch_titanic.sh                      # canonical CSV download
│   └── validate_output.py                    # compares pipeline output to golden
├── tests/
│   ├── conftest.py                           # puts task dir on sys.path
│   └── test_transformations.py               # pandas unit tests (no Spark)
└── README.md
```

---

## Prerequisites

| Requirement       | Version            | Why                                        |
|-------------------|--------------------|--------------------------------------------|
| Python            | 3.11 (CI target)   | Matches the workflow; 3.9+ also works.     |
| Java              | 11                 | Required by PySpark.                       |
| `ubunye-engine`   | `>=0.1.3`, `[spark]` extra | Provides the CLI and Spark backend. |
| `pandas`, `pyarrow` | latest          | Golden-file validation + unit tests.       |

Install from the repo root:

```bash
pip install -e ".[spark,dev]" pandas pyarrow
```

---

## How to run locally

```bash
# 1. Fetch the Titanic CSV (~60 KB) from the DataScienceDojo public mirror.
#    Safe to re-run; overwrites the existing file.
bash examples/production/titanic_local/scripts/fetch_titanic.sh

# 2. Declare the input/output paths. The config demands these via Jinja —
#    no silent defaults, so misconfiguration fails loudly.
export TITANIC_INPUT_PATH="file://$(pwd)/examples/production/titanic_local/data/titanic.csv"
export TITANIC_OUTPUT_PATH="file://$(pwd)/examples/production/titanic_local/output"

# 3. Validate config before touching Spark.
ubunye validate \
  -d examples/production/titanic_local/pipelines \
  -u titanic -p analytics -t survival_by_class \
  -dt 2026-04-15

# 4. Show the resolved I/O plan.
ubunye plan \
  -d examples/production/titanic_local/pipelines \
  -u titanic -p analytics -t survival_by_class \
  -dt 2026-04-15 -m DEV

# 5. Run the pipeline (starts a local SparkSession, writes Parquet).
ubunye run \
  -d examples/production/titanic_local/pipelines \
  -u titanic -p analytics -t survival_by_class \
  -dt 2026-04-15 -m DEV --lineage

# 6. Validate the output against the committed golden parquet.
python examples/production/titanic_local/scripts/validate_output.py \
  --actual "examples/production/titanic_local/output/mode=DEV/dt=2026-04-15" \
  --expected "examples/production/titanic_local/expected_output/survival_by_class.parquet"
```

### Expected output

```
OK: 3 rows match golden.
```

The on-disk layout after a DEV run:

```
output/mode=DEV/dt=2026-04-15/
├── _SUCCESS
├── part-00000-....snappy.parquet
└── ...
```

The `--lineage` flag writes a JSON run record under
`examples/production/titanic_local/.ubunye/lineage/` which `ubunye lineage show`
can render.

---

## How to test

```bash
pytest examples/production/titanic_local/tests -v
```

Five unit tests cover the pandas helper that mirrors the Spark aggregation:

| Test                                       | Purpose                                                    |
|--------------------------------------------|------------------------------------------------------------|
| `test_schema_matches_contract`             | Output columns match `OUTPUT_COLUMNS`.                     |
| `test_aggregation_values`                  | Hand-computed toy fixture produces known values.           |
| `test_missing_column_raises`               | Missing input column surfaces as `ValueError`.             |
| `test_deterministic_ordering`              | Output is sorted by `Pclass` regardless of input order.    |
| `test_golden_matches_canonical_titanic_stats` | Committed golden file matches Kaggle Titanic known stats.  |

No SparkSession, no Java, no network. The Spark version of the logic is
covered by the end-to-end CI run.

---

## Config anatomy

See `pipelines/titanic/analytics/survival_by_class/config.yaml`. Highlights:

| Concept                     | Where                                                             |
|-----------------------------|-------------------------------------------------------------------|
| Engine profiles             | `ENGINE.profiles.DEV`, `ENGINE.profiles.PROD` (shuffle partitions) |
| Runtime paths               | `{{ env.TITANIC_INPUT_PATH }}` / `{{ env.TITANIC_OUTPUT_PATH }}`  |
| Jinja templating            | `{{ dt | default('latest') }}`, `{{ mode | default('DEV') }}`     |
| CSV reader                  | `format: s3` (generic path reader) + `file_format: csv`           |
| Partitioned Parquet writer  | `format: s3` + `file_format: parquet` + templated path            |

Profile keys are uppercase (`DEV`, `PROD`) to match the CLI's default `-m DEV`.
`merged_spark_conf` silently falls back to the base config if the mode does
not match any profile key — see `docs/config/engine.md`.

---

## Troubleshooting

| Symptom                                                   | Cause / fix                                               |
|-----------------------------------------------------------|-----------------------------------------------------------|
| `Environment variable 'TITANIC_INPUT_PATH' is not set`    | Export the two env vars shown in step 2.                  |
| `No module named 'pyspark'`                               | Install the Spark extra: `pip install -e ".[spark]"`.     |
| `JAVA_HOME is not set`                                    | Install JDK 11 and export `JAVA_HOME`.                    |
| `OK: 0 rows match golden.`                                | Pipeline wrote to a different path — double-check `dt`/`mode` in the `validate_output.py` call. |
| Output parquet missing `_SUCCESS`                         | Previous run was interrupted. Re-run with the same args — `mode: overwrite` replaces the partition. |

---

## Why two implementations of the same aggregation?

`transformations.py` exposes both a Spark and a pandas version of the same
computation. The Task uses Spark; the unit tests use pandas. This is a
deliberate pattern — not a workaround — because it lets the business logic be
validated without a JVM, on any CI runner, in milliseconds. The same pattern
appears in dbt-core's adapter tests, Great Expectations' expectations, and
most Databricks reference examples.

The contract is: both functions return the same columns, in the same order,
with the same dtypes. `OUTPUT_COLUMNS` is the shared definition.
