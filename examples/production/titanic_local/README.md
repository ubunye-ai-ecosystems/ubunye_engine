# Titanic — Local Production Example

A production-grade reference pipeline that runs the Ubunye Engine locally with a
real Spark session, or on the pandas backend with no Java, against the canonical
Kaggle Titanic training set. Both give the same data hash in the run record.

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
│       └── transformations.py                # Task subclass, Narwhals aggregation
├── data/                                     # gitignored; CSV fetched at runtime
├── expected_output/
│   └── survival_by_class.parquet             # golden output (committed)
├── scripts/
│   ├── fetch_titanic.sh                      # canonical CSV download
│   ├── same_receipt.sh                       # Spark and pandas run records agree
│   └── validate_output.py                    # compares pipeline output to golden
├── tests/
│   ├── conftest.py                           # sys.path + session-scoped SparkSession
│   └── test_transformations.py               # the transform on Spark and pandas
└── README.md
```

---

## Prerequisites

| Requirement       | Version            | Why                                        |
|-------------------|--------------------|--------------------------------------------|
| Python            | 3.10 to 3.13       | 3.11 is the CI target.                     |
| Java              | 17                 | Required by PySpark (not by the pandas run). |
| `ubunye-engine`   | `>=0.6.0`, `[spark]` and `[pandas]` extras | The CLI and both backends. |
| `narwhals`        | `>=2`              | The transform is written with it.          |
| `pyarrow`         | latest             | Parquet read/write for golden validation.  |

Install from the repo root:

```bash
pip install -e ".[spark,dev]" pyarrow
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
`examples/production/titanic_local/pipelines/.ubunye/lineage/` which
`ubunye lineage show` can render.

### The same folder on pandas (no Java)

The transform is written once with Narwhals, so the same folder runs on the
pandas backend, with no Spark and no Java, into the same output:

```bash
# 7. Run on pandas: same task, same config, same output path.
ubunye run \
  -d examples/production/titanic_local/pipelines \
  -u titanic -p analytics -t survival_by_class \
  -dt 2026-04-15 -m DEV --lineage --backend pandas

# 8. The same golden check passes.
python examples/production/titanic_local/scripts/validate_output.py \
  --actual "examples/production/titanic_local/output/mode=DEV/dt=2026-04-15" \
  --expected "examples/production/titanic_local/expected_output/survival_by_class.parquet"

# 9. And the two run records agree, hash for hash.
bash examples/production/titanic_local/scripts/same_receipt.sh \
  examples/production/titanic_local/pipelines titanic analytics survival_by_class
```

```
  config_hash: sha256:...  (unchanged)
  Output 'survival_by_class':
      row_count: 3  (unchanged)
      schema_hash: sha256:...  (unchanged)
      data_hash: sha256:...  (unchanged)
OK: Spark and pandas left the same receipt for titanic/analytics/survival_by_class
```

CI runs all nine steps on every change (`.github/workflows/local_pipeline.yml`).

---

## How to test

```bash
pytest examples/production/titanic_local/tests -v
```

Six unit tests run against a session-scoped local `SparkSession` fixture
(and one also on pandas), so the production code is the code under test:

| Test                                       | Purpose                                                    |
|--------------------------------------------|------------------------------------------------------------|
| `test_schema_matches_contract`             | Output columns match `OUTPUT_COLUMNS`.                     |
| `test_aggregation_values`                  | Hand-computed toy fixture produces known values.           |
| `test_missing_column_raises`               | Missing input column surfaces as `ValueError`.             |
| `test_deterministic_ordering`              | Output is sorted by `Pclass` regardless of input order.    |
| `test_golden_matches_canonical_titanic_stats` | Golden parquet matches Kaggle Titanic known stats.      |
| `test_pandas_gives_the_same_rows_as_spark` | The same function on pandas gives Spark's rows, in order.  |

Requires Java 17 on PATH (PySpark prerequisite).

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
When a config defines profiles, the mode must name one of them: a mode that
matches none stops the run with an error listing the profiles (see
`docs/config/engine.md`). The pandas backend reads `spark.sql.session.timeZone`
from the same settings and ignores the rest.

---

## Troubleshooting

| Symptom                                                   | Cause / fix                                               |
|-----------------------------------------------------------|-----------------------------------------------------------|
| `Environment variable 'TITANIC_INPUT_PATH' is not set`    | Export the two env vars shown in step 2.                  |
| `No module named 'pyspark'`                               | Install the Spark extra: `pip install -e ".[spark]"`.     |
| `JAVA_HOME is not set`                                    | Install JDK 17 and export `JAVA_HOME`.                    |
| `OK: 0 rows match golden.`                                | Pipeline wrote to a different path — double-check `dt`/`mode` in the `validate_output.py` call. |
| Output parquet missing `_SUCCESS`                         | Previous run was interrupted. Re-run with the same args — `mode: overwrite` replaces the partition. |

---

## Single implementation

`transformations.py` exposes one function, `compute_survival_by_class`,
written once with [Narwhals](https://narwhals-dev.github.io/narwhals/). It takes
and returns the engine's own frame, so it runs unchanged on Spark (here and on
Databricks) and on pandas. The unit tests run it on both and require the same
rows, so there is no parallel implementation to keep in lock-step. Two things
make the engines agree: `Survived` is cast to a 64 bit integer before the sum
(Spark widens sums, pandas does not), and the survival rates never land exactly
halfway when rounded (Spark rounds half up, pandas half to even). See
[Execution backends](../../../docs/backends.md#one-transform-for-every-engine).
