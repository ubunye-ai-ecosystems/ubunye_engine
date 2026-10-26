# Titanic Multi-Task Pipeline (Local)

Two-task pipeline demonstrating **sequential task chaining** with Ubunye's
`run_pipeline()` / CLI `-t task1 -t task2` pattern.

## Pipeline shape

```
titanic.csv  -->  [clean_data]  -->  cleaned.parquet  -->  [aggregate]  -->  summary.parquet
```

| Task | Reads | Writes | Logic |
|------|-------|--------|-------|
| `clean_data` | Titanic CSV | `cleaned/` Parquet | Drop null survival/class, add `survived_label`, `age_group` |
| `aggregate` | `cleaned/` Parquet | `summary/` Parquet | Group by (Pclass, age_group), compute survival rate |

Task 2's input path matches task 1's output path via the same `TITANIC_CLEAN_PATH`
env var and `{{ dt }}` template. This is the simplest form of task chaining: the
intermediate dataset is a filesystem path both configs agree on.

## What this example exercises

- **`ubunye run -t task1 -t task2`**: sequential multi-task execution through a
  single backend, Spark or pandas.
- **One transform for every engine**: both tasks are written with Narwhals, so
  the pipeline runs on Spark and on the pandas backend (no Java) and each task
  leaves the same data hash on both (checked in CI).
- **Sibling-module isolation**: each task has its own `transformations.py`. The
  engine's `_with_task_dir_on_path` context manager evicts task-local modules
  from `sys.modules` between tasks so the second task doesn't accidentally import
  the first task's code.
- **Lineage across tasks**: `--lineage` writes one run record per task, each
  with its own `run_id`. The `aggregate` record's input location is the
  `clean_data` record's output location, so `ubunye lineage trace` shows how
  the two connect.

## Running locally

Run these from this folder (`examples/production/titanic_multitask_local`).

```bash
# 1. Fetch Titanic CSV into data/
bash scripts/fetch_titanic.sh

# 2. Set env vars
export TITANIC_INPUT_PATH="file://$(pwd)/data/titanic.csv"
export TITANIC_CLEAN_PATH="file://$(pwd)/output/cleaned"
export TITANIC_SUMMARY_PATH="file://$(pwd)/output/summary"

# 3. Validate both tasks
ubunye validate -d pipelines -u titanic -p pipeline -t clean_data -t aggregate

# 4. Dry run: what each task reads and writes
ubunye plan -d pipelines -u titanic -p pipeline -t clean_data -t aggregate

# 5. Run the pipeline on Spark
ubunye run -d pipelines -u titanic -p pipeline -t clean_data -t aggregate -m DEV --lineage

# 6. The same pipeline on pandas, no Java, into the same outputs
ubunye run -d pipelines -u titanic -p pipeline -t clean_data -t aggregate -m DEV --lineage \
  --backend pandas

# 7. Each task's Spark and pandas run records agree, hash for hash
for task in clean_data aggregate; do
  bash scripts/same_receipt.sh pipelines titanic pipeline "$task"
done
```

Each `run` prints `[OK] Run complete for clean_data` and then
`[OK] Run complete for aggregate`. With no `-dt`, both paths end in
`dt=latest`. The run records go under `pipelines/.ubunye/lineage/`.
Step 7 ends with one line per task:

```
OK: Spark and pandas left the same receipt for titanic/pipeline/clean_data
OK: Spark and pandas left the same receipt for titanic/pipeline/aggregate
```

## Running tests

From the repo root:

```bash
pip install -e ".[spark,dev]"
pytest examples/production/titanic_multitask_local/tests -v
```

Eight tests, on a local SparkSession and on pandas. They need Java 17 or newer.
