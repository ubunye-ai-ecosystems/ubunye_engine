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
- **Lineage across tasks**: `--lineage` records provenance for both tasks under
  a shared `run_id`.

## Running locally

```bash
# 1. Fetch Titanic CSV
bash scripts/fetch_titanic.sh

# 2. Set env vars
export TITANIC_INPUT_PATH="file://$(pwd)/data/titanic.csv"
export TITANIC_CLEAN_PATH="file://$(pwd)/output/cleaned"
export TITANIC_SUMMARY_PATH="file://$(pwd)/output/summary"

# 3. Validate both tasks
ubunye validate -d pipelines -u titanic -p pipeline -t clean_data -t aggregate

# 4. Run the pipeline
ubunye run -d pipelines -u titanic -p pipeline -t clean_data -t aggregate -m DEV --lineage

# 5. The same pipeline on pandas, no Java, into the same outputs
ubunye run -d pipelines -u titanic -p pipeline -t clean_data -t aggregate -m DEV --lineage \
  --backend pandas

# 6. Each task's Spark and pandas run records agree, hash for hash
for task in clean_data aggregate; do
  bash scripts/same_receipt.sh pipelines titanic pipeline "$task"
done
```

## Running tests

```bash
pip install -e ".[spark,dev]"
pytest tests/ -v
```
