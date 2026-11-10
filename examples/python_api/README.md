# Python API Example

This example shows how to run Ubunye pipelines from Python code
instead of the CLI. This is the recommended approach on Databricks.

## Files

```
python_api/
├── pipelines/
│   └── demo/
│       └── etl/
│           ├── clean_data/
│           │   ├── config.yaml
│           │   └── transformations.py
│           └── aggregate/
│               ├── config.yaml
│               └── transformations.py
├── run_single_task.py       ← run_task() example
├── run_full_pipeline.py     ← run_pipeline() example
├── run_as_notebook.py       ← ubunye.notebook() with block, as in a job notebook
├── data/                    ← sample input, written on the first run
└── README.md
```

## Running locally

You need Java 17 or newer for Spark. Run from this folder:

```bash
pip install "ubunye-engine[spark]"
cd examples/python_api

# Single task
python run_single_task.py

# Full pipeline (both tasks)
python run_full_pipeline.py

# One task the way a Databricks job notebook runs it
python run_as_notebook.py
```

Each script starts a SparkSession and passes it in with `spark=`. A session
that `run_task()` or `run_pipeline()` starts on its own is stopped when the
run ends, so the returned DataFrames could not be shown afterwards.

If `data/raw_events.parquet` does not exist, the script writes five sample
events to it first (one duplicate, one with a null). `clean_data` reads that
path through `dt` and writes 3 rows to `/tmp/ubunye_demo/clean_events`;
`aggregate` reads them and writes a one-row count to
`/tmp/ubunye_demo/summary`. On Windows `/tmp` is `C:\tmp`.

`run_full_pipeline.py` ends with:

```
=== aggregate ===
  summary: 1 rows
+-----+
|count|
+-----+
|    3|
+-----+
```

## The job notebook shape

`run_as_notebook.py` runs `clean_data` as one `with` block: READ, TRANSFORM,
WRITE, each with a plain comment. It prints one line per step, and the last
line says where the run record went:

```
ubunye: read 1 input (raw_events) in 0.23s
ubunye: transform 1 output (clean_events) in 0.04s
ubunye: write 1 output (clean_events) in 0.92s
ubunye: run 6ec77710 success in 1.82s; run record: filesystem <this folder>/pipelines/.ubunye/runs
```

`run_records=".ubunye/runs"` is a folder, kept next to `pipelines/`. Give it
a table name (`main.ops.ubunye_runs`) and the record goes to a Delta table
instead. A run that fails is recorded too, with the step it failed in.

## Running on Databricks

Copy the `pipelines/` folder to your workspace. A job notebook is the same
`with` block as `run_as_notebook.py`, with the task folder and date read from
the job's parameters:

```python
import ubunye

with ubunye.notebook(dbutils.widgets.get("task_dir"), dt=dbutils.widgets.get("dt")) as run:
    sources = run.read()
    outputs = run.transform(sources)
    run.write(outputs)
```

Give the job the parameter `ubunye_run_records` (a table name) to switch run
records on, and `git_commit: ${bundle.git.commit}`,
`git_branch: ${bundle.git.branch}` and `deploy_target: ${bundle.target}` so
each record says which code ran. A full bundle is in
[`../production/titanic_databricks/`](../production/titanic_databricks/).

`run_task()` and `run_pipeline()` work there too: `getOrCreate()` returns the
notebook's session, so no config changes are needed, and they read the same
job parameters.
