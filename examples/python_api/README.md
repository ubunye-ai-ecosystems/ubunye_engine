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

## Running on Databricks

Copy the `pipelines/` folder and either runner script to your workspace.
There `getOrCreate()` returns the notebook's session, so no config changes
are needed.
