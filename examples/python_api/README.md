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
└── README.md
```

## Running locally

```bash
pip install ubunye-engine[spark]

# Single task
python run_single_task.py

# Full pipeline (both tasks)
python run_full_pipeline.py
```

## Running on Databricks

Copy the `pipelines/` folder and either runner script to your workspace.
The Python API auto-detects the active SparkSession — no config changes needed.
