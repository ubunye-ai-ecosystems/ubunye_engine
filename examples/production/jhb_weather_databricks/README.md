# jhb_weather_databricks

End-to-end production example: ingest hourly weather for Johannesburg from the
[Open-Meteo](https://open-meteo.com) REST API, explode the parallel hourly
arrays into a tidy one-row-per-hour DataFrame, and write the result as a
Unity Catalog Delta table.

No API key is required - Open-Meteo is free for non-commercial use.

## Pipeline shape

```
Open-Meteo /v1/forecast  ->  rest_api reader  ->  transform_weather_spark  ->  Unity Catalog
  (lat=-26.2041, lon=28.0473)    (single doc)    (explode hourly arrays)         (Delta table)
```

Output table: `<catalog>.<schema>.jhb_hourly_forecast`, partitioned by
`forecast_date`. Columns:

| column                 | type      | note                               |
|------------------------|-----------|------------------------------------|
| latitude               | double    | repeats the request lat            |
| longitude              | double    | repeats the request lon            |
| forecast_timestamp     | timestamp | hourly tick in `Africa/Johannesburg` |
| forecast_date          | date      | partition key                      |
| temperature_c          | double    |                                    |
| relative_humidity_pct  | double    |                                    |
| precipitation_mm       | double    |                                    |
| wind_speed_kmh         | double    |                                    |

## Layout

```
jhb_weather_databricks/
├── README.md                               (this file)
├── databricks.yml                          Asset Bundle: serverless job + targets
├── notebooks/
│   └── run_jhb_weather.py                  Notebook wrapping ubunye.run_task()
├── pipelines/jhb_weather/ingestion/hourly_forecast/
│   ├── config.yaml                         rest_api reader -> unity writer
│   └── transformations.py                  Spark transform (arrays_zip + explode)
└── tests/
    ├── conftest.py                         sys.path + session-scoped SparkSession
    └── test_transformations.py             PySpark unit tests (arrays_zip/explode)
```

## Running locally (unit tests only)

```bash
pip install -e ".[spark,dev]" pyarrow
pytest examples/production/jhb_weather_databricks/tests -v
```

The unit tests spin up a local `SparkSession` (session-scoped fixture),
build a single-row DataFrame in the exact shape produced by the REST
reader, and assert on the output of `transform_weather` - so the
production code is the code under test. Requires Java 17 on PATH.

## Deploying to Databricks

```bash
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>

cd examples/production/jhb_weather_databricks
databricks bundle validate --target nonprod
databricks bundle deploy  --target nonprod
databricks bundle run     jhb_weather_forecast --target nonprod
```

The bundle deploys a scheduled job that fires daily at 06:00
`Africa/Johannesburg`. Override the Unity Catalog destination by setting the
`weather_catalog` and `weather_schema` bundle variables (see `databricks.yml`).

### Compute model

The job uses **serverless compute** — no `new_cluster` block in
`databricks.yml`. This is required on Databricks Free Edition (which does
not allow classic-cluster creation) and works identically on paid
workspaces. Dependencies are installed in-notebook via
`%pip install ubunye-engine==<ver>`.

The default `weather_catalog` is `workspace` because that is the catalog
Free Edition provisions automatically. On a paid workspace, override to
`main` (or your standard catalog) via:

```bash
databricks bundle deploy --target nonprod --var="weather_catalog=main"
```

## CI workflow

`.github/workflows/jhb_weather_databricks.yml` runs on every push/PR that
touches this directory:

1. Unit tests (local PySpark; Java 17 on the runner)
2. Smoke-check the Open-Meteo endpoint is reachable
3. If `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secrets are configured:
   - `databricks bundle validate` and `deploy --target nonprod`
4. Manual `workflow_dispatch` with `run_after_deploy: true` triggers the job.

If the Databricks secrets are absent (e.g. forks, or before the workspace is
wired up), steps 1-2 still run and steps 3-4 are skipped with a warning.
