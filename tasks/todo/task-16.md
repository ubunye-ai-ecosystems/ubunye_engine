# task-16: `test_output_column_types` omits `relative_humidity_pct` from schema assertion

**Discovered by:** fire-test of `jhb_weather_databricks` (run 24513411786, 2026-04-16)

## Symptom

`test_output_column_types` in
`examples/production/jhb_weather_databricks/tests/test_transformations.py`
asserts the dtype of `forecast_timestamp`, `forecast_date`, `temperature_c`,
`precipitation_mm`, and `wind_speed_kmh`, but silently omits `relative_humidity_pct`.
The test passes even if that column's cast is wrong or removed entirely.

```
examples/production/jhb_weather_databricks/tests/test_transformations.py::test_output_column_types PASSED
```

The assertion block at line 84-89 covers only 5 of the 8 output columns; `latitude`,
`longitude`, and `relative_humidity_pct` have no dtype check.

## Repro

```bash
gh run view --job=71650515658 --repo ubunye-ai-ecosystems/ubunye_engine --log \
  | grep "test_output_column_types"
```

Locally:

```bash
pytest examples/production/jhb_weather_databricks/tests/test_transformations.py \
  -k test_output_column_types -v
```

## Context

- Example: `jhb_weather_databricks`
- Workflow: `.github/workflows/jhb_weather_databricks.yml`
- Step: `Unit tests (Spark, local mode)`
- File: `examples/production/jhb_weather_databricks/tests/test_transformations.py`, line 82-89
- Run 24513411786, job 71650515658, 2026-04-16

## Suspected root cause

`relative_humidity_pct` (and `latitude`/`longitude`) were never added to the
`test_output_column_types` assertion block, leaving a gap in the schema contract test
where a broken cast on those columns would go undetected.
