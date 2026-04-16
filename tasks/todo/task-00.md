# task-00 — Fire-test the four production examples, batch the fixes

**Priority:** strategic (umbrella task — spawns the tasks below)

## Plan

Run the four `examples/production/*` pipelines in anger against real
Databricks, log every bug / papercut found, fix each on its own commit
(so each is independently bisectable), then cut **one** `v0.1.7`
release at the end.

Examples to exercise:

1. `titanic_local/` — local SparkSession, CSV → Parquet.
2. `titanic_databricks/` — Databricks serverless, DBFS CSV → UC Delta.
3. `jhb_weather_databricks/` — REST API → UC Delta on a schedule.
4. `titanic_ml_databricks/` — full ML lifecycle (already smoke-passed).

Gaps the current examples *won't* hit — see `todo/task-04.md` through
`todo/task-07.md`.

## Ground rules

- One bug → one commit → one changelog entry.
- Don't release intermediate patch versions — let 0.1.7 accumulate.
- If a fix is risky or non-obvious, capture the repro in a new unit test
  before touching the engine.
