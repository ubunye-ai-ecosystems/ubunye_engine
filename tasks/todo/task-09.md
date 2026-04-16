# task-09 — Exercise telemetry hooks on Databricks

**Priority:** low-medium — feature shipped in 0.1.6, unexercised in
production.

## What to try

1. Set `UBUNYE_TELEMETRY=1` on the training job. Confirm the three
   built-in hooks (`EventLoggerHook`, `OTelHook`, `PrometheusHook`) load
   without error on serverless.
2. Point `UBUNYE_PROM_PORT` at a reachable port and verify metrics are
   scrape-able from inside the notebook.
3. Write a custom Slack-alert `Hook` as a third-party package, register
   it via the `ubunye.hooks` entry-point group, and confirm it gets
   discovered by the engine.

## What to watch for

- `PrometheusHook` binding a port that serverless has already claimed.
- `OTelHook` defaulting to an endpoint that fails silently.
- Entry-point discovery racing with `importlib.metadata` on first run.
