# ADR 001: Backends are plugins, found by name

**Status:** accepted, 0.7.0

## Context

A backend is the engine that runs a task: Spark, Databricks' own session, or
pandas. Until 0.7.0 the CLI chose one with an `if/elif` naming Spark and pandas,
and the core's `Engine` imported `SparkBackend` for its default. Readers,
writers, transforms and hooks were already plugins; the most important plugin,
the engine itself, was not. A new engine meant editing Ubunye.

## Decision

Backends register in the `ubunye.backends` entry point group, the shipped ones
exactly like a third party one:

```toml
[project.entry-points."ubunye.backends"]
spark = "ubunye.backends.spark_backend:SparkBackend"
databricks = "ubunye.backends.databricks_backend:DatabricksBackend"
pandas = "ubunye.backends.pandas_backend:PandasBackend"
```

`ubunye.core.backends` finds them by name (case blind), loads a class only when
asked, and builds it with `Backend.create(app_name=..., conf=...)`. The CLI's
`--backend`, the API's `backend=` and `ubunye backends` all go through it. The
core no longer imports any backend.

## Consequences

- A new engine (DuckDB, Polars, Sail, a future Ubunye graph engine) is a package
  with one entry point. No edit to Ubunye.
- A backend whose dependencies are missing does not break the others: asking for
  it by name gives a clear error with the `pip install` that fixes it.
- `Backend.name`, `Backend.create` and `Backend.from_platform` are new, all with
  defaults, so a backend written before 0.7.0 still works unchanged.
