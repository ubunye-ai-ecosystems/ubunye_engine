# Config Reference — Overview

Every Ubunye task is driven by a single `config.yaml` file.
This page explains the top-level structure. Each section links to a dedicated reference page.

---

## Top-level keys

```yaml
MODEL: etl          # optional — job type: etl | ml (defaults to etl)
VERSION: "1.0.0"    # optional — semver string (defaults to "0.0.0-dev")

ENGINE: ...         # optional — Spark settings and per-profile overrides
CONFIG: ...         # required — inputs, transform, outputs
ORCHESTRATION: ...  # optional — Airflow / Databricks / Prefect / Dagster metadata
```

| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| `MODEL` | `etl` \| `ml` | No | `etl` | Declares the job type |
| `VERSION` | semver string | No | `"0.0.0-dev"` | Pipeline version (`MAJOR.MINOR.PATCH`, optional `-prerelease` suffix) |
| `ENGINE` | [EngineConfig](engine.md) | No | — | Spark conf + per-profile overrides |
| `CONFIG` | [TaskConfig](io.md) | Yes | — | Inputs, transform, outputs |
| `ORCHESTRATION` | [OrchestrationConfig](orchestration.md) | No | — | Export metadata for orchestrators |

Set `MODEL` and `VERSION` explicitly in production pipelines where job type or
version is load-bearing (lineage records, model registry, orchestrator
metadata). For quick local iteration the defaults are fine.

---

## Jinja templating

Config values are rendered through Jinja2 **before** Pydantic validation.
You can use environment variables, CLI-injected variables, and filters anywhere in the YAML:

```yaml
CONFIG:
  inputs:
    events:
      format: hive
      db_name: "{{ env.HIVE_DB | default('raw') }}"
      tbl_name: events_{{ dt | default('2024-01-01') | replace('-', '_') }}
```

See [Jinja Templating](jinja.md) for all supported syntax.

---

## Validation

Ubunye validates the rendered YAML against strict Pydantic v2 models.
Run validation before deploying:

```bash
ubunye validate -d pipelines -u fraud -p etl -t claims
```

---

## Reference pages

| Section | Description |
|---|---|
| [Inputs & Outputs](io.md) | `CONFIG.inputs` and `CONFIG.outputs` — connector format and options |
| [Engine & Profiles](engine.md) | `ENGINE` — Spark conf and dev/staging/prod profiles |
| [Transform](transform.md) | `CONFIG.transform` — noop, task, model, and custom types |
| [Orchestration](orchestration.md) | `ORCHESTRATION` — schedule, retries, tags, platform-specific settings |
| [Jinja Templating](jinja.md) | Variable interpolation, filters, and best practices |
