# Config Reference: Overview

Every Ubunye task is driven by a single `config.yaml` file.
This page explains the top-level structure. Each section links to a dedicated reference page.

---

## Top-level keys

```yaml
MODEL: etl          # optional: job type, etl | ml (defaults to etl)
VERSION: "1.0.0"    # optional: semver string (defaults to "0.0.0-dev")

ENGINE: ...         # optional: Spark settings and per-profile overrides
CONFIG: ...         # required: inputs, transform, outputs, expectations
ORCHESTRATION: ...  # optional: metadata for `ubunye export`
```

| Key | Type | Required | Default | Description |
|---|---|---|---|---|
| `MODEL` | `etl` \| `ml` | No | `etl` | Declares the job type |
| `VERSION` | semver string | No | `"0.0.0-dev"` | Pipeline version (`MAJOR.MINOR.PATCH`, optional `-prerelease` suffix) |
| `ENGINE` | [EngineConfig](engine.md) | No | none | Spark conf, catalog and schema, per-profile overrides |
| `CONFIG` | [TaskConfig](io.md) | Yes | none | Inputs, transform, outputs, [expectations](expectations.md) |
| `ORCHESTRATION` | [OrchestrationConfig](orchestration.md) | No | none | Export metadata for orchestrators |

`CONFIG` holds four keys: `inputs` and `outputs` (both required, at least one
entry each), `transform` (optional) and `expectations` (optional).

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
Unknown fields are rejected with typo suggestions; undefined Jinja
variables fail immediately. Run validation before deploying:

```bash
ubunye validate -d pipelines -u fraud -p etl -t claims
```

### Strict field checking

Every config section rejects unknown keys. A typo like `ENGNE` triggers a
`ConfigFieldError` with a "Did you mean?" suggestion.

Input and output blocks are checked by their connector. Each built-in connector
declares the keys it reads, so a typo such as `paht:` on an `s3` input is
refused with the closest real key. A third-party connector that declares no
keys accepts any key. See [Writing a Plugin](../connectors/plugin_guide.md).

### Strict template variables

The Jinja resolver uses `StrictUndefined`: any `{{ var }}` that isn't
provided as a CLI variable or environment variable raises immediately instead
of silently producing an empty string. Use `| default(...)` for optional
values.

---

## Reference pages

| Section | Description |
|---|---|
| [Inputs & Outputs](io.md) | `CONFIG.inputs` and `CONFIG.outputs`: connector format and options |
| [Expectations](expectations.md) | `CONFIG.expectations`: checks on inputs and outputs |
| [Engine & Profiles](engine.md) | `ENGINE`: Spark conf, catalog, schema and profiles |
| [Transform](transform.md) | `CONFIG.transform`: the task class and its `params` |
| [Orchestration](orchestration.md) | `ORCHESTRATION`: schedule, retries, tags, platform-specific settings |
| [Jinja Templating](jinja.md) | Variable interpolation, filters, and best practices |
| [Secrets](secrets.md) | `secret://` references to a secret store |
