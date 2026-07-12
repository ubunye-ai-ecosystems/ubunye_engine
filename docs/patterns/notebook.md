# Notebook Prototyping

The notebook API lets data scientists prototype Ubunye tasks step by step
in Databricks notebooks. Environment variables are auto-resolved from
widgets and secrets — no manual `os.environ` setup.

## Quick start

```python
import ubunye

ctx = ubunye.notebook(
    "/Workspace/pipelines/claims/claim_etl",
    mode="PROD",
    dt="2026-01-01",
)
```

That single call:

1. Scans `config.yaml` for `{{ env.VAR }}` references
2. Auto-resolves each from Databricks widgets (lowercase name convention)
   or an explicit `secrets_scope`
3. Loads and validates the config
4. Starts the backend (reuses the notebook's active SparkSession)
5. Loads `transformations.py`

## Step-by-step execution

```python
# Cell 2 — read and inspect inputs
sources = ctx.read()
sources["raw_claims"].display()
sources["raw_claims"].printSchema()

# Cell 3 — transform and inspect outputs
outputs = ctx.transform(sources)
outputs["bronze_claims"].show(20, truncate=False)

# Cell 4 — write when satisfied
ctx.write(outputs)
```

Each step can be re-run independently. `transform()` with no argument
uses the result of the last `read()`.

## One-shot execution

When you don't need to inspect intermediate DataFrames:

```python
outputs = ctx.run()  # read -> transform -> write
```

## Auto env-var resolution

The notebook context scans raw YAML for `{{ env.VAR }}` before Jinja
resolution and resolves each variable from a waterfall of sources:

| Priority | Source | Example |
|----------|--------|---------|
| 1 | Explicit `env=` dict | `env={"CATALOG": "my_catalog"}` |
| 2 | Already in `os.environ` | Set by Databricks job parameters |
| 3 | Databricks widget | Widget named `catalog` maps to `CATALOG` |
| 4 | Databricks secret | `secrets_scope="my-scope"`, key = `catalog` |
| 5 | Jinja `\| default()` | Falls through to config default |

### Widget naming convention

Widget names are the **lowercase** version of the env-var name.
A config referencing `{{ env.TELM_CATALOG }}` auto-reads from a widget
named `telm_catalog`.

### Secrets

Pass `secrets_scope` to auto-fetch secrets. By default, the secret key
is the lowercase env-var name. Override with `secrets_map`:

```python
ctx = ubunye.notebook(
    task_dir,
    secrets_scope="my-pipeline",
    secrets_map={"API_KEY": "my-custom-key"},
)
```

## Cleanup

Call `ctx.close()` to restore original environment variables and remove
the task directory from `sys.path`.

## Comparison with `run_task`

| | `ubunye.run_task()` | `ubunye.notebook()` |
|---|---|---|
| Execution | All-or-nothing | Step-by-step |
| Env vars | Manual `os.environ` setup | Auto-resolved |
| Inspection | Only final outputs | Between every stage |
| Use case | Jobs, CI, automation | Interactive prototyping |

Both share the same engine, config loader, and plugin system. Code that
works in the notebook works identically via `run_task`.
