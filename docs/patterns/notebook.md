# Notebook Prototyping

For anyone who wants to build or debug a task one step at a time in a notebook:
Jupyter or VS Code on your laptop, or a Databricks notebook.

- **The problem:** `ubunye run` reads, transforms and writes in one go. While you
  build a task you want to stop after the read, look at the data, change the
  code, and try the transform again.
- **When to use it:** prototyping a new task or fixing a broken one. For jobs and
  CI, use `ubunye run` or `run_task`.
- **The one command:** `ctx = ubunye.notebook("pipelines/shop/orders/clean", backend="pandas")`

It works the same locally and on Databricks. On Databricks it also fills
`{{ env.VAR }}` values from notebook widgets and secret scopes for you (see
[Auto env-var resolution](#auto-env-var-resolution)).

## Quick start: on your laptop

With the pandas backend you need no Spark and no Java. The task here keeps the
people aged 18 and over from a small CSV file:

```python
import ubunye

ctx = ubunye.notebook("pipelines/shop/orders/clean", backend="pandas")
sources = ctx.read()
sources["people"].head(3)
```

What success looks like:

```
     name  age        city
0  Thandi   34  Soshanguve
1   Sipho   17    Mamelodi
2  Lerato   22      Soweto
```

```python
outputs = ctx.transform()   # uses the last read()
outputs["adults"]
```

```
      name  age        city
0   Thandi   34  Soshanguve
2   Lerato   22      Soweto
4   Naledi   41    Pretoria
5  Bongani   19      Durban
7     Mpho   28   Polokwane
```

`ctx.write(outputs)` writes them, and `ctx.close()` cleans up.

## Quick start: on Databricks

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
4. Starts the backend (on Databricks, the notebook's own Spark session; or the
   `backend=` you name)
5. Loads `transformations.py`

## Step-by-step execution

```python
# Cell 2: read and inspect inputs
sources = ctx.read()
sources["raw_claims"].display()
sources["raw_claims"].printSchema()

# Cell 3: transform and inspect outputs
outputs = ctx.transform(sources)
outputs["bronze_claims"].show(20, truncate=False)

# Cell 4: write when satisfied
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

Sources 3 and 4 exist only on Databricks. On your laptop, pass `env=` or set the
variables in your shell before you start the notebook.

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

Call `ctx.close()` to put the environment variables back the way they were
and remove the task folder from `sys.path` (the list of folders Python imports
from).

## Comparison with `run_task`

| | `ubunye.run_task()` | `ubunye.notebook()` |
|---|---|---|
| Execution | All-or-nothing | Step-by-step |
| `{{ env.VAR }}` values | Read from the process environment only: export them in the shell (or set `os.environ` in Python) before the call | Also from `env=`, and on Databricks from widgets and secret scopes |
| Inspection | Only final outputs | Between every stage |
| Use case | Jobs, CI, automation | Interactive prototyping |

Both share the same engine, config loader, and plugin system. Code that
works in the notebook works identically via `run_task`.
