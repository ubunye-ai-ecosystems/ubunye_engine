# Upgrading from 0.8 to 0.9

What to change in your commands, configs and Python code when you move from Ubunye
Engine 0.8 to 0.9. Most projects need only the first section. Everything new is listed
in the [Changelog](changelog.md).

```bash
pip install --upgrade "ubunye-engine[pandas]"   # or the extras you use
ubunye version                                  # Ubunye Engine v0.9.0
```

---

## 1. `--package` is now `-p/--pipeline`

The middle folder level (use case, pipeline, task) is called a **pipeline** everywhere
now. Every command that took `-p/--package` takes `-p/--pipeline`.

| 0.8 | 0.9 |
|---|---|
| `ubunye run -d pipelines -u fraud -p etl -t claims` | the same: `-p` still works |
| `ubunye run -d pipelines -u fraud --package etl -t claims` | `ubunye run -d pipelines -u fraud --pipeline etl -t claims` |
| `ubunye.run_pipeline("pipelines", "fraud", package="etl", tasks=["claims"])` | `ubunye.run_pipeline("pipelines", "fraud", pipeline="etl", tasks=["claims"])` |
| `ubunye.api.job_health(..., package="etl")` | `ubunye.api.job_health(..., pipeline="etl")` |

The old names keep working until 1.0:

- `--package` is hidden from `--help` and prints one warning:
  `Warning: --package is renamed --pipeline; it will be removed in 1.0.`
- Giving both `--package` and `--pipeline` is an error (exit 2).
- In Python, `package=` gives a `DeprecationWarning`; giving both `pipeline=` and
  `package=` raises `TypeError`. The order of `run_pipeline()`'s positional arguments
  is unchanged, so `run_pipeline("pipelines", "fraud", "etl")` still works.
- The MCP `health` tool takes `pipeline` and still accepts `package`.
- Run records keep their field named `package`. It holds the pipeline name.

---

## 2. Behaviour changes

Check these if they touch your project.

**`replace_where` on an append now takes effect.** Before 0.9 it was ignored on an
append. Now, on a Delta or JDBC append, each run replaces that batch's rows in one step,
so a retry or `--rerun` writes the corrected batch instead of adding it twice:

```yaml
outputs:
  sales:
    format: delta
    path: /data/sales
    mode: append
    replace_where: "month = '{{ dt }}'"
```

A Delta append of a named batch with no `replace_where` now lands once: a retry adds
nothing. See [Safe Reruns](guides/safe-reruns.md) and the `replace_where` sections of
[Files and Delta](connectors/s3.md) and [JDBC](connectors/jdbc.md).

**`ubunye models demote` only moves a version down.** `--to` takes `development`,
`staging` or `archived` (and `staging` only from `production`). To put a version in
production, use `promote`, which checks its gates:

| 0.8 | 0.9 |
|---|---|
| `ubunye models demote -u fraud -m claims_model -s .ubunye/model_store -v 3 --to production` | `ubunye models promote -u fraud -m claims_model -s .ubunye/model_store -v 3 --to production` |

`models promote --to` takes only `staging` or `production`. A wrong `--to` is a usage
error (exit 2) that lists the choices.

**`--backend mlflow` now uses the MLflow Model Registry.** Each version is a registered
MLflow model version, and `staging` and `production` are MLflow aliases, so
`models:/<name>@production` works. In 0.8 this backend kept models in a folder. Point it
at MLflow with `MLFLOW_TRACKING_URI` (and `MLFLOW_REGISTRY_URI` if different).
See [Model Registry](ml/registry.md#mlflow-model-registry).

**The registry backend is not picked from the host.** The order is: the name you ask
for (`--backend` on `ubunye models`, `registry.backend` in a model transform), else
`UBUNYE_REGISTRY_BACKEND`, else `filesystem`. If you relied on MLflow being chosen on
Databricks, set `UBUNYE_REGISTRY_BACKEND=mlflow`.

**Features are checked at predict.** If a model version declares its features and the
frame is missing one, the run now logs it and records it before calling the model.
Set `registry.check_features: true` to refuse such a frame, or `false` to skip the check.

**`ubunye export databricks` has new defaults.** The default job cluster runtime is
`17.3.x-scala2.13` (Databricks Runtime 17.3 LTS), up from `13.3.x-scala2.12`, and the
default wheel name uses the installed version. Set `spark_version` or `wheel_dbfs_path`
under `ORCHESTRATION.databricks` to keep your own.

**`ubunye deploy dockerfile --out` takes a folder.** `--out build` now writes
`build/Dockerfile`, and missing folders are made.

**`ubunye prove` rejects a wrong value cleanly.** `prove skip --status` and
`prove observe --cost-basis` and `--kind` now give a usage error (exit 2) that lists the
valid choices, instead of a traceback or silently keeping the value.

---

## 3. Python API deprecations

These still work in 0.9 and warn with a `DeprecationWarning`.

| 0.8 | 0.9 |
|---|---|
| `LineageRecorder(store="filesystem")` | `LineageRecorder(backend="filesystem")` |
| `DeltaLineageBackend.search(task=, usecase=, pipeline=)` returning `LineageRecord`s | `search()` now returns run records; the old search is `search_records()` |
| `MLflowRegistryBackend` | the `mlflow` registry backend: `ModelRegistry(store, backend="mlflow")` |
| `detect_registry_backend()` (removed in 0.9.1: it was private, and nothing called it) | `ubunye.models.resolve_registry()`, or set `UBUNYE_REGISTRY_BACKEND` |

The Delta run record store now keeps the whole run record (steps, hashes, timings,
expectations, environment), only appends, and needs Spark. A record it cannot write goes
to the fallback manifest with a warning. `ubunye sync lineage` replays those into the
store named by `--lineage-backend` and `--lineage-dir`.

---

## 4. New, nothing to change

These are new in 0.9 and change nothing unless you use them:

- Run records can live in a shared store: `--lineage-backend delta --lineage-dir main.ops.ubunye_runs`.
  With nothing set, records stay in `<pipelines folder>/.ubunye/lineage`.
- `ubunye runs health` and `ubunye runs wait-for`, and `CONFIG.requires` to wait for an
  upstream task. See [Job Health and Upstream Checks](patterns/job-health.md).
- The run record names the model a `ModelTransform` trained or used.
- `registry.stamp_columns: true` adds `model_version` and `model_stage` to scored rows.

---

## If something breaks

Run `ubunye doctor` first, then see [Troubleshooting](troubleshooting.md).
