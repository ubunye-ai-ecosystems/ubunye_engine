# Titanic — Databricks Production Example (serverless + Unity Catalog)

A production-shaped reference pipeline that runs on **Databricks serverless
compute** and writes its output to a **Unity Catalog managed Delta table**.
Deployable via Asset Bundles from GitHub Actions.

The business logic (`transformations.py`) is **byte-identical** to
[`../titanic_local/`](../titanic_local/). A CI step diffs the two files to
prove it. Everything below describes only the deployment wrapper.

This example was validated end-to-end on Databricks Free Edition, which is
serverless-only and rejects classic-cluster creation. The same DAB works
unchanged on paid workspaces — only the `titanic_catalog` default likely
needs to change (`workspace` → `main`).

---

## What gets deployed

```
titanic_survival_by_class (Job)
├── environment "ubunye": ubunye-engine==0.9.3   # installed by the job, no %pip
├── parameters (read by the engine):
│   ├── dt: 2026-04-15                          # var.data_timestamp
│   ├── ubunye_mode: PROD
│   ├── ubunye_run_records: workspace.titanic.ubunye_runs
│   ├── git_commit: ${bundle.git.commit}
│   ├── git_branch: ${bundle.git.branch}
│   └── deploy_target: ${bundle.target}
└── run_pipeline (Notebook task, serverless compute)
    ├── notebook: /Workspace/.../notebooks/run_titanic.py
    └── base_parameters:
        ├── task_dir: /Workspace/.../pipelines/titanic/analytics/survival_by_class
        ├── titanic_catalog: workspace        # var.titanic_catalog
        └── titanic_schema:  titanic          # var.titanic_schema
```

The notebook:

1. Setup, not part of the task: `CREATE SCHEMA IF NOT EXISTS workspace.titanic`
   and `CREATE VOLUME IF NOT EXISTS workspace.titanic.data`, then downloads
   the Titanic CSV to `/Volumes/workspace/titanic/data/titanic.csv` once.
   UC volumes are the only local file path serverless allows
   (`file:///tmp/...` is blocked by `SharedUCWorkspaceLocalFileSystem`).
2. Runs the task as one `with` block, three steps with a plain comment each:

    ```python
    with ubunye.notebook(
        dbutils.widgets.get("task_dir"),
        dt=dbutils.widgets.get("dt"),
        env={"TITANIC_INPUT_PATH": csv_path},
    ) as run:
        sources = run.read()               # READ: every input, nothing changed yet
        outputs = run.transform(sources)   # TRANSFORM: transformations.py
        run.write(outputs)                 # WRITE: data-quality rules, then the table
    ```

   `TITANIC_CATALOG` and `TITANIC_SCHEMA` come from the parameters of the same
   name in lower case; `config.yaml` quotes them with `| ident`, so a catalog
   with a hyphen works. The mode comes from the `ubunye_mode` parameter.
3. The block is one run. Success or failure, it leaves one row in
   `workspace.titanic.ubunye_runs` (the first run creates the table): the
   status, the step that failed, rows in and out, the job run, and the git
   commit, branch and deploy target. The write creates
   `workspace.titanic.survival_by_class`.

Find the runs of a deployed commit:

```sql
SELECT run_id, status,
       get_json_object(record, '$.environment.identity.git_commit') AS git_commit
FROM workspace.titanic.ubunye_runs
ORDER BY run_id DESC
```

---

## Directory layout

```
titanic_databricks/
├── pipelines/titanic/analytics/survival_by_class/
│   ├── config.yaml                       # CSV reader -> unity writer
│   └── transformations.py                # byte-identical to titanic_local's copy
├── notebooks/
│   └── run_titanic.py                    # serverless notebook entry
├── expected_output/
│   └── survival_by_class.parquet         # golden for the unit test suite
├── tests/
│   ├── conftest.py
│   └── test_transformations.py           # shared PySpark unit tests
├── databricks.yml                        # Asset Bundle (serverless)
└── README.md
```

---

## Prerequisites

1. **A Databricks workspace with Unity Catalog enabled.** Free Edition
   ships with the `workspace` catalog out of the box. Paid workspaces
   typically have `main`.
2. **A personal access token (PAT)** — Workspace → User Settings → Developer
   → Access tokens. Free Edition has no service principals, so a PAT is the
   only option there.
3. **The Databricks CLI** (`>= v0.205`, the Go-based rewrite):
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks --version
   ```
4. **GitHub repository secrets** (for CI):
   - `DATABRICKS_HOST` — e.g. `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com`
   - `DATABRICKS_TOKEN` — the PAT from step 2.

---

## Deploy and run manually

```bash
export DATABRICKS_HOST=https://<your-workspace>.cloud.databricks.com
export DATABRICKS_TOKEN=<pat>

cd examples/production/titanic_databricks

# 1. Validate.
databricks bundle validate --target nonprod

# 2. Deploy. DAB uploads pipelines/ and notebooks/ to
#    /Workspace/Users/<you>/.bundle/titanic-survival/nonprod/files/...
databricks bundle deploy --target nonprod

# 3. Run the job. CLI prints the run URL and polls until it terminates.
databricks bundle run titanic_survival --target nonprod
```

To target the standard `main` catalog on a paid workspace:

```bash
databricks bundle deploy --target nonprod --var="titanic_catalog=main"
databricks bundle run    titanic_survival --target nonprod --var="titanic_catalog=main"
```

---

## Verify outputs

After a successful run, query the table from a Databricks notebook or via
the SQL Editor:

```sql
SELECT * FROM workspace.titanic.survival_by_class ORDER BY Pclass;
```

Expected — matches the committed `expected_output/survival_by_class.parquet`:

```
Pclass  passenger_count  survivors_count  survival_rate
1       216              136              0.6296
2       184              87               0.4728
3       491              119              0.2424
```

Lineage records are written under the workspace files location used by the
bundle and can be inspected with `ubunye lineage show` from a workstation
that has the output synced.

---

## CI — GitHub Actions

See `.github/workflows/databricks_deploy.yml`. The workflow:

1. Runs the PySpark unit tests (Java 17 on the runner).
2. Diffs `transformations.py` between the two production examples — **fails
   loudly if they drift**, which is the portability contract.
3. If `DATABRICKS_HOST` / `DATABRICKS_TOKEN` secrets are configured:
   - `databricks bundle validate --target nonprod`
   - `databricks bundle deploy --target nonprod`
4. On manual `workflow_dispatch` with `run_after_deploy=true`:
   - `databricks bundle run titanic_survival --target nonprod`

If the secrets are absent (forks, before workspace is wired up), steps 1-2
still run; steps 3-4 are skipped with a warning.

| Event                                                  | What runs                                |
|--------------------------------------------------------|------------------------------------------|
| `push` to `main` touching this example                 | Tests + portability diff + validate + deploy |
| Manual `workflow_dispatch` with `run_after_deploy=true` | + `bundle run`                          |

---

## Troubleshooting

| Symptom                                              | Cause / fix                                                                          |
|------------------------------------------------------|---------------------------------------------------------------------------------------|
| `bundle validate` fails with "unknown target"        | You passed `--target <name>` not matching `targets.*`. Only `nonprod` is defined.    |
| `bundle deploy` fails with "no associated worker environments" | You still have a `new_cluster` block. This DAB intentionally has none — confirm yours matches. |
| `[DATA_SOURCE_NOT_FOUND] Failed to find the data source: unity` | The unity writer was reading the plugin dispatch key as a Spark format. Fixed in `>=0.1.6`. |
| `[SCHEMA_NOT_FOUND]` on saveAsTable                  | The notebook didn't `CREATE SCHEMA IF NOT EXISTS` before writing. Re-run.            |
| `LocalFilesystemAccessDeniedException: Cannot access non /Workspace local filesystem path` | Spark on serverless can't read from `file:///tmp/...`. Use a UC volume path (`/Volumes/<catalog>/<schema>/<volume>/...`) instead. |
| `bundle deploy` succeeds but job not visible in UI  | DAB's `mode: development` prefixes the job name with `[dev <user>]`. Search by name. |
| Job fails installing its environment                 | Network egress to PyPI is blocked on some workspaces. Point the environment's `dependencies` at a wheel on a UC volume instead. |
