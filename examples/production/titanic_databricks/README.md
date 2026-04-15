# Titanic — Databricks Community Edition Production Example

A production-shaped reference pipeline targeted at **Databricks Community
Edition** (the successor "Free Edition" also applies) and deployable via
Asset Bundles from GitHub Actions.

The business logic (`transformations.py`) is **byte-identical** to
[`../titanic_local/`](../titanic_local/). A CI step diffs the two files to
prove it. Everything below describes only the deployment wrapper.

---

## Read this first — Community Edition realities

This section is not boilerplate. CE is a moving target that Databricks has
been winding down, and several of the constraints below will bite you if you
skip them.

| Limitation                                         | Consequence                                                                     |
|----------------------------------------------------|---------------------------------------------------------------------------------|
| **No service principals**                          | `DATABRICKS_TOKEN` must be a personal PAT. PATs expire and are per-user.        |
| **No Unity Catalog**                               | Storage must be DBFS (`dbfs:/FileStore/...`). Delta Lake still works; UC does not. |
| **Single-node clusters only**                      | The bundle sets `num_workers: 0` and `spark.master: "local[*]"`.                |
| **Asset Bundle deploys may be restricted**         | Some CE tiers block `jobs.create` via API, causing `bundle deploy` to 403. If this happens, either trigger the run manually from the UI or fall back to the `existing_cluster_id` pattern below. |
| **DBFS is deprecated for new workspaces**          | Works on CE; not recommended for net-new paid workspaces.                       |
| **CE signups are closed for new users**            | If you do not already have a CE workspace you will need a trial or paid one.    |
| **Time-bounded workspaces / idle-cluster shutoff** | Clusters auto-terminate. The job creates a fresh one per run.                   |

If any of the above are blockers in your environment, this example still has
value as a reference for what a production DAB looks like — the `databricks.yml`
structure is the same one you would use on a standard workspace.

---

## What gets deployed

```
titanic_survival_by_class (Job)
└── run_pipeline (Notebook task)
    ├── notebook: /Workspace/.../notebooks/run_titanic.py
    ├── new_cluster: single-node, 13.3.x-scala2.12
    └── base_parameters:
        ├── task_dir: /Workspace/.../pipelines/titanic/analytics/survival_by_class
        ├── dt: 2026-04-15
        ├── mode: PROD
        ├── titanic_input_path: dbfs:/FileStore/titanic/titanic.csv
        └── titanic_output_path: dbfs:/FileStore/titanic/output
```

The notebook:

1. Reads widgets populated by the job's `base_parameters`.
2. Installs `ubunye-engine==0.1.5` via `%pip`.
3. Bootstraps the Titanic CSV into DBFS (idempotent, no manual upload).
4. Exports `TITANIC_INPUT_PATH` / `TITANIC_OUTPUT_PATH` so the config resolves.
5. Calls `ubunye.run_task(task_dir, dt, mode, lineage=True)`.

---

## Directory layout

```
titanic_databricks/
├── pipelines/titanic/analytics/survival_by_class/
│   ├── config.yaml                       # DBFS paths, DEV/PROD profiles
│   └── transformations.py                # byte-identical to titanic_local's copy
├── notebooks/
│   └── run_titanic.py                    # Databricks notebook (source format)
├── expected_output/
│   └── survival_by_class.parquet         # golden for the unit test suite
├── tests/
│   ├── conftest.py
│   └── test_transformations.py           # shared pandas unit tests
├── databricks.yml                        # Asset Bundle
└── README.md
```

---

## Prerequisites

1. **A Community Edition workspace.**
   - Sign in at <https://community.cloud.databricks.com>.
   - If CE signups are no longer open, the example also works on any
     standard workspace; treat "CE" as a compatibility floor.
2. **A personal access token (PAT).**
   - Workspace → User Settings → Developer → Access tokens → Generate.
   - Copy the token — CE does not show it again.
3. **The Databricks CLI (>= v0.205, the Go-based rewrite).**
   ```bash
   curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
   databricks --version
   ```
4. **GitHub repository secrets** (for CI):
   - `DATABRICKS_HOST` — e.g. `https://community.cloud.databricks.com`
   - `DATABRICKS_TOKEN` — the PAT from step 2.

---

## One-time setup

```bash
# Authenticate your local CLI (interactive).
databricks auth login --host https://community.cloud.databricks.com

# Sanity check.
databricks workspace list /
```

The Titanic CSV is fetched by the notebook on first job run — **no manual
DBFS upload required**. If you prefer to pre-seed it:

```bash
curl -L https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv \
  -o /tmp/titanic.csv
databricks fs cp /tmp/titanic.csv dbfs:/FileStore/titanic/titanic.csv
```

---

## Deploy and run manually

```bash
cd examples/production/titanic_databricks

# 1. Validate the bundle (no API calls).
databricks bundle validate --target nonprod

# 2. Deploy to the workspace. DAB uploads pipelines/ and notebooks/ to
#    /Workspace/Users/<you>/.bundle/titanic-survival-ce/nonprod/files/...
databricks bundle deploy --target nonprod

# 3. Run the job.
databricks bundle run titanic_survival --target nonprod
```

CLI output reports the job run URL. Follow it to watch the notebook execute.

### Falling back to an existing cluster

If `bundle deploy` fails with a permissions error on `jobs.create` (common
symptom: `PERMISSION_DENIED: Only workspace admins can create jobs`), do the
following:

1. Create a single-node cluster manually in the UI. Note the cluster ID.
2. In `databricks.yml`, delete the `new_cluster` block under
   `resources.jobs.titanic_survival.tasks[0]` and replace it with
   `existing_cluster_id: ${var.existing_cluster_id}`.
3. Run `databricks bundle deploy --target nonprod --var="existing_cluster_id=<id>"`.

This is not automated because it requires a human decision about which
cluster to reuse. Do not paper over this with a CI hack.

---

## Verify outputs

After a successful run:

```bash
# List the Parquet partition directory.
databricks fs ls dbfs:/FileStore/titanic/output/mode=PROD/dt=2026-04-15

# Copy a sample part file locally and inspect.
databricks fs cp \
  dbfs:/FileStore/titanic/output/mode=PROD/dt=2026-04-15/part-00000-*.parquet \
  /tmp/out.parquet
python -c "import pandas as pd; print(pd.read_parquet('/tmp/out.parquet'))"
```

Expected content — matches the committed `expected_output/survival_by_class.parquet`:

```
   Pclass  passenger_count  survivors_count  survival_rate
0       1              216              136         0.6296
1       2              184               87         0.4728
2       3              491              119         0.2424
```

Lineage records are written under
`dbfs:/FileStore/titanic/output/.ubunye/lineage/` and can be inspected with
`ubunye lineage show` from a workstation with the output directory synced.

---

## CI — GitHub Actions

See `.github/workflows/databricks_deploy.yml`. The workflow:

1. Runs the pandas unit tests (same as local).
2. Diffs `transformations.py` between the two production examples — **fails
   loudly if they drift**, which is the portability contract.
3. Installs the Databricks CLI via the official `databricks/setup-cli` action.
4. Runs `databricks bundle validate --target nonprod`.
5. Runs `databricks bundle deploy --target nonprod`.
6. Optionally triggers the job (gated on `workflow_dispatch.inputs.run_after_deploy`)
   since CE clusters can take minutes to spin up and may exceed the job
   timeout for the CI runner.

### Trigger options

| Event                                              | What runs             |
|----------------------------------------------------|-----------------------|
| `push` to `main` touching this example            | Tests + validate + deploy |
| Manual `workflow_dispatch` with `run_after_deploy=true` | Tests + validate + deploy + bundle run |

If deploy fails against CE because of the API restrictions documented above,
the workflow fails loudly with the Databricks CLI's own error message — we
do not swallow it.

---

## Troubleshooting

| Symptom                                              | Cause / fix                                                                              |
|------------------------------------------------------|------------------------------------------------------------------------------------------|
| `bundle validate` fails with "unknown target"       | You passed `--target <name>` not matching `targets.*`. The only target defined is `nonprod`. |
| `bundle deploy` 403 on `jobs.create`                 | CE Jobs API restriction. See "Falling back to an existing cluster" above.                 |
| Notebook fails at `%pip install ubunye-engine`       | Network egress is blocked on some CE clusters. Mirror the wheel into DBFS and `%pip install /dbfs/...`. |
| `ValueError: Environment variable 'TITANIC_INPUT_PATH' is not set` | `base_parameters` did not reach the widgets. Check that the notebook is run via the job, not by pressing "Run" in the UI. |
| Output appears but is empty                          | CSV bootstrap failed silently. Check `/dbfs/FileStore/titanic/titanic.csv` exists and has 892 lines. |
| `bundle deploy` succeeds but job not visible in UI  | DAB's `mode: development` prefixes the job name with `[dev <user>]`. Search in the Jobs list for your username. |
