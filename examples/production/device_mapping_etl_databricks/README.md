# Device Mapping ETL — Policy/Device Mapping with Exposure

A production Ubunye pipeline that joins three Unity Catalog tables
(policy-device details, user-IMEI activity, MI premium exposure),
deduplicates to the latest policy version and latest device row,
corrects the installation datetime with the earliest IMEI detection, and
emits a curated Delta table partitioned by `inserted_timestamp`.

This is the reference port of a legacy internal script onto Ubunye. It is
paid-workspace only because every source and sink lives in Unity Catalog.
No confidential catalog or schema names are committed to this directory —
they flow through Databricks Asset Bundle variables sourced from GitHub
environment secrets.

---

## What this example demonstrates

1. **Unity Catalog reader + writer plugins** — three `format: unity` inputs,
   one `format: unity` output. No S3 paths; no DBFS.
2. **Jinja env-var templating for identifiers** — `{{ env.TELM_CATALOG }}` /
   `{{ env.TELM_SCHEMA }}` in `config.yaml`, real values injected at deploy
   time via `--var` from GitHub environment secrets.
3. **OAuth-gated CI** — `.github/workflows/device_mapping_etl_databricks.yml`
   runs the unit tests on every PR and deploys to nonprod on merge to main;
   the deploy step is skipped automatically when the workspace secrets are
   not wired (forks, unconfigured repos).
4. **Monthly schedule** — the DAB registers a Databricks Jobs schedule
   (`0 0 6 2 * ?`, UTC) so the bundle *is* the source of truth for the
   cadence.
5. **Engine observability** — no hand-rolled MLflow calls. The Ubunye event
   and lineage hooks capture run metadata; MLflow tags can be added later
   by opting into the mlflow monitor.

---

## Layout

```
device_mapping_etl_databricks/
├── databricks.yml                       # DAB: job, schedule, variables, targets
├── .env.example                         # Required env vars (no real values)
├── notebooks/
│   └── run_policy_device_mapping.py     # %pip install + ubunye.run_task()
├── pipelines/device/etl/policy_device_mapping/
│   ├── config.yaml                      # inputs/outputs via {{ env.TELM_* }}
│   └── transformations.py               # pure Spark business logic
└── tests/
    ├── conftest.py                      # local SparkSession fixture
    └── test_transformations.py          # per-stage + end-to-end tests
```

---

## Required secrets

Set these as **environment** secrets on a GitHub environment called
`databricks` (or whatever the workflow references):

| Secret | Purpose |
|---|---|
| `DATABRICKS_HOST`          | Workspace URL, e.g. `https://adb-1234567890.12.azuredatabricks.net`. |
| `DATABRICKS_CLIENT_ID`     | Service principal Application ID (UUID). |
| `DATABRICKS_CLIENT_SECRET` | OAuth secret for the service principal. |
| `TELM_CATALOG`             | Unity Catalog catalog name (per environment). |
| `TELM_SCHEMA`              | Unity Catalog schema name (per environment). |

See `docs/databricks-auth.md` for the OAuth flow. The workflow gates
`bundle deploy` on these secrets being present — unconfigured repos
still run the unit tests.

---

## Local testing

```bash
pip install -e ".[spark,dev]"
pytest examples/production/device_mapping_etl_databricks/tests -v
```

The tests exercise each transformation stage on toy DataFrames and assert
the end-to-end schema matches `OUTPUT_COLUMNS`. No Databricks access
required.

---

## Running on Databricks

1. Ensure the service principal has `USE CATALOG`, `USE SCHEMA`,
   `SELECT` on the three source tables, and `MODIFY` / `CREATE TABLE`
   on the target schema.
2. Merge a change under this directory — the workflow runs
   `databricks bundle deploy --target nonprod \
        --var="telm_catalog=${TELM_CATALOG}" \
        --var="telm_schema=${TELM_SCHEMA}"`.
3. Promote to prod with a manual `workflow_dispatch` run against the
   `prod` environment (separate secrets, separate catalog).

The job runs on the 2nd of every month at 06:00 UTC, after the previous
month's MI data is finalised.

---

## Deviation from the legacy script

The legacy script computed `installation_datetime_final` (PDD install
corrected against the earliest IMEI signal) but then selected the raw
`installation_datetime` in its final select — a latent bug. This port
propagates the corrected value, so `installation_datetime` in the
output table reflects the earlier of the PDD install and the first
IMEI detection, as intended.

The `rename_cols` dictionary maps `ItemNo` to `item_no_raw` rather than
`item_no` so the post-rename column cleanly names "the original item
number" — the final select already uses `lob_asset_id` as `item_no` in
the output, so no semantic change results.

All other business logic is a straight, literal port.
