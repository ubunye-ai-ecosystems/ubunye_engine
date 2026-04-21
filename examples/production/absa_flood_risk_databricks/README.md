# ABSA Flood Risk — TomTom geocoding + JBA flood metrics (two-task pipeline)

A production Ubunye pipeline that turns street addresses into quantified
flood-risk metrics via two chained tasks:

1. **`geocode_addresses`** — calls the TomTom Search API for every
   `(id, address)` row in the configured source table and writes the
   top-1 candidate per id (with lat/lon and address metadata) to the
   Unity Catalog Delta table `address_geocoded`.
2. **`flood_risk`** — reads `address_geocoded`, calls the JBA
   `floodscores` and `flooddepths` endpoints in batches, merges the two
   responses on `id`, renames the ~60 nested keys into snake-case, and
   writes one row per geocoded address to `address_flood_risk`.

Paid Databricks workspace only — everything lives in Unity Catalog.
API credentials come from a Databricks secret scope; UC identifiers come
from GitHub environment secrets passed through `--var` at deploy time.
No confidential names, API keys, or basic-auth tokens are committed.

---

## Why split into two tasks?

- **Cost**: TomTom calls are the expensive part. Splitting means
  `flood_risk` can rerun quarterly (or on a JBA refresh) without paying
  to re-geocode addresses that haven't changed.
- **Isolation**: the intermediate UC table `address_geocoded` is a
  stable handoff. If JBA is down, it doesn't block geocoding; if
  TomTom rejects an address, it doesn't break the flood step for the
  others.
- **Composability**: `flood_only` and `geocode_only` modes on the
  notebook widget let operators rerun just one half without a full
  redeploy.

---

## Input contract

The pipeline expects a Unity Catalog table at
`{TELM_CATALOG}.{TELM_SCHEMA}.{ADDRESS_SOURCE_TABLE}` with at least:

| Column   | Type     | Notes                                                           |
|----------|----------|-----------------------------------------------------------------|
| `id`     | string   | Stable identifier propagated through both tasks as `address_id`.|
| `address`| string   | Free-form address string in a format TomTom can parse.          |

Any additional columns on the source are ignored by the geocode task but
preserved in the `address_geocoded` output if joined back downstream.

---

## Layout

```
absa_flood_risk_databricks/
├── databricks.yml                              # DAB: 1 job, 2 chained tasks, quarterly schedule
├── .env.example                                # Required env vars + secret-scope setup
├── notebooks/
│   └── run_flood_pipeline.py                   # %pip install + ubunye.run_pipeline()
├── pipelines/flood/etl/
│   ├── geocode_addresses/                      # Task 1 - TomTom
│   │   ├── config.yaml
│   │   └── transformations.py
│   └── flood_risk/                             # Task 2 - JBA
│       ├── config.yaml
│       └── transformations.py
└── tests/
    ├── conftest.py                             # local SparkSession + sys.path for both tasks
    ├── test_geocode_addresses.py               # fake session, covers fallback + 429 + schema
    └── test_flood_risk.py                      # fake post, covers batching + merge + rename
```

---

## Required secrets and variables

**Databricks secret scope** (default name `absa-flood`, overridable):

```bash
databricks secrets create-scope absa-flood
databricks secrets put-secret absa-flood tomtom_api_key       # <TomTom API key>
databricks secrets put-secret absa-flood jba_basic_auth       # "Basic <base64 of user:pass>"
```

Make sure the deploying service principal and the job-runtime identity
both have `READ` permission on the scope.

**GitHub environment secrets**:

| Secret | Purpose |
|---|---|
| `DATABRICKS_HOST`           | Workspace URL. |
| `DATABRICKS_CLIENT_ID`      | Service principal Application ID. |
| `DATABRICKS_CLIENT_SECRET`  | OAuth secret. |
| `TELM_CATALOG`              | Unity Catalog catalog. |
| `TELM_SCHEMA`               | Unity Catalog schema. |
| `ADDRESS_SOURCE_TABLE`      | Unqualified source-table name (e.g. `policies_addresses`). |

`DATABRICKS_TOKEN` is accepted as a PAT fallback, but service-principal
OAuth is the recommended flow on paid workspaces. See
`docs/databricks-auth.md`.

---

## Local testing

No live API calls; both tests inject fake HTTP callables.

```bash
pip install -e ".[spark,dev]"
pytest examples/production/absa_flood_risk_databricks/tests -v
```

The tests cover: TomTom's 400/429/no-results/empty-address paths, the
three-step parameter-fallback chain, JBA batching arithmetic, merge on
`id`, missing-coord filtering, and the 60-entry column rename.

---

## Running on Databricks

```bash
cd examples/production/absa_flood_risk_databricks
databricks bundle deploy --target nonprod \
  --var="telm_catalog=${TELM_CATALOG}" \
  --var="telm_schema=${TELM_SCHEMA}" \
  --var="address_source_table=${ADDRESS_SOURCE_TABLE}"
databricks bundle run absa_flood_risk --target nonprod
```

The workflow in `.github/workflows/absa_flood_risk_databricks.yml`
does the same automatically on merge to `main`.

### Running one task only

Set the notebook widget `tasks_to_run` to `geocode_only` or `flood_only`
when launching manually. Defaults to `both` for scheduled runs.

---

## Deviations from the legacy notebook

- Removed all hard-coded credentials. `API_KEY` and the JBA `Basic`
  token are loaded from the Databricks secret scope at runtime.
- Fixed the header prefix typo (`"Base "` -> `"Basic "`) that the
  legacy code carried in several commented-out test blocks.
- Removed the Zscaler-cert probing code. On serverless, `certifi.where()`
  is the correct path; if a workspace needs a corporate bundle, supply it
  via `verify_tls` from the notebook.
- The geocode task keeps a single row per `id` (the top-1 candidate, or
  the error row if TomTom produced no results), so the downstream join
  in `flood_risk` is unambiguous.
