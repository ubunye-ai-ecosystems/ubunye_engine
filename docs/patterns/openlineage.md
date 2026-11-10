# OpenLineage

For anyone whose company tracks where data comes from in a data catalogue
(Marquez, DataHub, OpenMetadata, Google Dataplex, and others) and wants Ubunye
runs to show up there.

- **The problem:** the catalogue shows every other job's inputs and outputs, but
  not your Ubunye tasks.
- **When to use it:** your catalogue reads [OpenLineage](https://openlineage.io),
  the open standard for sending "this job read X and wrote Y" events.
- **The one command:** `OPENLINEAGE_URL=http://localhost:5000 ubunye run -d pipelines -u shop -p orders -t clean --lineage`

Every recorded run (`ubunye run --lineage`, `run_task(lineage=True)`) can send
its run record to the catalogue as OpenLineage events. Nothing extra to install.

```bash
export OPENLINEAGE_URL=http://localhost:5000      # a Marquez server
ubunye run -d pipelines -u shop -p orders -t clean --lineage
```

Each run sends a `START` event when it begins and a `COMPLETE` or `FAIL` event
when it ends (OpenLineage 2-0-2).

## Configuration

The same environment variables OpenLineage's own clients read:

| Variable | Meaning |
|---|---|
| `OPENLINEAGE_URL` | send over HTTP to this server |
| `OPENLINEAGE_ENDPOINT` | path on it, default `api/v1/lineage` |
| `OPENLINEAGE_API_KEY` | sent as `Authorization: Bearer <key>` |
| `OPENLINEAGE_NAMESPACE` | the job namespace, default `ubunye` (see below) |
| `OPENLINEAGE_DISABLED` | `true` turns emission off |
| `UBUNYE_OPENLINEAGE_FILE` | also append every event, one JSON per line, to this file |

A server that is down or refuses an event is logged and skipped. **Emission
never fails a run.**

## What is in an event

Two OpenLineage words first:

- A **namespace** is the group a name belongs to, so two things with the same
  name don't clash. Jobs are grouped under `OPENLINEAGE_NAMESPACE`; datasets under
  where they live (`s3://bucket`, `hive`, a database server).
- A **facet** is one labelled block of extra detail attached to a run, a job or a
  dataset, such as "rows written". Catalogues show the standard facets they know,
  and keep the rest.

- **The job**: the namespace and the task's name as `usecase.pipeline.task`
  (for example `shop.orders.clean`). **The run**: the run's UUID,
  the same `run_id` as in `ubunye lineage`.
- **Every input and output as a dataset**, named by the OpenLineage naming
  conventions: `s3://bucket` + key (also for `s3a://`), `gs://bucket` + path,
  `abfss://container@account...` + path, `file` + absolute path, `hive` +
  `db.table`, `databricks://<workspace>` + `catalog.schema.table`, a JDBC server +
  `database.schema.table`. Credentials in a URL are removed.
- **Standard facets**, which catalogues already display: `outputStatistics` (rows
  written), `dataQualityMetrics` (rows read), `dataQualityAssertions` (every
  [expectation](../config/expectations.md), passed or not), `errorMessage` on a
  failed run (the error's type and message, as the run record's `error` holds it,
  with the values of secret-looking variables masked).
- **The run record**: a `ubunye_evidence` run facet (config, code and environment
  hashes, the environment, template variables, per-step timings, expectation
  results) and a `ubunye_hash` facet on each dataset (row count, schema hash,
  data hash). Their schemas are in [`docs/schemas`](https://github.com/ubunye-ai-ecosystems/ubunye_engine/tree/main/docs/schemas).

Every event the test suite builds is validated against the OpenLineage 2-0-2 spec
and the facet schemas.

## Backfill: send runs recorded earlier

```bash
ubunye lineage openlineage -d pipelines -u shop -p orders -t clean          # print
ubunye lineage openlineage -d pipelines -u shop -p orders -t clean --send   # and send
```

## Google Dataplex

Dataplex takes OpenLineage events at its own endpoint, with a Google access token:

```bash
export OPENLINEAGE_URL=https://datalineage.googleapis.com
export OPENLINEAGE_ENDPOINT="v1/projects/$PROJECT/locations/$REGION:processOpenLineageRunEvent"
export OPENLINEAGE_API_KEY="$(gcloud auth print-access-token)"
```
