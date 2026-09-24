# OpenLineage

Every recorded run (`ubunye run --lineage`, `run_task(lineage=True)`) can send
its receipt to any catalogue that reads [OpenLineage](https://openlineage.io):
Marquez, DataHub, OpenMetadata, Google Dataplex, and others. Nothing extra to
install.

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
| `OPENLINEAGE_NAMESPACE` | the job namespace, default `ubunye` |
| `OPENLINEAGE_DISABLED` | `true` turns emission off |
| `UBUNYE_OPENLINEAGE_FILE` | also append every event, one JSON per line, to this file |

A server that is down or refuses an event is logged and skipped. **Emission
never fails a run.**

## What is in an event

- **The job**: namespace and `usecase.package.task`. **The run**: the run's UUID,
  the same `run_id` as in `ubunye lineage`.
- **Every input and output as a dataset**, named by the OpenLineage naming
  conventions: `s3://bucket` + key (also for `s3a://`), `gs://bucket` + path,
  `abfss://container@account...` + path, `file` + absolute path, `hive` +
  `db.table`, `databricks://<workspace>` + `catalog.schema.table`, a JDBC server +
  `database.schema.table`. Credentials in a URL are removed.
- **Standard facets**, which catalogues already display: `outputStatistics` (rows
  written), `dataQualityMetrics` (rows read), `dataQualityAssertions` (every
  [expectation](../config/expectations.md), passed or not), `errorMessage` on a
  failed run.
- **The receipt**: a `ubunye_evidence` run facet (config, code and environment
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
