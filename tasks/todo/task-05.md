# task-05 — Fire-test the JDBC reader / writer on Databricks

**Priority:** medium — the JDBC plugin has no production-grade example.

## Why

`ubunye/plugins/readers/jdbc.py` and `writers/jdbc.py` are listed in
`ubunye plugins` but have never been driven end-to-end on a real JDBC
source. Production users will hit this before ML.

## Suggested shape

Small example that points at a free Postgres instance (Neon, Supabase,
or a Databricks-hosted one via Lakebase):

- Reader: pull from a sample table.
- Transform: aggregate.
- Writer: push back to a different schema.

CI: skip if `JDBC_URL` secret is absent (mirrors the Databricks
soft-skip pattern already in `databricks_deploy.yml`).

## Likely bug hotspots

- Driver jar resolution on serverless (does `spark.jars.packages` work?).
- Auth leakage in logs (check we don't print the connection string).
- Predicate pushdown when reading partitioned tables.
