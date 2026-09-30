# F-024: the Databricks workspace refuses to start new runs

**Status:** resolved (2026-09-28, 23:02 UTC onwards)
**Severity:** major (blocks the Databricks column of the proving ground)
**Source:** proving ground, prove-c01 runs 36456300650 and 36457556308 (2026-09-28)
**Environment:** the sandbox Databricks workspace (serverless only: no clusters, one SQL warehouse)
**Promise:** none; not a Ubunye defect

## What happens
`databricks jobs submit` fails before anything runs: "Triggering new runs for
organization <id> is currently disabled temporarily." The token is valid (identity and
listing calls succeed); only new runs are refused. The last successful Databricks runs
were 2026-09-24/25 (databricks-rc, databricks-llm).

## What it means for the evidence
The proving report shows databricks FAIL with the platform's own words as the reason:
the environment was tried and did not execute. It is not shown as NOT RUN (it was
attempted) and never as PASS.

## Next
The account owner checks the workspace's state (quota, trial, billing hold). Rerun
`prove-c01` when runs are allowed again. Separately: the workflow authenticates with a
personal access token of the owner's user; the migration path is a service principal
with GitHub OIDC federation (keyless, scoped), like the AWS, GCP and Azure sandboxes.

## Resolution
From 2026-09-28 23:02 UTC runs start again, submitted by the new service principal
`ubunye-test` (OAuth machine-to-machine, secrets DATABRICKS_CLIENT_ID and
DATABRICKS_SECRET in the infra `databricks` environment). Which change lifted the
refusal is not separable from the evidence: the switch from the owner's token to the
service principal, or the Free Edition daily quota resetting. The service principal
needed its own schema (`workspace.ubunye_sp`, which it owns) because it had no rights on
`workspace.ubunye_prove`. C01 then passed on Databricks, run 36497932069.
