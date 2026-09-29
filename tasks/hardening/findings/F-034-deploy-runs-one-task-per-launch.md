# F-034: a deploy ran one task per launch, so a two step pipeline could not run in a container

**Status:** fixed on feat/prove-r1 (2026-09-29), not yet merged
**Severity:** major
**Source:** proving the real-world example R1 (food prices, `clean` then `monitor`) on
the seven environments (2026-09-29)
**Promise:** a pipeline that runs on a laptop runs unchanged on every deploy target

## What happens
`ubunye deploy glue|dataproc|k8s|container-apps|emr-serverless` took one `-t` and the
job's entry script ran that one task. R1's `monitor` reads what `clean` wrote under
`out_dir`. On Glue, Dataproc and Databricks `out_dir` can be object storage, so two
launches work (at twice the start up cost). On Kubernetes and Azure Container Apps the
container's disk goes when the job ends, so `monitor` in a second launch finds nothing:
the only way to run R1 there was to add shared storage the laptop run never needed.

## Repro
On hardening/real-world at 9f52b61:

```bash
ubunye deploy k8s -u food -p prices -t clean -t monitor --image img --dry-run
# the second -t silently replaced the first: the plan runs only monitor
```

## Fix
`-t` repeats. The tasks run in that order in one job (one session, one disk). The
entry script takes them comma separated in `--task` (one argument, which every
platform can pass, Glue included), stops at the first failure, and prints the records
as one document `{"ubunye_records": [...]}` through the same numbered, digest checked
parts, so no log reader changed. One task prints its record exactly as before.
`--record-out glue.json` writes `glue.clean.json` and `glue.monitor.json`; the deploy
fails if any task failed or was never reached.

Tests: `tests/unit/deploy/test_cloud_deploy.py` (the entry script runs two real tasks,
the second reading the first's output; a failing first task stops the second; one file
per task; a task never reached fails the deploy) and `test_container_deploy.py`.

## Evidence
R1 on all seven environments, one launch each: `docs/proving-ground/latest.md`.
