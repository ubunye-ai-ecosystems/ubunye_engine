# ADR 013: Job health and upstream checks are receipt queries

**Status:** accepted, unreleased (0.9)

## Context

Two questions come up in every team that runs jobs on a schedule:

- **Is the job healthy?** When did it last succeed, how many runs in a row
  failed, and is a run stuck saying `running` because its process died?
- **Has the upstream finished?** A monthly calculation and a payment send run
  nine days apart. Nothing checks that the first one ran before money goes out.

Schedulers answer these with platform parts: a Databricks Jobs API call, an
Airflow sensor, a Glue trigger. Each answer works on one platform only, and a
task moved to another platform loses it.

Every run already leaves a run record (the receipt, [ADR 006](adr-006-run-record.md))
with its task, status, start and end time and variables. Since
[ADR 011](adr-011-run-record-stores.md) those records can live in a shared store
chosen by name, so jobs that share no disk can read each other's receipts.

## Decision

1. **Both are queries over run records.** `ubunye.lineage.health` and
   `ubunye.lineage.upstream` use only the four `RunRecordStore` methods
   (`save`, `load`, `list_runs`, `search`), so they work on every store: the
   filesystem one, Delta, or a plugin. No Databricks Jobs plugin and no
   per-platform sensor.
2. **Health**, per task: the last run's status, when it last succeeded, the
   runs in a row that did not succeed since, and runs still `running` after
   `--stuck-after` (default 6 hours: likely dead). With `--max-age`, a task
   whose last success is older is stale. A task is unhealthy when it is
   failing, has a stuck run, or is stale; the command then exits 1, so it can be
   a scheduled check.
3. **Upstream** is a config key, `CONFIG.requires`, because it is platform
   invariant: the payment send needs the calculation on every platform.

   ```yaml
   CONFIG:
     requires:
       - task: pay/monthly/calc
         variables: [month]   # or {month: "2026-10"}; left out: every batch variable
         wait: 2h             # optional
   ```

   A requirement is met by a record of that task with status `success` whose
   variables match this run's batch. Batch means the same thing as in rerun
   safety ([ADR 008](adr-008-rerun-safety.md)): every variable but `mode` and
   `dtf`. The check runs in `execute_user_task`, before the task is loaded, so
   the CLI, `run_task`, `run_pipeline` and every deploy target share it. When the
   upstream has not succeeded the run is refused with what is missing, which
   batch, the upstream's latest run of that batch, and what to try. With `wait`
   it looks again every `poll` (default 60 s) until the time is up. A task with
   no `requires` does nothing new, and its config dumps and hashes as before.
4. **The store** the check reads is the one the run records into
   (`--lineage-backend`, `--lineage-dir`); with no store named, the filesystem
   one under the use case folder. A non-filesystem store chosen only by
   `UBUNYE_LINEAGE_BACKEND`, with no location, is an error that says to name it.
5. **A new command group, `ubunye runs`**, holds `health` and `wait-for`, not
   `ubunye lineage`. The `lineage` commands show what records say and exit 0
   when they find them. `runs` commands answer a yes or no question for a
   scheduler, and their exit code is the answer. `wait-for` is the same check as
   `requires`, for a job that cannot change its config or is not an Ubunye task.
6. **The same functions everywhere:** `ubunye.api.job_health`,
   `ubunye.api.wait_for_upstream`, and the read-only MCP tools `health` and
   `upstream`.

## Consequences

- A team gets job health and an upstream gate on any platform, from the records
  it already writes, by pointing jobs at one shared store.
- The upstream must record its runs (`--lineage`) into the store the downstream
  reads. A run that was never recorded cannot be seen.
- Any successful run of the batch counts, even if a later rerun of that batch is
  under way or failed: rerun safety replaces a batch only after the new run
  succeeds, so the earlier data stands until then.
- Health reads the newest 50 runs per task, and searches further back only for
  the last success. On a large Delta table each task is one filtered read.
- Not done: a lease or lock between the upstream and the downstream. The check is
  made once, when the downstream starts.
