# Job health and upstream checks

Every run leaves a run record. Two everyday questions are answered from those
records, on any platform, with no scheduler plugin ([ADR 013](../architecture/adr-013-receipt-queries.md)):

- **Is the job healthy?** `ubunye runs health`
- **Has the upstream finished this batch?** `CONFIG.requires`, or `ubunye runs wait-for`

Record your runs (`ubunye run --lineage`). For jobs that share no disk, point
them all at one shared store ([where run records are stored](../cli.md#where-run-records-are-stored)),
for example a Delta table:

```bash
export UBUNYE_LINEAGE_BACKEND=delta
ubunye run ... --lineage --lineage-dir main.ops.ubunye_runs
```

## Job health

```bash
ubunye runs health -d pipelines -u pay --max-age 36h
```

```text
TASK              STATE      LAST         SUCCEEDED   FAILS  STUCK
pay/monthly/calc  ok         success      3h ago          0      0
pay/monthly/send  failing    error        9d ago          2      0

pay/monthly/send: 2 runs in a row failed; last success 2026-11-01T10:05:00Z. Last error: ...
```

For each task: the last run's status, when it last succeeded, how many runs in a
row did not succeed, and runs still `running` after `--stuck-after` (default
`6h`), whose process most likely died. The command exits 1 when a task is
failing, has a stuck run, or (with `--max-age`) has not succeeded recently
enough. Schedule it and alert on the exit code. `--json` gives one document.

Name tasks with `-u` (and `-p`) or `--task uc/pkg/task` (repeatable). With `-u`
the tasks are the folders under `-d` and the tasks with records in the store.

From Python: `ubunye.api.job_health("pipelines", usecase="pay", max_age="36h")`.
Agents: the MCP tool `health`.

## Do not start until the upstream succeeded

A monthly calculation and a payment send run nine days apart. The send must not
go out unless the calculation for that month succeeded. Say so in the send's
config:

```yaml
CONFIG:
  requires:
    - task: pay/monthly/calc
      variables: [month]
  inputs: ...
  outputs: ...
```

```bash
ubunye run -d pipelines -u pay -p monthly -t send --var month=2026-10 --lineage
```

Before the task is loaded, Ubunye looks for a record of `pay/monthly/calc` with
status `success` and `month=2026-10`. If there is none, the run is refused:

```text
[ERROR] Run refused for send: 'pay/monthly/send' requires 'pay/monthly/calc' to have
succeeded for this batch first, and it has not: its latest run of this batch is error ...
  Requires: pay/monthly/calc
  Batch:    month=2026-10
  Hint: Run 'pay/monthly/calc' for this batch first. ...
```

`variables` picks the batch:

| `variables` | The upstream run counts when |
|---|---|
| `[month]` | its `month` equals this run's `month` |
| `{month: "2026-10"}` | its `month` is `2026-10` (values are templated like the rest of the config) |
| left out | every batch variable of this run matches (all but `mode` and `dtf`) |
| `[]` | it succeeded at all |

Add `wait: 2h` (and `poll: 5m`, default 60s) to wait for the upstream instead of
refusing at once. A task with no `requires` runs exactly as before.

The check reads the store the run records into (`--lineage-backend`,
`--lineage-dir`), or the filesystem store under `-d`.

## A check before any job

`ubunye runs wait-for` is the same check as a command, for a job that is not an
Ubunye task or whose config you cannot change:

```bash
ubunye runs wait-for pay/monthly/calc -d pipelines --var month=2026-10 --timeout 2h \
  && ./send-payments.sh
```

It exits 0 once the task has succeeded for that batch (`-dt` and every `--var`),
and 1 when the time is up. From Python: `ubunye.api.wait_for_upstream(...)`.
Agents: the MCP tool `upstream`.
