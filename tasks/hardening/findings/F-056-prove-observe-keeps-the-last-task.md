# F-056: `ubunye prove observe -t a -t b` quietly observes only the last task

**Status:** fixed on example/r2-olist
**Severity:** minor (no wrong result, a missing one, silently)
**Source:** example-author, writing Tutorial 2 (Olist), 2026-09-30
**Promise:** a newcomer can tell what went wrong

## What happens
`ubunye run` and `ubunye deploy` take several tasks (`-t clean -t orders_fact -t
monthly`). `prove observe` took one `-t`, but a repeated `-t` was not refused: click
keeps the last value. So

```bash
ubunye prove observe --workload r2-olist --env pandas-local \
  -d pipelines -u olist -p sales -t clean -t orders_fact -t monthly -o evidence
```

printed `observed r2-olist in pandas-local: run 89077a1b success` and wrote one
observation, of `monthly` only (`"task_path": "olist/sales/monthly"`), under the
workload name meant for all three. Nothing said the other two were dropped.

## Repro
From `examples/real-world/olist_ecommerce`, after running the three steps with
`--lineage`, the command above.

## Fix
`-t` may be given several times. Several tasks give one observation each, named
`<workload>-<task>`, the way ubunye-infra's `proving/observe.sh` names them; one task
keeps the workload's own name, as before. A task given twice is refused (exit 2), and
so are several tasks with `--record` or `--run-id` (each names one run).

Test: `tests/unit/proving/test_observe_tasks.py` (3; the several-tasks and repeated-task
tests fail before, all pass after).
