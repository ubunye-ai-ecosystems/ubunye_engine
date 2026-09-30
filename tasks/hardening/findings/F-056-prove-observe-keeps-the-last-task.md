# F-056: `ubunye prove observe -t a -t b` quietly observes only the last task

**Status:** open
**Severity:** minor (no wrong result, a missing one, silently)
**Source:** example-author, writing Tutorial 2 (Olist), 2026-09-30
**Promise:** a newcomer can tell what went wrong

## What happens
`ubunye run` and `ubunye deploy` take several tasks (`-t clean -t orders_fact -t
monthly`). `prove observe` takes one `-t`, but a repeated `-t` is not refused: click
keeps the last value. So

```bash
ubunye prove observe --workload r2-olist --env pandas-local \
  -d pipelines -u olist -p sales -t clean -t orders_fact -t monthly -o evidence
```

prints `observed r2-olist in pandas-local: run 89077a1b success` and writes one
observation, of `monthly` only, under the workload name meant for all three. Nothing
says the other two were dropped.

## Repro
From `examples/real-world/olist_ecommerce`, after running the three steps with
`--lineage`, the command above: `evidence/r2-olist/pandas-local.json` holds one record,
the `monthly` run.

## Expected
Either refuse a repeated `-t` ("observe takes one task; run it once per task, or see
proving/observe.sh"), or observe each task as `<workload>-<task>`, the way ubunye-infra's
`proving/observe.sh` already names them.

## Workaround
Tutorial 2 loops over the tasks, one `prove observe` each.
