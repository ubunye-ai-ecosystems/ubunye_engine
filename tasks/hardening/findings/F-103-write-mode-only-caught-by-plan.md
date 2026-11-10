# F-103: a write mode the writer cannot do passed `ubunye validate`; only `plan` caught it

**Status:** fixed (0.9.1)
**Severity:** minor (the run failed at write time, after the transform)
**Source:** docs audit (0.9.1)
**Promise:** `ubunye validate` catches a config the connector cannot run

## What happened
A `jdbc` output with `mode: merge` validated. `ubunye plan` reported it ("Write mode
'merge' is not supported by the 'jdbc' connector."), and so did the writer, but only
when it wrote, after reading and transforming. The check lived in `core/planning.py`.

## Fix
The check moved to `write_modes.resolve_for(writer_cls, cfg)`, which `plan` and
config validation both call, so they give the same message. Validation holds a writer
to it only when the writer declares its modes (`SUPPORTED_MODES` in its module or on
its class, not just `Writer`'s default), so a third-party writer that declares nothing
is still free, as before. Every writer the engine ships declares its modes.

A config that now fails validation was one its writer refused at write time: a mode
the writer does not support, `merge` with no `merge_keys` or into a non-Delta format,
`overwrite_partitions` with neither `partitionBy` nor `replace_where`.

## Evidence
`tests/unit/config/test_write_mode_validation.py::test_a_jdbc_merge_fails_validation_with_the_message_plan_gives`
fails before and passes after; the jdbc append and the undeclared-writer cases pass
both ways. `tests/unit/cli/test_plan.py` passes unchanged.
