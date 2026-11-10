# F-100: every Delta append of a named batch read the table history twice

**Status:** fixed (0.9.1)
**Severity:** minor (two extra Spark SQL calls per append, every run)
**Source:** review (0.9.1: fewer steps, same behaviour)
**Promise:** none

## What happened
`ubunye/adapters/spark/delta_batch.py` ran `DESCRIBE HISTORY ... LIMIT 1` before and
after every keyed Delta append of a named batch, only to warn when Delta skipped a
batch an earlier run had already committed. Delta can only skip it when an earlier run
of the same batch wrote it: a run that died (the next run takes over its lease) or a
run that failed after the write. A first run, the usual case, paid for two lookups
that could never warn.

## Fix
The lease says when an earlier run of the batch may have committed the write:
`BatchWrite.retry` is true when this run took over a dead run, or when a failed run
left a `<batch>.tried` note beside the lease (written in `RunLease.rollback` when it
had committed a keyed write). Only then does the append look up the history. A first
run makes no lookup.

## Evidence
`tests/unit/core/test_batch_write.py`: `test_a_first_run_makes_no_history_lookup` and
`test_a_run_after_a_failed_run_that_wrote_the_batch_is_a_retry` fail before the fix
and pass after; `test_delta_append_skipped_by_delta_is_reported` and the live
`tests/integration/test_delta_append_reruns.py::test_a_retry_that_finds_its_batch_already_committed_says_so`
still pass.
