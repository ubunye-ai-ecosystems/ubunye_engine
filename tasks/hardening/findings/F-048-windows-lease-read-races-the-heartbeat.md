# F-048: On Windows, reading a lease while its heartbeat replaces it can raise PermissionError

**Status:** open
**Severity:** minor (seen as a flaky test; production impact not yet checked)
**Source:** CI, while merging F-041 (2026-09-30)
**Promise:** 5 (nothing is lost silently)

## What happens
`tests/unit/core/test_run_lease.py::TestALostLeaseIsNotASuccess::test_the_heartbeat_survives_a_missing_lease_file`
failed once on `windows-latest`, Python 3.12 (run 36636343582, job 109637796802), and
passed on rerun:

```
PermissionError: [Errno 13] Permission denied: '...\.ubunye\leases\uc\pkg\t\07e1e3ea86d26365.json'
```

The test reads the lease file (`read_text`) while the heartbeat thread, set to beat
every 50 ms, replaces the same file. On Windows a read that meets the replace is
refused; on Linux and macOS the read sees the old or the new file.

## Why it may matter beyond the test
Anything that reads a lease while its owner is alive (a second run judging whether
the first is dead, `--resume`, a takeover) can meet the same race on Windows. If that
read is not retried, it may raise instead of judging the lease. Not yet checked.

## Expected
Lease reads retry a `PermissionError` briefly on Windows (the writer finishes its
replace in milliseconds), and the test does the same or reads through the same
helper. A test that fails the race on purpose (a reader thread against a fast
heartbeat) pins it.
