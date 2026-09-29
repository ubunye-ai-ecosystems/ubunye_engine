# F-048: On Windows, reading a lease while its heartbeat replaces it can raise PermissionError

**Status:** fixed on branch fix/f048-lease-read-race
**Severity:** minor (a wrong refusal message; no wrong outcome found)
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

## Production impact (checked 2026-09-30, dev box, Windows 11)
Every lease reader is in `ubunye/core/runs.py`; nothing else reads a lease. They are
`RunLease._read` (used by `acquire` to judge the holder, the heartbeat, `_owner` behind
every save and claim, `commit`, `landed` and `still_owned`, `check_finished` for the
finished note, and `_adopt_orphans`), the takeover's rename of a dead lease and its
read of the renamed file, `_dead`'s stat of the lease, and `release`'s removal. The
writer is `_write_atomic` (`os.replace`), which is refused while a reader has the
file open; it already retried 40 times.

`_read` turned the refused read into `{}` ("unreadable for now"), so nothing crashed
and no liveness was judged wrongly: `_owner` retried, the heartbeat skipped a beat,
and a second run was still refused. But it was refused with a blank message ("Run ?
of uc/pkg/t ... process None on None") instead of naming the live run.

Stress on the old code, 3 seconds, heartbeat every 1 ms, two reader threads:

| reader | old code | new code |
|---|---|---|
| plain `read_text` (the test's way) | 17, 17 PermissionError | still fails (not the engine's path) |
| engine `_read` called unreadable | 24, 19 | 0, 0 |
| `stat` of the lease | 0 errors | 0 errors |
| missing file or bad JSON seen | 0 | 0 |
| writer `os.replace` refused (retried) | 53, 54 | 127, 142 (retried, all landed) |
| heartbeat thread alive at the end | yes | yes |
| second run told "Run ?" instead of the run | 20, 11 (of about 4,170) | 0, 0 (of about 3,000) |

## Fix
One helper, `runs._patient(op, ...)`, runs a file operation and retries a
`PermissionError` for up to `BUSY_FOR` (2 s, a pause of 10 ms), then raises it. Every
other error is raised at once, so a missing lease is still missing at once. It is
used by `_read`, the takeover's rename and read, `_dead`'s stat, `_write_atomic`'s
replace (same 2 s budget as its old 40 x 50 ms loop) and `release`. It runs the same
way on every OS; only Windows ever meets the error in this race. The heartbeat now
beats more often under readers (10 ms pauses, not 50 ms).

The flaky test reads, removes and re-reads the lease through the same helpers. New
tests in `tests/unit/core/test_run_lease.py`:
`test_a_reader_never_meets_the_heartbeat_mid_replace` (1.5 s: another run reads the
lease in a loop against a 1 ms heartbeat; failed 3 of 3 on the old code, with 29, 47
and 23 unreadable reads) and `TestPatient` (retries, gives up, never waits for a
missing file).

## Left open
`check_finished` treats a finished note it cannot read (`{}`) as no note, so a note
refused for longer than 2 s would let the batch append again. Nothing replaces the
note while this run holds the lease, so the race here cannot reach it; a scanner
holding it open for seconds could. Refusing instead is a design call for F-031.
