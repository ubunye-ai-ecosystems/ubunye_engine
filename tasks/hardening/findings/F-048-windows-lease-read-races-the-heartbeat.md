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
(Closed by the skeptic review below.) `check_finished` treated a finished note it
cannot read as no note.

## Skeptic review (2026-09-30) and what changed
The skeptic proved three bugs on the first fix (7c0e39a), with a real Windows lock
(`CreateFileW`, share mode none or no read) and real ACL denials (`icacls`). The
decision: a lease file that exists but cannot be read or parsed refuses the run with
a clear message. It is never treated as absent, free or dead. A missing file is
still absent.

One reader, `_read_text`, now tells the two apart: None for a missing file, the
`OSError` raised for one that exists but cannot be read after `_patient`. The
refusal is a `RunLeaseHeld` naming the file (one line, exit 1). It is not an
`OSError`, since `held()` runs without a lease on an `OSError`.

1. **Double append (most severe).** A finished note locked for 5 s: run two appended
   the batch again (2 part files), and `--rerun` could not heal it (it replaced the
   second copy, not the first). An ACL denial did the same. Fix: `check_finished`
   refuses when the note cannot be read, or is not valid JSON (it is written in one
   replace, so it is never partly written). After: refused, 1 part file (lock 5 s,
   2.0 s; ACL denial, 2.1 s). Locks under 2 s still read the note and refuse with
   `BatchFinished`, as before.
2. **Live lease taken over when unreadable.** A live run A whose lease could not be
   read (no read sharing) and was 20 minutes old: `{}` skipped the same host pid
   check, so it was judged dead by its age. The renamed file could not be read
   either, and `None == None` passed the check. Run B took over, and A lost its lease
   while alive. Fix: a lease that exists but cannot be read refuses (never judged by
   age); a takeover that cannot read the file it renamed puts it back (a hard link,
   which never overwrites) and refuses. `_dead` calls a lease whose age cannot be
   read not dead, instead of raising an `OSError` that would have run B without a
   lease. After: B refused in 2.0 s in both cases, and A saved its claim, still
   owner. Kept on purpose: a lease that is empty or not valid JSON is still judged by
   its age. A crash inside `_create` leaves one, and it names no process and claims
   nothing.
3. **Slow failure on a real ACL denial.** `_owner` looped 5 times over a read that was
   now patient (2 s each). A run whose own lease was denied failed after 30.8 s (0.8 s
   before 7c0e39a). Fix: one patient read; the loop is kept only for an empty or
   partial lease being put back by `_create`. After: the whole run fails in 6.0 s
   (three saves: the write, the rollback, the kept lease), a claim in one `BUSY_FOR`.
   The `TAKEOVER_SETTLE` comment now states the real bound: a save checks ownership,
   then its replace lands within `BUSY_FOR`.

Also checked:

- **`_adopt_orphans` skipped an orphan it could not read**, and the run went on. The
  orphan's claimed files then stayed beside the new append: the batch twice, until a
  later run of the batch adopted it. Not provably safe, so it now refuses the same
  way, and the orphan is kept. An empty or non JSON orphan is recovered as empty and
  removed (the same judgement as a takeover of such a lease).
- **`_record_status` treated an unreadable record as "not success".** The record is
  written before the commit, so a run that died between the two has "success" only
  in its record. Taking it for unfinished removed its claimed files (the batch) and
  appended its unclaimed outputs again. Now a record that exists but cannot be read
  refuses the takeover, and the lease is handed back as it was. A record that is
  missing or not valid JSON is still "not success": it is written with `write_text`,
  so a run that died while writing it had not recorded success.

Tests (`TestUnreadableIsNeverAbsent`, 8 tests): an unreadable finished note, a
damaged one, one locked open for real (Windows only), an unreadable live lease with
an old mtime, a takeover whose renamed lease cannot be read, an unreadable orphan, an
unreadable record of a dead run, and a denied lease failing a claim in one
`BUSY_FOR`. All 8 failed on 7c0e39a (7 did not refuse; the claim took 1.78 s against
a 0.9 s bound) and pass on the new code.
