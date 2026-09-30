# F-020: two runs creating a new output at once: the loser gets a raw OS error

**Status:** fixed on branch fix/rerun-safety (ADR 008)
**Severity:** minor
**Source:** experiment E-02 (2026-09-28)
**Promise:** none

## What happens
When two runs start at nearly the same moment and the output folder does not exist
yet, both try to rename their staging folder into place; the second fails with
`[WinError 5] Access is denied: .../out/events` and a traceback. The data is correct
(the first run's), but the message says nothing a user can act on.

## Repro
tests/experiments/e02_concurrent.py: pairs started 0 to 0.1 s apart.

## Expected
A SinkWriteError saying another run wrote the output at the same moment, naming the
path; or no error at all once F-019's lease exists.

## Evidence
3 of 10 pairs (the closest starts): second run exit 1 with the raw WinError.

## Fix (2026-09-29)
The lease is created atomically before anything is written, so the loser of a race gets `RunLeaseHeld` and one line from `ubunye run`, not an OS error (ADR 008). E-02 rerun: all 10 losers refused this way.
