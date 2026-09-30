# F-008: llm step no progress

**Status:** fixed on fix/f008-f009-llm (2026-09-30)
**Severity:** minor
**Source:** stranger (reviews), round 1 (2026-09-28)
**Promise:** none

## What happens
A 300-call LLM step ran for about 6 minutes with no output between 'Starting task' and 'Run complete'.

## Repro
Run any task with a few hundred complete_many calls.

## Expected
Some sign of progress (calls done of total, spend so far), without noise in short runs.

## Evidence
`complete_many` counted nothing and wrote nothing until it returned. The only
message on its path, the retry note, is `log.info`, and `ubunye run` sets up no
logging, so even that is never seen.

## Fix
`complete_many` ticks a small counter (`ubunye/llm/progress.py`) as each call
finishes, in the worker, so sequential and concurrent batches both count. It writes
a line to stderr:

```
LLM anthropic/claude-haiku-4-5: 90/300 calls (30%), 1m48s, $0.0412 spent of $2
```

- Cadence: a line needs at least 10 seconds and at least 10% of the calls since the
  last one (whichever is rarer), so a slow batch gets about ten lines. A batch that
  wrote a line writes one more at the end.
- Quiet: a batch under 20 prompts, or done in under 10 seconds, writes nothing.
- Spend: see the skeptic review below (the first version picked one budget and
  could show `$0.0000` for a budget that tracks no money).
- Why stderr and not logging: the CLI prints its own lines with `typer.echo` and sets
  up no logging, so an INFO log would be invisible, and a WARNING would be wrong.
  stdout is not safe either: `ubunye mcp` speaks its protocol on stdout. The deploy
  commands already print their long-running progress to stderr.

Tests: `tests/unit/llm/test_llm_progress.py`, with a fake clock that moves one second
per read, so the lines are the same in any thread order.

- `test_a_long_batch_reports_calls_done_of_total[sequential]` and `[concurrent]`:
  300 calls at 1 s each give 10 lines, `30/300` to `300/300`.
- `test_the_line_carries_the_spend_when_a_budget_keeps_count`
- `test_ten_seconds_bound_the_lines_when_ten_percent_come_faster`
- `test_a_fast_batch_writes_nothing`, `test_a_handful_of_calls_writes_nothing_however_slow`

Before (the old `complete_many`): 4 failed, 2 passed, each failure
`assert [] == ['30/300', ...]` or `assert 0 == 10`: no line at all. The two quiet
tests pass on both, as they should. After: 6 passed.

## Skeptic review (2026-09-30) and what changed
The skeptic's scripts (`attack_progress.py`, `part_h.py`) found three problems in
the first version. Each is fixed and has a test.

1. **A progress write could kill a batch.** A stderr that raises (a closed pipe)
   stopped a batch that had succeeded (case A: `BrokenPipeError`, 300 answers lost)
   and hid a call's own error (case K: the user saw `BrokenPipeError`, the
   `LLMError('boom 10')` was hidden). With `sys.stderr` set to None, `print` wrote
   the lines to stdout (case B: 4 lines on stdout). Now the write is best effort:
   any error in it is swallowed, and no stderr means no line, never stdout.
   After: A `batch survived, 300 answers`; K `user sees: LLMError boom 10`; B `0`
   lines on stdout.
2. **Refused and failed calls counted as done, and the wrong spend.** At
   `max_calls=50` of 300 the last line read `300/300 calls (100%), $0.0000 spent`
   (case C). An unpriced model under `max_calls` showed `$0.0000 spent` (H). With
   `UBUNYE_LLM_MAX_USD=2` outside a run no spend showed although it was enforced
   (I). With a run and a port budget, the line showed the run's `$100` while the
   port's `$0.001` refused every call (I2). Now each call is ticked as answered,
   failed or refused; the line names what was not answered. The spend shows for
   every budget that enforces a dollar limit (the run's, the environment's outside
   a run, and the port's), both when both are set, and not at all otherwise.
   After: C `300/300 calls (100%), 50 answered, 250 refused by the budget, 5m00s`;
   C2 `60/60 calls (100%), 40 answered, 20 failed, 1m00s`; H no `$`; I
   `$0.0037 spent of $2`; I2 `0 answered, 40 refused by the budget, ... run $0.0000
   of $100, port $0.0000 of $0.001`.
3. **No off switch.** `UBUNYE_LLM_PROGRESS=0` (or `false`, `no`, `off`) turns it off.

Unchanged and still good: D (20,000 calls on 32 threads: 10 lines, counts rise, the
last is the total), E (320,000 ticks from 64 threads, none lost), G (about 0.5 kB),
L (a 10,000 call replay writes nothing). Overhead on 10,000 stubbed calls, progress
on minus off: -2.1 and +0.3 microseconds per call (noise).

New tests in `tests/unit/llm/test_llm_progress.py`:
`test_a_broken_stderr_never_stops_a_batch_that_succeeded`,
`test_a_broken_stderr_never_hides_the_calls_own_error`,
`test_no_stderr_means_no_line_and_never_stdout`,
`test_refused_calls_are_not_counted_as_answered`,
`test_failed_calls_are_not_counted_as_answered`,
`test_no_spend_shows_without_a_dollar_limit`,
`test_the_environment_dollar_limit_shows_outside_a_run`,
`test_a_run_limit_and_a_port_limit_both_show`,
`test_the_environment_turns_it_off` (4 spellings). On the first version all 12 fail;
after, all pass.
