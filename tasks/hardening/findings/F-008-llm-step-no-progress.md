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
- Spend: from the run's budget when it keeps count (a limit is set), else the port's
  own budget; nothing when neither keeps count.
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
