# F-009: llm structured output

**Status:** fixed on fix/f008-f009-llm (2026-09-30), docs only
**Severity:** minor
**Source:** stranger (reviews), round 1 (2026-09-28)
**Promise:** none

## What happens
complete_many returns raw text; the user hand-wrote a parser and 20 of 300 answers missed the format.

## Repro
Ask a small model for 'SENTIMENT | ASPECT'.

## Expected
Decide: a documented pattern, a plugin, or a small helper. Must not grow the core without evidence.

## Evidence
The question was whether the current API lets a task check each answer and ask
again only for the bad ones, cheaply and safely. It does:

- `complete_many(subset)` sends only the prompts it is given, so a task can resend
  just the failed positions.
- Each extra call goes through `send`, so it reserves against the run's budget
  (`max_calls`, `max_usd`) and a call past the limit is refused before sending.
- Each extra call is in the run record's `llm_calls`.
- Record and replay: the flow is deterministic given the answers, and an identical
  resent prompt is numbered as the next occurrence of its key (F-001), so replay gives
  the second ask the second recorded answer. A resent prompt with a reminder is a new
  key, recorded and replayed like any other.

The parser is business logic (what a good answer is), which the config boundary in
CLAUDE.md puts in the task, not in the engine. One report of one format is not the
evidence a core feature needs.

## Fix
No engine change. `docs/patterns/llm.md` gains "Check each answer, ask again for
the bad ones": a copyable `sentiment_aspect` parser and a 20 line `complete_parsed`
helper (parse each answer, resend only the failures, up to `tries` more times, with
an optional `reminder`), how to use it in a task, and what happens to budget, record
and replay.

Tests: `tests/unit/llm/test_llm_reask_recipe.py` reads the code block between the
`llm-reask` markers from the page and runs it against the fake provider, so the page
cannot drift from working code:

- `test_only_the_failed_answers_are_asked_again_up_to_the_cap`: 4 prompts, 2 bad,
  then 1 still bad after the cap: exactly 7 calls, only the failed prompts resent
  (with the reminder), all 7 in the call log.
- `test_the_extra_calls_count_against_the_budget`: `max_calls=5` refuses the sixth
  call (the second resend) before it is sent.
- `test_a_recorded_run_replays_call_for_call`: the same prompt resent with no
  reminder, 7 recorded calls; replay gives the same values and sends nothing.

Before (the page without the recipe): 3 errors, `ValueError: not enough values to
unpack (expected 1, got 0)`, the block is not there. After: 3 passed, with the
engine unchanged.
