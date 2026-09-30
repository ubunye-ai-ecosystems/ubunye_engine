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

## Skeptic review (2026-09-30) and what changed
The skeptic's `attack_recipe.py` found four bugs in the first recipe, and one
claim that was too strong. The fix is still docs only; the engine is unchanged.

1. **A pandas column with its own index put answers on the wrong rows, silently.**
   `prompts[i]` read by label: after a sort (index 3, 2, 1, 0) the row `beta` got
   `gamma`'s answer (case 4). Now `prompts = list(prompts)` first. After: every row
   gets its own answer.
2. **A budget refusal on an ask again lost every answer already paid for.** 10
   prompts, `max_calls=10`, one bad answer: `LLMBudgetError`, 10 paid calls, nothing
   returned (case 1). Now a refusal after the first round stops the asking and
   returns what parsed, with the rest in `failed` and the `LLMBudgetError` as the
   reason. A budget that cannot cover the first pass still raises. After: 10 paid,
   9 values kept, `failed {9: LLMBudgetError(...)}`.
3. **Only `ValueError` meant "did not parse".** A `KeyError` from a JSON parse
   escaped after paying and lost all values (case 2). Now any `Exception` from the
   parse counts, and `failed` maps each position to the last error. After:
   `{3: KeyError('aspect')}`, the other four values kept.
4. **Chat message prompts with a reminder crashed** in round two, `list + str`
   (case 3). Now the reminder goes on the last user message of a copy (or a new user
   message when there is none with text). After: parsed, no crash, the caller's
   list unchanged.
5. `tries=-1` sent nothing and returned all failed (case 8); now `ValueError`.
6. **"Replay call for call" holds only inside a run** (the engine opens a call log
   for every task run). Outside one, a second identical ask is recorded over the
   first, and replay can make fewer calls (cases 5 and 5b: 4 calls recorded, 2
   replayed). The page now says so plainly.

The recipe now returns `(values, failed)` with `failed` a dict of position to
reason, not a list.

New tests in `tests/unit/llm/test_llm_reask_recipe.py`:
`test_a_pandas_column_with_its_own_index_answers_by_position`,
`test_the_extra_calls_count_against_the_budget_and_paid_answers_are_kept`,
`test_a_budget_that_cannot_cover_the_first_pass_raises`,
`test_any_error_in_parse_means_did_not_parse`,
`test_the_reminder_works_on_both_prompt_shapes` (text and messages),
`test_negative_tries_are_refused`. On the first recipe 7 of the 9 tests fail: every
bug test, and the two older tests on the new return shape. The two that pass there
guard behaviour that was already right (a text prompt with a reminder, and a budget
that cannot cover the first pass). After: all 9 pass.
