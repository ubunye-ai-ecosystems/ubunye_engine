# F-001: replay repeated prompts

**Status:** fixed in PR #98
**Severity:** blocker
**Source:** stranger (Amazon Fine Food Reviews, local Ollama), round 1 (2026-09-28)
**Promise:** 2

## What happens
A run sent the same prompt twice (two identical reviews); the model answered each differently; replay gave both the same answer. A free rerun changed 2 of 300 rows and nothing flagged it.

## Repro
Record then replay a task whose prompts repeat, with a model that is not deterministic (tests/unit/llm/test_llm_replay.py::test_repeated_prompts_replay_answer_for_answer).

## Expected
Replay is the recorded run, call for call.

## Evidence
The nth identical request now replays the nth recorded answer (occurrence and session in the file). The new tests fail on 0.7.1 and pass on the branch. The end-to-end rerun on the real 300 reviews is pending (E-08): it was stopped when the dev box ran out of memory.
