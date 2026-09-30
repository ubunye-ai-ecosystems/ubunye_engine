# F-007: max usd unpriced model

**Status:** fixed in PR #98 (hint)
**Severity:** minor
**Source:** stranger (reviews), round 1 (2026-09-28)
**Promise:** none

## What happens
UBUNYE_LLM_MAX_USD refused every call to a local model that has no price.

## Repro
llm.port('openai_compatible', ..., max_usd=1) against Ollama.

## Expected
Refusing is right (a dollar cap cannot be enforced without a price), but the hint must name the free case.

## Evidence
The hint and plan now say price=(0, 0) for a free local model; test added.
