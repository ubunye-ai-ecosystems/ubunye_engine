# Model calls, replayed: a task that labels reviews

A task that calls a language model through `ubunye.llm`, with its answers
committed next to it, so it runs anywhere with no key, no network and no cost,
and writes the same rows every time.

## What it does

It reads six product reviews (`data/reviews.csv`), asks a model whether each is
positive or negative, and writes the reviews with a `label` column.

## The answers are from a stub

The committed answers in `pipelines/shop/reviews/label/llm-replay.jsonl` were
recorded from `scripts/stub_model.py`, a tiny server that speaks the OpenAI chat
format and labels by looking for a few words. It is not a language model. The
example proves the replay machinery (the same rows and the same hash on Linux,
Windows and macOS, with nothing to call), not a model's judgement.

## Run it

```bash
export UBUNYE_LLM_MODE=replay
export REVIEWS_INPUT_PATH="$PWD/examples/production/llm_replay/data/reviews.csv"
export REVIEWS_OUTPUT_PATH="$PWD/out/labelled"
ubunye plan -d examples/production/llm_replay/pipelines -u shop -p reviews -t label
ubunye run  -d examples/production/llm_replay/pipelines -u shop -p reviews -t label \
  --backend pandas --lineage
```

`ubunye lineage trace` then shows the 6 calls, all `replay`.

## Record real answers

Point the task at a real OpenAI-compatible server and record once:

```bash
export LLM_BASE_URL=https://api.openai.com/v1 LLM_MODEL=<model> OPENAI_API_KEY=...
export UBUNYE_LLM_PRICES=prices.json   # the model's price, from the provider's page
UBUNYE_LLM_MODE=record UBUNYE_LLM_MAX_USD=0.05 ubunye run ... --backend pandas --lineage
```

A dollar ceiling needs the model's price: the engine's own table lists only
Anthropic's models, and an unpriced model under a ceiling is refused.

Commit the new `llm-replay.jsonl`, and update `expected_output/golden.json` from the
run record. `scripts/record.py` does both from the stub.

## In CI

`.github/workflows/llm_replay.yml` replays the task on Linux, Windows and macOS,
checks every call replayed and the rows match `expected_output/golden.json`, and
runs `ubunye gate --require-replay` over two replayed runs.
