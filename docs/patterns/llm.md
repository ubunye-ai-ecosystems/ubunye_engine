# Language Model Steps

A task can call a language model through one port. The engine sees every call and
writes it into the run record, so a model step is as visible as a read or a write.

## Call a model from a task

```python
from ubunye import llm
from ubunye.core.interfaces import Task


class LabelReviews(Task):
    def setup(self):
        self.model = llm.port("anthropic", model="claude-haiku-4-5-20251001")

    def transform(self, sources):
        df = sources["reviews"]
        prompts = [f"Is this review positive or negative? One word.\n\n{t}" for t in df["text"]]
        df["label"] = [a.text for a in self.model.complete_many(prompts, max_tokens=5)]
        return {"labelled": df}
```

- `complete(prompt)` makes one call. `prompt` is a user message, or a list of chat
  messages (`[{"role": "user", "content": "..."}]`). Options: `system`, `max_tokens`
  (default 1024), `temperature`, `stop`.
- `complete_many(prompts, max_concurrency=4)` makes one call per prompt, a few at a
  time, and returns the answers in the order of the prompts.
- Each answer has `text`, `input_tokens`, `output_tokens`, `stop_reason`, `model` and
  `request_key`.

Calls run where the task's Python runs. On Spark that is the driver: collect the
column you want to label, or keep the frame small.

## Check each answer, ask again for the bad ones

`complete_many` gives back text. A model asked for a format, such as
`SENTIMENT | ASPECT`, will miss it now and then: a small local model missed it 20
times in 300. Parse every answer, and ask again only for the ones that fail, a set
number of times. Copy this into your task:

<!-- llm-reask:begin -->
```python
SENTIMENTS = {"positive", "negative", "neutral"}


def sentiment_aspect(text):
    """'positive | battery life' -> ('positive', 'battery life'); ValueError if not."""
    parts = [p.strip() for p in text.strip().split("|")]
    if len(parts) != 2 or parts[0].lower() not in SENTIMENTS or not parts[1]:
        raise ValueError(f"not 'SENTIMENT | ASPECT': {text!r}")
    return parts[0].lower(), parts[1]


def with_reminder(prompt, reminder):
    """The prompt with the reminder added to its last user message; a new copy."""
    if not reminder:
        return prompt
    if isinstance(prompt, str):
        return prompt + reminder
    messages = [dict(m) for m in prompt]
    users = [i for i, m in enumerate(messages) if m.get("role") == "user"]
    if not users or not isinstance(messages[users[-1]].get("content"), str):
        return messages + [{"role": "user", "content": reminder.strip()}]
    messages[users[-1]]["content"] += reminder
    return messages


def complete_parsed(port, prompts, parse, *, tries=2, reminder=None, **options):
    """Ask every prompt; ask again, up to `tries` more times, only where `parse` fails.

    Returns (values, failed). values[i] is parse(answer i), or None. failed maps
    each position that never parsed to the last reason: what parse raised, or the
    LLMBudgetError that stopped the asking again.
    """
    from ubunye.core.errors import LLMBudgetError

    if tries < 0:
        raise ValueError(f"tries must be 0 or more, not {tries}")
    prompts = list(prompts)  # by position: a pandas Series would index by label
    values = [None] * len(prompts)
    failed = {i: None for i in range(len(prompts))}
    for attempt in range(tries + 1):
        todo = list(failed)
        asked = [with_reminder(prompts[i], reminder if attempt else None) for i in todo]
        try:
            answers = port.complete_many(asked, **options)
        except LLMBudgetError as exc:
            if attempt == 0:
                raise  # the budget does not cover one pass: nothing to keep
            failed = {i: exc for i in todo}  # keep what was paid for, stop asking
            break
        for i, answer in zip(todo, answers):
            try:
                values[i] = parse(answer.text)
                del failed[i]
            except Exception as exc:  # any error in parse means "did not parse"
                failed[i] = exc
        if not failed:
            break
    return values, failed
```
<!-- llm-reask:end -->

In the task:

```python
values, failed = complete_parsed(
    self.model, list(df["text"]), sentiment_aspect, tries=2, max_tokens=20,
    reminder="\n\nAnswer with one line: SENTIMENT | ASPECT.",
)
df["sentiment"] = [v[0] if v else None for v in values]
df["aspect"] = [v[1] if v else None for v in values]
```

Then decide what a row that failed every try means: drop it, keep it as unknown, or
fail the run with `raise ValueError(f"{len(failed)} answers never parsed: {failed}")`.
`failed` says why for each one.

How it behaves:

- Answers go back by position. `prompts` can be a list or a pandas column; a
  column's own index (after a sort or a filter) is not used.
- Any error in `parse` (a `ValueError`, or a `KeyError` from a missing JSON field)
  counts as "did not parse". That prompt is asked again, and the error is kept in
  `failed`.
- Prompts can be text or chat message lists. The reminder is added to the last
  user message, on a copy; your prompts are not changed.
- Only the failed prompts are sent again. Each extra call counts against the budget
  like any other, so `UBUNYE_LLM_MAX_CALLS` and `UBUNYE_LLM_MAX_USD` still cap it.
  When the budget refuses an ask again, the answers already paid for are kept and
  the rest are returned as failed, with the `LLMBudgetError` as the reason. The
  answers of that last, refused round are lost. When the budget cannot cover the
  first pass, the error is raised.
- Every call, first or again, is in the run record's `llm_calls`.
- Record and replay, inside a run: the engine opens a call log for every task run,
  and there replay gives back the recorded run call for call, also when the same
  prompt is sent twice with no `reminder` (the second ask gets the second recorded
  answer). Outside a run (a notebook, with no `llm.recording()` block) that is not
  so: a second identical ask is recorded over the first, and replay can make fewer
  calls than the recording did. Record and replay inside a run.
- `reminder` changes the prompt on a second try. At `temperature=0` a model asked
  the very same prompt often gives the very same wrong answer; a reminder gives it a
  reason to change.

## Progress on a long batch

A long `complete_many` writes how far it is to stderr:

```
LLM anthropic/claude-haiku-4-5: 90/300 calls (30%), 1m48s, $0.0412 spent of $2
```

A line comes at most every 10 seconds and every 10% of the calls, whichever is
rarer, so a slow batch gets about ten lines. A batch that wrote a line gets one
more line at the end. A batch of fewer than 20 prompts, or one that is done in
under 10 seconds, writes nothing. A loop of `complete()` calls has no total and
writes no progress: use `complete_many`.

When some calls did not get an answer, the line says so:

```
LLM anthropic/claude-haiku-4-5: 300/300 calls (100%), 50 answered, 250 refused by the budget, 5m00s
```

The spend shows only when a dollar limit is enforced
([Cap the bill](#cap-the-bill-before-the-run)): `UBUNYE_LLM_MAX_USD` (in a run, or
outside one), or the port's `max_usd=`. With both, the line shows both, as
`run $0.0412 of $2, port $0.0412 of $0.5`. With no dollar limit, no spend shows.

The line goes to stderr, so stdout stays clean for pipes and for `ubunye mcp`.
When stderr is missing or broken, the line is dropped; it never stops a batch.
Turn it off with `UBUNYE_LLM_PROGRESS=0` (also `false`, `no` or `off`).

## Backends

| Backend | For | Key (when `api_key=` is not given) |
| --- | --- | --- |
| `anthropic` | Anthropic's Messages API | `ANTHROPIC_API_KEY` |
| `openai_compatible` | OpenAI, Azure OpenAI, vLLM, Ollama, LiteLLM, any `/chat/completions` server; set `base_url=` | `OPENAI_API_KEY`, optional |
| `databricks_serving` | A Databricks Model Serving endpoint; `model` is the endpoint name | `DATABRICKS_TOKEN`, workspace from `DATABRICKS_HOST` |

All three use the standard library only: no extra packages. `api_key` can be a
[secret reference](../config/secrets.md), such as `secret://aws-sm/prod/anthropic`.
A missing key fails when the port is made, before any call.

A local model with Ollama:

```python
self.model = llm.port("openai_compatible", model="llama3.2", base_url="http://localhost:11434/v1")
```

Another provider is a plugin: subclass `ubunye.llm.LLMBackend`, implement `build()`
and `parse()`, and register the class in the `ubunye.llm_backends` entry-point group.

## Failures and retries

Rate limits, overload and server errors (408, 409, 429, 5xx, 529) are retried 3 times
with a growing wait, or the wait the provider asks for in `retry-after` (at most 60
seconds). Other refusals, such as a bad key (401) or a bad request (400), fail at once
with the provider's reason. The key is never shown in an error or a log.
Change the policy with `llm.port(..., retries=, backoff_s=, timeout_s=)`.

## What the run record keeps

Every call, passed or failed, is in the record's `llm_calls`:

```json
{"backend": "anthropic", "model": "claude-haiku-4-5-20251001",
 "request_key": "sha256:9c1e...", "status": "ok",
 "input_tokens": 41, "output_tokens": 2, "attempts": 1,
 "stop_reason": "end_turn", "source": "live", "seconds": 0.62}
```

The prompt and the answer are never stored in the record. `request_key` is a hash of
everything that can change the answer (backend, model, system prompt, messages,
`max_tokens`, `temperature`, `stop`): the same key means the same call.
`ubunye lineage trace` sums the calls per model:

```
  MODEL CALLS
    anthropic/claude-haiku-4-5-20251001: 120 calls, 0 failed, 4920 tokens in, 240 out
```

## Record once, replay anywhere

A port has three modes, set with `UBUNYE_LLM_MODE` (or `llm.port(..., mode=)`):

| Mode | What it does |
| --- | --- |
| `live` (default) | Calls the provider. |
| `record` | Calls the provider and keeps each answer in a replay file. |
| `replay` | Answers from the replay file only. No key, no network, no cost. |

```bash
UBUNYE_LLM_MODE=record ubunye run -d pipelines -u shop -p reviews -t label   # once, with a key
UBUNYE_LLM_MODE=replay ubunye run -d pipelines -u shop -p reviews -t label   # anywhere, for nothing
```

The replay file is `llm-replay.jsonl` in the task's folder, next to `config.yaml`,
or the path in `UBUNYE_LLM_STORE` (or `store=`). It holds one line per answer, under
the request's key; it never holds the prompt. Commit it with the task and CI, a
colleague's laptop or another cloud replays the same answers, so the run writes the
same data and the same row hashes.

Replay gives back the recorded run call for call. A run may send the same request
more than once (two identical reviews), and a model can answer each differently; the
nth identical request is replayed the nth recorded answer, so the replayed output is
the recorded output, row for row. Recording again replaces a request's answers.
`complete_many` numbers repeats by their position in the list, so parallel calls
replay the same way.

Replay fails closed. A request with no recorded answer (a new prompt, another
model, another `temperature`) stops the run with the request's key and the hint to
record again; it never falls back to a live call. So does a run that sends a
request more times than the recording holds answers for it. Each call in the run record says
where its answer came from: `"source": "live"`, `"record"` or `"replay"`.

## Cap the bill before the run

Set a ceiling and a call that could pass it is refused before it is sent:

| Variable | Limit |
| --- | --- |
| `UBUNYE_LLM_MAX_USD` | Dollars for the whole run |
| `UBUNYE_LLM_MAX_CALLS` | Number of calls |
| `UBUNYE_LLM_MAX_SECONDS` | Wall clock since the run's first transform started |

```bash
UBUNYE_LLM_MAX_USD=2 ubunye run -d pipelines -u shop -p reviews -t label
```

One budget covers every port in the run. A port can add its own, narrower limits
with `llm.port(..., max_usd=, max_calls=, max_seconds=)`. Outside a run (a notebook),
the variables limit each port.

Before each call the port reserves the call's worst case: the prompt's tokens counted
high (one per 3 characters, plus 4 per message) and the whole `max_tokens` of output.
If what was spent, plus what is in flight, plus that worst case would pass
`UBUNYE_LLM_MAX_USD`, the call is refused with `LLMBudgetError` and never sent.
After the call the reservation becomes the real cost, so many small answers fit
under a ceiling their worst cases would not. Calls running at the same time share
the ceiling. A refused call stops the task before anything is written.

Prices come from a table in the engine (`ubunye.llm.prices`), dated and taken from
the provider's own page: Anthropic's current models, as of 2026-09-24. For any other
model, give the price in USD per million input and output tokens, read from your
provider's pricing page (the numbers below are placeholders):

```python
self.model = llm.port("openai_compatible", model="my-model", price=(0.5, 1.5))
```

or in a JSON file named by `UBUNYE_LLM_PRICES`:

```json
{"openai_compatible": {"my-model": [0.5, 1.5], "llama3.2": [0, 0]}}
```

A model with no price has an unknown cost, never zero: its calls show
`"cost_usd": null`, and a dollar ceiling on it fails closed. Replayed calls cost
nothing and are never refused.

Each call in the run record has `cost_usd` and the `estimated_usd` it reserved. The
record's `llm_budget` keeps the limits, what was spent, and how many calls were made
and refused; `ubunye lineage trace` prints both.

## See the bill before the run

`ubunye plan` prices a task's recorded calls (its replay file) at today's prices and
sets them against the ceiling. It reads no data and calls no model:

```
  Model calls  mode live, max_usd=0.5
    anthropic/claude-haiku-4-5: 120 recorded calls, 492000 tokens in, 24000 out, $0.612000
    estimated $0.612000 of $0.5 (prices as of 2026-09-24)
  warning: llm: a run like the recorded one costs $0.612000, over UBUNYE_LLM_MAX_USD=$0.5; ...
```

The plan fails (exit 1) when the run would: replay mode with nothing recorded, a
limit that is not a number, or a dollar ceiling on a recorded model with no price.
It warns when the estimate is over the ceiling, and when live calls have no dollar
ceiling at all. A task never recorded gets its mode and limits checked, and no
estimate. `ubunye plan --json` carries the same in each task's `llm` section.

## The model bill in FinOps tools (FOCUS)

`ubunye lineage focus` writes a run's model calls as
[FOCUS](https://focus.finops.org/) 1.4 cost rows, the FinOps Foundation's format for
cost and usage data, so they load next to the cloud bill:

```bash
ubunye lineage focus -d pipelines -u shop -p reviews -t label > focus.csv
ubunye lineage focus -d pipelines -u shop -p reviews -t label --format jsonl -o focus.jsonl
```

There is one row per provider, model and token direction (input and output tokens
have different prices). Every mandatory FOCUS 1.4 column is filled: `ChargeCategory`
is `Usage`, `ServiceCategory` is `AI and Machine Learning`, `PricingUnit` is
`1000000 Tokens`, the charge period is the run's start and end, and the billing
period is its calendar month, all in UTC. Custom columns start with `x_`: the run id,
the task, the model, the token direction, the date of the prices used, and the cost
basis.

The costs are tokens times the list price the run used, so `BilledCost`,
`EffectiveCost`, `ContractedCost` and `ListCost` are equal. The engine cannot see
discounts or credits: the provider's invoice is the authority, and `x_CostBasis`
says so on every row. Replayed calls are not charges and make no rows; calls with
no price are left out, and the command says how many. Name the billing account with
`UBUNYE_FOCUS_BILLING_ACCOUNT_ID` and `UBUNYE_FOCUS_BILLING_ACCOUNT_NAME`
(default `unknown`).

## In CI: replay, and gate

Commit the replay file with the task and replay it in CI: no key, no spend, the
same answers on every machine. `ubunye gate --require-replay` fails a run whose
calls went live, and `--max-llm-cost-increase` fails a change that makes the task
send more tokens (see [Gate Pull Requests](gate.md)).
`examples/production/llm_replay` does this on Linux, Windows and macOS: its CI job
replays six calls with nothing listening at the model's address, checks the rows
against a golden hash, and gates two replayed runs.
