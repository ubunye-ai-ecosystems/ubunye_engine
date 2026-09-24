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

The replay file is `.ubunye/llm-replay.jsonl` in the task's folder, or the path in
`UBUNYE_LLM_STORE` (or `store=`). It holds one line per answer, under the request's
key; it never holds the prompt. Commit it next to the task and CI, a colleague's
laptop or another cloud replays the same answers, so the run writes the same data
and the same row hashes. If your `.gitignore` excludes `.ubunye/`, point
`UBUNYE_LLM_STORE` at a path you commit.

Replay fails closed. A request with no recorded answer (a new prompt, another
model, another `temperature`) stops the run with the request's key and the hint to
record again; it never falls back to a live call. Each call in the run record says
where its answer came from: `"source": "live"`, `"record"` or `"replay"`.
