# task-06 — Structured-streaming example on Databricks

**Priority:** low-medium — a real gap but not blocking v0.1.7.

## Why

Every example today is batch. The engine has never been run in a
`readStream` / `writeStream` shape. Unknown whether the current
Task / Transform contract is expressive enough for a streaming query
(checkpoint location, trigger, output mode).

## Suggested shape

A minimal "kafka-ish" source that's free to run:

- Option A: Delta source with CDF → aggregate → Delta sink with
  `.writeStream.trigger(availableNow=True)`.
- Option B: File-trigger streaming over a UC volume drop zone.

## Design questions first

- How does the Reader plugin signal "this is a stream, not a batch"?
- Where does the checkpoint path live in config?
- Does `ubunye lineage` make sense for a long-running stream, or only
  for micro-batch triggers?

These may need a small schema addition rather than a pure example.
