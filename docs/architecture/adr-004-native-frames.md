# ADR 004: Your transform gets native frames; the engine gets the port

**Status:** accepted, 0.7.0

## Context

The engine talks to data through a small port, `DataFramePort`: `schema`,
`count()` (rows) and `collect()`. A Spark DataFrame already fits it. A pandas
DataFrame does not: its `count()` returns the non-null count of every column,
which looks like a number and means something else. So the pandas backend
wrapped its frames in an adapter, and a transform had to write
`sources["orders"].native` to get the pandas DataFrame back. That wart would
have been in every pandas task anyone wrote.

## Decision

Two methods on the `Backend` port mark the boundary:

- `to_native(frame)`: what a transform sees. On pandas, a plain
  `pandas.DataFrame`.
- `to_port(frame)`: what the engine sees (writers, hooks, lineage). On pandas,
  the adapter, where `count()` means rows.

Both default to doing nothing, so on Spark, where one object is both, nothing
changes. The engine converts at the edges: transforms get native frames and
may return native frames (or ports); writers and hooks get ports; `run_task`,
`run_pipeline` and the notebook's `read()` and `transform()` give you native
frames back.

## Consequences

- A pandas transform is ordinary pandas code:
  `frame.assign(total=frame.price * frame.qty)`.
- Lineage and monitors never see a raw pandas frame, so they cannot be fooled by
  its `count()`.
- `run_task(..., backend="pandas")` returns pandas DataFrames: use `len(frame)`
  for the row count.
- A backend that declares neither method behaves exactly as before.
