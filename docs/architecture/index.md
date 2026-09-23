# Architecture decisions

Each record says what was decided, why, and what it costs. They are short on
purpose: the code and its tests are the detail.

| ADR | Decision | Since |
|---|---|---|
| [001](adr-001-backends-are-plugins.md) | Backends are plugins, found by name | 0.6.0 |
| [002](adr-002-backend-capabilities.md) | Backends say what they can do; tasks are checked before they run | 0.6.0 |
| [003](adr-003-backend-resolution.md) | One order for choosing a backend; the task never names it | 0.6.0 |

## The rule behind all of them

The engine is a hexagon. The core (`ubunye/core`) holds the contract and the
decisions; everything that touches the outside world (Spark, pandas, a database,
a cloud) is an adapter behind a port, found through an entry point. The core
never imports an engine. A test reads every import in `ubunye/core`, even ones
inside functions, and fails if one names Spark, pandas or pyarrow.
