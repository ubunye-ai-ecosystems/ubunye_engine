# Architecture decisions

Why the engine is built the way it is. Each record (an ADR, architecture decision
record) says what was decided, why, and what it costs. They are short on purpose: the
code and its tests are the detail. They are written for contributors and for anyone who
wants to know why Ubunye behaves as it does.

Three words used on these pages, in plain terms:

- **Hexagon:** the engine's core, drawn as a shape with many sides, one per kind of
  outside thing it talks to. The core holds the rules and decisions and knows nothing
  about any particular engine or cloud.
- **Port:** a side of that shape: the contract the core sets for one kind of outside
  thing, such as "something that holds a table" or "somewhere to store run records".
- **Adapter:** the code that makes one real thing (Spark, pandas, a database, a cloud)
  fit a port. Swapping an adapter changes what Ubunye talks to without changing the
  core.

| ADR | Decision | Since |
|---|---|---|
| [001](adr-001-backends-are-plugins.md) | Backends are plugins, found by name | 0.7.0 |
| [002](adr-002-backend-capabilities.md) | Backends say what they can do; tasks are checked before they run | 0.7.0 |
| [003](adr-003-backend-resolution.md) | One order for choosing a backend; the task never names it | 0.7.0 |
| [004](adr-004-native-frames.md) | Your transform gets native frames; the engine gets the port | 0.7.0 |
| [005](adr-005-portable-transforms.md) | One transform for every engine: Narwhals, detected not declared | 0.7.0 |
| [006](adr-006-run-record.md) | The run record is correct before it is sold: every row, any order, same on every engine | 0.7.0 |
| [007](adr-007-one-time-zone.md) | One time zone for every backend, UTC unless the task says | 0.8.0 |
| [008](adr-008-rerun-safety.md) | A rerun is safe: one live run per batch, appends land once | 0.8.0 |
| [009](adr-009-compute-outputs-once.md) | Each output is computed once; checks, write and run record get that copy | 0.8.0 |
| [010](adr-010-registry-backends.md) | Registry backends are plugins, found by name | 0.9.0 |
| [011](adr-011-run-record-stores.md) | Run records are stored through a plugin, found by name | 0.9.0 |
| [012](adr-012-model-runs-and-feature-contract.md) | Model runs on the receipt; features checked at predict | 0.9.0 |
| [013](adr-013-receipt-queries.md) | Job health and upstream checks are receipt queries | 0.9.0 |
| [014](adr-014-mlflow-registry-aliases.md) | The MLflow registry backend uses the Model Registry: stages are aliases, the rest are tags | 0.9.0 |
| [016](adr-016-notebook-as-a-run.md) | A notebook's `with` block is one recorded run; every failure names its stage | 0.9.2 |

## The rule behind all of them

The engine is a hexagon. The core (`ubunye/core`) holds the contract and the
decisions; everything that touches the outside world (Spark, pandas, a database,
a cloud) is an adapter behind a port, found through an entry point. The core
never imports an engine. A test reads every import in `ubunye/core`, even ones
inside functions, and fails if one names Spark, pandas or pyarrow.
