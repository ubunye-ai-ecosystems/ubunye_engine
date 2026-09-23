# ADR 002: Backends say what they can do; tasks are checked before they run

**Status:** accepted, 0.6.0

## Context

With more than one backend, a task can ask for something its backend cannot do:
a Hive table on pandas, a `merge` on plain files, a cloud path on a laptop
engine. Before 0.6.0 that failed wherever it happened to break, often halfway
through a run, with an error from deep inside (`AttributeError: ... has no
attribute 'spark'`). The engine also tested `backend.is_spark`, which assumes
there are only two kinds of engine.

## Decision

A backend declares `CAPABILITIES`, a small frozen record:

| Field | Meaning |
|---|---|
| `features` | What it provides: `spark` (a SparkSession), `path_io` (read and write paths), `partitioned_writes`, `remote_paths`, `catalog` |
| `file_formats` | The path formats it handles, or "any" |
| `write_modes` | The write modes it can do, or "any" |
| `distributed`, `lazy`, `needs_jvm` | Facts a person or a tool may want to know |

A connector declares what it `REQUIRES` of a backend, using the same feature
names: hive, jdbc, delta, unity, binary and rest_api require `spark`; the `s3`
path connector requires `path_io`.

Before anything starts, the engine compares the two for every input and output
and reports every problem at once. `ubunye validate --backend NAME` runs the same
check without starting anything.

## Consequences

- A task that cannot run fails in the first second with the whole list, not
  halfway through with the first error.
- The core asks; it never assumes what an engine can do. `is_spark` still works
  but is now read from the capabilities, and is deprecated.
- A backend or connector that declares nothing is not pre-checked, so older
  plugins behave exactly as before.
