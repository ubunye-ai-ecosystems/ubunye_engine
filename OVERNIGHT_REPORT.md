# Overnight Session Report — 2026-04-16

Branch: `overnight/2026-04-16` (23 commits ahead of `main`)

## Bugs found and fixed (this session)

| Bug | Severity | Commit | Tests |
|-----|----------|--------|-------|
| Sibling modules (`model.py`, `utils.py`) leaked between sequential tasks in `run_pipeline` — Python's `sys.modules` cache keyed on the shared short name | High | `1732d25` | `tests/unit/test_task_runner.py` (3 tests) |
| Undefined CLI template variables silently passed through Jinja render — `{{ dt }}` with no `dt` would hand Spark a literal `{{ dt }}` | High | `a955e0f` | `tests/unit/config/test_resolver.py` (3 tests) |
| GitHub Actions Node 20 deprecation — `actions/checkout@v4` and `actions/setup-python@v5` trigger warnings ahead of 2026-09-16 removal | Low | `59d1fc4` | CI green |
| Unused `pytest` import in `test_task_runner.py` (ruff F401) | Trivial | `41cd83b` | CI green |
| Black formatting in `test_task_runner.py` | Trivial | `6158a0f` | CI green |

## Fire-test results (Round 2)

All four production examples passed on the overnight branch:

| Example | Run ID | Result |
|---------|--------|--------|
| `titanic_local` | 24486921439 | Pass |
| `titanic_databricks` | 24486925775 | Pass |
| `jhb_weather_databricks` | 24486926643 | Pass |
| `titanic_ml_databricks` | 24486927572 | Pass |

Zero bugs found during fire-testing. The two bugs above were found via offline code audit (Round 1).

## New production example (Round 3)

**`titanic_multitask_local`** (`aab60c4`): two-task pipeline demonstrating
sequential task chaining via `ubunye run -t clean_data -t aggregate`.
Exercises `run_pipeline()`, sibling-module isolation, and cross-task lineage.
CI workflow at `.github/workflows/multitask_local.yml` — can't be triggered
via `workflow_dispatch` until merged to `main`.

## Audits with no bugs found

- `FileSystemLineageStore`: all pathlib-based, no OS-specific assumptions.
  UC volumes should work via POSIX FUSE mount.
- `ubunye/config/schema.py` Pydantic validators: no edge-case bugs worth
  fixing. Minor schema smells noted (e.g. `options` vs `model_extra`).
- `ubunye/` codebase: zero `TODO`/`FIXME`/`HACK`/`XXX` markers.

## Test coverage improvements (Round 5)

| Commit | New tests | Coverage area |
|--------|-----------|---------------|
| `7ae9279` | 8 | `ubunye/api.py` — `_make_app_name`, `_build_extra_hooks` |
| `5692833` | 20 | `ubunye/core/runtime.py` — Engine validation, Registry, EngineContext; `ubunye/core/catalog.py` — `set_catalog_and_schema` |

## Test suite

330 unit tests pass (up from 302 at session start). No regressions introduced.

## Tasks

### Done (`tasks/done/`)

| Task | Description |
|------|-------------|
| task-00 | Overnight plan + tasks scaffold |
| task-01 | Titanic ML Databricks example |
| task-02 | sys.path import fix for task_dir |
| task-03 | Node 24 action bumps |
| task-04 | sys.modules sibling leak + Jinja resolver fix |
| task-05 | Multi-task pipeline example |

### Remaining (`tasks/todo/`)

| Task | Description | Status |
|------|-------------|--------|
| task-00 | Fire-test umbrella | Rounds 1-2 complete; Round 3 partially done |
| task-01 | Ship v0.1.7 | Blocked on human review |
| task-02 | Delete legacy `pypi` GitHub environment | Low priority |
| task-04 | Multi-task DAG on Databricks | Needs UC tables |
| task-05 | JDBC reader/writer fire-test | Needs JDBC secret |
| task-06 | Streaming example design doc | Blocked on human (schema change) |
| task-07 | Failure/retry/rollback paths | Medium priority |
| task-08 | Lineage on Databricks | Medium priority |
| task-09 | Telemetry hooks on Databricks | Low-medium |
| task-10 | Research paper | Low (after v0.1.7) |

## Recommendations for morning review

1. Review the two high-severity bug fixes (`1732d25`, `a955e0f`) — both have
   regression tests and are well-scoped.
2. The multi-task example needs its CI validated after merge (workflow_dispatch
   only works on default branch).
3. Consider squash-merging the overnight branch or cherry-picking the
   substantive commits (skip the lint/style fixes).
