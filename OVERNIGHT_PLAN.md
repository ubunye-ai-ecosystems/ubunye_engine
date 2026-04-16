# Overnight autonomous run — 2026-04-16

Branch: `overnight/2026-04-16`. Main is untouched. PR in the morning.

## Ground rules the loop holds itself to

1. **No merges to main, no tags, no releases.** Publish workflow is tag-gated; I will not create tags.
2. **No `--no-verify`, no `--force`, no destructive git ops.** Commits only.
3. **One bug = one commit.** Conventional commit messages. Failing test first where possible.
4. **Docs + changelog move with code.** Every commit that changes behaviour updates `docs/changelog.md` under `[Unreleased]` and touches `tasks/todo|done/` as appropriate.
5. **Stop and document rather than guess** on design decisions (new env var, schema change, public-API break, anything touching `ubunye/core/` public shape). File to `tasks/todo/` with `**Blocked-on-human:**` marker and move on.
6. **Databricks runs cost money/quota.** Cap at ~6 workflow_dispatch triggers per loop iteration. If `gh run` returns quota/auth errors, stop triggering runs and switch to offline work for the rest of the night.
7. **Keep the changelog narrative coherent.** Bug fixes go under `Fixed`, new examples under `Added`.

## Work queue (ordered, revisable)

### Round 1 — low-risk, no Databricks required ✅

- [x] Node 20 → Node 24 action bumps across `.github/workflows/*.yml` (task-03). Commit `59d1fc4`.
- [x] Investigate `FileSystemLineageStore` for `/Volumes/...` path handling (task-08 prep). No bug found — all pathlib-based.
- [x] Audit `ubunye/core/task_runner.py` and `ubunye/api.py` for sys.path / import landmines. Found and fixed sibling-module leak. Commit `1732d25`.
- [x] Audit `ubunye/config/schema.py` Pydantic validators. No bugs found. Found and fixed Jinja resolver silent pass-through. Commit `a955e0f`.
- [x] Lint fix: unused pytest import in test_task_runner. Commit `41cd83b`.

### Round 2 — fire-test existing examples ✅

All four examples passed on branch `overnight/2026-04-16`:

- [x] `titanic_local` — run 24486921439 ✅
- [x] `titanic_databricks` — run 24486925775 ✅
- [x] `jhb_weather_databricks` — run 24486926643 ✅
- [x] `titanic_ml_databricks` — run 24486927572 ✅

Zero bugs found during fire-testing.

Each bug found: new `tasks/todo/task-NN.md`, then fix on this branch, then `mv` to `tasks/done/` and reference the commit.

### Round 3 — build coverage-gap examples (partially done)

- [x] `examples/production/titanic_multitask_local/` — two-task pipeline (clean → aggregate). Commit `aab60c4`.
- [ ] `examples/production/churn_pipeline_databricks/` — 3-task DAG. Needs UC tables. Deferred.
- [ ] `examples/production/jdbc_example/` — Needs JDBC secret. Deferred.
- [ ] Streaming example — **Blocked-on-human** (schema change needed).

### Round 4 — paper prep (partially done)

- [x] Draft `papers/outline.md` — skeleton + argument. Commit `f55f017`.
- [x] Related-work comparison matrix (`papers/related_work_matrix.md`). Commit `f55f017`.
- [ ] Pull MSc thesis and extract voice sheet. Deferred (needs PDF download).

### Round 5 — opportunistic engine hardening ✅

- [x] 330 tests (up from 302). Added coverage for api.py, Engine, Registry, catalog.
- [x] No TODO/FIXME/HACK/XXX in `ubunye/` — clean.
- [x] Plugin discovery verified via `Registry.from_entrypoints()` test.

## Loop cadence

Dynamic pacing via `ScheduleWakeup`. Default ~1500s (25 min) idle tick; shorter (~270s, in-cache) when watching an in-flight Databricks run; back to 1500s once blocked or waiting.

## Halting conditions

Stop scheduling new wakeups if any of:

- Databricks returns auth/quota error — note in plan, continue offline only.
- Unit test suite goes red in a way I can't fix in one commit.
- Queue exhausted (all sections ticked or blocked).
- User message arrives (human takes over).

## Morning handoff

Last iteration before halting:

1. Summarise work in `OVERNIGHT_REPORT.md` at branch root — commits, bugs found, bugs fixed, unblocked tasks, blocked tasks, Databricks run IDs.
2. Leave `tasks/todo/` and `tasks/done/` tidy.
3. Do not open the PR — that's the user's review step.
