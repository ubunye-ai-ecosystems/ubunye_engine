# task-04 — Bump actions from Node 20 → Node 24

**Status:** done (2026-04-16), overnight branch `overnight/2026-04-16`.
**Note:** renamed from todo/task-03 on move (done/task-03 was already in use).

GitHub Actions was flagging `actions/checkout@v4` and `actions/setup-python@v5`
as running on Node 20 — forced default flips to Node 24 on 2026-06-02.

## What changed

- `actions/checkout@v4` → `@v6` (v6.2.0 latest, Node 24).
- `actions/setup-python@v5` → `@v6` (v6.0.2 latest, Node 24).
- 23 occurrences across 9 workflow files in `.github/workflows/`.
- Pinned to major (`@v6`) rather than full SHA — matches existing repo style.

## Verification pending

These bumps need a CI run to confirm the two majors are drop-in. Will be
validated when the next push to this branch triggers `tests.yml` / `ci.yml`.
