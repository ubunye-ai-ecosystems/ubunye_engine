# task-03 — Bump actions from Node 20 → Node 24

**Priority:** low (deprecation warning, not yet blocking)

GitHub Actions is flagging `actions/checkout@v4` and `actions/setup-python@v5`
as running on Node 20 — forced default flips to Node 24 on 2026-06-02.

## Steps

1. Check the latest major of each action on the Marketplace.
2. Bump across all workflows under `.github/workflows/`.
3. Alternatively, set `FORCE_JAVASCRIPT_ACTIONS_TO_NODE24=true` in repo
   variables to opt-in early without touching pins.
