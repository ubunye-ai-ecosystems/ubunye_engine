# task-02 — Delete the legacy `pypi` GitHub environment

**Priority:** low (hygiene)

The publish workflow now uses the `pypip` environment. The old `pypi`
environment is no longer referenced anywhere.

## Steps

1. GitHub → Settings → Environments → `pypi` → Delete.
2. Confirm `publish_pypip.yml` still references `environment: pypip`.

No code change needed.
