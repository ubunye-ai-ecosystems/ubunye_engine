# task-00 — Fix PyPI Trusted Publisher env mismatch

**Status:** done (2026-04-16)

## What

Renamed the GitHub environment used by `publish_pypip.yml` from `pypi` to
`pypip` so it matches the Trusted Publisher entry configured on PyPI. OIDC
was failing with `invalid-publisher` because the five fields (owner, repo,
workflow filename, environment, ref) must match byte-for-byte.

## Result

- `v0.1.6` re-tagged and published to PyPI successfully via OIDC.
- No long-lived PyPI token stored in the repo.

## Follow-up

The legacy `pypi` GitHub environment still exists. Optional cleanup —
see `todo/task-02.md`.
