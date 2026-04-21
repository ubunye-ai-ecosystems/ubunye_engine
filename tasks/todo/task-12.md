# task-12 — `actions/setup-java@v4` uses deprecated Node.js 20 runtime

**Discovered by:** fire-test of `titanic_local` (run 24513418103, 2026-04-16)

## Symptom

```
Node.js 20 actions are deprecated. The following actions are running on
Node.js 20 and may not work as expected: actions/setup-java@v4. Actions
will be forced to run with Node.js 24 by default starting June 2nd, 2026.
Node.js 20 will be removed from the runner on September 16th, 2026.
```

Emitted as a GitHub Actions `##[warning]` in the "Complete job" step of
`.github/workflows/local_pipeline.yml`.

## Repro

```bash
gh run view --job=71650530074 --repo ubunye-ai-ecosystems/ubunye_engine --log \
  | grep "Node.js 20"
```

## Context

- Example: `titanic_local`
- Workflow: `.github/workflows/local_pipeline.yml`
- Step: "Complete job" (annotation surfaced at job end)
- The workflow pins `actions/setup-java@v4`

## Suspected root cause

`actions/setup-java@v4` bundles a Node.js 20 runner; the action needs to
be bumped to a version that ships a Node.js 24-compatible runtime before
GitHub enforces the cutover on 2026-06-02.
