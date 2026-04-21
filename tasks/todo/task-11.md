# task-11 — `typer[all]` extra missing from pyproject.toml

**Discovered by:** fire-test of `titanic_local` (run 24513418103, 2026-04-16)

## Symptom

```
WARNING: typer 0.24.1 does not provide the extra 'all'
```

Emitted during the "Install engine with Spark + dev extras" step of
`.github/workflows/local_pipeline.yml`.

## Repro

```bash
gh run view --job=71650530074 --repo ubunye-ai-ecosystems/ubunye_engine --log \
  | grep "typer"
```

Or locally:

```bash
pip install "typer[all]==0.24.1"
```

## Context

- Example: `titanic_local`
- Workflow: `.github/workflows/local_pipeline.yml`
- Step: "Install engine with Spark + dev extras"
- The install line runs: `pip install -e ".[spark,dev]" pandas pyarrow`

## Suspected root cause

`pyproject.toml` specifies `typer[all]` as a dependency but `typer 0.24.1`
dropped the `[all]` extra; the dependency declaration needs to be updated
to plain `typer` or pinned to a version that still ships the extra.
