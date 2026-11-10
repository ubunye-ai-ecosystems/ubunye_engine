# F-091: `models demote --to production` skips the promotion gates

**Status:** fixed on fix/0.9-cli-findings (2026-11-10)
**Severity:** blocker (a model that fails its gates reaches production, unmarked)
**Source:** review (full CLI test run before 0.9.0)
**Promise:** 5 (nothing is lost silently: here, a skipped gate)

## What happens
Version 1.0.0 has auc 0.80 and the model's stored gate is `min_auc: 0.85`.
`models promote -v 1.0.0 --to production` is blocked, as it should be. But
`models demote -v 1.0.0 --to production` prints `[OK] demo/Toy v1.0.0 -> production`,
exits 0, and leaves no `promotion_forced` mark. `ModelRegistry.demote` and the
backends' `demote` did the same from Python. `--to bogus` listed
"development, staging, archived", so production was never meant to be a choice.
`models promote --to development` was also accepted, though its help says
staging | production.

## Repro
```python
reg = ModelRegistry(store)
reg.register("demo", "Toy", "1.0.0", model, {"auc": 0.80}, promotion_gates={"min_auc": 0.85})
```
```bash
ubunye models demote -u demo -m Toy -s <store> -v 1.0.0 --to production
```

## Expected
Demote only moves a version down. Moving up is a promotion and must pass the gates.

## Fix
`ubunye.models.lifecycle.check_demote` refuses a demote that moves a version up
(`development` and `archived` from anywhere; `staging` only from `production`).
`ModelRegistry.demote` (filesystem), the backend lifecycle, the MLflow Model Registry
backend and the folder backend all call it and raise `RegistryError`. The CLI
`--to` options are enums: promote takes staging | production, demote takes
development | staging | archived; anything else is a usage error (exit 2) listing
the choices. The registry conformance suite gained two tests, so every backend is
checked, through `ModelRegistry` and directly.

## Evidence
Failed before, pass after: `tests/unit/cli/test_models_cli.py` (demote to
production/staging, the usage errors) and, for the filesystem, folder, MLflow Model
Registry and third party backends, `test_demote_cannot_reach_production` and
`test_the_backend_demote_cannot_reach_production`.
