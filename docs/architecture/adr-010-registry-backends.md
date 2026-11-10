# ADR 010: Registry backends are plugins, found by name

**Status:** accepted, 0.9.0

## Context

The model registry had a protocol (`RegistryBackend`) and an entry point group
(`ubunye.registry_backends`, with `filesystem` and `mlflow`), but no running
code used them. `ubunye models *` and the model transform built
`ModelRegistry(store)` directly, so a third party registry could be installed
and never reached. A private auto detect picked `mlflow` when it saw
Databricks, but only tests called it. ADR 001 fixed the same problem for
execution backends.

The protocol is also narrower than what users call: it has register, get,
list_versions, promote, demote and delete, while the CLI and the Python API
also roll back, archive, compare and check promotion gates.

## Decision

Registry backends are found by name in `ubunye.registry_backends`, the shipped
ones exactly like a third party one (`ubunye.models.backends`). A registry
backend is chosen in one order, shared with lineage backends:

1. **Asked for by name:** `--backend` / `-b` on `ubunye models *`,
   `registry.backend` in a model transform, `backend=` on `ModelRegistry` or
   `resolve_registry`.
2. **The environment:** `UBUNYE_REGISTRY_BACKEND`.
3. **The default:** `filesystem`. It needs no cloud and always works.

The store string (`--store`, `registry.store`) goes to the backend's
constructor unchanged. An unknown name is an error that lists the installed
names and says how to add one. The host is never sniffed: Databricks alone no
longer means `mlflow`.

A backend implements the six protocol methods. Everything else is done once in
core, on top of them (`ubunye.models.lifecycle`), so every backend gets it for
free: version numbers when none is given, refusing a duplicate version,
promotion gates (stored with the model as the `promotion_gates` metadata key,
checked before `promote`, `force` skips them), one production version at a
time, rollback, archive and compare. A backend may keep gates its own way by
offering `promotion_gates(use_case, model_name)`.

The stages `development`, `staging`, `production`, `archived` are the public
contract. A backend may call them something else inside and maps them at its
edge.

`ModelRegistry` stays the public class. On `filesystem` it runs the same code as
before, byte for byte on disk. On any other backend it delegates to the
lifecycle layer.

`ubunye.testing.registry_conformance.RegistryConformance` is the suite a
backend runs to prove itself. The filesystem, MLflow and a test-only third
party backend (installed through a real entry point) all run it.

## Consequences

- A new registry (a real MLflow Model Registry, SageMaker, Vertex, a database)
  is a package with one entry point and six methods. No edit to Ubunye.
- `ModelRegistry(store)` with no backend and no env var is unchanged, so
  existing user code and stores keep working.
- If `UBUNYE_REGISTRY_BACKEND` is set, `ModelRegistry(store)` in user code
  follows it too. That is the point of the order, and it is the one way to
  change backend without editing code.
- `_internal.auto_detect.detect_registry_backend` follows the new order and is
  deprecated. It no longer returns `mlflow` because of `DATABRICKS_RUNTIME_VERSION`.
  Nothing called it; the private module was removed in 0.9.1.
- Gates and "one production version" are checked by core with a read before a
  write, not atomically. Two promotions racing on one model can both pass. The
  filesystem registry had the same gap.
