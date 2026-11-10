"""Model registry CLI — inspect and manage model versions.

Mounted on the main app as the ``models`` sub-command group.

Commands::

    ubunye models list    --use-case fraud --model FraudRiskModel --store .ubunye/model_store
    ubunye models info    --use-case fraud --model FraudRiskModel --version 1.2.0 --store ...
    ubunye models promote --use-case fraud --model FraudRiskModel --version 1.2.0 --to production
    ubunye models demote  --use-case fraud --model FraudRiskModel --version 1.2.0 --to staging
    ubunye models rollback --use-case fraud --model FraudRiskModel --version 1.1.0 --store ...
    ubunye models archive  --use-case fraud --model FraudRiskModel --version 1.0.0 --store ...
    ubunye models compare  --use-case fraud --model FraudRiskModel --store ... --versions 1.1.0 1.2.0

Every command takes ``--backend`` / ``-b`` to choose the registry backend
(ADR 010); without it ``UBUNYE_REGISTRY_BACKEND`` decides, else ``filesystem``.
"""

from __future__ import annotations

from dataclasses import asdict
from enum import Enum
from typing import Any, Dict, List, Optional

import typer

from ubunye.cli.output import emit, fail, json_option
from ubunye.core.errors import UbunyeError
from ubunye.models.registry import ModelRegistry, ModelStage, ModelVersion

models_app = typer.Typer(
    name="models",
    help="Manage model versions and lifecycle (list, promote, rollback, compare).",
    add_completion=False,
)

# ---------------------------------------------------------------------------
# Shared option factories
# ---------------------------------------------------------------------------


class PromoteTarget(str, Enum):
    """Stages ``models promote`` moves a version up to."""

    staging = "staging"
    production = "production"


class DemoteTarget(str, Enum):
    """Stages ``models demote`` moves a version down to. Never production."""

    development = "development"
    staging = "staging"
    archived = "archived"


_use_case_opt = typer.Option(..., "--use-case", "-u", help="Use-case grouping.")
_model_opt = typer.Option(..., "--model", "-m", help="Model name.")
_store_opt = typer.Option(..., "--store", "-s", help="Model store path.")
_version_opt = typer.Option(..., "--version", "-v", help="Version string.")
_backend_opt = typer.Option(
    None,
    "--backend",
    "-b",
    help="Registry backend (an entry point in ubunye.registry_backends). "
    "Default: UBUNYE_REGISTRY_BACKEND, else filesystem.",
)


def _open(store: str, backend: Optional[str], as_json: bool = False) -> ModelRegistry:
    """The registry at ``store`` on the chosen backend, or a clear error and exit 1."""
    try:
        return ModelRegistry(store, backend=backend)
    except UbunyeError as e:
        fail(str(e), as_json=as_json)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@models_app.command("list")
def list_versions(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    as_json: bool = json_option(),
):
    """List all registered versions for a model (newest first)."""
    registry = _open(store, backend, as_json)
    try:
        versions = registry.list_versions(use_case, model)
    except FileNotFoundError as e:
        fail(str(e), as_json=as_json)

    if as_json:
        emit([_version_dict(mv) for mv in versions])
        return
    if not versions:
        typer.echo(f"No versions registered for {use_case}/{model}.")
        return

    _print_versions_table(versions)


# ---------------------------------------------------------------------------
# info
# ---------------------------------------------------------------------------


@models_app.command("info")
def info(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    version: str = _version_opt,
):
    """Show full details of a specific model version as JSON."""
    registry = _open(store, backend)
    try:
        versions = registry.list_versions(use_case, model)
    except FileNotFoundError as e:
        typer.secho(f"[ERROR] {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    mv = next((v for v in versions if v.version == version), None)
    if mv is None:
        typer.secho(
            f"[ERROR] Version '{version}' not found for {use_case}/{model}.",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    emit(_version_dict(mv))


# ---------------------------------------------------------------------------
# promote
# ---------------------------------------------------------------------------


@models_app.command("promote")
def promote(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    version: str = _version_opt,
    to: PromoteTarget = typer.Option(..., "--to", help="Target stage: staging | production."),
    promoted_by: Optional[str] = typer.Option(None, "--promoted-by", help="Username."),
    force: bool = typer.Option(
        False,
        "--force",
        help="Promote without checking the model's promotion gates. The version "
        "records that it was forced.",
    ),
):
    """Promote a model version to a higher lifecycle stage.

    The version must pass the promotion gates stored with the model (set by
    the training run's promotion_gates); --force skips them, with a warning.
    """
    target_stage = ModelStage(to.value)

    registry = _open(store, backend)
    try:
        gates = registry.promotion_gates(use_case, model)
        if force and gates:
            typer.secho(
                f"[WARN] Promoting without checking its {len(gates)} gate(s): "
                + ", ".join(f"{k}={v}" for k, v in sorted(gates.items())),
                fg=typer.colors.YELLOW,
            )
        mv = registry.promote(
            use_case, model, version, target_stage, promoted_by=promoted_by, force=force
        )
        checked = f"  ({len(gates)} gate(s) passed)" if gates and not force else ""
        typer.secho(
            f"[OK] {use_case}/{model} v{mv.version} -> {mv.stage.value}{checked}",
            fg=typer.colors.GREEN,
        )
    except (FileNotFoundError, ValueError) as e:
        typer.secho(f"[ERROR] {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# demote
# ---------------------------------------------------------------------------


@models_app.command("demote")
def demote(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    version: str = _version_opt,
    to: DemoteTarget = typer.Option(
        ..., "--to", help="Target stage: development | staging | archived."
    ),
):
    """Demote a model version to a lower lifecycle stage.

    Demote never moves a version up: staging is reached from production only.
    Use promote to move up; it checks the promotion gates.
    """
    target_stage = ModelStage(to.value)

    registry = _open(store, backend)
    try:
        mv = registry.demote(use_case, model, version, target_stage)
        typer.secho(
            f"[OK] {use_case}/{model} v{mv.version} -> {mv.stage.value}",
            fg=typer.colors.GREEN,
        )
    except (FileNotFoundError, ValueError) as e:
        typer.secho(f"[ERROR] {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# rollback
# ---------------------------------------------------------------------------


@models_app.command("rollback")
def rollback(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    version: str = _version_opt,
):
    """Roll back production to a specific previous version."""
    registry = _open(store, backend)
    try:
        mv = registry.rollback(use_case, model, version)
        typer.secho(
            f"[OK] Rolled back {use_case}/{model} — production is now v{mv.version}",
            fg=typer.colors.GREEN,
        )
    except (FileNotFoundError, ValueError) as e:
        typer.secho(f"[ERROR] {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# archive
# ---------------------------------------------------------------------------


@models_app.command("archive")
def archive(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    version: str = _version_opt,
):
    """Archive a model version."""
    registry = _open(store, backend)
    try:
        mv = registry.archive(use_case, model, version)
        typer.secho(
            f"[OK] {use_case}/{model} v{mv.version} archived.",
            fg=typer.colors.YELLOW,
        )
    except (FileNotFoundError, ValueError) as e:
        typer.secho(f"[ERROR] {e}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


@models_app.command("compare")
def compare(
    use_case: str = _use_case_opt,
    model: str = _model_opt,
    store: str = _store_opt,
    backend: Optional[str] = _backend_opt,
    versions: List[str] = typer.Option(..., "--versions", help="Two version strings to compare."),
    as_json: bool = json_option(),
):
    """Compare metrics between two model versions."""
    if len(versions) != 2:
        fail("Provide exactly two --versions values.", as_json=as_json)

    version_a, version_b = versions[0], versions[1]
    registry = _open(store, backend, as_json)
    try:
        diff = registry.compare_versions(use_case, model, version_a, version_b)
    except (FileNotFoundError, ValueError) as e:
        fail(str(e), as_json=as_json)

    if as_json:
        emit(diff)
        return
    if not diff:
        typer.echo("No metrics to compare.")
        return

    typer.echo(f"\n  Comparing {use_case}/{model}: {version_a} vs {version_b}\n")
    typer.echo(f"  {'Metric':<20} {'v' + version_a:<18} {'v' + version_b:<18} Delta")
    typer.echo(f"  {'-'*20} {'-'*18} {'-'*18} {'-'*10}")
    for metric, vals in diff.items():
        a_str = f"{vals['a']:.4f}" if isinstance(vals["a"], (int, float)) else str(vals["a"])
        b_str = f"{vals['b']:.4f}" if isinstance(vals["b"], (int, float)) else str(vals["b"])
        d = vals["delta"]
        delta_str = f"{d:+.4f}" if d is not None else "n/a"
        color = None
        if d is not None:
            color = typer.colors.GREEN if d > 0 else (typer.colors.RED if d < 0 else None)
        line = f"  {metric:<20} {a_str:<18} {b_str:<18} {delta_str}"
        if color:
            typer.secho(line, fg=color)
        else:
            typer.echo(line)
    typer.echo()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STAGE_COLORS = {
    "production": typer.colors.GREEN,
    "staging": typer.colors.YELLOW,
    "development": typer.colors.BLUE,
    "archived": typer.colors.WHITE,
}


def _version_dict(mv: ModelVersion) -> Dict[str, Any]:
    d = asdict(mv)
    d["stage"] = mv.stage.value  # asdict keeps the enum
    return d


def _print_versions_table(versions: List[ModelVersion]) -> None:
    typer.echo(f"\n  {'Version':<12} {'Stage':<14} {'Registered':<28} {'Key metrics'}")
    typer.echo(f"  {'-'*12} {'-'*14} {'-'*28} {'-'*30}")
    for mv in versions:
        stage_str = mv.stage.value
        color = _STAGE_COLORS.get(stage_str)
        metric_preview = ", ".join(
            f"{k}={v:.3f}" if isinstance(v, float) else f"{k}={v}"
            for k, v in list(mv.metrics.items())[:3]
        )
        line = f"  {mv.version:<12} {stage_str:<14} {mv.registered_at[:26]:<28} {metric_preview}"
        typer.secho(line, fg=color)
    typer.echo()
