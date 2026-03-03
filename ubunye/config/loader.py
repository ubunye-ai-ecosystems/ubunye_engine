"""YAML config loader with Jinja2 resolution and Pydantic validation.

Usage
-----
    from ubunye.config import load_config

    # From a task directory (looks for config.yaml inside)
    cfg = load_config("./pipelines/fraud_detection/ingestion/claim_etl")

    # From a direct file path (backward-compatible)
    cfg = load_config("./pipelines/fraud_detection/ingestion/claim_etl/config.yaml")

    # With CLI variables and a deploy profile
    cfg = load_config(task_dir, variables={"dt": "2025-01-01"}, profile="dev")
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

import yaml
from pydantic import ValidationError

from .resolver import resolve_config
from .schema import UbunyeConfig


def load_config(
    path: str, variables: Optional[Dict[str, Any]] = None, profile: Optional[str] = None,
) -> UbunyeConfig:
    """Load, resolve, and validate a Ubunye task config.

    Parameters
    ----------
    path:
        Either a path to a ``config.yaml`` file **or** a task directory that
        contains one. If a directory is given, ``config.yaml`` must be present
        inside it.
    variables:
        CLI-provided template variables (e.g. ``{"dt": "2025-01-01"}``).
        Passed as Jinja2 globals alongside ``{{ env.* }}``.
    profile:
        Optional profile name (``dev``, ``prod``, …). When provided, the
        returned config's ``ENGINE.spark_conf`` is pre-merged with the
        profile's overrides via :meth:`UbunyeConfig.merged_spark_conf`.
        The config object itself is returned unmodified; callers can call
        ``cfg.merged_spark_conf(profile)`` at any time.

    Returns
    -------
    UbunyeConfig
        Fully resolved and validated config object.

    Raises
    ------
    FileNotFoundError
        If the config file does not exist.
    ValueError
        If Jinja resolution fails (e.g. missing env var) or Pydantic
        validation fails — with a human-readable error message.
    """
    config_path = _resolve_config_path(path)
    raw_yaml = config_path.read_text(encoding="utf-8")
    raw: Dict[str, Any] = yaml.safe_load(raw_yaml) or {}

    try:
        resolved = resolve_config(raw, cli_vars=variables or {})
    except ValueError as exc:
        raise ValueError(f"Template resolution failed for {config_path}:\n  {exc}") from exc

    try:
        cfg = UbunyeConfig.model_validate(resolved)
    except ValidationError as exc:
        raise ValueError(_format_validation_error(exc, str(config_path))) from exc

    if profile and profile not in cfg.ENGINE.profiles:
        available = sorted(cfg.ENGINE.profiles.keys())
        raise ValueError(
            f"Profile '{profile}' is not defined in {config_path}. "
            f"Available profiles: {available or ['(none)']}"
        )

    return cfg


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_config_path(path: str) -> Path:
    """Return Path to config.yaml, resolving directory or file paths."""
    p = Path(path)
    if p.is_dir():
        candidate = p / "config.yaml"
        if not candidate.exists():
            raise FileNotFoundError(f"No config.yaml found in task directory: {p}")
        return candidate
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")
    return p


def _format_validation_error(exc: ValidationError, config_path: str) -> str:
    """Build a human-readable validation error message.

    Example output
    --------------
    Config validation failed for pipelines/fraud/ingestion/claim_etl/config.yaml:

      CONFIG.inputs.raw_claims:
        - format 'hive' requires either ('db_name' + 'tbl_name') or 'sql'

      CONFIG.outputs.bronze:
        - 'mode' must be one of: overwrite, append, merge
    """
    lines = [f"Config validation failed for {config_path}:\n"]
    errors_by_loc: Dict[str, list] = {}

    for error in exc.errors():
        loc = ".".join(str(part) for part in error["loc"])
        msg = error["msg"]
        # Pydantic wraps validator messages with "Value error, "
        msg = msg.removeprefix("Value error, ")
        errors_by_loc.setdefault(loc, []).append(msg)

    for loc, messages in errors_by_loc.items():
        lines.append(f"  {loc}:")
        for msg in messages:
            lines.append(f"    - {msg}")
        lines.append("")

    return "\n".join(lines).rstrip()
