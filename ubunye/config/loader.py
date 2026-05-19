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

import difflib
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

import yaml
from pydantic import BaseModel, ValidationError

from ubunye.core.errors import ConfigFieldError, ConfigProfileError, ConfigTemplateError

from .resolver import resolve_config
from .schema import UbunyeConfig


def load_config(
    path: str,
    variables: Optional[Dict[str, Any]] = None,
    profile: Optional[str] = None,
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
    except ConfigTemplateError:
        raise
    except ValueError as exc:
        raise ConfigTemplateError(
            f"Template resolution failed for {config_path}:\n  {exc}"
        ) from exc

    try:
        cfg = UbunyeConfig.model_validate(resolved)
    except ValidationError as exc:
        unknown = _extract_unknown_field_errors(exc)
        if unknown:
            raise ConfigFieldError(
                _format_unknown_fields(unknown, str(config_path))
            ) from exc
        raise ValueError(_format_validation_error(exc, str(config_path))) from exc

    if profile and profile not in cfg.ENGINE.profiles:
        available = sorted(cfg.ENGINE.profiles.keys())
        raise ConfigProfileError(
            f"Profile '{profile}' is not defined in {config_path}.",
            context={"Profile": profile, "Available": available or ["(none)"]},
            hint=f"Check your --profile flag. Valid profiles: {', '.join(available) or '(none defined)'}",
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


def _extract_unknown_field_errors(
    exc: ValidationError,
) -> List[Tuple[str, str, List[str]]]:
    """Pull unknown-field errors from a Pydantic ``ValidationError``.

    Returns a list of ``(section_path, unknown_field, valid_fields)`` tuples —
    one per ``extra_forbidden`` error in *exc*.
    """
    results: List[Tuple[str, str, List[str]]] = []
    for error in exc.errors():
        if error["type"] != "extra_forbidden":
            continue
        loc = error["loc"]
        unknown_field = str(loc[-1])
        parent_loc = loc[:-1]
        parent_model = _walk_model(UbunyeConfig, parent_loc)
        valid = sorted(parent_model.model_fields.keys()) if parent_model else []
        section = ".".join(str(p) for p in parent_loc) if parent_loc else "(top level)"
        results.append((section, unknown_field, valid))
    return results


def _walk_model(
    root: Type[BaseModel], loc_parts: Tuple[Any, ...]
) -> Optional[Type[BaseModel]]:
    """Resolve the Pydantic model class at a given location path."""
    model: Type[BaseModel] = root
    for part in loc_parts:
        part_str = str(part)
        if part_str not in model.model_fields:
            return model
        annotation = model.model_fields[part_str].annotation
        # Unwrap Optional[X] → X
        origin = getattr(annotation, "__origin__", None)
        if origin is type(None):
            return model
        args = getattr(annotation, "__args__", None)
        if args:
            annotation = next((a for a in args if a is not type(None)), annotation)
        if isinstance(annotation, type) and issubclass(annotation, BaseModel):
            model = annotation
        else:
            return model
    return model


def _format_unknown_fields(
    unknowns: List[Tuple[str, str, List[str]]], config_path: str
) -> str:
    """Format unknown-field errors with typo suggestions."""
    lines = [f"Unknown fields in {config_path}:\n"]
    for section, field, valid_fields in unknowns:
        lines.append(f"  {section}:")
        lines.append(f"    Unknown field '{field}'")
        matches = difflib.get_close_matches(field, valid_fields, n=1, cutoff=0.6)
        if matches:
            lines.append(f"    Did you mean '{matches[0]}'?")
        else:
            lines.append(f"    Valid fields are: {', '.join(valid_fields)}")
        lines.append("")
    return "\n".join(lines).rstrip()
