"""Jinja2 template resolver for Ubunye config dicts.

Resolves ``{{ env.VAR }}`` and ``{{ cli_var }}`` expressions in the string
values of a nested dict/list structure **after** YAML parsing. Non-string
values (int, bool, None) are passed through unchanged.

``StrictUndefined`` is used so that any reference to an undefined variable
raises immediately rather than silently producing an empty string.  The
``| default()`` filter continues to work — Jinja evaluates it before the
undefined check fires.

Usage
-----
    from ubunye.config.resolver import resolve_config

    raw = yaml.safe_load(open("config.yaml"))
    resolved = resolve_config(raw, cli_vars={"dt": "2025-01-01"})
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, List, Optional

from jinja2 import Environment, StrictUndefined, UndefinedError


def resolve_config(
    raw: Any,
    cli_vars: Optional[Dict[str, Any]] = None,
    _env: Optional[Dict[str, str]] = None,
) -> Any:
    """Recursively resolve Jinja2 templates in a nested structure.

    Parameters
    ----------
    raw:
        A dict, list, or scalar value — typically the result of
        ``yaml.safe_load()``.
    cli_vars:
        Extra variables available in templates (e.g. ``{"dt": "2025-01-01"}``).
        These are passed as top-level Jinja globals alongside ``env``.
    _env:
        Override for ``os.environ`` (used in tests). Defaults to ``os.environ``.

    Returns
    -------
    The same structure with all resolvable Jinja expressions replaced by their
    values.

    Raises
    ------
    ValueError
        If a template variable or ``{{ env.VAR }}`` is undefined and no
        ``| default()`` filter is applied.
    """
    env_source = _env if _env is not None else os.environ
    variables = dict(cli_vars or {})

    jinja_env = Environment(undefined=StrictUndefined)
    jinja_env.globals["env"] = env_source

    available_vars = sorted(set(list(variables.keys()) + ["env"]))

    return _resolve_node(raw, jinja_env, variables, available_vars)


def _resolve_node(
    node: Any,
    jinja_env: Environment,
    variables: Dict[str, Any],
    available_vars: List[str],
) -> Any:
    """Recursively walk the config structure and resolve string values."""
    if isinstance(node, dict):
        return {k: _resolve_node(v, jinja_env, variables, available_vars) for k, v in node.items()}

    if isinstance(node, list):
        return [_resolve_node(item, jinja_env, variables, available_vars) for item in node]

    if isinstance(node, str):
        return _render_string(node, jinja_env, variables, available_vars)

    # int, float, bool, None — pass through unchanged
    return node


# Extracts the variable name from a Jinja2 UndefinedError message.
_UNDEF_VAR_RE = re.compile(r"'(\w[\w.]*)'")


def _render_string(
    value: str,
    jinja_env: Environment,
    variables: Dict[str, Any],
    available_vars: List[str],
) -> str:
    """Render a single string value as a Jinja2 template.

    ``StrictUndefined`` raises ``UndefinedError`` on any reference to an
    undefined variable. The ``| default()`` filter still works — Jinja
    evaluates it before the undefined check fires.
    """
    if "{{" not in value:
        return value

    try:
        return jinja_env.from_string(value).render(**variables)
    except UndefinedError as exc:
        msg = str(exc)
        var_match = _UNDEF_VAR_RE.search(msg)
        var_name = var_match.group(1) if var_match else "unknown"

        if "has no attribute" in msg:
            raise ValueError(
                f"Environment variable '{var_name}' is not set "
                f"(referenced in {value!r}). "
                f"Set it in your environment or use "
                f"{{{{ env.{var_name} | default('fallback') }}}}."
            ) from exc

        raise ValueError(
            f"Undefined variable '{var_name}' in config value {value!r}. "
            f"Available variables: {available_vars}. "
            f"Use '| default(...)' for optional values."
        ) from exc
