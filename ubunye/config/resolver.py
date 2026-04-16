"""Jinja2 template resolver for Ubunye config dicts.

Resolves ``{{ env.VAR }}`` and ``{{ cli_var }}`` expressions in the string
values of a nested dict/list structure **after** YAML parsing. Non-string
values (int, bool, None) are passed through unchanged.

Missing environment variable detection
---------------------------------------
``DebugUndefined`` is used so that unresolvable expressions are left in
the output as their original ``{{ ... }}`` text rather than silently becoming
empty strings. After rendering, any remaining ``{{ env.<NAME> }}`` patterns
are detected and surfaced as clear ``ValueError`` messages.

Usage
-----
    from ubunye.config.resolver import resolve_config

    raw = yaml.safe_load(open("config.yaml"))
    resolved = resolve_config(raw, cli_vars={"dt": "2025-01-01"})
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional

from jinja2 import DebugUndefined, Environment

# Matches {{ env.VAR_NAME }} in a template, capturing the variable name.
# The negative lookahead (?![^}]*default) detects usage WITHOUT a default filter.
_ENV_REF_RE = re.compile(r"\{\{[^}]*env\.(\w+)([^}]*)\}\}")

# Matches any residual Jinja2 expression in a post-render string — used to
# surface undefined CLI variables that ``DebugUndefined`` silently leaves in
# place. Captures the first identifier inside the braces so the error can
# name the offending variable.
_JINJA_RESIDUE_RE = re.compile(r"\{\{\s*([\w.]+)")


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
        If a ``{{ env.VAR }}`` expression references an environment variable
        that is not set and no ``default()`` filter is applied.
    """
    env_source = _env if _env is not None else os.environ
    variables = dict(cli_vars or {})

    jinja_env = Environment(undefined=DebugUndefined)
    jinja_env.globals["env"] = env_source

    return _resolve_node(raw, jinja_env, variables, env_source)


def _resolve_node(
    node: Any,
    jinja_env: Environment,
    variables: Dict[str, Any],
    env_source: Dict[str, str],
) -> Any:
    """Recursively walk the config structure and resolve string values."""
    if isinstance(node, dict):
        return {k: _resolve_node(v, jinja_env, variables, env_source) for k, v in node.items()}

    if isinstance(node, list):
        return [_resolve_node(item, jinja_env, variables, env_source) for item in node]

    if isinstance(node, str):
        return _render_string(node, jinja_env, variables, env_source)

    # int, float, bool, None — pass through unchanged
    return node


def _render_string(
    value: str,
    jinja_env: Environment,
    variables: Dict[str, Any],
    env_source: Dict[str, str],
) -> str:
    """Render a single string value as a Jinja2 template.

    Pre-scans the template for ``{{ env.VAR }}`` expressions **before**
    rendering, so that missing variables are caught with a clear error message
    rather than silently becoming empty strings. Variables that use a
    ``| default(...)`` filter are exempt — Jinja2 handles those correctly.
    """
    if "{{" not in value:
        return value

    # Pre-render check: find {{ env.VAR }} uses without a default filter.
    for match in _ENV_REF_RE.finditer(value):
        var_name = match.group(1)
        rest_of_expr = match.group(2)  # everything after the var name inside {{ }}
        has_default = "default" in rest_of_expr
        if not has_default and var_name not in env_source:
            raise ValueError(
                f"Environment variable '{var_name}' is not set. "
                f"Either set it in your environment or use "
                f"{{{{ env.{var_name} | default('fallback') }}}} in your config."
            )

    rendered = jinja_env.from_string(value).render(**variables)

    # Post-render check: ``DebugUndefined`` leaves unresolvable expressions
    # (e.g. ``{{ dt }}`` when ``dt`` wasn't supplied) in the output verbatim.
    # Silent pass-through would let a literal ``{{ dt }}`` flow into a Spark
    # path — fail loudly so it's caught at config-load time.
    residue = _JINJA_RESIDUE_RE.search(rendered)
    if residue:
        var_name = residue.group(1)
        raise ValueError(
            f"Template variable '{var_name}' is undefined in config value "
            f"{value!r}. Either pass it via the CLI (e.g. '-dt 2026-04-16'), "
            f"set it in the environment, or use "
            f"{{{{ {var_name} | default('fallback') }}}}."
        )

    return rendered
