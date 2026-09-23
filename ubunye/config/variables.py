"""The template variables a config is rendered with, built the same way everywhere.

Every command that renders a ``config.yaml`` (``run``, ``validate``, ``plan``,
``config``, ``test run``) and every Python entry point (``run_task``,
``run_pipeline``, ``notebook``) builds its variables here, so a config renders
the same whichever way it is run.

The standard variables are ``dt``, ``dtf`` and ``mode``; anything else comes
from ``--var key=value`` on the command line or ``variables=`` in Python.
``{{ env.NAME }}`` is the environment, so ``env`` cannot be a variable.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Optional

#: Names a variable cannot take, and why.
RESERVED = {
    "env": "it is the environment ({{ env.NAME }})",
}
#: Standard variables that have their own flag and must be set there.
OWN_FLAG = {
    "mode": "-m/--mode",
}


def _check_name(key: str) -> None:
    if not key:
        raise ValueError("a variable has an empty name")
    if not key.isidentifier():
        raise ValueError(
            f"'{key}' is not a valid template name: use letters, digits and "
            "underscores, not starting with a digit (region, run_date)"
        )
    if key in RESERVED:
        raise ValueError(f"'{key}' is reserved: {RESERVED[key]}")


def parse_var_flags(items: Optional[Iterable[str]]) -> Dict[str, str]:
    """``["region=gp", "n=3"]`` as ``{"region": "gp", "n": "3"}``, or ``ValueError``."""
    parsed: Dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(
                f"--var expects key=value, got '{item}'. Example: --var region=gauteng"
            )
        key, value = item.split("=", 1)
        key = key.strip()
        if not key:
            raise ValueError(f"--var has an empty name in '{item}'")
        _check_name(key)
        if key in OWN_FLAG:
            raise ValueError(f"set {key} with {OWN_FLAG[key]}, not --var")
        parsed[key] = value
    return parsed


def build_variables(
    *,
    dt: Optional[str] = None,
    dtf: Optional[str] = None,
    mode: Optional[str] = None,
    extra: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    """The variables a config is rendered with: the standard three plus ``extra``.

    ``extra`` may set ``dt`` or ``dtf`` when the flag was not given (the docs show
    ``--var dt=2024-06-01``). The same name given two different values is refused
    rather than one silently winning.
    """
    variables: Dict[str, Any] = {"dt": dt, "dtf": dtf, "mode": mode}
    for key, value in (extra or {}).items():
        _check_name(str(key))
        current = variables.get(key)
        if current is not None and current != value:
            raise ValueError(
                f"'{key}' is given twice with different values ({current!r} and {value!r}); pass one"
            )
        variables[key] = value
    return variables
