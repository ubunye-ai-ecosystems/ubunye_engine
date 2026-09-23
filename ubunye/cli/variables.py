"""``--var key=value`` for every command that renders a config."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import typer

from ubunye.config.variables import build_variables, parse_var_flags

VAR_HELP = (
    "Extra template variable for the config, key=value (repeatable): "
    "--var region=gauteng makes {{ region }} available."
)


def var_option() -> Any:
    """A fresh ``--var`` option (each command needs its own)."""
    return typer.Option(None, "--var", help=VAR_HELP)


def cli_variables(
    *,
    dt: Optional[str],
    dtf: Optional[str],
    mode: Optional[str],
    var: Optional[List[str]],
) -> Dict[str, Any]:
    """The variables for a command, or a usage error (exit code 2) saying why."""
    try:
        return build_variables(dt=dt, dtf=dtf, mode=mode, extra=parse_var_flags(var))
    except ValueError as exc:
        raise typer.BadParameter(str(exc), param_hint="--var") from None
