"""How a command ends: ``--json`` output, and one clean ``[ERROR]`` line.

With ``--json`` a command prints exactly one JSON document to stdout and
nothing else, errors included (as ``{"ok": false, "error": ...}``), and its
exit code still says whether it succeeded. Everything a person would read goes
to the text form instead.
"""

from __future__ import annotations

import json
from typing import Any, NoReturn

import typer
import yaml
from pydantic import ValidationError
from typer.core import TyperGroup

from ubunye.core.errors import UbunyeError

JSON_HELP = "Print one JSON document to stdout, for scripts and agents. Errors are JSON too."


def json_option() -> Any:
    """A fresh ``--json`` option (each command needs its own)."""
    return typer.Option(False, "--json", help=JSON_HELP)


def emit(document: Any) -> None:
    """Print ``document`` as the command's only output."""
    typer.echo(json.dumps(document, indent=2, default=str, ensure_ascii=False))


def fail(message: str, *, as_json: bool, **details: Any) -> NoReturn:
    """Stop with exit code 1: as a JSON error document, or as red text on stderr."""
    if as_json:
        emit({"ok": False, "error": message, **details})
    else:
        typer.secho(f"[ERROR] {message}", fg=typer.colors.RED, err=True)
    raise typer.Exit(code=1)


#: Errors the user can fix: they say what, where and what to try, so a traceback
#: would only bury the message. Anything else is a bug and keeps its traceback.
EXPECTED_ERRORS = (UbunyeError, FileNotFoundError, yaml.YAMLError, ValidationError)


class CleanErrors(TyperGroup):
    """A command group whose commands end an expected error in one ``[ERROR]``
    message and exit code 1, the way ``ubunye run`` does (F-096)."""

    def invoke(self, ctx: Any) -> Any:
        try:
            return super().invoke(ctx)
        except EXPECTED_ERRORS as exc:
            fail(str(exc), as_json=False)
