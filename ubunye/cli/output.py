"""``--json``: one JSON document on stdout, for a script or an agent.

With ``--json`` a command prints exactly one JSON document to stdout and
nothing else, errors included (as ``{"ok": false, "error": ...}``), and its
exit code still says whether it succeeded. Everything a person would read goes
to the text form instead.
"""

from __future__ import annotations

import json
from typing import Any, NoReturn

import typer

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
