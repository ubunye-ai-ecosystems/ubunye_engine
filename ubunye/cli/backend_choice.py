"""Choosing a backend from the command line, the same way for every command.

``run``, ``test run`` and ``validate`` take ``--backend NAME``; ``ubunye backends``
lists what is installed and what each can do. All of it goes through the one
registry in :mod:`ubunye.core.backends`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

import typer

from ubunye.core import backends
from ubunye.core.errors import BackendNotFoundError


def resolve_or_exit(name: Optional[str], *, app_name: str, conf: Dict[str, Any]) -> Any:
    """The backend for a run, or a clear message and exit code 1."""
    try:
        return backends.resolve(name, app_name=app_name, conf=conf)
    except BackendNotFoundError as exc:
        typer.secho(f"[ERROR] {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


def capabilities_or_exit(name: str) -> Any:
    """A backend's declared capabilities, without starting it."""
    try:
        return backends.load_class(name).CAPABILITIES
    except BackendNotFoundError as exc:
        typer.secho(f"[ERROR] {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)


def describe_all() -> List[Dict[str, Any]]:
    """One entry per registered backend; a backend that fails to load says why."""
    rows = []
    for name, target in sorted(backends.entry_point_values().items()):
        row: Dict[str, Any] = {
            "name": name,
            "entry_point": target,
            "default": name == backends.DEFAULT_BACKEND,
        }
        try:
            cls = backends.load_class(name)
        except BackendNotFoundError as exc:
            row["loaded"] = False
            row["error"] = str(exc).splitlines()[0]
            rows.append(row)
            continue
        missing = backends.missing_packages(cls)
        row["loaded"] = not missing
        row["capabilities"] = cls.CAPABILITIES.describe()
        if missing:
            extra = backends._EXTRAS.get(name)
            fix = (
                f"pip install 'ubunye-engine[{extra}]'"
                if extra
                else "pip install " + " ".join(missing)
            )
            row["error"] = f"needs {', '.join(missing)} ({fix})"
        rows.append(row)
    return rows


def backends_command(
    as_json: bool = typer.Option(False, "--json", help="Print JSON only, for scripts and agents."),
) -> None:
    """List the installed execution backends and what each can do."""
    rows = describe_all()
    if as_json:
        typer.echo(json.dumps(rows, indent=2))
        return
    for row in rows:
        mark = " (default)" if row["default"] else ""
        typer.secho(f"{row['name']}{mark}", bold=True)
        if not row["loaded"]:
            typer.secho(f"  not usable here: {row['error']}", fg=typer.colors.YELLOW)
            if "capabilities" not in row:
                continue
        caps = row["capabilities"]
        formats = caps["file_formats"]
        modes = caps["write_modes"]
        typer.echo(f"  features:     {', '.join(caps['features']) or '-'}")
        typer.echo(f"  file formats: {formats if isinstance(formats, str) else ', '.join(formats)}")
        typer.echo(f"  write modes:  {modes if isinstance(modes, str) else ', '.join(modes)}")
        typer.echo(
            f"  distributed:  {'yes' if caps['distributed'] else 'no'}"
            f"   needs Java: {'yes' if caps['needs_jvm'] else 'no'}"
        )
