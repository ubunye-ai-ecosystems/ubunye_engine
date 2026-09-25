"""The CLI never crashes on a console that cannot print a character.

A legacy Windows console encodes output as cp1252, which has no arrow, so
`ubunye --help` crashed with UnicodeEncodeError on a fresh Windows install
(the CI Package job on windows-latest caught it). The CLI's own text is kept
printable there, and its output streams replace anything that is not.
"""

from __future__ import annotations

import io
import sys

import pytest
import typer

from ubunye.cli import main as cli_main


def _all_help_texts():
    """Every help string of every command and option, read from the objects.

    Not ``get_help()``: with rich installed, Typer prints help straight to the
    console and returns an empty string, so that would check nothing.
    """
    root = typer.main.get_command(cli_main.app)

    def walk(cmd, path):
        for text in (cmd.help, cmd.short_help, getattr(cmd, "epilog", None)):
            if text:
                yield path, text
        for param in cmd.params:
            if getattr(param, "help", None):
                yield f"{path} {param.name}", param.help
        if hasattr(cmd, "list_commands"):  # a group (Typer ships its own copy of click)
            ctx = cmd.context_class(cmd, info_name=path)
            for name in cmd.list_commands(ctx):
                yield from walk(cmd.get_command(ctx, name), f"{path} {name}")

    yield from walk(root, "ubunye")


def test_every_help_text_prints_on_a_cp1252_console():
    texts = list(_all_help_texts())
    assert len(texts) > 50  # the walk really reached the commands and options
    for where, text in texts:
        try:
            text.encode("cp1252")
        except UnicodeEncodeError as exc:
            raise AssertionError(f"{where}: {ascii(text[:80])}") from exc


def test_the_entry_point_makes_output_unbreakable(monkeypatch):
    raw = io.BytesIO()
    console = io.TextIOWrapper(raw, encoding="cp1252", errors="strict")
    monkeypatch.setattr(sys, "stdout", console)
    monkeypatch.setattr(sys, "stderr", console)
    cli_main._safe_console_streams()
    print("a path with an arrow → and 中")  # would raise before
    console.flush()
    assert b"arrow ? and ?" in raw.getvalue()


def test_the_console_script_is_the_safe_entry_point():
    pytest.importorskip("tomllib")
    from pathlib import Path

    import tomllib

    project = tomllib.loads(
        (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
    )
    assert project["project"]["scripts"]["ubunye"] == "ubunye.cli.main:main"
