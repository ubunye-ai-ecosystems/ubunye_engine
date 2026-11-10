"""``--pipeline`` and its old name ``--package``.

The middle level of use case, pipeline, task was once called "package" in the
CLI. The flag is now ``-p/--pipeline``. ``--package`` still works until 1.0: it
is rewritten to ``--pipeline`` before parsing, prints one warning line to
stderr, and is left out of ``--help``. Giving both is an error.

A command opts in with ``cls=PipelineCommand`` (or ``PipelineGroup`` for a
group callback that takes the flag).
"""

from __future__ import annotations

from typing import Any, List

import typer
from typer.core import TyperCommand, TyperGroup

OLD = "--package"
NEW = "--pipeline"
WARNING = "Warning: --package is renamed --pipeline; it will be removed in 1.0."


def pipeline_option(default: Any = ..., help: str = "Pipeline name.") -> Any:
    """The ``-p/--pipeline`` option. Pass ``None`` as the default for an optional one."""
    return typer.Option(default, "-p", NEW, help=help)


def _is_new(arg: str) -> bool:
    if arg == NEW or arg.startswith(NEW + "="):
        return True
    return arg.startswith("-p") and not arg.startswith("--")


def rewrite_package_flag(args: List[str]) -> "tuple[List[str], bool, bool]":
    """``args`` with ``--package`` turned into ``--pipeline``.

    Returns the new list, whether the old name was used and whether the new one
    (``-p`` or ``--pipeline``) was used. Nothing after ``--`` is touched.
    """
    out: List[str] = []
    used_old = used_new = False
    rest = False
    for arg in args:
        if rest:
            out.append(arg)
            continue
        if arg == "--":
            rest = True
        elif arg == OLD or arg.startswith(OLD + "="):
            arg = NEW + arg[len(OLD) :]
            used_old = True
        elif _is_new(arg):
            used_new = True
        out.append(arg)
    return out, used_old, used_new


def _parse(args: List[str], ctx: Any) -> List[str]:
    new_args, used_old, used_new = rewrite_package_flag(args)
    if used_old and used_new:
        raise typer.BadParameter(
            "give the pipeline once: use -p/--pipeline and drop --package (its old name).",
            ctx=ctx,
            param_hint="'--pipeline'",
        )
    if used_old:
        typer.echo(WARNING, err=True)
    return new_args


class PipelineCommand(TyperCommand):
    """A command whose ``--pipeline`` also answers to the old ``--package``."""

    def parse_args(self, ctx: Any, args: List[str]) -> List[str]:
        return super().parse_args(ctx, _parse(args, ctx))


class PipelineGroup(TyperGroup):
    """A group (callback with ``--pipeline``) that also takes the old ``--package``."""

    def parse_args(self, ctx: Any, args: List[str]) -> List[str]:
        return super().parse_args(ctx, _parse(args, ctx))
