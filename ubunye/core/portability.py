"""Which dataframe API a transform is written against (ADR 005).

Portability is detected, not declared: there is no config field for it. A
transform written with the Spark API (``F.col``, ``groupBy``) cannot run on the
pandas backend, and one written with pandas cannot run on Spark; one written
with Narwhals runs on both, with the same result (checked against Spark).

This module reads ``transformations.py`` without running it and says which of
those it is, from its imports, so ``ubunye plan --backend pandas`` can warn
before the run fails on the transform's first line. It never imports an engine.

What counts: an import anywhere in the file (a function body too), except
under ``if TYPE_CHECKING:`` and inside a ``try`` that catches ImportError (an
optional import). Only the file itself is read, not modules it imports.
"""

from __future__ import annotations

import ast
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Set, Tuple, Union

#: The dataframe libraries looked for, in the order they decide the answer: one
#: Spark call ties a transform to Spark even if the rest is Narwhals.
FRAME_LIBRARIES = ("pyspark", "narwhals", "pandas", "polars")

_HELP = "write it with narwhals to run on both (docs: backends.md, one transform for every engine)"


@dataclass(frozen=True)
class FrameApi:
    """What a transform's imports say about the frames it expects."""

    #: ``pyspark``, ``narwhals``, ``pandas`` or ``polars``; None when nothing says.
    api: Optional[str]
    #: Every dataframe library it imports, sorted.
    imports: Tuple[str, ...]
    #: Why the file could not be read, if it could not.
    error: str = ""


def frame_api(path: Union[str, Path]) -> FrameApi:
    """Read ``path`` (a transformations.py) and say what it is written for."""
    try:
        tree = ast.parse(Path(path).read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError, ValueError) as exc:
        return FrameApi(api=None, imports=(), error=str(exc).splitlines()[0])
    found: Set[str] = set()
    _collect(tree, found)
    imports = tuple(sorted(found & set(FRAME_LIBRARIES)))
    api = next((lib for lib in FRAME_LIBRARIES if lib in found), None)
    return FrameApi(api=api, imports=imports)


def mismatch(found: FrameApi, *, backend: str, spark_backend: bool) -> str:
    """A warning when the transform expects frames the backend does not give, else ""."""
    if found.api == "pyspark" and not spark_backend:
        return (
            f"transformations.py is written for pyspark (it imports it), but the "
            f"{backend} backend gives it {backend} frames; run it on a Spark backend, "
            f"or {_HELP}"
        )
    if found.api in ("pandas", "polars") and spark_backend:
        return (
            f"transformations.py is written for {found.api} (it imports it), but the "
            f"{backend} backend gives it Spark DataFrames; run it on the pandas backend, "
            f"or {_HELP}"
        )
    return ""


def _collect(node: ast.AST, found: Set[str]) -> None:
    if isinstance(node, ast.Import):
        found.update(alias.name.split(".")[0] for alias in node.names)
    elif isinstance(node, ast.ImportFrom):
        if node.module and not node.level:
            found.add(node.module.split(".")[0])
    elif isinstance(node, ast.If) and _is_type_checking(node.test):
        for stmt in node.orelse:  # only the else branch runs
            _collect(stmt, found)
    elif isinstance(node, ast.Try) and _catches_import_error(node):
        for part in (*node.handlers, *node.orelse, *node.finalbody):  # not the optional body
            _collect(part, found)
    else:
        for child in ast.iter_child_nodes(node):
            _collect(child, found)


def _is_type_checking(test: ast.expr) -> bool:
    if isinstance(test, ast.Name):
        return test.id == "TYPE_CHECKING"
    return isinstance(test, ast.Attribute) and test.attr == "TYPE_CHECKING"


def _catches_import_error(node: ast.Try) -> bool:
    names = {"ImportError", "ModuleNotFoundError", "Exception", "BaseException"}
    for handler in node.handlers:
        if handler.type is None:
            return True
        kinds = handler.type.elts if isinstance(handler.type, ast.Tuple) else [handler.type]
        if any(isinstance(k, ast.Name) and k.id in names for k in kinds):
            return True
    return False
