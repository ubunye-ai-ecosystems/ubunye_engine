"""Adapters: the thin shims that make non-Spark data satisfy the engine's ports.

Ports and adapters, the second half. ``ubunye.core.ports`` defines the shape;
these fill it. A Spark DataFrame needs no adapter — it satisfies ``DataFramePort``
natively, which is exactly why the port could ship without breaking anything.
"""

from typing import Any

from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter


def as_port(frame: Any) -> Any:
    """Return *frame* behind an adapter when it needs one, unchanged when it does not.

    A user transform is free to return the native thing it was working with, and on
    the pandas backend it usually does. A raw pandas frame answers ``count()`` with
    per-column non-null counts rather than a row count, so anything in the engine
    that asks a frame about itself, lineage above all, gets an answer that looks
    numeric and means something else.

    The module name is checked rather than the type, so this never imports pandas
    in a process that does not already have it.
    """
    if frame is None or isinstance(frame, PandasDataFrameAdapter):
        return frame
    module = getattr(type(frame), "__module__", "") or ""
    if module.split(".")[0] == "pandas" and hasattr(frame, "to_dict"):
        return PandasDataFrameAdapter(frame)
    return frame


__all__ = ["PandasDataFrameAdapter", "as_port"]
