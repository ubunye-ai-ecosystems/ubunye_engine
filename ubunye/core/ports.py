"""The data plane's port — any tabular thing can flow through the engine.

This file was specified in the project's founding design notes (blog.md, "The Design
Principle Worth Stealing") and never shipped. The philosophy is hexagonal architecture —
ports and adapters: the model layer already has its port (``UbunyeModel``: the engine
never imports sklearn or torch), but the data layer never got one, so every reader
returns and every transform receives a ``pyspark.sql.DataFrame``. The blog's own
diagnosis, verbatim:

    Reads      -> pyspark.sql.DataFrame   <- coupled to Spark
    Writes     -> pyspark.sql.DataFrame   <- coupled to Spark
    UbunyeModel.train(df: Any)            <- decoupled (the one correct layer)

``DataFramePort`` is the missing port. It is a ``runtime_checkable`` **Protocol** —
structural, matching the design's central rule: *any class that implements the required
methods satisfies it; no inheritance needed.*

Spark DataFrames satisfy it **already**, by duck-typing — they have ``schema``,
``count()`` and ``collect()`` without modification. That is what makes this additive
rather than a rewrite: nothing that flows through the engine today changes, the engine
just stops *naming* Spark as the only thing that may flow.

pandas and Polars need thin adapters (see ``ubunye.adapters``), because their native
spellings differ — ``len(df)`` instead of ``count()``, ``to_dict`` instead of
``collect()``.

A note on honesty: ``isinstance(x, DataFramePort)`` checks that the *names exist*, not
that they mean the right thing — that is as far as Python's structural checks go. The
contract's meaning is enforced where it always is in this engine: by tests that run
real data through, not by the type system.
"""

from __future__ import annotations

from typing import Any, Dict, List, Protocol, runtime_checkable


@runtime_checkable
class DataFramePort(Protocol):
    """Abstract port for any tabular data structure.

    Anything with these three members can flow through the engine: a Spark DataFrame
    (natively), a pandas or Polars frame (via ``ubunye.adapters``), or anything a
    third party cares to build. The engine core asks for nothing else — which is the
    point: what it does not ask for, it cannot be coupled to.
    """

    @property
    def schema(self) -> Any:
        """The tabular schema. Spark returns a ``StructType``; adapters return a dict."""
        ...

    def count(self) -> int:
        """Number of rows."""
        ...

    def collect(self) -> List[Dict[str, Any]]:
        """All rows, materialised. Small data only — this is the escape hatch, not the
        transport. A million-row frame should stay inside its engine."""
        ...
