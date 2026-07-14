"""Interfaces for Ubunye Engine components.

These abstract base classes define the contracts for backends, readers, writers,
transforms, and user-defined tasks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, FrozenSet, List


class Backend(ABC):
    """Abstract execution backend (e.g., Spark or Pandas)."""

    @abstractmethod
    def start(self) -> None:
        """Start a backend session (e.g., create SparkSession)."""

    @abstractmethod
    def stop(self) -> None:
        """Stop the backend session and release resources."""

    @property
    @abstractmethod
    def is_spark(self) -> bool:
        """Whether this backend is Spark-based."""
        ...


class Connector(ABC):
    """What every connector can say about itself.

    The engine used to know all of this *for* them. ``config/schema.py`` carried an
    ``if/elif`` chain naming ``hive``, ``jdbc``, ``s3``, ``delta``, ``unity`` and
    ``rest_api`` and spelling out what each one required; ``core/write_modes.py``
    carried ``_MERGE_FORMATS = {"delta"}`` and decided, on the connector's behalf, which
    formats were allowed to merge.

    So a third-party connector could not declare its own required fields, and could not
    declare that it supports MERGE. "Adding a connector = write the class, register the
    entry point" was not true: you also had to edit the engine, in three places.

    The engine now **asks**. A plugin declares its own requirements and its own
    capabilities, and the core validates by asking whichever plugin the config named. It
    holds no list of implementations, so adding one requires no edit to it.
    """

    @classmethod
    def validate_config(cls, cfg: Dict[str, Any]) -> List[str]:
        """Return a list of problems with ``cfg`` — empty means it is usable.

        The connector is the only thing that knows what it needs. Say so here, and the
        engine will surface it at ``ubunye validate`` time rather than halfway through a
        job on a cluster.
        """
        return []


class Reader(Connector):
    """Base interface for input readers."""

    @abstractmethod
    def read(self, cfg: dict, backend: Backend) -> Any:
        """Read input into a DataFrame-like object."""


class Writer(Connector):
    """Base interface for output writers."""

    #: The write modes this connector can honour. The engine does not assume.
    SUPPORTED_MODES: FrozenSet[str] = frozenset({"append", "overwrite"})

    #: Whether this connector can do a real upsert. Delta can. Iceberg and Hudi can, and
    #: must be able to SAY so without anyone editing the engine — which is precisely
    #: what ``_MERGE_FORMATS = {"delta"}`` in the core made impossible.
    SUPPORTS_MERGE: bool = False

    #: The underlying table formats this connector can MERGE into. The core used to hold
    #: this as ``_MERGE_FORMATS = {"delta"}`` and hand down a verdict; an Iceberg or Hudi
    #: connector could not overrule it without a patch to the engine. It says so itself
    #: now.
    MERGE_FILE_FORMATS: FrozenSet[str] = frozenset({"delta"})

    @abstractmethod
    def write(self, df: Any, cfg: dict, backend: Backend) -> Any:
        """Write a DataFrame-like object to a destination.

        Returns ``None`` for an ordinary write. A connector that starts something
        asynchronous may return a handle for the engine to wait on.
        """


class Transform(ABC):
    """Base interface for transforms."""

    @abstractmethod
    def apply(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        """Transform inputs and return new outputs mapping."""


class Task(ABC):
    """User-defined task contract (lives in transformations.py)."""

    def __init__(self, config: dict):
        self.config = config

    def setup(self) -> None:
        """Optional hook executed before transform."""
        ...

    @abstractmethod
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        """Transform input sources into outputs mapping."""
