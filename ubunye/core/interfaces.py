"""Interfaces for Ubunye Engine components.

These abstract base classes define the contracts for backends, readers, writers,
transforms, and user-defined tasks.
"""
from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Dict, Any


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


class Reader(ABC):
    """Base interface for input readers."""

    @abstractmethod
    def read(self, cfg: dict, backend: Backend) -> Any:
        """Read input into a DataFrame-like object."""


class Writer(ABC):
    """Base interface for output writers."""

    @abstractmethod
    def write(self, df: Any, cfg: dict, backend: Backend) -> None:
        """Write a DataFrame-like object to a destination."""


class Transform(ABC):
    """Base interface for transforms."""

    @abstractmethod
    def apply(self, inputs: Dict[str, Any], cfg: dict, backend: Backend) -> Dict[str, Any]:
        """Transform inputs and return new outputs mapping."""


class Task(ABC):
    """User-defined task contract (lives in feature_class.py)."""

    def __init__(self, config: dict):
        self.config = config

    def setup(self) -> None:
        """Optional hook executed before transform."""
        ...

    @abstractmethod
    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        """Transform input sources into outputs mapping."""
