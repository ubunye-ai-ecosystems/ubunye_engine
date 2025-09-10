"""Spark backend implementation.

This module lazily imports pyspark so the package can be installed without Spark.
"""
from __future__ import annotations
from typing import Optional, Dict
from ubunye.core.interfaces import Backend


class SparkBackend(Backend):
    """Creates and manages a SparkSession for a Ubunye run."""

    def __init__(self, app_name: str = "ubunye", conf: Optional[Dict[str, str]] = None) -> None:
        self._spark = None  # type: ignore
        self.app_name = app_name
        self.conf = conf or {}

    def start(self) -> None:
        from pyspark.sql import SparkSession  # lazy import
        builder = SparkSession.builder.appName(self.app_name)
        for k, v in self.conf.items():
            builder = builder.config(k, v)
        self._spark = builder.getOrCreate()

    def stop(self) -> None:
        if self._spark is not None:
            self._spark.stop()
            self._spark = None

    @property
    def spark(self):
        if self._spark is None:
            raise RuntimeError("Spark session not started. Call start() first.")
        return self._spark

    @property
    def is_spark(self) -> bool:
        return True
