"""Shared pytest fixtures used across all test modules."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


# ---------------------------------------------------------------------------
# MockBackend — lightweight Backend stub that requires no Spark
# ---------------------------------------------------------------------------

class MockBackend:
    """Backend stub wrapping a MagicMock spark object.

    Use in unit tests that exercise plugin logic without starting a real
    Spark session. Pass a custom ``spark_mock`` to control return values.
    """
    is_spark = True

    def __init__(self, spark_mock: Any = None) -> None:
        self.spark = spark_mock if spark_mock is not None else MagicMock()

    def start(self) -> None:
        pass

    def stop(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

_VALID_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        "inputs": {
            "source": {"format": "hive", "db_name": "raw_db", "tbl_name": "claims"},
        },
        "transform": {"type": "noop"},
        "outputs": {
            "sink": {"format": "s3", "path": "s3a://bucket/out/", "mode": "overwrite"},
        },
    },
}

_INVALID_CONFIG = {
    "MODEL": "etl",
    "VERSION": "0.1.0",
    "CONFIG": {
        # Missing format on source → Pydantic ValidationError
        "inputs": {"source": {"db_name": "raw_db", "tbl_name": "claims"}},
        "outputs": {"sink": {"format": "s3", "path": "s3a://bucket/out/"}},
    },
}

_TRANSFORMATIONS_PY = """\
from ubunye.core.interfaces import Task

class NoOpTask(Task):
    def transform(self, sources):
        return sources
"""


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_backend() -> MockBackend:
    """MagicMock-based backend — no Spark required."""
    return MockBackend()


@pytest.fixture
def valid_task_dir(tmp_path: Path) -> Path:
    """Temp directory tree with one valid task.

    Layout::

        <tmp>/proj/fraud_detection/ingestion/claim_etl/
            config.yaml
            transformations.py

    Returns the project root (``<tmp>/proj``) so callers pass it as
    ``usecase_dir`` to CLI commands.
    """
    task = tmp_path / "proj" / "fraud_detection" / "ingestion" / "claim_etl"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(yaml.dump(_VALID_CONFIG), encoding="utf-8")
    (task / "transformations.py").write_text(_TRANSFORMATIONS_PY, encoding="utf-8")
    return tmp_path / "proj"


@pytest.fixture
def invalid_task_dir(tmp_path: Path) -> Path:
    """Temp directory tree with one invalid task config (missing 'format')."""
    task = tmp_path / "proj" / "fraud_detection" / "ingestion" / "bad_etl"
    task.mkdir(parents=True)
    (task / "config.yaml").write_text(yaml.dump(_INVALID_CONFIG), encoding="utf-8")
    (task / "transformations.py").write_text(_TRANSFORMATIONS_PY, encoding="utf-8")
    return tmp_path / "proj"


@pytest.fixture
def multi_task_dir(tmp_path: Path) -> Path:
    """Temp directory tree with two valid tasks under one package."""
    for task_name in ["claim_etl", "policy_etl"]:
        task = tmp_path / "proj" / "fraud_detection" / "ingestion" / task_name
        task.mkdir(parents=True)
        (task / "config.yaml").write_text(yaml.dump(_VALID_CONFIG), encoding="utf-8")
        (task / "transformations.py").write_text(_TRANSFORMATIONS_PY, encoding="utf-8")
    return tmp_path / "proj"
