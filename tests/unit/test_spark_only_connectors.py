"""Connectors that need Spark say so on a backend without it.

hive, jdbc, delta, unity, binary and rest_api build on a SparkSession. On the
pandas backend they must fail before doing any work (no HTTP call, no JDBC
connection), with a message that names the connector and the way out, not a
stray ``AttributeError: 'PandasBackend' object has no attribute 'spark'``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ubunye.core.errors import SinkWriteError, SourceReadError
from ubunye.plugins.readers.binary import BinaryReader
from ubunye.plugins.readers.delta import DeltaReader
from ubunye.plugins.readers.hive import HiveReader
from ubunye.plugins.readers.jdbc import JdbcReader
from ubunye.plugins.readers.rest_api import RestApiReader
from ubunye.plugins.readers.unity import UnityTableReader
from ubunye.plugins.writers.delta import DeltaWriter
from ubunye.plugins.writers.hive import HiveWriter
from ubunye.plugins.writers.unity import UnityTableWriter


class NoSparkBackend:
    """A backend with no session, like the pandas one."""

    is_spark = False


READERS = [
    ("binary", BinaryReader, {"path": "/x"}),
    ("delta", DeltaReader, {"path": "/x"}),
    ("hive", HiveReader, {"db_name": "d", "tbl_name": "t"}),
    ("jdbc", JdbcReader, {"url": "jdbc:x", "table": "t"}),
    ("rest_api", RestApiReader, {"url": "https://example.invalid/api"}),
    ("unity", UnityTableReader, {"table": "c.s.t"}),
]
WRITERS = [
    ("delta", DeltaWriter, {"path": "/x"}),
    ("hive", HiveWriter, {"db_name": "d", "tbl_name": "t"}),
    ("unity", UnityTableWriter, {"table": "c.s.t"}),
]


@pytest.mark.parametrize("name, cls, cfg", READERS, ids=[r[0] for r in READERS])
def test_reader_refuses_by_name(name, cls, cfg, monkeypatch):
    # Nothing may go over the network before the refusal.
    monkeypatch.setattr("requests.Session.request", MagicMock(side_effect=AssertionError))
    with pytest.raises(SourceReadError) as caught:
        cls().read(cfg, NoSparkBackend())
    message = str(caught.value)
    assert f"'{name}' connector needs Spark" in message
    assert "NoSparkBackend" in message
    assert "--backend spark" in message


@pytest.mark.parametrize("name, cls, cfg", WRITERS, ids=[w[0] for w in WRITERS])
def test_writer_refuses_by_name(name, cls, cfg):
    with pytest.raises(SinkWriteError) as caught:
        cls().write(MagicMock(), cfg, NoSparkBackend())
    assert f"'{name}' connector needs Spark" in str(caught.value)


def test_a_spark_backend_is_untouched():
    from ubunye.adapters.spark.session import spark_of

    backend = MagicMock()
    assert spark_of(backend, "hive", error=SourceReadError) is backend.spark
