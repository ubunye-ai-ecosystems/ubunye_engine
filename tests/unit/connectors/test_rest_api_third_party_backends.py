"""A backend written before ``records`` existed keeps running rest_api (F-015 review).

On b6ff30b a plugin backend with a SparkSession that declared ``spark`` ran the
REST connector, and the writer ignored its backend argument (``None`` worked).
The F-015 fix made both require ``records``. Now ``spark`` implies ``records``,
the ``Backend`` base builds and reads records the Spark way when it has a
session, and the writer falls back to the frame's ``toLocalIterator``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

pytest.importorskip("requests")

from rest_api_server import served  # noqa: E402

from ubunye.core.capabilities import PATH_IO, SPARK, Capabilities, check_task  # noqa: E402
from ubunye.core.interfaces import Backend  # noqa: E402
from ubunye.plugins.readers.rest_api import RestApiReader  # noqa: E402
from ubunye.plugins.writers.rest_api import RestApiWriter  # noqa: E402


class EmrBackend(Backend):
    """A plugin backend with a SparkSession, declaring what it had before F-015."""

    CAPABILITIES = Capabilities(features=frozenset({SPARK, PATH_IO}))

    def __init__(self):
        self.spark = MagicMock(name="spark")

    def start(self):
        pass

    def stop(self):
        pass


class Legacy(Backend):
    """Declares nothing and has no session."""

    def start(self):
        pass

    def stop(self):
        pass


def _spark_frame(rows):
    df = MagicMock()
    records = []
    for r in rows:
        row = MagicMock()
        row.asDict.return_value = r
        records.append(row)
    df.toLocalIterator.return_value = iter(records)
    return df


def test_spark_satisfies_records_in_the_plan():
    registry = MagicMock()
    registry.readers = {"rest_api": RestApiReader}
    registry.writers = {"rest_api": RestApiWriter}
    cfg = {
        "CONFIG": {
            "inputs": {"api": {"format": "rest_api", "url": "http://x"}},
            "outputs": {"sink": {"format": "rest_api", "url": "http://x"}},
        }
    }
    assert check_task(EmrBackend.CAPABILITIES, cfg, registry, backend_name="emr") == []


def test_a_spark_plugin_backend_reads_the_spark_way():
    backend = EmrBackend()
    with served([{"id": 1}, {"id": 2}]) as api:
        frame = RestApiReader().read({"url": f"{api.base}/all"}, backend)
    assert frame is backend.spark.createDataFrame.return_value
    backend.spark.createDataFrame.assert_called_once_with([{"id": 1}, {"id": 2}])


@pytest.mark.parametrize("backend", [EmrBackend(), Legacy(), None], ids=["emr", "legacy", "none"])
def test_the_writer_posts_from_any_spark_frame(backend):
    with served() as api:
        RestApiWriter().write(
            _spark_frame([{"id": 1}, {"id": 2}]), {"url": f"{api.base}/s"}, backend
        )
        posted = api.posted
    assert posted == [{"records": [{"id": 1}, {"id": 2}]}]
