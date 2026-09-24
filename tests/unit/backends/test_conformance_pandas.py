"""The pandas backend passes the conformance suite every backend must pass.

The same suite ships as :mod:`ubunye.testing.backend_conformance` for third
party backends; the Spark backend runs it in the integration tier.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.testing.backend_conformance import BackendConformance  # noqa: E402


class TestPandasBackend(BackendConformance):
    @pytest.fixture
    def backend(self):
        backend = PandasBackend(conf={"spark.sql.session.timeZone": "UTC"})
        backend.start()
        yield backend
        backend.stop()
