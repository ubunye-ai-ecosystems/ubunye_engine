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


def test_without_pytest_the_suite_says_what_to_install():
    import subprocess
    import sys

    code = (
        "import sys; sys.modules['pytest'] = None\n"
        "try:\n"
        "    import ubunye.testing.backend_conformance\n"
        "except ImportError as exc:\n"
        "    print(exc)\n"
    )
    done = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert "pip install pytest" in done.stdout, done.stdout + done.stderr
