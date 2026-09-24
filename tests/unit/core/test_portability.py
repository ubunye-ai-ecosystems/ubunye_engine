"""Which dataframe API a transform is written against, read from its imports (ADR 005).

Portability is detected, not declared: there is no config field for it. The
plan reads ``transformations.py`` (without running it) and says what the
transform expects, so ``plan --backend pandas`` can warn about a Spark API
transform before the run fails on its first line.
"""

from __future__ import annotations

import textwrap

import pytest

from ubunye.core.portability import frame_api


def _api(tmp_path, source: str):
    path = tmp_path / "transformations.py"
    path.write_text(textwrap.dedent(source), encoding="utf-8")
    return frame_api(path)


class TestWhatATransformIsWrittenFor:
    def test_the_spark_api(self, tmp_path):
        found = _api(tmp_path, "from pyspark.sql import functions as F\n")
        assert found.api == "pyspark" and found.imports == ("pyspark",)

    def test_narwhals_is_portable(self, tmp_path):
        found = _api(tmp_path, "import narwhals as nw\nimport pandas as pd\n")
        assert found.api == "narwhals"
        assert found.imports == ("narwhals", "pandas")

    def test_pandas(self, tmp_path):
        assert _api(tmp_path, "import pandas as pd\n").api == "pandas"

    def test_spark_wins_over_narwhals(self, tmp_path):
        # One F.col is enough to tie the transform to Spark.
        found = _api(tmp_path, "import narwhals as nw\nimport pyspark.sql.functions as F\n")
        assert found.api == "pyspark"

    def test_nothing_detected_is_not_a_guess(self, tmp_path):
        # The init scaffold: plain indexing that means the same on both.
        assert _api(tmp_path, "from ubunye.core.interfaces import Task\n").api is None

    def test_imports_only_for_type_checking_do_not_count(self, tmp_path):
        source = """
            from typing import TYPE_CHECKING
            import narwhals as nw
            if TYPE_CHECKING:
                from pyspark.sql import DataFrame
        """
        assert _api(tmp_path, source).api == "narwhals"

    def test_an_optional_import_does_not_count(self, tmp_path):
        source = """
            try:
                import pyspark
            except ImportError:
                pyspark = None
        """
        assert _api(tmp_path, source).api is None

    def test_an_import_inside_a_function_counts(self, tmp_path):
        source = """
            def transform(self, sources):
                from pyspark.sql import functions as F
        """
        assert _api(tmp_path, source).api == "pyspark"

    def test_a_file_that_does_not_parse_says_so(self, tmp_path):
        found = _api(tmp_path, "def broken(:\n")
        assert found.api is None and found.error


@pytest.mark.parametrize(
    "api,spark_backend,warns",
    [
        ("pyspark", True, False),
        ("pyspark", False, True),
        ("pandas", False, False),
        ("pandas", True, True),
        ("narwhals", True, False),
        ("narwhals", False, False),
        (None, False, False),
    ],
)
def test_the_mismatch_rule(api, spark_backend, warns):
    from ubunye.core.portability import FrameApi, mismatch

    message = mismatch(FrameApi(api=api, imports=()), backend="x", spark_backend=spark_backend)
    assert bool(message) is warns
