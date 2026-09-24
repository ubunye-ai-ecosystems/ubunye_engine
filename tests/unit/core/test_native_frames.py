"""Users get native frames; the engine gets the port (ADR 004).

A transform on the pandas backend receives a plain ``pandas.DataFrame`` and may
return one, with no ``.native`` in sight. Everything the engine itself looks at
(writers, hooks, lineage) gets the ``DataFramePort``, where ``count()`` means
rows. On Spark both are the same object, so nothing changes there.
"""

from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.adapters.pandas_adapter import PandasDataFrameAdapter  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core.hooks import Hook  # noqa: E402

TRANSFORM = """\
from pathlib import Path

from ubunye.core.interfaces import Task


class Enrich(Task):
    def transform(self, sources):
        frame = sources["src"]
        # Exactly pandas.DataFrame, compared as a type: its __module__ reads
        # "pandas.core.frame" on pandas 2 and "pandas" on pandas 3.
        import pandas

        kind = "pandas.DataFrame" if type(frame) is pandas.DataFrame else repr(type(frame))
        (Path(__file__).parent / "seen.txt").write_text(kind)
        out = frame.assign(city_upper=frame["city"].str.upper())  # plain pandas
        return {"out": RETURN(out)}
"""


def _seen(task: Path) -> str:
    """The type the transform received, as it recorded it."""
    return (task / "seen.txt").read_text()


def _task(root: Path, returns: str = "out") -> Path:
    task = root / "uc" / "pkg" / "enrich"
    task.mkdir(parents=True)
    (root / "in.csv").write_text("id,city\n1,jhb\n2,cpt\n3,pta\n", encoding="utf-8")
    body = TRANSFORM.replace("RETURN(out)", returns)
    (task / "transformations.py").write_text(body, encoding="utf-8")
    (task / "config.yaml").write_text(
        f"""\
MODEL: etl
VERSION: "1.0.0"
CONFIG:
  inputs:
    src:
      format: s3
      path: "{(root / "in.csv").as_posix()}"
      file_format: csv
      options:
        header: "true"
  transform: {{}}
  outputs:
    out:
      format: s3
      path: "{(root / "out").as_posix()}"
      file_format: parquet
      mode: overwrite
""",
        encoding="utf-8",
    )
    return task


class CaptureOutputs(Hook):
    """What a hook (lineage, monitors) sees at the end of a task."""

    def __init__(self) -> None:
        self.outputs: Dict[str, Any] = {}

    @contextmanager
    def task(self, ctx, cfg, state):
        yield
        self.outputs = dict(state.get("outputs") or {})


class TestPandas:
    def test_a_transform_receives_a_plain_pandas_frame(self, tmp_path):
        task = _task(tmp_path)
        ubunye.run_task(str(task), backend="pandas")
        assert _seen(task) == "pandas.DataFrame"

    def test_it_may_return_a_plain_frame_and_the_engine_writes_it(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path)), backend="pandas")
        written = PandasBackend().read_frame("parquet", str(tmp_path / "out")).native
        assert sorted(written["city_upper"]) == ["CPT", "JHB", "PTA"]

    def test_returning_the_adapter_still_works(self, tmp_path):
        adapter = "__import__('ubunye.adapters.pandas_adapter', fromlist=['x'])"
        task = _task(tmp_path, returns=f"{adapter}.PandasDataFrameAdapter(out)")
        ubunye.run_task(str(task), backend="pandas")
        assert (tmp_path / "out" / "_SUCCESS").exists()

    def test_the_caller_gets_native_frames_back(self, tmp_path):
        outputs = ubunye.run_task(str(_task(tmp_path)), backend="pandas")
        assert isinstance(outputs["out"], pd.DataFrame)
        assert len(outputs["out"]) == 3

    def test_hooks_see_the_port_where_count_means_rows(self, tmp_path):
        hook = CaptureOutputs()
        ubunye.run_task(str(_task(tmp_path)), backend="pandas", hooks=[hook])
        port = hook.outputs["out"]
        assert isinstance(port, PandasDataFrameAdapter)
        assert port.count() == 3  # not pandas' per-column non-null counts

    def test_notebook_steps_are_native_too(self, tmp_path):
        nb = ubunye.notebook(str(_task(tmp_path)), backend="pandas")
        try:
            sources = nb.read()
            assert isinstance(sources["src"], pd.DataFrame)
            outputs = nb.transform(sources)
            assert isinstance(outputs["out"], pd.DataFrame)
            nb.write(outputs)
        finally:
            nb.close()
        assert (tmp_path / "out" / "_SUCCESS").exists()


class TestNarwhals:
    """ADR 005: one transform for both engines, written with Narwhals.

    The transform wraps what it gets (``nw.from_native``) and may return the
    Narwhals frame as it is: the engine unwraps anything that offers
    ``to_native()``, without importing Narwhals itself.
    """

    @pytest.fixture(autouse=True)
    def _narwhals(self):
        pytest.importorskip("narwhals")

    RETURNS = "__import__('narwhals').from_native(out)"

    def test_returning_a_narwhals_frame_is_written(self, tmp_path):
        ubunye.run_task(str(_task(tmp_path, returns=self.RETURNS)), backend="pandas")
        written = PandasBackend().read_frame("parquet", str(tmp_path / "out")).native
        assert sorted(written["city_upper"]) == ["CPT", "JHB", "PTA"]

    def test_the_caller_and_the_hooks_never_see_the_wrapper(self, tmp_path):
        hook = CaptureOutputs()
        outputs = ubunye.run_task(
            str(_task(tmp_path, returns=self.RETURNS)), backend="pandas", hooks=[hook]
        )
        assert type(outputs["out"]) is pd.DataFrame
        assert isinstance(hook.outputs["out"], PandasDataFrameAdapter)

    def test_the_notebook_unwraps_it_too(self, tmp_path):
        nb = ubunye.notebook(str(_task(tmp_path, returns=self.RETURNS)), backend="pandas")
        try:
            outputs = nb.transform(nb.read())
            assert type(outputs["out"]) is pd.DataFrame
            nb.write(outputs)
        finally:
            nb.close()
        assert (tmp_path / "out" / "_SUCCESS").exists()


class TestTheBoundary:
    def test_pandas_backend_converts_both_ways(self):
        backend = PandasBackend()
        frame = pd.DataFrame({"x": [1]})
        port = backend.to_port(frame)
        assert isinstance(port, PandasDataFrameAdapter) and port.native is frame
        assert backend.to_native(port) is frame
        assert backend.to_port(port) is port  # already a port
        assert backend.to_native(frame) is frame  # already native

    def test_other_things_pass_through_untouched(self):
        backend = PandasBackend()
        marker = object()
        assert backend.to_port(marker) is marker and backend.to_native(marker) is marker

    def test_the_default_is_the_identity(self):
        """Spark: the DataFrame is both native and the port, so nothing changes."""
        from ubunye.backends.spark_backend import SparkBackend

        backend = SparkBackend()
        marker = object()
        assert backend.to_native(marker) is marker and backend.to_port(marker) is marker
