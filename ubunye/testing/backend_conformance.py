"""The tests every Ubunye backend must pass, shipped so a new backend can prove itself.

A backend is a plugin (ADR 001): anyone can write one. This suite is how anyone
can show theirs keeps the promises the engine makes about every backend: it is
registered under its name, says what it can do, hands transforms their own
frames and the engine a port, reads a CSV file exactly as Spark does, writes
and reads back what it wrote, honours the write modes it claims, and leaves the
same run record hash as every other engine for the same data. The Spark and
pandas backends run it too (``tests/unit/backends/test_conformance_pandas.py``,
``tests/integration/test_conformance_spark.py``).

Use it from your backend's tests (needs pytest)::

    import pytest

    from ubunye.testing.backend_conformance import BackendConformance


    class TestMyBackend(BackendConformance):
        @pytest.fixture
        def backend(self):
            # Session time zone UTC: the reference data's timestamps are UTC.
            backend = MyBackend(conf={"spark.sql.session.timeZone": "UTC"})
            backend.start()
            yield backend
            backend.stop()

Tests for what a backend does not claim (a file format, a write mode, path IO)
are skipped, and say why.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
from pathlib import Path
from typing import Any, Dict, List

try:
    import pytest
except ImportError as exc:  # the suite is a set of pytest tests
    raise ImportError(
        "ubunye.testing.backend_conformance is a pytest suite; install pytest to run it "
        "(pip install pytest)."
    ) from exc

from ubunye.core import backends as registry
from ubunye.core.capabilities import PATH_IO, Capabilities
from ubunye.core.write_modes import ResolvedWriteMode
from ubunye.lineage.content_hash import Fingerprint, fingerprint, fingerprint_rows

#: The reference input: a CSV with a header, one column of every type Spark
#: infers, a quoted comma, doubled quotes Spark keeps as written, and nulls.
REFERENCE_CSV = (
    "id,name,score,passed,day,seen\n"
    '1,"Mokoena, Thabo",71.5,true,2024-01-02,2024-01-02 03:04:05\n'
    '2,"Anna ""Annie"" Dlamini",,false,2024-02-29,2024-02-29 23:59:59\n'
    "3,Sipho,88.0,,,\n"
)

#: How the reference is read: Spark's CSV reader with a header and inference.
READ_OPTIONS = {"header": "true", "inferSchema": "true"}

#: What Spark 4 reads from :data:`REFERENCE_CSV`, as plain Python values.
REFERENCE_ROWS: List[Dict[str, Any]] = [
    {
        "id": 1,
        "name": "Mokoena, Thabo",
        "score": 71.5,
        "passed": True,
        "day": dt.date(2024, 1, 2),
        "seen": dt.datetime(2024, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc),
    },
    {
        "id": 2,
        "name": '"Anna ""Annie"" Dlamini"',
        "score": None,
        "passed": False,
        "day": dt.date(2024, 2, 29),
        "seen": dt.datetime(2024, 2, 29, 23, 59, 59, tzinfo=dt.timezone.utc),
    },
    {"id": 3, "name": "Sipho", "score": 88.0, "passed": None, "day": None, "seen": None},
]

#: Their types, in the run record's canonical names (ADR 006).
REFERENCE_SCHEMA = [
    ("id", "int32"),
    ("name", "string"),
    ("score", "float64"),
    ("passed", "bool"),
    ("day", "date"),
    ("seen", "timestamp"),
]


def reference_fingerprint() -> Fingerprint:
    """The run record fingerprint every backend must give the reference data."""
    return fingerprint_rows(REFERENCE_ROWS, REFERENCE_SCHEMA)


_NATIVE_MODES = ("overwrite", "append", "errorifexists", "ignore")


class BackendConformance:
    """Subclass this in your backend's tests and provide the ``backend`` fixture."""

    @pytest.fixture
    def backend(self) -> Any:
        """A started backend with session time zone UTC. Override it."""
        raise NotImplementedError("Override the `backend` fixture in your subclass.")

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _caps(backend: Any) -> Capabilities:
        return backend.capabilities

    def _needs_path_io(self, backend: Any) -> None:
        if PATH_IO not in self._caps(backend).features:
            pytest.skip(f"{backend.name} does not claim path IO")

    def _needs_format(self, backend: Any, file_format: str) -> None:
        self._needs_path_io(backend)
        formats = self._caps(backend).file_formats  # None means any
        if formats is not None and file_format not in formats:
            pytest.skip(f"{backend.name} does not claim the {file_format} format")

    def _needs_mode(self, backend: Any, mode: str) -> None:
        modes = self._caps(backend).write_modes  # None means any
        if modes is not None and mode not in modes:
            pytest.skip(f"{backend.name} does not claim write mode {mode!r}")

    @pytest.fixture
    def reference(self, backend: Any, tmp_path: Path) -> Any:
        """The reference CSV read by the backend."""
        self._needs_format(backend, "csv")
        path = tmp_path / "reference.csv"
        path.write_text(REFERENCE_CSV, encoding="utf-8")
        return backend.read_frame("csv", str(path), options=dict(READ_OPTIONS))

    @staticmethod
    def _write(backend: Any, frame: Any, path: Path, file_format: str, mode: str) -> None:
        backend.execute_write(
            backend.to_port(frame),
            ResolvedWriteMode(mode=mode, save_mode=mode),
            connector="s3",
            file_format=file_format,
            path=str(path),
            options={"header": "true"} if file_format == "csv" else {},
        )

    @staticmethod
    def _rows(backend: Any, frame: Any) -> int:
        return int(backend.to_port(frame).count())

    # ----------------------------------------------------------------- identity

    def test_it_has_a_name(self, backend: Any) -> None:
        assert isinstance(backend.name, str) and backend.name

    def test_it_declares_what_it_can_do(self, backend: Any) -> None:
        caps = self._caps(backend)
        assert isinstance(caps, Capabilities)
        assert caps.declared, "declare CAPABILITIES so tasks can be checked before a run"

    def test_it_is_registered_under_its_name(self, backend: Any) -> None:
        assert (
            backend.name in registry.available()
        ), f"register it in the 'ubunye.backends' entry point group as {backend.name!r}"
        assert registry.load_class(backend.name) is type(backend)

    def test_the_packages_it_needs_are_named(self, backend: Any) -> None:
        for package in type(backend).REQUIRES_PACKAGES:
            assert importlib.util.find_spec(package) is not None, package

    def test_it_can_be_built_by_name(self, backend: Any) -> None:
        built = type(backend).create(app_name="conformance", conf={})
        assert isinstance(built, type(backend))

    def test_check_io_answers_with_a_list(self, backend: Any) -> None:
        cfg = {"format": "s3", "path": "x", "file_format": "csv"}
        assert isinstance(type(backend).check_io("input", cfg), list)

    # ------------------------------------------------------------------ frames

    def test_the_engine_gets_a_port_that_counts_rows(self, backend: Any, reference: Any) -> None:
        assert self._rows(backend, reference) == len(REFERENCE_ROWS)

    def test_native_and_port_convert_both_ways(self, backend: Any, reference: Any) -> None:
        native = backend.to_native(reference)
        assert self._rows(backend, native) == len(REFERENCE_ROWS)
        port = backend.to_port(native)
        assert backend.to_port(port).count() == len(REFERENCE_ROWS)  # already a port

    # ------------------------------------------------------------------- reads

    def test_it_reads_csv_as_spark_does(self, backend: Any, reference: Any) -> None:
        """Same types, same values, so the same run record hash as every engine."""
        got, want = fingerprint(backend.to_port(reference)), reference_fingerprint()
        assert got.error is None, got.error
        assert got.schema_hash == want.schema_hash, "the column types differ from Spark's"
        assert got.data_hash == want.data_hash, "the values differ from Spark's"

    def test_a_missing_path_fails_and_names_it(self, backend: Any, tmp_path: Path) -> None:
        self._needs_format(backend, "csv")
        with pytest.raises(Exception) as caught:
            frame = backend.read_frame("csv", str(tmp_path / "nowhere.csv"))
            self._rows(backend, frame)  # a lazy engine fails when asked
        assert "nowhere.csv" in str(caught.value)

    # ------------------------------------------------------------------ writes

    @pytest.mark.parametrize("file_format", ["parquet", "csv"])
    def test_what_it_writes_it_reads_back_the_same(
        self, backend: Any, reference: Any, tmp_path: Path, file_format: str
    ) -> None:
        self._needs_format(backend, file_format)
        self._needs_mode(backend, "overwrite")
        target = tmp_path / f"out_{file_format}"
        self._write(backend, reference, target, file_format, "overwrite")
        options = dict(READ_OPTIONS) if file_format == "csv" else {}
        back = backend.read_frame(file_format, str(target), options=options)
        assert fingerprint(backend.to_port(back)).data_hash == reference_fingerprint().data_hash

    def test_it_writes_json_lines_it_can_read(
        self, backend: Any, reference: Any, tmp_path: Path
    ) -> None:
        self._needs_format(backend, "json")
        self._needs_mode(backend, "overwrite")
        target = tmp_path / "out_json"
        self._write(backend, reference, target, "json", "overwrite")
        assert self._rows(backend, backend.read_frame("json", str(target))) == 3

    @pytest.mark.parametrize("mode", _NATIVE_MODES)
    def test_the_write_modes_it_claims(
        self, backend: Any, reference: Any, tmp_path: Path, mode: str
    ) -> None:
        self._needs_format(backend, "parquet")
        self._needs_mode(backend, mode)
        target = tmp_path / "modes"
        self._write(backend, reference, target, "parquet", "overwrite")  # 3 rows there

        def rows_now() -> int:
            return self._rows(backend, backend.read_frame("parquet", str(target)))

        if mode == "errorifexists":
            with pytest.raises(Exception):
                self._write(backend, reference, target, "parquet", mode)
            assert rows_now() == 3, "a refused write must leave the target as it was"
            return
        self._write(backend, reference, target, "parquet", mode)
        assert rows_now() == {"overwrite": 3, "append": 6, "ignore": 3}[mode]
