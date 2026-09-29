"""A partitioned append is rerun safe, exactly like a plain one (ADR 008, F-012).

A partitioned append lands one part file in each partition folder. Every one of
them is claimed before it lands, so a run that fails, or dies, has exactly its
own files taken back, and ``--rerun`` replaces exactly the finished batch.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

import ubunye  # noqa: E402
from ubunye.backends.pandas_backend import PandasBackend  # noqa: E402
from ubunye.core import runs  # noqa: E402
from ubunye.core.write_modes import ResolvedWriteMode  # noqa: E402

TASK = "uc/pkg/t"
APPEND = ResolvedWriteMode(mode="append", save_mode="append")

PASS_THROUGH = (
    "from ubunye.core.interfaces import Task\n\n\n"
    "class T(Task):\n"
    "    def transform(self, sources):\n"
    "        return {'out': sources['src']}\n"
)


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


def _task(tmp_path: Path, also: str = "") -> Path:
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True, exist_ok=True)
    (tmp_path / "in.csv").write_text("id,g\n1,a\n2,b\n", encoding="utf-8")
    (task / "transformations.py").write_text(PASS_THROUGH, encoding="utf-8")
    (task / "config.yaml").write_text(
        "MODEL: etl\n"
        'VERSION: "1.0.0"\n'
        "CONFIG:\n"
        "  inputs:\n"
        "    src:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "in.csv").as_posix()}"\n'
        "      file_format: csv\n"
        "      options: {header: 'true'}\n"
        "  transform: {}\n"
        "  outputs:\n"
        "    out:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "out").as_posix()}"\n'
        "      file_format: parquet\n"
        "      mode: append\n"
        "      partitionBy: [g]\n" + also,
        encoding="utf-8",
    )
    return task


def _parts(tmp_path: Path) -> set:
    return {
        p.relative_to(tmp_path / "out").as_posix() for p in (tmp_path / "out").rglob("*.parquet")
    }


def _ids(tmp_path: Path) -> list:
    frame = PandasBackend().read_frame("parquet", str(tmp_path / "out")).native
    return sorted(frame["id"].tolist())


def test_a_failed_run_takes_back_exactly_its_partition_files(tmp_path):
    out = tmp_path / "out"
    PandasBackend().execute_write(
        pd.DataFrame({"id": ["0"], "g": ["a"]}),
        APPEND,
        connector="s3",
        file_format="parquet",
        path=str(out),
        partition_by=["g"],
    )
    before = _parts(tmp_path)
    with pytest.raises(RuntimeError, match="later output"):
        with runs.held(tmp_path, TASK, {}, "run-x"):
            runs.writing("out", appends=True, exact=True)
            PandasBackend().execute_write(
                pd.DataFrame({"id": ["1", "2"], "g": ["a", "b"]}),
                APPEND,
                connector="s3",
                file_format="parquet",
                path=str(out),
                partition_by=["g"],
            )
            (out / "g=a" / "part-other-run.parquet").write_bytes(b"x")  # lands meanwhile
            mine = _parts(tmp_path) - before - {"g=a/part-other-run.parquet"}
            assert len(mine) == 2  # one file in g=a, one in the new g=b
            lease = runs.current()
            claimed = lease._doc["outputs"]["out"]["claimed"]
            assert sorted(os.path.basename(c) for c in claimed) == sorted(
                os.path.basename(m) for m in mine
            )
            runs.written("out")
            raise RuntimeError("a later output failed")
    assert _parts(tmp_path) == before | {"g=a/part-other-run.parquet"}


def test_the_engine_takes_back_a_partitioned_append_when_a_later_output_fails(tmp_path):
    also = (
        "    also:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "in.csv").as_posix()}"\n'  # a file: append refused
        "      file_format: parquet\n"
        "      mode: append\n"
    )
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-01")
    first = _parts(tmp_path)
    assert len(first) == 2  # g=a and g=b
    _task(tmp_path, also)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    assert _parts(tmp_path) == first
    assert _ids(tmp_path) == ["1", "2"]


_DIE_AFTER_APPEND = """
import os, sys
from pathlib import Path
import pandas as pd
from ubunye.backends.pandas_backend import PandasBackend
from ubunye.core import runs
from ubunye.core.write_modes import ResolvedWriteMode

root = Path(sys.argv[1])
held = runs.held(root, "uc/pkg/t", {"dt": "2024-01-02", "mode": "DEV"}, "run-killed")
held.__enter__()
runs.writing("out", appends=True, exact=True)
PandasBackend().execute_write(
    pd.DataFrame({"id": ["7", "8"], "g": ["a", "c"]}),
    ResolvedWriteMode(mode="append", save_mode="append"),
    connector="s3", file_format="parquet", path=str(root / "out"), partition_by=["g"],
)
os._exit(9)  # killed after its files landed, before it could record anything
"""


def test_a_killed_partitioned_append_is_taken_back_by_the_next_run(tmp_path):
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-01")
    first = _parts(tmp_path)
    died = subprocess.run([sys.executable, "-c", _DIE_AFTER_APPEND, str(tmp_path)])
    assert died.returncode == 9
    assert _ids(tmp_path) == ["1", "2", "7", "8"]  # the killed run's rows landed
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    assert _ids(tmp_path) == ["1", "1", "2", "2"]  # each batch once, nothing of the dead run
    assert first <= _parts(tmp_path)  # the other batch's files untouched
    assert not list((tmp_path / "out" / "g=c").glob("*.parquet"))


def test_rerun_replaces_a_finished_partitioned_batch(tmp_path):  # F-031
    task = _task(tmp_path)
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    first = _parts(tmp_path)
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-03")
    other = _parts(tmp_path) - first
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02", rerun=True)
    assert _ids(tmp_path) == ["1", "1", "2", "2"]
    assert _parts(tmp_path).isdisjoint(first)  # the finished run's files are gone
    assert other <= _parts(tmp_path)  # the other batch is left alone
    with pytest.raises(runs.BatchFinished):  # and a plain second run is still refused
        ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")


def test_overwrite_partitions_is_not_an_append(tmp_path):
    # It replaces what it writes, so it claims nothing and a second run of the
    # batch is not refused (it is already rerun safe).
    task = _task(tmp_path)
    config = task / "config.yaml"
    config.write_text(
        config.read_text(encoding="utf-8").replace("mode: append", "mode: overwrite_partitions"),
        encoding="utf-8",
    )
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    assert _ids(tmp_path) == ["1", "2"]
    # Spark's dynamic overwrite writes no _SUCCESS into a new target (SPEC O4).
    assert sorted(os.listdir(tmp_path / "out")) == ["g=a", "g=b"]


def test_taking_back_a_first_partitioned_append_drops_the_root_success_marker(tmp_path):
    # The append created the target, so after the take back it holds no data and must
    # not say it is complete. _SUCCESS is at the root, above the partition folders.
    out = tmp_path / "out"
    with pytest.raises(RuntimeError):
        with runs.held(tmp_path, TASK, {}, "run-first"):
            runs.writing("out", appends=True, exact=True)
            PandasBackend().execute_write(
                pd.DataFrame({"id": ["1", "2"], "g": ["a", "b"]}),
                APPEND,
                connector="s3",
                file_format="parquet",
                path=str(out),
                partition_by=["g"],
            )
            assert (out / "_SUCCESS").exists()
            runs.written("out")
            raise RuntimeError("a later output failed")
    assert _parts(tmp_path) == set()
    assert not (out / "_SUCCESS").exists()


def test_the_root_success_marker_stays_while_other_partitions_hold_data(tmp_path):
    out = tmp_path / "out"
    kw = dict(connector="s3", file_format="parquet", path=str(out), partition_by=["g"])
    PandasBackend().execute_write(pd.DataFrame({"id": ["0"], "g": ["z"]}), APPEND, **kw)
    with pytest.raises(RuntimeError):
        with runs.held(tmp_path, TASK, {}, "run-second"):
            runs.writing("out", appends=True, exact=True)
            PandasBackend().execute_write(pd.DataFrame({"id": ["1"], "g": ["a"]}), APPEND, **kw)
            runs.written("out")
            raise RuntimeError("a later output failed")
    assert (out / "_SUCCESS").exists() and _parts(tmp_path) == {
        p for p in _parts(tmp_path) if p.startswith("g=z/")
    }
