"""Spark path appends claim each file before it lands (ADR 008, F-070).

Spark-free: a fake ``df.write`` chain writes files into the folder it is given, as
Spark would. The live Spark proof (kills, reruns, foreign files, four formats) is
``tests/integration/test_spark_append_claims.py``.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ubunye.adapters.spark import claimed_append, write_exec
from ubunye.core import runs
from ubunye.core.errors import SinkWriteError
from ubunye.core.runs import RunLease
from ubunye.core.write_modes import ResolvedWriteMode

TASK = "uc/pkg/t"


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


class _Writer:
    """``df.write``: ``save(folder)`` writes what Spark writes on a local disk."""

    def __init__(self, files):
        self.files = files
        self.saved = []

    def mode(self, _m):
        return self

    def format(self, _f):
        return self

    def partitionBy(self, *_c):
        return self

    def option(self, _k, _v):
        return self

    def save(self, folder):
        self.saved.append(folder)
        os.makedirs(folder)
        for rel in self.files + ["_SUCCESS", "._SUCCESS.crc"]:
            full = os.path.join(folder, rel)
            os.makedirs(os.path.dirname(full), exist_ok=True)
            Path(full).write_text(rel)


def _df(files):
    df = MagicMock()
    df.write = _Writer(files)
    return df


@pytest.fixture
def local(tmp_path, monkeypatch):
    """Spark resolves the output to this machine's disk (``_local`` needs a JVM)."""
    out = tmp_path / "out" / "events"

    def fake(_spark, _path):
        staging = str(tmp_path / "out" / ".events.ubunye-abc")
        return str(out), staging, staging

    monkeypatch.setattr(claimed_append, "_local", fake)
    return out


def _held(tmp_path, **kw):
    return runs.held(tmp_path, TASK, {"dt": "2024-01-02"}, kw.pop("run_id", "run-1"), **kw)


def _lease_doc(tmp_path):
    return json.loads(RunLease(tmp_path, TASK, {"dt": "2024-01-02"}, "x").path.read_text())


class TestWhatIsData:
    @pytest.mark.parametrize(
        "rel,hidden",
        [
            ("part-00000-u.c000.snappy.parquet", False),
            ("p=a/part-00000-u.c000.csv", False),
            (os.path.join("_part=1", "part-0.json"), False),  # a partition, to Spark
            ("_SUCCESS", True),
            (".part-00000-u.c000.snappy.parquet.crc", True),
            (os.path.join("p=a", ".part-0.crc"), True),
            ("_metadata", True),
            ("_temporary", True),
            ("part-0.parquet._COPYING_", True),
        ],
    )
    def test_spark_rules(self, rel, hidden):
        assert claimed_append._hidden(rel) is hidden


class TestNotClaimedHere:
    def test_without_a_run_lease_spark_appends_directly(self, local):
        df = _df(["part-0.parquet"])
        assert not claimed_append.append(
            df, MagicMock(), path="x", file_format="parquet", partition_by=[], opts={}
        )
        assert df.write.saved == []

    def test_delta_is_never_moved_file_by_file(self, tmp_path, local):
        df = _df(["part-0.parquet"])
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            assert not claimed_append.append(
                df, MagicMock(), path="x", file_format="delta", partition_by=[], opts={}
            )
        assert df.write.saved == []

    @pytest.mark.parametrize("scheme", ["s3a", "abfss", "gs", "dbfs", "hdfs"])
    def test_a_path_spark_does_not_resolve_to_this_disk_is_not_claimed(self, scheme):
        spark = MagicMock()
        spark._jvm.org.apache.hadoop.fs.Path.return_value.toUri.return_value.getScheme.return_value = (
            scheme
        )
        assert claimed_append._local(spark, f"{scheme}://b/x") is None

    def test_a_scheme_less_path_on_a_cluster_whose_default_is_hdfs_is_not_claimed(self):
        spark = MagicMock()
        hpath = spark._jvm.org.apache.hadoop.fs.Path.return_value
        hpath.toUri.return_value.getScheme.return_value = None
        hpath.getFileSystem.return_value.getScheme.return_value = "hdfs"
        assert claimed_append._local(spark, "/data/x") is None

    def test_spark_connect_has_no_jvm(self):
        class Connect:
            pass

        assert claimed_append._local(Connect(), "/tmp/x") is None


class TestClaimedBeforeTheyLand:
    FILES = ["part-00000-j.c000.snappy.parquet", "p=a/part-00001-j.c000.snappy.parquet"]

    def test_every_file_is_in_the_lease_before_any_lands(self, tmp_path, local, monkeypatch):
        seen = []
        real = os.replace

        def spy(src, dst):
            if str(dst).startswith(str(local)):  # a data file, not the lease itself
                seen.append((str(dst), _lease_doc(tmp_path)["outputs"]["events"]["claimed"]))
            real(src, dst)

        monkeypatch.setattr(claimed_append.os, "replace", spy)
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            assert claimed_append.append(
                _df(self.FILES), MagicMock(), path="x", file_format="parquet",
                partition_by=["p"], opts={},
            )  # fmt: skip
        assert len(seen) == 2
        for _dst, claimed in seen:
            assert len(claimed) == 2  # all claimed, before the first move
        assert sorted(p.relative_to(local).as_posix() for p in local.rglob("part-*")) == sorted(
            self.FILES
        )
        assert (local / "_SUCCESS").exists()
        assert not list(local.rglob("*.crc"))  # checksums stay behind, as do summaries
        assert not (tmp_path / "out" / ".events.ubunye-abc").exists()

    def test_a_failed_run_removes_its_files_and_never_a_foreign_one(self, tmp_path, local):
        local.mkdir(parents=True)
        (local / "part-00000-other.c000.snappy.parquet").write_text("another job's")
        with pytest.raises(RuntimeError):
            with _held(tmp_path):
                runs.writing("events", appends=True, exact=False)
                claimed_append.append(
                    _df(self.FILES), MagicMock(), path="x", file_format="parquet",
                    partition_by=["p"], opts={},
                )  # fmt: skip
                raise RuntimeError("a later output failed")
        left = sorted(p.name for p in local.rglob("part-*"))
        assert left == ["part-00000-other.c000.snappy.parquet"]

    def test_a_file_already_there_by_the_same_name_is_never_replaced(self, tmp_path, local):
        local.mkdir(parents=True)
        (local / self.FILES[0]).write_text("not this run's")
        with pytest.raises(SinkWriteError, match="already there"):
            with _held(tmp_path):
                runs.writing("events", appends=True, exact=False)
                claimed_append.append(
                    _df(self.FILES), MagicMock(), path="x", file_format="parquet",
                    partition_by=[], opts={},
                )  # fmt: skip
        # Never claimed, so the failed run's take back left it alone.
        assert (local / self.FILES[0]).read_text() == "not this run's"
        assert not (tmp_path / "out" / ".events.ubunye-abc").exists()

    def test_the_output_is_exact_and_its_staging_is_named_before_spark_writes(
        self, tmp_path, local
    ):
        seen = {}

        class Writer(_Writer):
            def save(self, folder):
                seen.update(_lease_doc(tmp_path)["outputs"]["events"])
                super().save(folder)

        df = MagicMock()
        df.write = Writer(self.FILES)
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            claimed_append.append(
                df, MagicMock(), path="x", file_format="csv", partition_by=[], opts={}
            )
        assert seen["exact"] is True
        assert seen["staging"].endswith(".events.ubunye-abc")
        assert seen["claimed"] == []

    def test_write_exec_routes_a_path_append_here(self, monkeypatch):
        calls = []
        monkeypatch.setattr(claimed_append, "append", lambda *a, **k: calls.append(k) or True)
        df = MagicMock()
        write_exec.apply(
            df,
            MagicMock(),
            ResolvedWriteMode(mode="append", save_mode="append"),
            connector="s3",
            file_format="parquet",
            path="/tmp/x",
        )
        assert calls and calls[0]["path"] == "/tmp/x"
        df.write.mode.assert_not_called()


class TestTakeBack:
    def test_a_dead_runs_staging_folder_is_removed_with_its_claims(self, tmp_path):
        staging = tmp_path / "out" / ".events.ubunye-dead"
        (staging / "p=a").mkdir(parents=True)
        (staging / "p=a" / "part-0.parquet").write_text("s")
        out = tmp_path / "out" / "events"
        out.mkdir()
        (out / "part-other.parquet").write_text("o")
        note = {
            "appends": True,
            "exact": True,
            "state": "writing",
            "claimed": [],
            "staging": str(staging),
        }
        dead = RunLease(tmp_path, TASK, {}, "dead-run")
        dead.path.parent.mkdir(parents=True, exist_ok=True)
        pid = subprocess.run(
            [sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True
        )
        dead.path.write_text(
            json.dumps(
                {
                    "run_id": "dead-run",
                    "pid": int(pid.stdout.strip()),  # a process that has ended
                    "host": runs._host(),
                    "heartbeat": 0,
                    "outputs": {"events": note},
                }
            ),
            encoding="utf-8",
        )
        RunLease(tmp_path, TASK, {}, "new").acquire().release()
        assert not staging.exists()
        assert [p.name for p in out.iterdir()] == ["part-other.parquet"]
