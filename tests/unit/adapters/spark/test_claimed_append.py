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
        staging = str(out / "_ubunye-abc")  # inside the output, as _local names it
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
        with pytest.raises(claimed_append._NotLocal, match=scheme):
            claimed_append._local(spark, f"{scheme}://b/x")

    def test_a_scheme_less_path_on_a_cluster_whose_default_is_hdfs_is_not_claimed(self):
        spark = MagicMock()
        hpath = spark._jvm.org.apache.hadoop.fs.Path.return_value
        hpath.toUri.return_value.getScheme.return_value = None
        hpath.getFileSystem.return_value.getScheme.return_value = "hdfs"
        with pytest.raises(claimed_append._NotLocal, match="hdfs"):
            claimed_append._local(spark, "/data/x")

    def test_spark_connect_has_no_jvm(self):
        class Connect:
            pass

        with pytest.raises(claimed_append._NotLocal, match="Spark Connect"):
            claimed_append._local(Connect(), "/tmp/x")

    @pytest.mark.parametrize(
        "fmt,path,why",
        [
            ("delta", "/tmp/x", "format delta"),
            ("parquet", "s3a://b/x", "s3a:// is not the local file system"),
        ],
    )
    def test_each_fallback_says_why_in_one_info_line(self, tmp_path, caplog, fmt, path, why):
        # F-070 skeptic review: the fallback was silent.
        spark = MagicMock()
        spark._jvm.org.apache.hadoop.fs.Path.return_value.toUri.return_value.getScheme.return_value = (
            "s3a"
        )
        caplog.set_level("INFO", logger="ubunye.adapters.spark.claimed_append")
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            claimed_append.append(
                _df([]), spark, path=path, file_format=fmt, partition_by=[], opts={}
            )
        lines = [r.getMessage() for r in caplog.records if "is not claimed" in r.getMessage()]
        assert len(lines) == 1
        assert "output events" in lines[0] and why in lines[0]


class _FakeHadoopPath:
    """Enough of ``org.apache.hadoop.fs.Path`` on the local file system."""

    def __init__(self, a, b=None):
        self.s = str(a) if b is None else a.s.rstrip("/") + "/" + b

    def toUri(self):
        uri = MagicMock()
        uri.getScheme.return_value = None
        uri.getPath.return_value = self.s
        return uri

    def getFileSystem(self, _conf):
        fs = MagicMock()
        fs.getScheme.return_value = "file"
        fs.makeQualified.side_effect = lambda p: p
        return fs

    def toString(self):
        return "file:" + self.s


class TestStagingInsideTheOutput:
    """F-070 skeptic review: a staging folder *beside* the output could not be renamed
    into an output mounted on another drive (WinError 17, EXDEV), made a name over 255
    characters for a long output name, and was read by a glob of the parent folder. It
    is now inside the output, named ``_ubunye-<12 hex>``."""

    def test_the_staging_folder_is_a_short_hidden_folder_inside_the_output(self, tmp_path):
        spark = MagicMock()
        spark._jvm.org.apache.hadoop.fs.Path = _FakeHadoopPath
        out = (tmp_path / ("o" * 240)).as_posix()
        local, uri, staging = claimed_append._local(spark, out)
        assert os.path.dirname(staging) == local == os.path.normpath(out)
        name = os.path.basename(staging)
        assert name.startswith("_ubunye-") and len(name) == 20
        assert uri.endswith("/" + name)

    def test_the_output_folder_exists_before_spark_writes(self, tmp_path, local):
        seen = {}

        class Writer(_Writer):
            def save(self, folder):
                seen["output"] = os.path.isdir(local)
                super().save(folder)

        df = MagicMock()
        df.write = Writer(["part-0.parquet"])
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            claimed_append.append(
                df, MagicMock(), path="x", file_format="parquet", partition_by=[], opts={}
            )
        assert seen["output"] is True
        assert sorted(os.listdir(local)) == ["_SUCCESS", "part-0.parquet"]

    def test_a_staging_folder_left_in_the_output_is_named_never_removed(
        self, tmp_path, local, caplog
    ):
        (local / "_ubunye-0123456789ab").mkdir(parents=True)
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            claimed_append.append(
                _df(["part-0.parquet"]), MagicMock(), path="x", file_format="parquet",
                partition_by=[], opts={},
            )  # fmt: skip
        assert (local / "_ubunye-0123456789ab").is_dir()
        assert any("_ubunye-0123456789ab" in r.getMessage() for r in caplog.records)


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
        assert not (local / "_ubunye-abc").exists()

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
        assert not (local / "_ubunye-abc").exists()

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
        assert seen["staging"].endswith("_ubunye-abc")
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
        staging = tmp_path / "out" / "events" / "_ubunye-dead"
        (staging / "p=a").mkdir(parents=True)
        (staging / "p=a" / "part-0.parquet").write_text("s")
        out = tmp_path / "out" / "events"
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
        lease = RunLease(tmp_path, TASK, {}, "new").acquire()
        lease.release()
        assert not staging.exists()
        assert [p.name for p in out.iterdir()] == ["part-other.parquet"]
        # Killed before its claims: nothing of it reached the output, so it is not
        # reported as maybe holding part of the batch (F-070 skeptic review).
        assert lease.recovered["unrepaired"] == []

    def test_a_failed_staged_append_with_no_claims_is_not_called_unrepaired(
        self, tmp_path, local, caplog
    ):
        # A name clash refused before any claim: the failed run's log must not say the
        # output may hold part of the batch.
        local.mkdir(parents=True)
        (local / "part-0.parquet").write_text("not this run's")
        with pytest.raises(SinkWriteError):
            with _held(tmp_path):
                runs.writing("events", appends=True, exact=False)
                claimed_append.append(
                    _df(["part-0.parquet"]), MagicMock(), path="x", file_format="parquet",
                    partition_by=[], opts={},
                )  # fmt: skip
        assert not any("cannot be taken back" in r.getMessage() for r in caplog.records)

    def test_an_exact_output_with_no_staging_and_no_claims_is_still_named(self):
        # A custom writer on a claiming backend may append without claiming.
        outputs = {"custom": {"appends": True, "exact": True, "claimed": []}}
        assert runs._unrepaired(outputs) == ["custom"]
        staged = {"events": {"appends": True, "exact": True, "claimed": [], "staging": "s"}}
        assert runs._unrepaired(staged) == []


class TestCheapLandedChecks:
    def test_claiming_many_files_reads_the_lease_a_fixed_number_of_times(
        self, tmp_path, local, monkeypatch
    ):
        # F-070 skeptic review: landed() read the whole lease per file, and the lease
        # grows with each claim: 12,000 files took 92 s. Now one stat per file.
        reads = []
        real = RunLease._owner
        monkeypatch.setattr(RunLease, "_owner", lambda self: reads.append(1) or real(self))
        files = [f"p={i % 7}/part-{i:05d}-j.c000.snappy.parquet" for i in range(300)]
        with _held(tmp_path):
            runs.writing("events", appends=True, exact=False)
            before = len(reads)
            claimed_append.append(
                _df(files), MagicMock(), path="x", file_format="parquet",
                partition_by=["p"], opts={},
            )  # fmt: skip
            during = len(reads) - before
        assert during <= 5, during
        assert len(list(local.rglob("part-*"))) == 300

    def test_a_takeover_during_the_moves_still_removes_what_landed(self, tmp_path, local):
        # The tombstone a takeover writes is seen at the next file.
        moved = []

        def landed(path):
            moved.append(path)
            if len(moved) == 2:
                lease = runs.current()
                lease._tombstone(lease.run_id).write_text("x")
            real_landed(path)

        real_landed = runs.landed
        files = [f"part-{i}.parquet" for i in range(5)]
        with pytest.raises(runs.RunLeaseLost):
            with _held(tmp_path):
                runs.writing("events", appends=True, exact=False)
                runs_landed = runs.landed
                runs.landed = landed
                try:
                    claimed_append.append(
                        _df(files), MagicMock(), path="x", file_format="parquet",
                        partition_by=[], opts={},
                    )  # fmt: skip
                finally:
                    runs.landed = runs_landed
        # The file that landed after the tombstone was removed by this run; the run
        # that took over removes the claims it saw (here: none, it is simulated).
        assert not (local / "part-1.parquet").exists()
        assert not (local / "part-2.parquet").exists()


class TestInterrupted:
    def test_ctrl_c_during_the_spark_write_cancels_its_job_group(self, tmp_path, local):
        # F-070 skeptic review: after an interrupt the JVM could keep writing into the
        # staging folder after it was removed. The write runs in a job group of its
        # own, which is cancelled before the folder is removed.
        spark = MagicMock()
        spark.sparkContext.getLocalProperty.return_value = None

        class Writer(_Writer):
            def save(self, folder):
                super().save(folder)
                raise KeyboardInterrupt

        df = MagicMock()
        df.write = Writer(["part-0.parquet"])
        with pytest.raises(KeyboardInterrupt):
            with _held(tmp_path):
                runs.writing("events", appends=True, exact=False)
                claimed_append.append(
                    df, spark, path="x", file_format="parquet", partition_by=[], opts={}
                )
        group = spark.sparkContext.setJobGroup.call_args.args[0]
        assert group.startswith("ubunye-append-")
        spark.sparkContext.cancelJobGroup.assert_called_once_with(group)
        assert not (local / "_ubunye-abc").exists()
        assert not list(local.rglob("part-*"))
        # The caller's job group is put back.
        restored = {c.args[0] for c in spark.sparkContext.setLocalProperty.call_args_list}
        assert "spark.jobGroup.id" in restored
