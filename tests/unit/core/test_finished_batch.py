"""A finished batch is never appended twice by accident (F-031).

E-01 caught it once in 20: a run finished dt=2, then a second full run of dt=2 appended
the batch again. The lease (ADR 008) cannot know, since it only lives while a run does.
A finished run now leaves a note; a later run of the batch is refused, and ``--rerun``
replaces the batch: the files the finished run claimed are removed once the new run
has succeeded, so a rerun that fails loses nothing.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from ubunye.core import runs
from ubunye.core.runs import BatchFinished, RunLease, RunLeaseHeld

TASK = "uc/pkg/t"
DT = {"dt": "2024-01-02", "mode": "DEV"}


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


PASS_THROUGH = (
    "from ubunye.core.interfaces import Task\n\n\n"
    "class T(Task):\n"
    "    def transform(self, sources):\n"
    "        return {'out': sources['src']}\n"
)


def _task(tmp_path: Path, mode_line: str = "      mode: append\n", fail: bool = False) -> Path:
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True, exist_ok=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
    code = PASS_THROUGH
    if fail:
        code = code.replace("return {", "raise RuntimeError('boom'); return {")
    (task / "transformations.py").write_text(code, encoding="utf-8")
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
        "      file_format: parquet\n" + mode_line,
        encoding="utf-8",
    )
    return task


def _rows(tmp_path: Path) -> int:
    import pandas as pd

    return len(pd.read_parquet(tmp_path / "out"))


def _parts(tmp_path: Path) -> set:
    return {p.name for p in (tmp_path / "out").rglob("*.parquet")}


@pytest.fixture
def engine():
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import ubunye

    return ubunye


class TestThroughTheEngine:
    def test_a_second_run_of_a_finished_batch_is_refused_and_writes_nothing(self, tmp_path, engine):
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        before = _parts(tmp_path)
        with pytest.raises(BatchFinished, match="would add the batch twice") as info:
            engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        assert "--rerun" in info.value.hint
        assert _rows(tmp_path) == 2 and _parts(tmp_path) == before
        # The refusal gave its lease back: nothing is left looking like a live run.
        leases = tmp_path / ".ubunye" / "leases"
        assert not [p for p in leases.rglob("*.json") if not p.name.endswith(".finished.json")]

    def test_rerun_replaces_the_batch_it_does_not_add_to_it(self, tmp_path, engine):
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        first = _parts(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02", rerun=True)
        assert _rows(tmp_path) == 2
        assert _parts(tmp_path).isdisjoint(first)  # the earlier run's files are gone
        # And a rerun of the rerun replaces the rerun, not the first run.
        second = _parts(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02", rerun=True)
        assert _rows(tmp_path) == 2 and _parts(tmp_path).isdisjoint(second)

    def test_a_rerun_leaves_other_batches_alone(self, tmp_path, engine):
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        other = _parts(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-03")
        engine.run_task(str(task), backend="pandas", dt="2024-01-03", rerun=True)
        assert _rows(tmp_path) == 4 and other <= _parts(tmp_path)

    def test_a_rerun_that_fails_keeps_the_finished_batch(self, tmp_path, engine):
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        first = _parts(tmp_path)
        _task(tmp_path, fail=True)
        with pytest.raises(Exception, match="boom"):
            engine.run_task(str(task), backend="pandas", dt="2024-01-02", rerun=True)
        assert _parts(tmp_path) == first and _rows(tmp_path) == 2
        # Still finished: a plain rerun is still refused.
        _task(tmp_path)
        with pytest.raises(BatchFinished):
            engine.run_task(str(task), backend="pandas", dt="2024-01-02")

    def test_a_run_with_no_batch_variables_appends_each_time(self, tmp_path, engine):
        # A snapshot job run with no dt and no --var: every run is "the same batch",
        # so it cannot be refused without breaking the job.
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas")
        engine.run_task(str(task), backend="pandas")
        assert _rows(tmp_path) == 4

    def test_an_overwrite_task_is_never_refused(self, tmp_path, engine):
        task = _task(tmp_path, "      mode: overwrite\n")
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        assert _rows(tmp_path) == 2

    def test_an_append_after_an_overwrite_of_the_batch_is_refused(self, tmp_path, engine):
        # The overwrite wrote the batch: appending it again would hold it twice.
        task = _task(tmp_path)
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        _task(tmp_path, "      mode: overwrite\n")
        engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        _task(tmp_path)
        with pytest.raises(BatchFinished) as info:
            engine.run_task(str(task), backend="pandas", dt="2024-01-02")
        assert "cannot be taken back" in info.value.hint  # --rerun would not replace it
        assert _rows(tmp_path) == 2

    @staticmethod
    def _pipeline_task(tmp_path, name, mode_line="      mode: append\n", fail=False):
        d = _task(tmp_path, mode_line, fail=fail)
        target = tmp_path / "uc" / "pkg" / name
        target.mkdir(parents=True, exist_ok=True)
        for f in ("transformations.py", "config.yaml"):
            text = (d / f).read_text("utf-8").replace('/out"', f'/{name}"')
            (target / f).write_text(text, encoding="utf-8")

    def test_a_pipeline_that_failed_half_way_resumes_when_asked(self, tmp_path, engine):
        import pandas as pd

        self._pipeline_task(tmp_path, "t1")
        self._pipeline_task(tmp_path, "t2", fail=True)
        run = dict(backend="pandas", dt="2024-01-02")
        with pytest.raises(Exception, match="boom"):
            engine.run_pipeline(str(tmp_path), "uc", "pkg", ["t1", "t2"], **run)
        self._pipeline_task(tmp_path, "t2")
        with pytest.raises(BatchFinished):  # not asked to resume: refused at t1
            engine.run_pipeline(str(tmp_path), "uc", "pkg", ["t1", "t2"], **run)
        done = engine.run_pipeline(str(tmp_path), "uc", "pkg", ["t1", "t2"], resume=True, **run)
        assert list(done) == ["t2"]  # t1 kept its batch, t2 ran
        assert len(pd.read_parquet(tmp_path / "t1")) == 2
        assert len(pd.read_parquet(tmp_path / "t2")) == 2

    def test_an_hourly_pipeline_is_refused_not_silently_skipped(self, tmp_path, engine):
        # Skeptic round 6: t1 appends, t2 overwrites, the same dt every hour. Skipping
        # t1 by default would drop its new rows and exit 0.
        self._pipeline_task(tmp_path, "t1")
        self._pipeline_task(tmp_path, "t2", "      mode: overwrite\n")
        run = dict(backend="pandas", dt="2024-01-02")
        engine.run_pipeline(str(tmp_path), "uc", "pkg", ["t1", "t2"], **run)
        with pytest.raises(BatchFinished):
            engine.run_pipeline(str(tmp_path), "uc", "pkg", ["t1", "t2"], **run)

    def test_the_cli_refuses_in_one_line_and_names_rerun(self, tmp_path, engine):
        _task(tmp_path)
        cmd = [sys.executable, "-c", "from ubunye.cli.main import app; app()"]
        cmd += ["run", "-d", str(tmp_path)]
        cmd += ["-u", "uc", "-p", "pkg", "-t", "t", "--backend", "pandas", "-dt", "2024-01-02"]
        env = {**os.environ, "PYTHONIOENCODING": "utf-8", "NO_COLOR": "1"}
        first = subprocess.run(cmd, capture_output=True, text=True, env=env)
        assert first.returncode == 0, first.stderr
        again = subprocess.run(cmd, capture_output=True, text=True, env=env)
        assert again.returncode == 1
        assert "Run refused for t" in again.stderr and "--rerun" in again.stderr
        assert "Traceback" not in again.stderr
        replaced = subprocess.run(cmd + ["--rerun"], capture_output=True, text=True, env=env)
        assert replaced.returncode == 0, replaced.stderr
        assert _rows(tmp_path) == 2


def _finished_note(root: Path, variables, outputs) -> Path:
    lease = RunLease(root, TASK, variables, "probe")
    path = lease._finished_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    note = {"run_id": "run-old", "finished_at": "2026-09-29T00:00:00Z", "outputs": outputs}
    path.write_text(json.dumps(note), encoding="utf-8")
    return path


class TestTheNote:
    def test_rerun_warns_when_an_append_cannot_be_taken_back(self, tmp_path, caplog):
        _finished_note(tmp_path, DT, {"events": {"claimed": []}})  # a Spark append
        with caplog.at_level("WARNING"):
            with runs.held(tmp_path, TASK, DT, "new", appends=["events"], rerun=True):
                pass
        assert "appends the batch to them again" in caplog.text

    def test_a_task_with_no_append_output_is_not_refused(self, tmp_path):
        _finished_note(tmp_path, DT, {"events": {"claimed": []}})
        with runs.held(tmp_path, TASK, DT, "new", appends=[]):
            pass

    def test_mode_and_dtf_alone_do_not_name_a_batch(self):
        assert not runs.names_a_batch({"mode": "DEV", "dtf": "%Y", "dt": None})
        assert runs.names_a_batch({"mode": "DEV", "dt": "2024-01-02"})
        assert runs.names_a_batch({"mode": "DEV", "region": "gauteng"})

    def test_a_replaced_file_in_use_keeps_the_lease_for_the_next_run(self, tmp_path, monkeypatch):
        out = tmp_path / "out"
        out.mkdir()
        old = out / "part-old.parquet"
        old.write_text("old")
        _finished_note(tmp_path, DT, {"o": {"claimed": [str(old)]}})
        real_remove = os.remove

        def locked(path):
            if os.path.basename(path) == old.name:
                raise PermissionError(32, "in use", path)
            return real_remove(path)

        monkeypatch.setattr(os, "remove", locked)
        with runs.held(tmp_path, TASK, DT, "run-a", appends=["o"], rerun=True) as lease:
            runs.writing("o", appends=True, exact=True)
            target = out / "part-a.parquet"
            runs.claim(str(target))
            target.write_text("a")
        assert old.exists() and lease.path.exists()  # kept, listing the file
        monkeypatch.setattr(os, "remove", real_remove)
        with runs.held(tmp_path, TASK, DT, "run-b", appends=["o"], rerun=True):
            pass  # takes run-a's kept lease over and finishes the replacement
        assert not old.exists()

    def test_a_note_that_cannot_be_written_keeps_the_lease_and_replaces_nothing(
        self, tmp_path, monkeypatch
    ):
        # Skeptic round 6: a reader held the note open during a --rerun. The run went
        # on to remove the old files, leaving a note naming them; the next --rerun
        # then removed nothing and appended: the batch twice.
        out = tmp_path / "out"
        out.mkdir()
        old = out / "part-old.parquet"
        old.write_text("old")
        note = _finished_note(tmp_path, DT, {"o": {"claimed": [str(old)]}})
        real_write = runs._write_atomic

        def note_locked(path, text):
            if path.name.endswith(".finished.json"):
                return False
            return real_write(path, text)

        monkeypatch.setattr(runs, "_write_atomic", note_locked)
        with runs.held(tmp_path, TASK, DT, "run-a", appends=["o"], rerun=True) as lease:
            runs.writing("o", appends=True, exact=True)
            new = out / "part-a.parquet"
            runs.claim(str(new))
            new.write_text("a")
            runs.written("o")
        assert old.exists() and new.exists() and lease.path.exists()
        with pytest.raises(RunLeaseHeld, match="note of its finished batch"):
            with runs.held(tmp_path, TASK, DT, "run-b", appends=["o"]):
                pass
        assert old.exists() and new.exists()
        monkeypatch.setattr(runs, "_write_atomic", real_write)
        with pytest.raises(BatchFinished, match="run-a"):  # noted, then replaced
            with runs.held(tmp_path, TASK, DT, "run-c", appends=["o"]):
                pass
        assert new.exists() and not old.exists()
        assert json.loads(note.read_text("utf-8"))["run_id"] == "run-a"


class TestALostLeaseReplacesNothing:
    def test_a_rerun_taken_over_at_commit_leaves_the_finished_batch(self, tmp_path, monkeypatch):
        # Skeptic round 5: B (--rerun) is judged dead just after its last ownership
        # check; C takes over, removes B's file, and is refused by A's note. B must
        # then not remove A's files, nor note itself as the batch's writer.
        monkeypatch.setattr(runs, "HEARTBEAT_EVERY", 3600)
        out = tmp_path / "out"
        out.mkdir()
        old = out / "part-A.parquet"
        old.write_text("A")
        note = _finished_note(tmp_path, DT, {"o": {"claimed": [str(old)]}})
        h = runs.held(tmp_path, TASK, DT, "run-B", appends=["o"], rerun=True)
        lease = h.__enter__()
        runs.writing("o", appends=True, exact=True)
        new = out / "part-B.parquet"
        runs.claim(str(new))
        new.write_text("B")
        runs.landed(str(new))
        runs.written("o")
        real = lease.still_owned

        def taken_over_now():
            ok = real()
            doc = json.loads(lease.path.read_text("utf-8"))
            doc["host"] = "elsewhere"
            lease.path.write_text(json.dumps(doc), encoding="utf-8")
            stale = lease._disk_now() - 10_000
            os.utime(lease.path, (stale, stale))
            with pytest.raises(BatchFinished):
                with runs.held(tmp_path, TASK, DT, "run-C", appends=["o"]):
                    pass
            return ok

        monkeypatch.setattr(lease, "still_owned", taken_over_now)
        with pytest.raises(runs.RunLeaseLost):
            h.__exit__(None, None, None)
        assert old.exists() and not new.exists()
        assert json.loads(note.read_text("utf-8"))["run_id"] == "run-old"


def _dead_pid() -> int:
    done = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True
    )
    return int(done.stdout.strip())


class TestACrashInTheMiddle:
    def _dead_committing_run(self, root: Path, new: Path, old: Path) -> RunLease:
        """A --rerun that died after it marked itself done but before it wrote the note
        or removed the files of the run it replaced."""
        lease = RunLease(root, TASK, DT, "run-dead")
        lease.path.parent.mkdir(parents=True, exist_ok=True)
        lease._done_mark("run-dead").write_text("x", encoding="utf-8")
        doc = {
            "run_id": "run-dead",
            "pid": _dead_pid(),
            "host": runs._host(),
            "heartbeat": 0,
            "outputs": {
                "o": {"appends": True, "exact": True, "state": "done", "claimed": [str(new)]}
            },
            "replaces": {"o": [str(old)]},
        }
        lease.path.write_text(json.dumps(doc), encoding="utf-8")
        return lease

    def test_the_next_run_finishes_the_replacement_and_keeps_the_new_batch(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        old, new = out / "part-old.parquet", out / "part-new.parquet"
        old.write_text("old")
        new.write_text("new")
        _finished_note(tmp_path, DT, {"o": {"claimed": [str(old)]}})
        dead = self._dead_committing_run(tmp_path, new, old)
        with pytest.raises(BatchFinished, match="run-dead"):
            with runs.held(tmp_path, TASK, DT, "next", appends=["o"]):
                pass
        assert new.exists() and not old.exists()
        note = json.loads(dead._finished_path().read_text("utf-8"))
        assert note["run_id"] == "run-dead" and note["outputs"]["o"]["claimed"]

    def test_a_run_that_died_before_it_finished_keeps_the_replaced_batch(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        old, new = out / "part-old.parquet", out / "part-new.parquet"
        old.write_text("old")
        new.write_text("new")
        _finished_note(tmp_path, DT, {"o": {"claimed": [str(old)]}})
        dead = self._dead_committing_run(tmp_path, new, old)
        dead._done_mark("run-dead").unlink()  # it never got as far as done
        with pytest.raises(BatchFinished, match="run-old"):
            with runs.held(tmp_path, TASK, DT, "next", appends=["o"]):
                pass
        assert old.exists() and not new.exists()


def test_a_refusal_is_a_lease_refusal():
    # The CLI already turns RunLeaseHeld into one line and exit 1.
    assert issubclass(BatchFinished, RunLeaseHeld)
