"""Rerun safety (ubunye.core.runs): one live run per task and batch, clean reruns.

Experiments E-01 and E-02 found: a run killed after appending was appended again by the
rerun (16 of 16), two runs of one date at once both appended (7 of 10), the loser of a
first-write race got a raw OS error, and a killed run's record said "running" for ever
(findings F-011, F-019, F-020, F-013). Two adversarial reviews then showed that working
out a run's files by listing a folder deletes other runs' data; only files a backend
claims before they land are ever removed. These pin all of it (ADR 008).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

from ubunye.core import runs
from ubunye.core.runs import RunLease, RunLeaseHeld, RunLeaseLost

TASK = "uc/pkg/t"


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


def _dead_pid() -> int:
    done = subprocess.run(
        [sys.executable, "-c", "import os; print(os.getpid())"], capture_output=True, text=True
    )
    return int(done.stdout.strip())


def _leave_dead_lease(root: Path, variables, *, run_id="dead-run", outputs=None) -> Path:
    lease = RunLease(root, TASK, variables, run_id)
    lease.path.parent.mkdir(parents=True, exist_ok=True)
    lease.path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "pid": _dead_pid(),
                "host": runs._host(),  # with the pid namespace on Linux, as the engine writes it
                "started_at": "2026-09-29T00:00:00Z",
                "heartbeat": 0,
                "outputs": outputs or {},
            }
        ),
        encoding="utf-8",
    )
    return lease.path


def _record(root: Path, run_id="dead-run") -> Path:
    record = root / ".ubunye" / "lineage" / TASK / f"{run_id}.json"
    record.parent.mkdir(parents=True, exist_ok=True)
    record.write_text(json.dumps({"run_id": run_id, "status": "running"}), encoding="utf-8")
    return record


def _append(folder: Path, name: str) -> Path:
    """What the pandas backend does: claim a part file, then move it in."""
    target = folder / name
    runs.claim(str(target))
    target.write_text(name)
    return target


class TestOneLiveRunPerBatch:
    def test_a_second_run_of_the_same_batch_is_refused_naming_the_first(self, tmp_path):
        first = RunLease(tmp_path, TASK, {"dt": "2024-01-02"}, "run-one-1234").acquire()
        try:
            with pytest.raises(RunLeaseHeld, match="run-one-") as info:
                RunLease(tmp_path, TASK, {"dt": "2024-01-02"}, "run-two").acquire()
            assert "still running" in str(info.value)
            assert "listed under 'claimed'" in str(info.value)
        finally:
            first.release()

    def test_different_dates_do_not_block_each_other(self, tmp_path):
        a = RunLease(tmp_path, TASK, {"dt": "2024-01-02"}, "a").acquire()
        b = RunLease(tmp_path, TASK, {"dt": "2024-01-03"}, "b").acquire()
        a.release()
        b.release()

    def test_the_lease_is_gone_after_release(self, tmp_path):
        lease = RunLease(tmp_path, TASK, {}, "a").acquire()
        lease.release()
        assert not lease.path.exists()
        RunLease(tmp_path, TASK, {}, "b").acquire().release()


class TestADeadRun:
    def test_its_claimed_appends_are_taken_back_and_nothing_else(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-old.parquet").write_text("x")
        (out / "part-dead-run.parquet").write_text("y")  # claimed, landed, then it died
        (out / "part-another-run.parquet").write_text("z")  # not claimed by it
        note = {
            "appends": True,
            "exact": True,
            "state": "done",
            "claimed": [str(out / "part-dead-run.parquet")],
        }
        _leave_dead_lease(tmp_path, {}, outputs={"events": note})
        lease = RunLease(tmp_path, TASK, {}, "new-run").acquire()
        try:
            names = sorted(p.name for p in out.iterdir())
            assert names == ["part-another-run.parquet", "part-old.parquet"]
            assert lease.recovered["run_id"] == "dead-run"
            assert lease.recovered["unrepaired"] == []
        finally:
            lease.release()

    def test_a_claim_that_never_landed_removes_nothing(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-old.parquet").write_text("x")
        note = {"appends": True, "exact": True, "state": "writing", "claimed": [str(out / "p")]}
        _leave_dead_lease(tmp_path, {}, outputs={"events": note})
        RunLease(tmp_path, TASK, {}, "new").acquire().release()
        assert [p.name for p in out.iterdir()] == ["part-old.parquet"]

    def test_its_running_record_is_marked_interrupted(self, tmp_path):
        record = _record(tmp_path)
        _leave_dead_lease(tmp_path, {})
        RunLease(tmp_path, TASK, {}, "new-run-5678").acquire().release()
        doc = json.loads(record.read_text(encoding="utf-8"))
        assert doc["status"] == "interrupted" and "new-run-" in doc["error"]

    def test_an_append_it_could_not_claim_is_named_never_deleted(self, tmp_path):
        # A Spark append: the backend cannot claim its files, so none are removed.
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-00000-spark.parquet").write_text("s")
        record = _record(tmp_path)
        note = {"appends": True, "exact": False, "state": "done", "claimed": []}
        _leave_dead_lease(tmp_path, {}, outputs={"events": note, "snap": {"appends": False}})
        lease = RunLease(tmp_path, TASK, {}, "new").acquire()
        lease.release()
        assert [p.name for p in out.iterdir()] == ["part-00000-spark.parquet"]
        assert lease.recovered["unrepaired"] == ["events"]
        error = json.loads(record.read_text(encoding="utf-8"))["error"]
        assert "events may hold part or all" in error

    def test_the_record_under_a_custom_lineage_dir_is_marked(self, tmp_path):
        custom = tmp_path / "elsewhere"
        record = custom / TASK / "dead-run.json"
        record.parent.mkdir(parents=True)
        record.write_text(json.dumps({"run_id": "dead-run", "status": "running"}), "utf-8")
        _leave_dead_lease(tmp_path, {})
        RunLease(tmp_path, TASK, {}, "new", lineage_dir=custom).acquire().release()
        assert json.loads(record.read_text(encoding="utf-8"))["status"] == "interrupted"


class TestAnotherHost:
    @staticmethod
    def _as_other_host(path: Path, age: float, heartbeat: float) -> None:
        doc = json.loads(path.read_text(encoding="utf-8"))
        doc.update(host="another-host", pid=os.getpid(), heartbeat=heartbeat)
        path.write_text(json.dumps(doc), encoding="utf-8")
        then = time.time() - age
        os.utime(path, (then, then))

    def test_a_lease_untouched_too_long_counts_as_dead(self, tmp_path):
        path = _leave_dead_lease(tmp_path, {})
        self._as_other_host(path, runs.HEARTBEAT_TIMEOUT + 60, heartbeat=time.time())
        RunLease(tmp_path, TASK, {}, "new").acquire().release()

    def test_a_recently_touched_lease_is_respected_whatever_its_clock_says(self, tmp_path):
        # Judged by the disk's time, not the heartbeat the other host wrote: a skewed
        # clock there cannot make a live run look dead.
        path = _leave_dead_lease(tmp_path, {})
        self._as_other_host(path, 5, heartbeat=0)
        with pytest.raises(RunLeaseHeld):
            RunLease(tmp_path, TASK, {}, "new").acquire()


class TestAFailedRun:
    def test_takes_back_its_claimed_appends_only(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-old.parquet").write_text("x")
        with pytest.raises(RuntimeError):
            with runs.held(tmp_path, TASK, {}, "run-x"):
                runs.writing("events", appends=True, exact=True)
                _append(out, "part-mine.parquet")
                (out / "part-other-run.parquet").write_text("o")  # lands meanwhile
                runs.written("events")
                raise RuntimeError("a later output failed")
        names = sorted(p.name for p in out.iterdir())
        assert names == ["part-old.parquet", "part-other-run.parquet"]
        assert not RunLease(tmp_path, TASK, {}, "any").path.exists()

    def test_a_successful_run_keeps_them(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        with runs.held(tmp_path, TASK, {}, "run-y"):
            runs.writing("events", appends=True, exact=True)
            _append(out, "part-new.parquet")
            runs.written("events")
        assert [p.name for p in out.iterdir()] == ["part-new.parquet"]

    def test_a_failing_take_back_never_hides_the_runs_own_error(self, tmp_path, monkeypatch):
        def broken(self):
            raise OSError("disk full")

        monkeypatch.setattr(RunLease, "rollback", broken)
        with pytest.raises(RuntimeError, match="the real error"):
            with runs.held(tmp_path, TASK, {}, "run-z"):
                raise RuntimeError("the real error")
        assert RunLease(tmp_path, TASK, {}, "any").path.exists()  # kept: take-back unfinished


class TestALostLease:
    def test_a_run_whose_lease_was_taken_stops_before_moving_a_file_in(self, tmp_path):
        mine = RunLease(tmp_path, TASK, {}, "mine").acquire()
        try:
            other = json.loads(mine.path.read_text(encoding="utf-8"))
            other["run_id"] = "someone-else"
            mine.path.write_text(json.dumps(other), encoding="utf-8")  # taken over
            with pytest.raises(RunLeaseLost):
                mine.writing("events", appends=True, exact=True)
            mine._current_output = "events"
            mine._doc["outputs"]["events"] = {"appends": True, "exact": True, "claimed": []}
            with pytest.raises(RunLeaseLost):
                mine.claim(str(tmp_path / "part-x.parquet"))
            assert json.loads(mine.path.read_text(encoding="utf-8"))["run_id"] == "someone-else"
        finally:
            mine.release()
        assert mine.path.exists()  # it was not ours to remove

    def test_a_lease_is_only_ever_created_never_overwritten(self, tmp_path):
        # The put-back after a mistaken takeover uses this: it can never replace a
        # lease another run created meanwhile.
        lease = RunLease(tmp_path, TASK, {}, "new")
        lease.path.parent.mkdir(parents=True)
        assert lease._create('{"run_id": "first"}')
        assert not lease._create('{"run_id": "second"}')
        assert json.loads(lease.path.read_text(encoding="utf-8"))["run_id"] == "first"


class TestProcesses:
    def test_checking_a_process_never_signals_it(self):
        # On Windows os.kill(pid, 0) terminates the process; the check must not.
        assert runs._pid_alive(os.getpid()) is True
        assert runs._pid_alive(os.getpid()) is True  # still here
        assert runs._pid_alive(_dead_pid()) is False

    def test_a_reused_pid_is_not_the_run_that_held_it(self):
        start = runs._process_start(os.getpid())
        if start is None:
            pytest.skip("process start time not readable on this platform")
        assert runs._pid_alive(os.getpid(), start) is True
        assert runs._pid_alive(os.getpid(), "not-" + start) is False


def test_it_can_be_turned_off(tmp_path, monkeypatch):
    monkeypatch.setenv("UBUNYE_RUN_LEASE", "off")
    with runs.held(tmp_path, TASK, {}, "a") as lease:
        assert lease is None
        with runs.held(tmp_path, TASK, {}, "b") as other:
            assert other is None


PASS_THROUGH = (
    "from ubunye.core.interfaces import Task\n\n\n"
    "class T(Task):\n"
    "    def transform(self, sources):\n"
    "        return {'out': sources['src'], 'also': sources['src']}\n"
)


def _task(tmp_path: Path, mode_line: str, also: str = "") -> Path:
    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n", encoding="utf-8")
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
        "      file_format: parquet\n" + mode_line + also,
        encoding="utf-8",
    )
    return task


def test_a_run_through_the_engine_holds_and_releases_the_lease(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import pandas as pd

    import ubunye

    task = _task(tmp_path, "      mode: append\n")
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    ubunye.run_task(str(task), backend="pandas", dt="2024-01-03")
    leases = (tmp_path / ".ubunye" / "leases").rglob("*.json")
    # Only the notes saying which run finished each batch stay (F-031).
    assert [p for p in leases if not p.name.endswith(".finished.json")] == []
    assert len(pd.read_parquet(tmp_path / "out")) == 4  # two appends, each once


@pytest.mark.parametrize("mode_line", ["      mode: append\n", ""], ids=["append", "no mode"])
def test_the_engine_takes_back_an_append_when_a_later_output_fails(tmp_path, mode_line):
    # The first append creates the folder (another branch of the writer): it is claimed
    # too. A later output that cannot be written fails the run.
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import ubunye

    also = (
        "    also:\n"
        "      format: s3\n"
        f'      path: "{(tmp_path / "in.csv").as_posix()}"\n'  # a file: append refused
        "      file_format: parquet\n"
        "      mode: append\n"
    )
    task = _task(tmp_path, mode_line, also)
    with pytest.raises(Exception):
        ubunye.run_task(str(task), backend="pandas", dt="2024-01-02")
    assert not [p for p in (tmp_path / "out").rglob("*.parquet")]


# --- third review: claims are never forgotten, and a lost lease is never a success ----


class TestNeverForgetAClaim:
    def test_a_file_in_use_keeps_the_lease_so_the_next_run_takes_it_back(
        self, tmp_path, monkeypatch
    ):
        out = tmp_path / "out"
        out.mkdir()
        real_remove = os.remove

        def locked(path):
            raise PermissionError(32, "in use", path)

        with pytest.raises(RuntimeError):
            with runs.held(tmp_path, TASK, {}, "run-a"):
                runs.writing("events", appends=True, exact=True)
                _append(out, "part-a.parquet")
                monkeypatch.setattr(os, "remove", locked)
                raise RuntimeError("boom")
        monkeypatch.setattr(os, "remove", real_remove)
        lease_file = RunLease(tmp_path, TASK, {}, "x").path
        claims = json.loads(lease_file.read_text("utf-8"))["outputs"]["events"]["claimed"]
        assert claims  # not forgotten
        doc = json.loads(lease_file.read_text("utf-8"))
        doc["pid"] = _dead_pid()  # the failed process has ended
        lease_file.write_text(json.dumps(doc), encoding="utf-8")
        RunLease(tmp_path, TASK, {}, "run-b").acquire().release()
        assert not (out / "part-a.parquet").exists()

    def test_a_takeover_that_cannot_remove_a_file_refuses_and_keeps_the_claim(
        self, tmp_path, monkeypatch
    ):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-dead.parquet").write_text("d")
        note = {"appends": True, "exact": True, "claimed": [str(out / "part-dead.parquet")]}
        path = _leave_dead_lease(tmp_path, {}, outputs={"events": note})

        def locked(p):
            raise PermissionError(32, "in use", p)

        monkeypatch.setattr(os, "remove", locked)
        with pytest.raises(RunLeaseHeld, match="cannot be removed yet"):
            RunLease(tmp_path, TASK, {}, "new").acquire()
        kept = json.loads(path.read_text("utf-8"))["outputs"]["events"]["claimed"]
        assert kept == [str(out / "part-dead.parquet")]

    def test_claims_are_kept_relative_so_another_mount_finds_them(self, tmp_path):
        lease = RunLease(tmp_path / "usecase", TASK, {}, "r")
        claim = lease._portable(str(tmp_path / "usecase" / "data" / "part-1.parquet"))
        assert not os.path.isabs(claim)
        moved = RunLease(tmp_path / "mounted-elsewhere", TASK, {}, "r")
        assert moved._resolve(claim) == str(
            tmp_path / "mounted-elsewhere" / "data" / "part-1.parquet"
        )

    def test_taking_back_a_first_append_drops_the_success_marker(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "_SUCCESS").write_text("")
        with pytest.raises(RuntimeError):
            with runs.held(tmp_path, TASK, {}, "r"):
                runs.writing("events", appends=True, exact=True)
                _append(out, "part-1.parquet")
                raise RuntimeError("boom")
        assert list(out.iterdir()) == []

    def test_an_exact_output_that_claimed_nothing_is_named(self, tmp_path):
        # A custom writer on the pandas backend that does not go through the claiming
        # write: its append cannot be taken back, so it is named.
        note = {"appends": True, "exact": True, "state": "done", "claimed": []}
        _leave_dead_lease(tmp_path, {}, outputs={"custom": note})
        lease = RunLease(tmp_path, TASK, {}, "new").acquire()
        lease.release()
        assert lease.recovered["unrepaired"] == ["custom"]


class TestALostLeaseIsNotASuccess:
    def test_the_run_fails_when_its_lease_was_taken_before_it_ended(self, tmp_path):
        with pytest.raises(RunLeaseLost):
            with runs.held(tmp_path, TASK, {}, "mine") as lease:
                doc = json.loads(lease.path.read_text("utf-8"))
                doc["run_id"] = "taker"
                lease.path.write_text(json.dumps(doc), encoding="utf-8")
                assert runs.lost() and "lost its lease" in runs.lost()

    def test_the_heartbeat_survives_a_missing_lease_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr(runs, "HEARTBEAT_EVERY", 0.05)
        lease = RunLease(tmp_path, TASK, {}, "mine").acquire()
        try:
            # Read and remove through the engine's own patient helpers: on Windows a
            # plain read that meets the heartbeat's replace is refused (F-048).
            text = runs._patient(lease.path.read_text, encoding="utf-8")
            runs._patient(lease.path.unlink)  # a takeover renamed it away for a moment
            time.sleep(0.2)
            lease._create(text)  # and put it back
            before = lease._read()["heartbeat"]
            time.sleep(0.3)
            assert lease._beat.is_alive()
            assert lease._read()["heartbeat"] > before
        finally:
            lease.release()

    def test_a_reader_never_meets_the_heartbeat_mid_replace(self, tmp_path, monkeypatch):
        # F-048: another run reads the lease in a tight loop while its heartbeat
        # replaces it every millisecond. On Windows the old reader called the lease
        # unreadable about 8 times a second; a second run then refused with "Run ?".
        monkeypatch.setattr(runs, "HEARTBEAT_EVERY", 0.001)
        lease = RunLease(tmp_path, TASK, {}, "mine").acquire()
        other = RunLease(tmp_path, TASK, {}, "other")
        seen: dict = {}
        try:
            stop = time.monotonic() + 1.5
            while time.monotonic() < stop:
                held = other._read()
                key = "missing" if held is None else held.get("run_id", "unreadable")
                seen[key] = seen.get(key, 0) + 1
                assert other._dead(held) is False
            assert lease._beat.is_alive()
        finally:
            lease.release()
        assert set(seen) == {"mine"}, seen


class TestPatient:
    """The one helper every lease read, stat, rename and removal goes through (F-048)."""

    def test_a_brief_permission_error_is_retried(self, monkeypatch):
        monkeypatch.setattr(runs, "_BUSY_PAUSE", 0)
        calls = []

        def busy_twice():
            calls.append(1)
            if len(calls) < 3:
                raise PermissionError("a replace is under way")
            return "read"

        assert runs._patient(busy_twice) == "read"
        assert len(calls) == 3

    def test_a_lasting_permission_error_is_raised(self, monkeypatch):
        monkeypatch.setattr(runs, "BUSY_FOR", 0.05)

        def denied():
            raise PermissionError("denied for good")

        started = time.monotonic()
        with pytest.raises(PermissionError):
            runs._patient(denied)
        assert time.monotonic() - started < 1

    def test_a_missing_lease_is_missing_at_once(self, tmp_path):
        calls = []

        def gone():
            calls.append(1)
            return (tmp_path / "none.json").read_text()

        with pytest.raises(FileNotFoundError):
            runs._patient(gone)
        assert calls == [1]
        assert RunLease(tmp_path, TASK, {}, "r")._read() is None

    def test_a_lease_busy_for_a_moment_is_read_not_called_unreadable(self, tmp_path, monkeypatch):
        lease = RunLease(tmp_path, TASK, {}, "mine").acquire()
        lease.release()
        lease.path.parent.mkdir(parents=True, exist_ok=True)
        lease.path.write_text(json.dumps({"run_id": "mine"}), encoding="utf-8")
        real = Path.read_text
        refused = [2]

        def read_text(self, *a, **k):
            if self == lease.path and refused[0]:
                refused[0] -= 1
                raise PermissionError(13, "Permission denied", str(self))
            return real(self, *a, **k)

        monkeypatch.setattr(Path, "read_text", read_text)
        assert lease._read() == {"run_id": "mine"}


class TestJudgedDeadButAlive:
    def test_a_run_that_writes_its_lease_back_is_left_alone(self, tmp_path, monkeypatch):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-a.parquet").write_text("a")
        note = {"appends": True, "exact": True, "claimed": [str(out / "part-a.parquet")]}
        path = _leave_dead_lease(tmp_path, {}, run_id="alive", outputs={"events": note})
        alive_text = path.read_text(encoding="utf-8")

        def settle(_):
            path.write_text(alive_text, encoding="utf-8")  # its save lands meanwhile

        monkeypatch.setattr(runs.time, "sleep", settle)
        with pytest.raises(RunLeaseHeld, match="wrote its lease again"):
            RunLease(tmp_path, TASK, {}, "taker").acquire()
        assert (out / "part-a.parquet").exists()  # nothing taken back

    def test_a_dead_lease_left_beside_the_lease_is_adopted(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-orphan.parquet").write_text("o")
        lease = RunLease(tmp_path, TASK, {}, "new")
        lease.path.parent.mkdir(parents=True)
        orphan = lease.path.with_name(f"{lease.path.stem}.dead-1234abcd.json")
        note = {"appends": True, "exact": True, "claimed": [str(out / "part-orphan.parquet")]}
        orphan.write_text(json.dumps({"run_id": "dead", "outputs": {"e": note}}), "utf-8")
        lease.acquire().release()
        assert not (out / "part-orphan.parquet").exists()
        assert not orphan.exists()


def test_a_run_frozen_mid_save_learns_it_was_taken_over(tmp_path, monkeypatch):
    # Review round 3, p5: A is judged dead while frozen between its ownership check and
    # its write; B takes over and takes back A's claims; then A's write lands. A must
    # not carry on as the owner, and must not record success.
    out = tmp_path / "out"
    out.mkdir()
    a = RunLease(tmp_path, TASK, {"dt": "1"}, "run-A").acquire()
    a._stop.set()
    a._beat.join()
    a.writing("o1", appends=True, exact=True)
    part = out / "part-A1.parquet"
    a.claim(str(part))
    part.write_text("A1")
    b = RunLease(tmp_path, TASK, {"dt": "1"}, "run-B")
    monkeypatch.setattr(b, "_dead", lambda held: True)
    original = runs._write_atomic

    def frozen_then_lands(path, text):
        if "run-A" in text:
            monkeypatch.setattr(runs, "_write_atomic", original)
            b.acquire()  # the takeover happens while A is frozen here
        return original(path, text)

    monkeypatch.setattr(runs, "_write_atomic", frozen_then_lands)
    with pytest.raises(RunLeaseLost):
        a.writing("o2", appends=True, exact=True)
    assert not part.exists()  # B took back A's part: A must not report success
    assert a.still_owned() is False


class TestRoundFour:
    def test_a_successful_run_whose_lease_could_not_be_removed_keeps_its_data(
        self, tmp_path, monkeypatch
    ):
        # q1: a reader held the lease open while the run finished, so neither the
        # commit save nor the unlink landed. The done marker still says it finished.
        out = tmp_path / "out"
        out.mkdir()
        monkeypatch.setattr(runs, "_write_atomic", lambda path, text: False)
        real_unlink = Path.unlink

        def blocked(self, missing_ok=False):
            if self.suffix == ".json" and ".ubunye" in str(self):
                raise PermissionError(32, "in use")
            return real_unlink(self, missing_ok=missing_ok)

        with runs.held(tmp_path, TASK, {}, "run-A") as lease:
            lease._doc["outputs"]["events"] = {
                "appends": True,
                "exact": True,
                "claimed": [str(out / "part-A.parquet")],
            }
            real = lease.path.read_text("utf-8")
            doc = json.loads(real)
            doc["outputs"] = lease._doc["outputs"]
            lease.path.write_text(json.dumps(doc), encoding="utf-8")
            (out / "part-A.parquet").write_text("A")
            monkeypatch.setattr(Path, "unlink", blocked)
        monkeypatch.setattr(Path, "unlink", real_unlink)
        monkeypatch.undo()
        doc = json.loads(lease.path.read_text("utf-8"))
        doc["pid"] = _dead_pid()  # the process has ended
        lease.path.write_text(json.dumps(doc), encoding="utf-8")
        taker = RunLease(tmp_path, TASK, {}, "run-B").acquire()
        taker.release()
        assert (out / "part-A.parquet").exists()
        assert taker.recovered["removed"] == []

    def test_a_dead_lease_whose_record_says_success_is_not_undone(self, tmp_path):
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-A.parquet").write_text("A")
        record = _record(tmp_path, "run-A")
        record.write_text(json.dumps({"run_id": "run-A", "status": "success"}), "utf-8")
        note = {"appends": True, "exact": True, "claimed": [str(out / "part-A.parquet")]}
        _leave_dead_lease(tmp_path, {}, run_id="run-A", outputs={"events": note})
        RunLease(tmp_path, TASK, {}, "run-B").acquire().release()
        assert (out / "part-A.parquet").exists()

    def test_a_file_that_lands_after_a_takeover_is_taken_straight_back(self, tmp_path):
        # q4: D claimed, froze before the move, was taken over; then the move lands.
        out = tmp_path / "out"
        out.mkdir()
        d = RunLease(tmp_path, TASK, {}, "run-D").acquire()
        d._stop.set()
        d._beat.join()
        d.writing("o1", appends=True, exact=True)
        x = out / "part-D.parquet"
        d.claim(str(x))
        b = RunLease(tmp_path, TASK, {}, "run-B")
        b._dead = lambda held: True
        b.acquire()
        x.write_text("D")  # D wakes: the move lands
        with pytest.raises(RunLeaseLost):
            d.landed(str(x))
        assert not x.exists()
        b.release()

    def test_adopting_a_dead_lease_whose_file_is_in_use_refuses(self, tmp_path, monkeypatch):
        # q2
        out = tmp_path / "out"
        out.mkdir()
        (out / "part-D.parquet").write_text("D")
        lease = RunLease(tmp_path, TASK, {}, "run-B")
        lease.path.parent.mkdir(parents=True)
        orphan = lease.path.with_name(f"{lease.path.stem}.dead-0000aaaa.json")
        note = {"appends": True, "exact": True, "claimed": [str(out / "part-D.parquet")]}
        orphan.write_text(json.dumps({"run_id": "run-D", "outputs": {"o": note}}), "utf-8")

        def locked(p):
            raise PermissionError(32, "in use", p)

        monkeypatch.setattr(os, "remove", locked)
        with pytest.raises(RunLeaseHeld, match="cannot be removed"):
            lease.acquire()
        assert orphan.exists() and not lease.path.exists()

    def test_a_kept_lease_of_this_process_does_not_block_its_next_run(self, tmp_path):
        # A notebook kernel: the failed run's lease is kept (a file was in use); the
        # next run in the same process takes it over instead of waiting for ever.
        _leave_dead_lease(tmp_path, {})
        path = RunLease(tmp_path, TASK, {}, "x").path
        doc = json.loads(path.read_text("utf-8"))
        doc.update(pid=os.getpid(), kept=True, host=runs._host())
        path.write_text(json.dumps(doc), encoding="utf-8")
        RunLease(tmp_path, TASK, {}, "next").acquire().release()
