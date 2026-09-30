"""The parallel hash (F-038) must give the same digest as one process, byte for byte.

A large table is hashed by helper processes, one per core. Each hashes a run of
rows with the same code and sends back two lane sums. These tests shrink the
thresholds so small tables go through real helpers, and hold every digest to the
one process path (the code before F-038, unchanged) and to the row at a time
reference.
"""

from __future__ import annotations

import datetime as dt
import gc
import hashlib
import logging
import os
import subprocess
import sys
import threading
import time

import pytest

pa = pytest.importorskip("pyarrow")
np = pytest.importorskip("numpy")
hypothesis = pytest.importorskip("hypothesis")
from hypothesis import HealthCheck, given, settings  # noqa: E402

from ubunye.lineage import content_hash as ch  # noqa: E402
from ubunye.lineage.content_hash import fingerprint_arrow  # noqa: E402

from .test_content_hash_fast_path import _reference, tables  # noqa: E402


def _serial(table, monkeypatch):
    before = os.environ.get(ch.HASH_WORKERS_ENV)
    os.environ[ch.HASH_WORKERS_ENV] = "1"
    try:
        return fingerprint_arrow(table)
    finally:
        if before is None:
            del os.environ[ch.HASH_WORKERS_ENV]
        else:
            os.environ[ch.HASH_WORKERS_ENV] = before


#: The bytes of content_hash.py as this test run imported them. A helper loads the
#: file by its path and refuses to hash when the file's digest is not the caller's
#: (exit code 5). The file on disk can change while the tests run (another agent
#: edits the same tree), which made helpers refuse mid run and five tests fail. The
#: helpers here are handed a copy of these bytes, so they run the imported code.
_IMPORTED_SOURCE = open(ch.__file__, "rb").read()
_SNAPSHOT_MATCHES = hashlib.sha256(_IMPORTED_SOURCE).hexdigest() == ch._SOURCE_DIGEST


@pytest.fixture(scope="module")
def helper_file(tmp_path_factory):
    """A copy of the imported content_hash.py, for the helpers to load."""
    if not _SNAPSHOT_MATCHES:
        pytest.fail(
            "content_hash.py changed on disk between its import and this test module's "
            "import; the imported code cannot be recovered, run the tests again"
        )
    path = tmp_path_factory.mktemp("f038") / "content_hash.py"
    path.write_bytes(_IMPORTED_SOURCE)
    return str(path)


class _Seen(list):
    """Every parallel result (None is a fallback), and every logged reason."""

    reasons: list

    def why(self):
        return "; ".join(self.reasons) or "nothing logged"


@pytest.fixture
def parallel(monkeypatch, helper_file):
    """Helpers for any table of 2 rows or more, and a record of every parallel result.

    Nothing here depends on the machine: the thresholds, the core count and the
    setting are fixed, the budget must be free, and the helpers run a copy of the
    imported code, never the file on disk.
    """
    assert ch._helpers_running == 0, "an earlier hash kept helper slots"
    monkeypatch.setattr(ch, "_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(ch, "_ROWS_PER_WORKER", 1)
    monkeypatch.setattr(ch, "_cores", lambda: 8)
    monkeypatch.setattr(ch, "__file__", helper_file)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "3")
    seen = _Seen()
    seen.reasons = []
    real = ch._parallel_lanes

    def spy(*args, **kwargs):
        out = real(*args, **kwargs)
        seen.append(out)
        return out

    class Reasons(logging.Handler):
        def emit(self, record):
            seen.reasons.append(record.getMessage())

    handler = Reasons(logging.DEBUG)
    level = ch._log.level
    ch._log.addHandler(handler)
    ch._log.setLevel(logging.DEBUG)
    monkeypatch.setattr(ch, "_parallel_lanes", spy)
    try:
        yield seen
    finally:
        ch._log.removeHandler(handler)
        ch._log.setLevel(level)
    assert ch._helpers_running == 0, "this test's hash kept helper slots"


@settings(
    max_examples=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(tables())
def test_any_table_hashes_the_same_in_helpers(parallel, monkeypatch, table):
    got = fingerprint_arrow(table)
    extension = not all(ch._plain_type(f.type) for f in table.schema)
    if not extension:
        assert parallel and parallel[-1] is not None, f"the helpers did not run: {parallel.why()}"
    assert got == _serial(table, monkeypatch)
    assert got == _reference(table)


def test_the_e06_shape_hashes_the_same_in_helpers(parallel, monkeypatch):
    rng = np.random.default_rng(606)
    n = 200_000
    cats = pa.array([f"cat_{i:02d}" for i in range(20)])
    table = pa.table(
        {
            "id": np.arange(n, dtype=np.int64),
            "region": rng.integers(0, 1000, n, dtype=np.int64),
            "cat": cats.take(pa.array(rng.integers(0, 20, n))),
            "amount": rng.integers(1, 100_000, n, dtype=np.int64),
            "qty": rng.integers(-2, 20, n, dtype=np.int64),
            "big": rng.integers(0, 2, n).astype(bool),
            "price": rng.normal(0, 1e6, n),
            "ts": pa.array(rng.integers(0, 2**40, n), pa.timestamp("us", tz="UTC")),
        }
    )
    got = fingerprint_arrow(table)
    assert parallel == [parallel[0]] and parallel[0] is not None, parallel.why()
    assert got == _serial(table, monkeypatch)


def test_golden_digest_does_not_move_in_helpers(parallel):
    table = pa.table(
        {
            "i": pa.array([1, None, 3, 4], pa.int64()),
            "s": pa.array(["a", 'q"uote', None, "é"]),
            "d": pa.array([dt.date(2024, 1, 31), None, dt.date(1, 1, 1), dt.date(1970, 1, 1)]),
        }
    )
    reference = _reference(table)
    assert fingerprint_arrow(table) == reference
    assert parallel and parallel[-1] is not None, parallel.why()


def test_a_failed_helper_falls_back_to_one_process(parallel, monkeypatch):
    table = pa.table({"x": list(range(10)), "s": [str(i) for i in range(10)]})
    monkeypatch.setattr(ch.sys, "executable", ch.sys.executable + "-missing")
    assert fingerprint_arrow(table) == _reference(table)
    assert parallel == [None]


def test_a_helper_that_exits_non_zero_falls_back(parallel, monkeypatch):
    table = pa.table({"x": list(range(10))})
    monkeypatch.setattr(ch, "_WORKER_BOOT", "import sys\nsys.exit(4)\n")
    assert fingerprint_arrow(table) == _reference(table)
    assert parallel == [None]


def test_invalid_utf8_still_records_the_error(parallel):
    raw = pa.array([b"ok", b"\xff\xfe", b"fine"], pa.binary())
    table = pa.table({"s": raw.cast(pa.string(), safe=False)})
    got = ch.fingerprint(table)
    assert got.data_hash is None
    assert "UnicodeDecodeError" in (got.error or "")


def test_worker_count_setting(monkeypatch):
    monkeypatch.setattr(ch, "_cores", lambda: 16)
    monkeypatch.delenv(ch.HASH_WORKERS_ENV, raising=False)
    assert ch._worker_count(ch._PARALLEL_MIN_ROWS - 1) == 1
    assert ch._worker_count(10_000_000) == ch._DEFAULT_MAX_WORKERS
    assert ch._worker_count(ch._PARALLEL_MIN_ROWS) == ch._PARALLEL_MIN_ROWS // ch._ROWS_PER_WORKER
    for raw, want in (("1", 1), ("0", 1), ("-3", 1), ("six", 1), ("12", 12)):
        monkeypatch.setenv(ch.HASH_WORKERS_ENV, raw)
        assert ch._worker_count(10_000_000) == want


def test_one_worker_starts_no_process(monkeypatch):
    monkeypatch.setattr(ch, "_PARALLEL_MIN_ROWS", 2)
    monkeypatch.setattr(ch, "_ROWS_PER_WORKER", 1)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "1")

    def boom(*a, **k):
        raise AssertionError("no helper should start")

    monkeypatch.setattr(ch, "_parallel_lanes", boom)
    table = pa.table({"x": list(range(10))})
    assert fingerprint_arrow(table) == _reference(table)


# --------------------------------------------------------------------------- #
# Skeptic review: process safety. Every helper a test starts is recorded and
# killed at the end, and a watchdog kills them after WATCHDOG_S, so a test of the
# old code fails on time instead of hanging.
# --------------------------------------------------------------------------- #

WATCHDOG_S = 20.0
_SLEEPER = "import time\ntime.sleep(120)\n"
_ECHO = "import shutil, sys\nshutil.copyfileobj(sys.stdin.buffer, sys.stdout.buffer)\n"


@pytest.fixture
def popens(monkeypatch):
    """Every Popen made while the test runs, with its keyword arguments."""
    made = []
    real = subprocess.Popen

    class Recorded(real):  # type: ignore[misc, valid-type]
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            made.append((self, kwargs))

    monkeypatch.setattr(subprocess, "Popen", Recorded)
    yield made
    for proc, _ in made:
        if proc.poll() is None:
            proc.kill()
        try:
            proc.wait(timeout=10)
        except Exception:
            pass
        for stream in (proc.stdin, proc.stdout):
            try:
                if stream is not None:
                    stream.close()
            except Exception:
                pass


def _watched(fn, popens):
    """Run ``fn`` with a watchdog that kills the recorded helpers; (result, seconds)."""

    def kill_all():
        for proc, _ in popens:
            if proc.poll() is None:
                proc.kill()

    timer = threading.Timer(WATCHDOG_S, kill_all)
    timer.daemon = True
    timer.start()
    t0 = time.monotonic()
    try:
        return fn(), time.monotonic() - t0
    finally:
        timer.cancel()


def _wide(n):
    return pa.table({"x": np.arange(n), "s": pa.array(np.arange(n).astype(str))})


def test_a_helper_that_never_answers_is_stopped_at_the_deadline(parallel, popens, monkeypatch):
    monkeypatch.setattr(ch, "_WORKER_BOOT", _SLEEPER)
    monkeypatch.setattr(ch, "_DEADLINE_FLOOR_S", 2.0)
    monkeypatch.setattr(ch, "_DEADLINE_PER_CELL_S", 0.0)
    table = _wide(50)
    got, secs = _watched(lambda: fingerprint_arrow(table), popens)
    assert secs < 12, f"took {secs:.1f} s: no deadline"
    assert got == _reference(table)
    assert parallel == [None]
    assert popens and all(p.poll() is not None for p, _ in popens)


def test_a_helper_that_echoes_its_input_cannot_deadlock(parallel, popens, monkeypatch):
    monkeypatch.setattr(ch, "_WORKER_BOOT", _ECHO)
    table = _wide(300_000)  # megabytes: far more than a pipe holds
    got, secs = _watched(lambda: fingerprint_arrow(table), popens)
    assert secs < WATCHDOG_S - 5, f"took {secs:.1f} s: stdin and stdout deadlocked"
    assert got == _reference(table)
    assert parallel == [None]


def test_ctrl_c_stops_every_helper_at_once(parallel, popens, monkeypatch):
    import _thread

    monkeypatch.setattr(ch, "_WORKER_BOOT", _SLEEPER)
    table = _wide(50)
    timer = threading.Timer(1.5, _thread.interrupt_main)
    timer.daemon = True
    t0 = time.monotonic()
    timer.start()
    with pytest.raises(KeyboardInterrupt):
        _watched(lambda: fingerprint_arrow(table), popens)
    secs = time.monotonic() - t0
    timer.cancel()
    assert secs < 10, f"Ctrl+C took {secs:.1f} s to land"
    assert popens and all(p.poll() is not None for p, _ in popens), "helpers left running"


def test_a_frozen_app_starts_no_helper(parallel, popens, monkeypatch):
    monkeypatch.setattr(sys, "frozen", True, raising=False)
    table = _wide(50)
    assert fingerprint_arrow(table) == _reference(table)
    assert popens == []


def test_a_program_that_is_not_python_is_not_started(parallel, popens, monkeypatch, tmp_path):
    app = tmp_path / "myapp.exe"
    app.write_bytes(b"not a python")
    monkeypatch.setattr(sys, "executable", str(app))
    table = _wide(50)
    assert fingerprint_arrow(table) == _reference(table)
    assert popens == []


def test_a_helper_cannot_start_helpers_or_stop_in_a_prompt(parallel, popens, monkeypatch):
    monkeypatch.setenv("PYTHONINSPECT", "1")
    table = _wide(50)
    assert fingerprint_arrow(table) == _reference(table)
    assert parallel and parallel[-1] is not None, parallel.why()
    for _, kwargs in popens:
        env = kwargs.get("env") or {}
        assert env.get(ch.HASH_WORKERS_ENV) == "1"
        assert "PYTHONINSPECT" not in env


def test_a_failed_fallback_is_silent_and_logged(parallel, popens, monkeypatch, capfd, caplog):
    monkeypatch.setattr(ch, "_WORKER_BOOT", "import sys\nsys.exit(0)\n")  # reads nothing
    table = _wide(300_000)
    with caplog.at_level(logging.DEBUG, logger=ch.__name__):
        got, _ = _watched(lambda: fingerprint_arrow(table), popens)
        gc.collect()
    assert got == _reference(table)
    assert capfd.readouterr().err == ""
    assert any("helpers failed" in r.getMessage() for r in caplog.records)
    assert all(p.stdin.closed and p.stdout.closed for p, _ in popens)


def test_a_hash_file_changed_on_disk_is_not_used(popens, monkeypatch, tmp_path):
    import importlib.util

    copy = tmp_path / "content_hash.py"
    copy.write_bytes(open(ch.__file__, "rb").read())
    spec = importlib.util.spec_from_file_location("_ch_copy_f038", copy)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    monkeypatch.setitem(sys.modules, spec.name, mod)
    spec.loader.exec_module(mod)
    mod._PARALLEL_MIN_ROWS, mod._ROWS_PER_WORKER = 2, 1
    monkeypatch.setattr(mod, "_cores", lambda: 8)
    table = _wide(50)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "1")
    serial = mod.fingerprint_arrow(table)
    # A later build replaces the file: its lane sums differ by one.
    text = copy.read_bytes()
    marker = b"    return int(a), int(b)"
    assert text.count(marker) == 1
    copy.write_bytes(text.replace(marker, b"    return int(a) + 1, int(b)"))
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "3")
    got, _ = _watched(lambda: mod.fingerprint_arrow(table), popens)
    assert popens, "the helpers were not tried"
    assert got == serial


def test_a_helper_with_another_pyarrow_is_not_used(parallel, popens, monkeypatch, caplog):
    monkeypatch.setattr(pa, "__version__", "0.0.1")
    table = _wide(50)
    with caplog.at_level(logging.DEBUG, logger=ch.__name__):
        assert fingerprint_arrow(table) == _reference(table)
    assert parallel == [None]
    assert any(str(ch._EXIT_PYARROW) in r.getMessage() for r in caplog.records)


def test_the_setting_never_exceeds_the_usable_cores(monkeypatch):
    monkeypatch.setattr(ch, "_cores", lambda: 2)
    monkeypatch.setenv(ch.HASH_WORKERS_ENV, "64")
    assert ch._worker_count(10_000_000) == 2


def test_a_container_cpu_quota_is_read(tmp_path):
    f = tmp_path / "cpu.max"
    for text, want in (("200000 100000\n", 2), ("150000 100000\n", 2), ("max 100000\n", None)):
        f.write_text(text)
        assert ch._cgroup_cpus(str(f)) == want
    assert ch._cgroup_cpus(str(tmp_path / "missing")) is None


def test_concurrent_hashes_share_one_helper_budget(parallel, popens, monkeypatch):
    assert ch._reserve(3, 3) == 3
    try:
        assert ch._reserve(3, 3) == 0  # the budget is in use: this hash runs here
        table = _wide(50)
        assert fingerprint_arrow(table) == _reference(table)
        assert popens == []
    finally:
        ch._release(3)
    assert ch._helpers_running == 0
