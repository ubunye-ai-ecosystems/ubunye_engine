"""benchmarks/guard.py: slower than the base fails; a base too old to time does not.

A release pull request compares the release with `main`, which may predate what
the guard times (0.5.0 has no pandas backend). The base could not even import the
guard's operations, so the guard failed a change for being newer, not slower.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

GUARD = Path(__file__).resolve().parents[2] / "benchmarks" / "guard.py"


@pytest.fixture
def guard():
    spec = importlib.util.spec_from_file_location("perf_guard", GUARD)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _timings(value):
    return {"timings": {"config_load": value, "run_record_hash_60k": value}}


def test_slower_than_the_base_fails(guard, monkeypatch):
    times = {"base": _timings(0.100), "head": _timings(0.200)}
    monkeypatch.setattr(guard, "_time_copy", lambda source, repeat: times[source.name])
    assert guard.compare(Path("base"), Path("head"), rounds=1, repeat=1) == 1


def test_a_base_that_cannot_import_the_operations_is_reported_not_failed(
    guard, monkeypatch, capsys
):
    times = {"base": guard.cannot_run("No module named 'ubunye.backends.pandas_backend'"),
             "head": _timings(0.100)}  # fmt: skip
    monkeypatch.setattr(guard, "_time_copy", lambda source, repeat: times[source.name])
    assert guard.compare(Path("base"), Path("head"), rounds=1, repeat=1) == 0
    out = capsys.readouterr().out
    assert "| new |" in out and "pandas_backend" in out


def test_every_operation_is_named_when_the_base_cannot_run(guard):
    result = guard.cannot_run("too old")
    assert set(result["timings"]) == set(guard.OPERATIONS)
    assert all(v.startswith("cannot run") for v in result["timings"].values())


def test_a_base_whose_process_fails_is_reported_but_the_head_must_run(guard, monkeypatch):
    def time_copy(source, repeat):
        if source.name == "base":
            raise SystemExit("timing base failed:\nImportError")
        return _timings(0.100)

    monkeypatch.setattr(guard, "_time_copy", time_copy)
    assert guard.compare(Path("base"), Path("head"), rounds=1, repeat=1) == 0

    def head_fails(source, repeat):
        raise SystemExit("timing head failed")

    monkeypatch.setattr(guard, "_time_copy", head_fails)
    with pytest.raises(SystemExit):
        guard.compare(Path("base"), Path("head"), rounds=1, repeat=1)
