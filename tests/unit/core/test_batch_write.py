"""A Delta or JDBC append of a named batch is written in one transaction a rerun does
not repeat (ADR 008 addendum, F-071, F-072).

The lease tells the writer how: ``once`` (Delta's idempotent write, keyed by task,
output and batch), ``replace`` (the output's ``replace_where`` names the batch's rows)
or ``again`` (``--rerun`` of a finished batch with nothing to replace by: a plain
append, as before). The live-Spark proof is
``tests/integration/test_delta_append_reruns.py``; these pin the bookkeeping.
"""

from __future__ import annotations

import json
import logging
import uuid

import pytest

from ubunye.adapters.spark import delta_batch
from ubunye.core import runs, write_modes
from ubunye.core.runs import BatchFinished, RunLease

TASK = "uc/pkg/t"
DT = {"dt": "2024-01-02"}


@pytest.fixture(autouse=True)
def _quick_takeover(monkeypatch):
    monkeypatch.setattr(runs, "TAKEOVER_SETTLE", 0.05)


def _lease(root, variables=DT, *, rerun=False, appends=("events",)) -> RunLease:
    lease = RunLease(root, TASK, variables, uuid.uuid4().hex).acquire()
    try:
        lease.check_finished(list(appends), rerun)
    except BaseException:
        lease.release()  # as runs.held does
        raise
    return lease


def _finish(lease: RunLease) -> None:
    lease.written("events")
    assert lease.commit()
    lease.release()


def test_a_named_batch_is_written_once_keyed_by_task_output_and_batch(tmp_path):
    lease = _lease(tmp_path)
    lease.writing("events", appends=True, exact=False)
    bw = lease.batch_write(replaces=False)
    assert bw.how == "once" and bw.version == 0
    assert bw.app_id == f"ubunye/{TASK}/events/{lease.key}"
    assert lease._doc["outputs"]["events"]["idempotent"] == "once"
    # A crash now is not an unrepaired append: a rerun commits nothing.
    assert runs._unrepaired(lease._doc["outputs"]) == []
    lease.release()


def test_another_batch_has_another_key(tmp_path):
    a = _lease(tmp_path)
    a.writing("events", appends=True, exact=False)
    first = a.batch_write(replaces=False).app_id
    a.release()
    b = _lease(tmp_path, {"dt": "2024-01-03"})
    b.writing("events", appends=True, exact=False)
    assert b.batch_write(replaces=False).app_id != first
    b.release()


def test_no_named_batch_means_a_plain_append(tmp_path):
    lease = _lease(tmp_path, {"mode": "DEV"})
    lease.writing("events", appends=True, exact=False)
    assert lease.batch_write(replaces=False) is None
    assert runs._unrepaired(lease._doc["outputs"]) == ["events"]
    lease.release()


def test_no_lease_means_a_plain_append():
    assert runs.batch_write(replaces=False) is None


def test_replace_where_replaces_and_rerun_replaces_a_finished_batch(tmp_path, caplog):
    lease = _lease(tmp_path)
    lease.writing("events", appends=True, exact=False)
    assert lease.batch_write(replaces=True).how == "replace"
    _finish(lease)
    note = json.loads(lease._finished_path().read_text())
    assert note["outputs"]["events"]["idempotent"] == "replaces"

    with pytest.raises(BatchFinished) as refused:
        _lease(tmp_path)
    assert "--rerun" in str(refused.value.hint) and "cannot be taken" not in refused.value.hint

    with caplog.at_level(logging.WARNING):
        again = _lease(tmp_path, rerun=True)
    assert "appends the batch to them again" not in caplog.text
    again.writing("events", appends=True, exact=False)
    assert again.batch_write(replaces=True).how == "replace"
    again.release()


def test_rerun_without_replace_where_appends_again_and_says_so(tmp_path, caplog):
    lease = _lease(tmp_path)
    lease.writing("events", appends=True, exact=False)
    lease.batch_write(replaces=False)
    _finish(lease)

    with caplog.at_level(logging.WARNING):
        again = _lease(tmp_path, rerun=True)
    assert "replace_where" in caplog.text
    again.writing("events", appends=True, exact=False)
    assert again.batch_write(replaces=False).how == "again"
    assert "idempotent" not in again._doc["outputs"]["events"]
    again.release()


def test_resolve_keeps_replace_where_on_an_append():
    resolved = write_modes.resolve(
        {"mode": "append", "replace_where": "dt = '1'"},
        connector="delta",
        supported=write_modes.ALL_MODES,
        default="append",
        file_format="delta",
    )
    assert resolved.batch_where == "dt = '1'" and resolved.options == {}
    plain = write_modes.resolve(
        {}, connector="delta", supported=write_modes.ALL_MODES, default="append"
    )
    assert plain.batch_where is None


class _Spark:
    """Enough of a SparkSession for DESCRIBE HISTORY."""

    def __init__(self, versions):
        self.versions = list(versions)

    def sql(self, statement):
        version = self.versions.pop(0)
        if version is None:
            raise RuntimeError("not a Delta table")
        return type("R", (), {"collect": lambda _self: [{"version": version}]})()


def _held(tmp_path, monkeypatch):
    lease = _lease(tmp_path)
    lease.writing("events", appends=True, exact=False)
    monkeypatch.setattr(runs, "_CURRENT", _Var(lease))
    return lease


class _Var:
    def __init__(self, value):
        self.value = value

    def get(self):
        return self.value


def test_delta_append_carries_the_batch_key(tmp_path, monkeypatch):
    lease = _held(tmp_path, monkeypatch)
    saved = []
    assert delta_batch.append(
        spark=_Spark([3, 4]),
        table=None,
        path="/x",
        opts={"mergeSchema": "true"},
        batch_where=None,
        save=lambda mode, opts: saved.append((mode, opts)),
    )
    assert saved == [
        (
            "append",
            {
                "mergeSchema": "true",
                "txnAppId": f"ubunye/{TASK}/events/{lease.key}",
                "txnVersion": 0,
            },
        )
    ]
    lease.release()


def test_delta_append_skipped_by_delta_is_reported(tmp_path, monkeypatch, caplog):
    lease = _held(tmp_path, monkeypatch)
    with caplog.at_level(logging.WARNING):
        delta_batch.append(
            spark=_Spark([7, 7]),
            table="db.t",
            path=None,
            opts={},
            batch_where=None,
            save=lambda mode, opts: None,
        )
    assert "already holds this batch" in caplog.text and "replace_where" in caplog.text
    lease.release()


def test_delta_append_with_replace_where_replaces_the_rows(tmp_path, monkeypatch):
    lease = _held(tmp_path, monkeypatch)
    saved = []
    delta_batch.append(
        spark=_Spark([]),
        table="db.t",
        path=None,
        opts={},
        batch_where="dt = '1'",
        save=lambda mode, opts: saved.append((mode, opts)),
    )
    assert saved == [("overwrite", {"replaceWhere": "dt = '1'"})]
    assert lease._doc["outputs"]["events"]["idempotent"] == "replaces"
    lease.release()


def test_a_config_that_keys_its_own_delta_writes_is_left_alone(tmp_path, monkeypatch):
    lease = _held(tmp_path, monkeypatch)
    assert not delta_batch.append(
        spark=_Spark([]),
        table="db.t",
        path=None,
        opts={"txnAppId": "mine", "txnVersion": "3"},
        batch_where=None,
        save=lambda mode, opts: pytest.fail("must not write"),
    )
    lease.release()


def test_a_jdbc_batch_append_needs_a_jvm_and_says_so():
    from ubunye.adapters.spark import jdbc_batch
    from ubunye.core.errors import SinkWriteError

    with pytest.raises(SinkWriteError) as refused:
        jdbc_batch._connect(object(), "jdbc:postgresql://db/x", None, {})
    assert "classic Spark" in str(refused.value)


def test_a_jdbc_staging_table_sits_beside_the_target_and_is_named_by_the_batch():
    from ubunye.adapters.spark import jdbc_batch

    bw = runs.BatchWrite(app_id="ubunye/uc/pkg/t/events/abc", version=0, how="replace", run_id="r")
    a = jdbc_batch._stage_name("sales.events", bw)
    assert a.startswith("sales.ubunye_stage_") and len(a.split(".")[1]) == 25
    assert jdbc_batch._stage_name("sales.events", bw) == a  # a retry replaces a left one
    assert jdbc_batch._stage_name("events", None) != jdbc_batch._stage_name("events", None)
