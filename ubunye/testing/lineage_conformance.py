"""The tests every run record store must pass (ADR 011), shipped so a new one can prove itself.

A run record store is a plugin in the ``ubunye.lineage_backends`` group. This
suite checks the promises the engine relies on: a record (every field of a
version 2 :class:`~ubunye.lineage.context.RunContext`) is read back exactly as
it was saved, saving a run again replaces it, a task's runs come newest first,
``limit`` and ``status`` filter, a missing run id is an error, and the runs of
two tasks never mix. The filesystem store runs it in the unit tier and the
Delta store in the integration tier.

Use it from your store's tests (needs pytest)::

    import pytest

    from ubunye.testing.lineage_conformance import LineageStoreConformance


    class TestMyStore(LineageStoreConformance):
        @pytest.fixture
        def store(self, tmp_path):
            return MyStore(str(tmp_path / "runs"))
"""

from __future__ import annotations

import dataclasses
import time
import uuid
from typing import Any, List

try:
    import pytest
except ImportError as exc:  # the suite is a set of pytest tests
    raise ImportError(
        "ubunye.testing.lineage_conformance is a pytest suite; install pytest to run it "
        "(pip install pytest)."
    ) from exc

from ubunye.core.errors import LineageRecordNotFoundError
from ubunye.lineage.context import RECORD_VERSION, RunContext, StepRecord

TASK_A = ("shop", "orders", "clean")
TASK_B = ("shop", "orders", "publish")


def full_step(name: str, direction: str) -> StepRecord:
    """A step with every field set to a value that is not its default."""
    return StepRecord(
        name=name,
        direction=direction,
        format="delta",
        location=f"lake/{name}",
        row_count=42,
        schema_hash="sha256:" + "a" * 64,
        data_hash="sha256:" + "b" * 64,
        hash_method="rows-v1",
        hash_error="none, but set so the field is checked",
        hash_seconds=0.125,
        hash_reused_from="output:other",
        hash_basis="recomputed",
        source_version={"kind": "delta", "version": 7, "timestamp": "2026-11-01T00:00:00Z"},
        source_version_at_hash={"kind": "delta", "version": 8, "timestamp": None},
        source_changed=True,
        source_note="The source moved after it was read.",
    )


def full_record(
    task: tuple = TASK_A,
    *,
    status: str = "success",
    started_at: str = "2026-11-01T10:00:00+00:00",
    run_id: str = "",
) -> RunContext:
    """A version 2 run record with every field set to a value that is not its default."""
    usecase, package, name = task
    return RunContext(
        run_id=run_id or str(uuid.uuid4()),
        task_path=f"{usecase}/{package}/{name}",
        usecase=usecase,
        package=package,
        task_name=name,
        profile="prod",
        model="etl",
        version="1.2.3",
        config_hash="sha256:" + "c" * 64,
        started_at=started_at,
        ended_at="2026-11-01T10:05:00+00:00",
        duration_sec=300.5,
        status=status,
        inputs=[full_step("orders", "input")],
        outputs=[full_step("clean", "output"), full_step("rejects", "output")],
        error="ValueError: a field that must survive (with unicode: Sekhoto)",
        engine_version="0.9.0",
        backend="spark",
        variables={"dt": "2026-11-01", "dtf": "%Y-%m-%d", "mode": "PROD", "region": "gauteng"},
        record_version=RECORD_VERSION,
        code_hash="sha256:" + "d" * 64,
        environment={"python": "3.13.1", "packages": {"pyspark": "4.2.0"}, "platform": "linux"},
        environment_hash="sha256:" + "e" * 64,
        timings=[{"step": "orders", "input": True, "seconds": 1.5}],
        expectations=[{"output": "clean", "kind": "not_null", "column": "id", "passed": True}],
        llm_calls=[{"backend": "replay", "model": "m", "tokens": 10, "status": "ok"}],
        llm_budget={"max_calls": 5, "calls": 1},
        time_zone="Africa/Johannesburg",
        model_runs=[
            {
                "action": "train",
                "model": "ChurnModel",
                "version": "1.0.3",
                "stage": "development",
                "promote_to": "production",
                "gate_result": "blocked",
                "blocked_by": [
                    {"gate": "min_auc", "metric": "auc", "actual": 0.71, "threshold": 0.8}
                ],
                "metrics": {"auc": 0.71},
            },
            {"action": "predict", "model": "ChurnModel", "version": "1.0.2", "stage": "staging"},
        ],
    )


def _tp(task: tuple) -> str:
    return "/".join(task)


def _ids(records: List[RunContext]) -> List[str]:
    return [r.run_id for r in records]


class LineageStoreConformance:
    """Subclass and provide a ``store`` fixture: an empty store."""

    #: Seconds between saves, so "newest first" has an order to keep.
    pause = 0.05

    @pytest.fixture
    def store(self) -> Any:  # pragma: no cover - the subclass provides it
        raise NotImplementedError("Provide a 'store' fixture that returns an empty store.")

    def _save_in_order(self, store: Any, records: List[RunContext]) -> None:
        for record in records:
            store.save(record)
            time.sleep(self.pause)

    # -- the record ---------------------------------------------------------

    def test_the_reference_record_sets_every_field(self) -> None:
        # Guards the suite itself: a field added to RunContext or StepRecord must be
        # set here too, or a store could drop it and still pass.
        record = full_record()
        for cls, obj in ((RunContext, record), (StepRecord, record.outputs[0])):
            for f in dataclasses.fields(cls):
                default = (
                    f.default_factory()
                    if f.default_factory is not dataclasses.MISSING
                    else f.default
                )
                assert getattr(obj, f.name) != default or f.name == "record_version", (
                    f"{cls.__name__}.{f.name} is at its default in the reference record; "
                    "set it in ubunye.testing.lineage_conformance"
                )

    def test_a_saved_record_reads_back_equal(self, store: Any) -> None:
        record = full_record()
        store.save(record)
        loaded = store.load(_tp(TASK_A), record.run_id)
        assert isinstance(loaded, RunContext)
        assert loaded.to_dict() == record.to_dict()

    def test_a_record_found_by_listing_is_whole(self, store: Any) -> None:
        record = full_record()
        store.save(record)
        (listed,) = store.list_runs(_tp(TASK_A), n=5)
        assert listed.to_dict() == record.to_dict()
        (found,) = store.search(task_path=_tp(TASK_A))
        assert found.to_dict() == record.to_dict()

    def test_saving_a_run_again_replaces_it(self, store: Any) -> None:
        record = full_record(status="running")
        store.save(record)
        time.sleep(self.pause)
        record.status = "success"
        store.save(record)
        assert store.load(_tp(TASK_A), record.run_id).status == "success"
        assert _ids(store.list_runs(_tp(TASK_A))) == [record.run_id]
        assert store.search(task_path=_tp(TASK_A), status="running") == []

    def test_a_missing_run_id_is_an_error(self, store: Any) -> None:
        store.save(full_record())
        with pytest.raises(LineageRecordNotFoundError):
            store.load(_tp(TASK_A), "no-such-run")
        with pytest.raises(LineageRecordNotFoundError):
            store.load(_tp(TASK_B), "no-such-run")

    # -- finding runs -------------------------------------------------------

    def test_a_tasks_runs_come_newest_first(self, store: Any) -> None:
        runs = [full_record(started_at=f"2026-11-0{i}T10:00:00+00:00") for i in (1, 2, 3)]
        self._save_in_order(store, runs)
        newest_first = list(reversed(_ids(runs)))
        assert _ids(store.list_runs(_tp(TASK_A))) == newest_first
        assert _ids(store.search(task_path=_tp(TASK_A))) == newest_first

    def test_limit(self, store: Any) -> None:
        runs = [full_record(started_at=f"2026-11-0{i}T10:00:00+00:00") for i in (1, 2, 3)]
        self._save_in_order(store, runs)
        assert _ids(store.list_runs(_tp(TASK_A), n=2)) == [runs[2].run_id, runs[1].run_id]
        assert _ids(store.search(task_path=_tp(TASK_A), limit=1)) == [runs[2].run_id]

    def test_status_filter(self, store: Any) -> None:
        ok1, bad, ok2 = (full_record(status=s) for s in ("success", "error", "success"))
        self._save_in_order(store, [ok1, bad, ok2])
        assert _ids(store.search(task_path=_tp(TASK_A), status="error")) == [bad.run_id]
        assert _ids(store.search(task_path=_tp(TASK_A), status="success")) == [
            ok2.run_id,
            ok1.run_id,
        ]
        assert _ids(store.search(status="success", limit=1)) == [ok2.run_id]

    def test_since(self, store: Any) -> None:
        old = full_record(started_at="2026-10-01T10:00:00+00:00")
        new = full_record(started_at="2026-11-05T10:00:00+00:00")
        self._save_in_order(store, [old, new])
        assert _ids(store.search(since="2026-11-01")) == [new.run_id]

    def test_two_tasks_do_not_mix(self, store: Any) -> None:
        a1, b1, a2 = full_record(TASK_A), full_record(TASK_B), full_record(TASK_A)
        self._save_in_order(store, [a1, b1, a2])
        assert _ids(store.list_runs(_tp(TASK_A))) == [a2.run_id, a1.run_id]
        assert _ids(store.list_runs(_tp(TASK_B))) == [b1.run_id]
        assert _ids(store.search(task_path=_tp(TASK_B))) == [b1.run_id]
        with pytest.raises(LineageRecordNotFoundError):
            store.load(_tp(TASK_B), a1.run_id)
        assert _ids(store.search()) == [a2.run_id, b1.run_id, a1.run_id]
        # The first parts of a task path find every task under them.
        assert set(_ids(store.search(task_path="shop/orders"))) == {
            a1.run_id,
            a2.run_id,
            b1.run_id,
        }

    def test_an_empty_store_finds_nothing(self, store: Any) -> None:
        assert store.list_runs(_tp(TASK_A)) == []
        assert store.search() == []
        assert store.search(task_path=_tp(TASK_A), status="error") == []
