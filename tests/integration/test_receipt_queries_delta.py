"""Job health and upstream checks on the Delta run record store (ADR 013).

Two jobs that share no disk, only a Delta table: the payment send sees whether the
monthly calculation succeeded for its month.
"""

from __future__ import annotations

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from ubunye.lineage import health, upstream  # noqa: E402
from ubunye.lineage.delta_store import DeltaLineageBackend  # noqa: E402
from ubunye.testing.lineage_conformance import full_record  # noqa: E402

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(_spark_jvm_with_delta):
    if not _spark_jvm_with_delta:
        pytest.skip("Delta jars could not be resolved")
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def test_health_and_upstream_read_a_shared_delta_table(spark, tmp_path):
    loc = str(tmp_path / "runs")
    calc = ("pay", "monthly", "calc")
    ok = full_record(calc, status="success", started_at="2026-11-01T10:00:00+00:00")
    bad = full_record(calc, status="error", started_at="2026-11-02T10:00:00+00:00")
    DeltaLineageBackend(loc).save(ok)  # the calculation job
    DeltaLineageBackend(loc).save(bad)

    store = DeltaLineageBackend(loc)  # the payment job, elsewhere
    (h,) = health.check(store, ["pay/monthly/calc"])
    assert h.state == "failing" and h.consecutive_failures == 1
    assert h.last_success_at == ok.ended_at

    req = upstream.Requirement(task="pay/monthly/calc", variables=["dt"])
    assert upstream.find(store, req, {"dt": "2026-11-01"}).run_id == ok.run_id
    with pytest.raises(upstream.UpstreamNotReady):
        upstream.check(store, [req], {"dt": "2026-10-01"}, task="pay/monthly/send")
