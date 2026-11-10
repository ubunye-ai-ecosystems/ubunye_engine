"""The Delta run record store on live Spark with Delta Lake (ADR 011).

A version 2 run record goes into a Delta table and comes back with every field;
the conformance suite runs on a path table and on a named table.
"""

from __future__ import annotations

import json
import uuid
import warnings

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("delta")

from ubunye.lineage.delta_store import DeltaLineageBackend  # noqa: E402
from ubunye.testing.lineage_conformance import (  # noqa: E402
    LineageStoreConformance,
    full_record,
)

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(_spark_jvm_with_delta):
    if not _spark_jvm_with_delta:
        pytest.skip("Delta jars could not be resolved")
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


class TestDeltaPathStore(LineageStoreConformance):
    pause = 0.0  # rows carry their own write time

    @pytest.fixture
    def store(self, spark, tmp_path):
        return DeltaLineageBackend(str(tmp_path / "runs"))


class TestDeltaTableStore(LineageStoreConformance):
    pause = 0.0

    @pytest.fixture
    def store(self, spark, tmp_path):
        db = f"ubunye_lineage_{uuid.uuid4().hex[:8]}"
        spark.sql(f"CREATE DATABASE {db} LOCATION '{(tmp_path / db).as_posix()}'")
        yield DeltaLineageBackend(f"{db}.runs")
        spark.sql(f"DROP DATABASE {db} CASCADE")


def test_the_table_has_the_record_and_its_index_columns(spark, tmp_path):
    loc = str(tmp_path / "runs")
    record = full_record()
    DeltaLineageBackend(loc).save(record)
    (row,) = spark.read.format("delta").load(loc).collect()
    assert json.loads(row["record"]) == record.to_dict()
    assert (row["run_id"], row["task_path"], row["status"]) == (
        record.run_id,
        "shop/orders/clean",
        "success",
    )
    assert (row["usecase"], row["package"], row["task"]) == ("shop", "orders", "clean")
    assert row["started_at"] == record.started_at
    assert row["variables"]["dt"] == "2026-11-01"
    assert row["record_version"] == 2


def test_two_stores_on_one_table_see_each_others_runs(spark, tmp_path):
    # Two jobs that share no disk but share the table.
    loc = str(tmp_path / "runs")
    a, b = full_record(), full_record()
    DeltaLineageBackend(loc).save(a)
    DeltaLineageBackend(loc).save(b)
    assert {r.run_id for r in DeltaLineageBackend(loc).list_runs("shop/orders/clean")} == {
        a.run_id,
        b.run_id,
    }


def test_a_run_records_its_receipt_in_delta(spark, tmp_path):
    pytest.importorskip("pandas")
    import ubunye

    task = tmp_path / "uc" / "shop" / "orders" / "clean"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n2\n3\n", encoding="utf-8")
    (task / "config.yaml").write_text(
        "VERSION: 0.1.0\nCONFIG:\n  inputs:\n"
        f"    src: {{format: s3, path: '{(tmp_path / 'in.csv').as_posix()}', file_format: csv,"
        " options: {header: 'true'}}\n  outputs:\n"
        f"    out: {{format: s3, path: '{(tmp_path / 'out').as_posix()}', file_format: parquet,"
        " mode: overwrite}\n",
        encoding="utf-8",
    )
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    loc = str(tmp_path / "receipts")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        ubunye.run_task(
            str(task), backend="pandas", lineage=True, lineage_dir=loc, lineage_backend="delta"
        )
    (saved,) = DeltaLineageBackend(loc).search(task_path="shop/orders/clean")
    assert saved.status == "success"
    assert saved.record_version == 2
    assert saved.outputs[0].row_count == 3 and saved.outputs[0].data_hash
    assert saved.code_hash and saved.environment


# ---------------------------------------------------------------------------
# Scale (F-099): one append per save must not leave one file per save forever
# ---------------------------------------------------------------------------


def _detail(spark, loc):
    return spark.sql(f"DESCRIBE DETAIL delta.`{loc}`").first()


@pytest.fixture
def quick_log(spark):
    """Read the Delta log in one task, not 50: a local[1] save takes 0.7 s, not 5 s."""
    key = "spark.databricks.delta.snapshotPartitions"
    was = spark.conf.get(key, None)
    spark.conf.set(key, "1")
    yield
    if was is None:
        spark.conf.unset(key)
    else:
        spark.conf.set(key, was)


def test_a_new_table_is_clustered_by_task_and_compacts_itself(spark, tmp_path, quick_log):
    loc = str(tmp_path / "runs")
    store = DeltaLineageBackend(loc)
    tasks = [("shop", "orders", "clean"), ("shop", "orders", "load")]
    run_ids = {t: [] for t in tasks}
    for i in range(100):  # 100 runs, each saved twice (running, then finished): 200 appends
        task = tasks[i % 2]
        record = full_record(task, status="running", started_at=f"2026-11-01T10:{i:02d}:00")
        store.save(record)
        record.status = "success"
        store.save(record)
        run_ids[task].append(record.run_id)
    detail = _detail(spark, loc)
    assert list(detail["clusteringColumns"]) == ["task_path"]
    assert detail["properties"]["delta.autoOptimize.autoCompact"] == "true"
    # 200 appends, one small file each; auto compaction merged them as they piled up.
    assert detail["numFiles"] < 60, detail["numFiles"]
    for task in tasks:
        runs = store.list_runs("/".join(task), n=80)
        assert [r.run_id for r in runs] == run_ids[task][::-1]
        assert {r.status for r in runs} == {"success"}


def test_an_existing_table_is_used_as_it_is(spark, tmp_path, quick_log):
    loc = str(tmp_path / "runs")
    from ubunye.lineage.delta_store import _row, _schema

    old = full_record()  # a table made by 0.9.0: the first append created it
    spark.createDataFrame([_row(old)], schema=_schema()).write.format("delta").save(loc)
    new = full_record()
    DeltaLineageBackend(loc).save(new)
    detail = _detail(spark, loc)
    assert list(detail["clusteringColumns"]) == []
    assert "delta.autoOptimize.autoCompact" not in detail["properties"]
    assert [r.run_id for r in DeltaLineageBackend(loc).list_runs(old.task_path)] == [
        new.run_id,
        old.run_id,
    ]
