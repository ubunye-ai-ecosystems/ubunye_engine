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
