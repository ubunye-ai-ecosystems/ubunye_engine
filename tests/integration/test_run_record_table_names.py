"""The run-record table name is quoted once, so hyphens and three-part names work.

Absa's write catalogs have hyphens (``aws-db-nonprod-aic-telematics-catalog``) and a
job passes them as plain names. Spark cannot parse an unquoted hyphenated name, so
the table was never created and every run went unrecorded.
"""

from __future__ import annotations

import datetime as dt
import uuid

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    builder = (
        SparkSession.builder.master("local[1]")
        .appName("run-record-table-names")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


def _record(task_path: str = "uc/pkg/t"):
    from ubunye.lineage.context import RunContext

    return RunContext(
        run_id=str(uuid.uuid4()),
        task_path=task_path,
        usecase="uc",
        package="pkg",
        task_name="t",
        profile="PROD",
        model="",
        version="",
        config_hash="h",
        started_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        status="success",
    )


@pytest.mark.parametrize(
    "name",
    [
        "{db}.uby_run_records",  # two parts
        "spark_catalog.{db}.uby_run_records",  # three parts, as on Unity Catalog
        "`spark_catalog`.`{db}`.`uby_run_records`",  # already quoted: unchanged
    ],
)
def test_a_two_or_three_part_name_is_created_and_written(spark, name):
    from ubunye.lineage.delta_store import DeltaLineageBackend

    db = f"run_db_{uuid.uuid4().hex[:6]}"
    spark.sql(f"CREATE DATABASE `{db}`")
    store = DeltaLineageBackend(name.format(db=db))
    ctx = _record()
    store.save(ctx)
    store.save(_record())

    assert spark.table(f"`{db}`.`uby_run_records`").count() == 2
    assert store.load("uc/pkg/t", ctx.run_id).run_id == ctx.run_id


def test_the_store_hands_spark_each_part_quoted():
    # A Unity Catalog catalog with hyphens cannot be made on local Spark, so check
    # the name the store gives Spark: one quoted part each, a path left alone.
    from ubunye.lineage.delta_store import DeltaLineageBackend

    store = DeltaLineageBackend("aws-db-nonprod-aic-telematics-catalog.edla_dev.uby_run_records")
    assert (
        store._table_or_path
        == "`aws-db-nonprod-aic-telematics-catalog`.`edla_dev`.`uby_run_records`"
    )
    assert DeltaLineageBackend("/Volumes/c/s/runs")._table_or_path == "/Volumes/c/s/runs"
