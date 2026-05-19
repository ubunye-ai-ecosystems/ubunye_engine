"""Spark unit tests for the policy/device mapping transformation.

The three stages that encode business logic are exercised independently:
    * ``_derive_policy_features`` - regex-based product + version extraction.
    * ``_latest_mi`` - latest policy version per (policy, item, month).
    * ``_imei_first_detection`` + ``_correct_installation`` - earliest IMEI
      signal overrides a stale PDD installation datetime.

An end-to-end test composes the whole pipeline on a toy fixture and asserts
the output schema matches ``OUTPUT_COLUMNS``.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

MI_SCHEMA = StructType([
    StructField("source_data", StringType(), True),
    StructField("policy_no", StringType(), True),
    StructField("policy_number", StringType(), True),
    StructField("policy_inception_date", TimestampType(), True),
    StructField("item_inception_date", TimestampType(), True),
    StructField("cancelled_date", TimestampType(), True),
    StructField("eff_from_date", TimestampType(), True),
    StructField("eff_to_date", TimestampType(), True),
    StructField("section_code", StringType(), True),
    StructField("cover_code", StringType(), True),
    StructField("item_description", StringType(), True),
    StructField("veh_make", StringType(), True),
    StructField("veh_model", StringType(), True),
    StructField("registration", StringType(), True),
    StructField("vin_number", StringType(), True),
    StructField("imei_number", StringType(), True),
    StructField("cover_premium", DoubleType(), True),
    StructField("cover_sum_insured", DoubleType(), True),
    StructField("cover_description", StringType(), True),
    StructField("policy_status", StringType(), True),
    StructField("cover_status", StringType(), True),
    StructField("lob_asset_id", StringType(), True),
    StructField("item_no_raw", StringType(), True),
    StructField("policy_cancellation_reason_code", StringType(), True),
    StructField("end_date", TimestampType(), True),
    StructField("effective_from_date", TimestampType(), True),
    StructField("effective_to_date", TimestampType(), True),
    StructField("effective_year_month", StringType(), True),
    StructField("effective_year", IntegerType(), True),
    StructField("effective_quarter", IntegerType(), True),
    StructField("effective_month", IntegerType(), True),
    StructField("effective_year_quarter", StringType(), True),
    StructField("item_cancelled_date", TimestampType(), True),
    StructField("premium_collected", DoubleType(), True),
    StructField("active_lob_asset_id", StringType(), True),
])
from transformations import (  # noqa: E402 (conftest mutates sys.path)
    OUTPUT_COLUMNS,
    _correct_installation,
    _derive_policy_features,
    _imei_first_detection,
    _latest_mi,
    _latest_device_record,
    build_policy_device_mapping,
)


def test_derive_policy_features_extracts_product_and_version(spark):
    df = spark.createDataFrame(
        [("ABC-12345/7",), ("PQR-999/1",), ("ZZZ/42",)],
        ["policy_no"],
    )
    rows = {r["policy_no"]: r for r in _derive_policy_features(df).collect()}
    assert rows["ABC-12345/7"]["policy_product_description"] == "ABC"
    assert rows["ABC-12345/7"]["policy_version_number"] == 7
    assert rows["PQR-999/1"]["policy_product_description"] == "PQR"
    assert rows["PQR-999/1"]["policy_version_number"] == 1
    assert rows["ZZZ/42"]["policy_product_description"] == "ZZZ"
    assert rows["ZZZ/42"]["policy_version_number"] == 42


def test_latest_mi_keeps_highest_version(spark):
    df = spark.createDataFrame(
        [
            # (policy_number, lob_asset_id, effective_year_month, policy_version_number)
            ("P1", "A", "2026-03", 1),
            ("P1", "A", "2026-03", 3),
            ("P1", "A", "2026-03", 2),
            ("P1", "B", "2026-03", 5),
            ("P2", "A", "2026-03", 9),
        ],
        ["policy_number", "lob_asset_id", "effective_year_month", "policy_version_number"],
    )
    rows = _latest_mi(df).collect()
    versions = {(r["policy_number"], r["lob_asset_id"]): r["policy_version_number"] for r in rows}
    assert versions[("P1", "A")] == 3
    assert versions[("P1", "B")] == 5
    assert versions[("P2", "A")] == 9


def test_latest_device_record_dedups_per_policy_item(spark):
    df = spark.createDataFrame(
        [
            # (policy_number, item_no, inserted_datetime, imei_number, installation_datetime)
            ("P1", "A", datetime(2026, 1, 1), "I1", datetime(2025, 12, 1)),
            ("P1", "A", datetime(2026, 2, 1), "I1", datetime(2025, 12, 1)),
            ("P1", "A", datetime(2026, 3, 1), "I1", datetime(2025, 12, 1)),
        ],
        ["policy_number", "item_no", "inserted_datetime", "imei_number", "installation_datetime"],
    )
    rows = _latest_device_record(df).collect()
    assert len(rows) == 1
    assert rows[0]["inserted_datetime"] == datetime(2026, 3, 1)


def test_correct_installation_picks_earliest_signal(spark):
    device_df = spark.createDataFrame(
        [
            # PDD install exists but IMEI seen earlier -> use earlier
            ("I1", datetime(2026, 6, 1)),
            # PDD install missing -> use IMEI first-detection
            ("I2", None),
            # PDD install earlier than IMEI first-detection -> keep PDD
            ("I3", datetime(2025, 1, 1)),
        ],
        ["imei_number", "installation_datetime"],
    )
    imei_first = spark.createDataFrame(
        [
            ("I1", datetime(2026, 3, 1)),
            ("I2", datetime(2026, 4, 1)),
            ("I3", datetime(2026, 5, 1)),
        ],
        ["imei_number", "imei_first_detection_datetime"],
    )
    result = _correct_installation(device_df, imei_first).collect()
    by_imei = {r["imei_number"]: r["installation_datetime_final"] for r in result}
    assert by_imei["I1"] == datetime(2026, 3, 1)
    assert by_imei["I2"] == datetime(2026, 4, 1)
    assert by_imei["I3"] == datetime(2025, 1, 1)


def test_imei_first_detection_is_min_per_imei(spark):
    df = spark.createDataFrame(
        [
            ("I1", datetime(2026, 3, 1)),
            ("I1", datetime(2026, 2, 1)),
            ("I1", datetime(2026, 4, 1)),
            ("I2", datetime(2026, 5, 1)),
        ],
        ["imei_number", "retrieved_datetime"],
    )
    rows = {r["imei_number"]: r["imei_first_detection_datetime"] for r in _imei_first_detection(df).collect()}
    assert rows["I1"] == datetime(2026, 2, 1)
    assert rows["I2"] == datetime(2026, 5, 1)


def test_end_to_end_schema_matches_contract(spark):
    # Minimal MI exposure row - column names match the post-rename space that
    # _rename_mi would produce, i.e. the source columns already snake-cased.
    mi = spark.createDataFrame(
        [
            (
                "source", "ABC-1/2", "POL1", datetime(2024, 1, 1), datetime(2024, 1, 10),
                None, datetime(2024, 1, 1), datetime(2024, 12, 31),
                "SEC1", "COV1", "item", "Make", "Model", "REG1", "VIN1", "IMEI1",
                100.0, 1000.0, "cover-desc", "ACTIVE", "ACTIVE", "A1", "A1_raw", None,
                None, None, None, "2024-01", 2024, 1, 1, "2024Q1", None,
                100.0, "A1",
            )
        ],
        MI_SCHEMA,
    )

    pdd = spark.createDataFrame(
        [("POL1", "A1", datetime(2024, 1, 5), "IMEI1", datetime(2024, 1, 6))],
        ["policy_number", "item_no", "inserted_datetime", "imei_number", "installation_datetime"],
    )

    ui = spark.createDataFrame(
        [("IMEI1", datetime(2024, 1, 2))],
        ["imei_number", "retrieved_datetime"],
    )

    result = build_policy_device_mapping(pdd, ui, mi)
    assert tuple(result.columns) == OUTPUT_COLUMNS

    row = result.collect()[0]
    # Earliest IMEI signal (2024-01-02) beats PDD install (2024-01-06).
    assert row["installation_datetime"] == datetime(2024, 1, 2)
    assert row["imei_first_detection_datetime"] == datetime(2024, 1, 2)
    assert row["policy_version_number"] == 2
    assert row["policy_product_description"] == "ABC"


def test_right_join_keeps_mi_rows_without_device(spark):
    mi = spark.createDataFrame(
        [
            (
                "source", "ABC-1/1", "POL2", datetime(2024, 1, 1), datetime(2024, 1, 10),
                None, datetime(2024, 1, 1), datetime(2024, 12, 31),
                "SEC1", "COV1", "item", "Make", "Model", "REG2", "VIN2", "IMEI_UNSEEN",
                50.0, 500.0, "cover-desc", "ACTIVE", "ACTIVE", "A_UNSEEN", "A_UNSEEN_raw", None,
                None, None, None, "2024-01", 2024, 1, 1, "2024Q1", None,
                50.0, "A_UNSEEN",
            )
        ],
        MI_SCHEMA,
    )
    pdd = spark.createDataFrame(
        [],
        "policy_number string, item_no string, inserted_datetime timestamp, imei_number string, installation_datetime timestamp",
    )
    ui = spark.createDataFrame(
        [],
        "imei_number string, retrieved_datetime timestamp",
    )

    result = build_policy_device_mapping(pdd, ui, mi).collect()
    assert len(result) == 1
    # Right-join preserves MI; device cols are null.
    assert result[0]["installation_datetime"] is None
    assert result[0]["imei_first_detection_datetime"] is None
    assert result[0]["policy_number"] == "POL2"


@pytest.mark.parametrize(
    "columns",
    [
        OUTPUT_COLUMNS,
    ],
)
def test_output_columns_are_unique_and_ordered(columns):
    assert len(columns) == len(set(columns)), "duplicate column names in OUTPUT_COLUMNS"
