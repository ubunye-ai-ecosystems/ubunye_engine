"""Spark unit tests for the JHB hourly-forecast transformation.

The transformation operates on the exact shape produced by the REST API
reader: a single-row DataFrame with nested struct / array columns. These
tests build that shape with ``spark.createDataFrame`` so they exercise
``arrays_zip`` + ``explode`` end-to-end on a real local SparkSession.
"""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict

import pytest

from transformations import (  # noqa: E402 (conftest mutates sys.path)
    OUTPUT_COLUMNS,
    transform_weather,
)


@pytest.fixture
def sample_row() -> Dict[str, Any]:
    """Trimmed Open-Meteo response - three hourly ticks for JHB."""
    return {
        "latitude": -26.2041,
        "longitude": 28.0473,
        "hourly": {
            "time": [
                "2026-04-15T00:00",
                "2026-04-15T01:00",
                "2026-04-15T02:00",
            ],
            "temperature_2m": [13.6, 13.5, 13.1],
            "relative_humidity_2m": [72, 74, 75],
            "precipitation": [0.0, 0.0, 0.1],
            "wind_speed_10m": [8.2, 7.9, 7.5],
        },
    }


@pytest.fixture
def source_df(spark, sample_row):
    """Single-row DataFrame matching the rest_api reader's output shape."""
    return spark.createDataFrame([sample_row])


def test_output_columns_match_contract(source_df):
    result = transform_weather(source_df)
    assert tuple(result.columns) == OUTPUT_COLUMNS


def test_row_count_matches_hourly_array(source_df, sample_row):
    result = transform_weather(source_df)
    assert result.count() == len(sample_row["hourly"]["time"])


def test_coordinates_broadcast_to_every_row(source_df):
    result = transform_weather(source_df).collect()
    assert all(row["latitude"] == -26.2041 for row in result)
    assert all(row["longitude"] == 28.0473 for row in result)


def test_forecast_timestamp_is_parsed(source_df):
    rows = transform_weather(source_df).orderBy("forecast_timestamp").collect()
    assert rows[0]["forecast_timestamp"] == dt.datetime(2026, 4, 15, 0, 0)
    assert rows[2]["forecast_timestamp"] == dt.datetime(2026, 4, 15, 2, 0)


def test_forecast_date_derived_from_timestamp(source_df):
    rows = transform_weather(source_df).collect()
    assert all(row["forecast_date"] == dt.date(2026, 4, 15) for row in rows)


def test_values_copied_verbatim(source_df):
    rows = transform_weather(source_df).orderBy("forecast_timestamp").collect()
    assert [r["temperature_c"] for r in rows] == [13.6, 13.5, 13.1]
    assert [r["precipitation_mm"] for r in rows] == [0.0, 0.0, 0.1]
    assert [r["wind_speed_kmh"] for r in rows] == [8.2, 7.9, 7.5]


def test_output_column_types(source_df):
    """Delta table contract: timestamp + date partition key + doubles."""
    schema = {f.name: f.dataType.simpleString() for f in transform_weather(source_df).schema}
    assert schema["forecast_timestamp"] == "timestamp"
    assert schema["forecast_date"] == "date"
    assert schema["temperature_c"] == "double"
    assert schema["precipitation_mm"] == "double"
    assert schema["wind_speed_kmh"] == "double"
