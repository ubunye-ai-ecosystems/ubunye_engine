"""Unit tests for the JHB hourly forecast transformation (pandas twin)."""

from __future__ import annotations

import datetime as dt
from typing import Any, Dict

import pandas as pd
import pytest

from transformations import (  # noqa: E402 (conftest mutates sys.path)
    OUTPUT_COLUMNS,
    transform_weather_pandas,
)


@pytest.fixture
def sample_response() -> Dict[str, Any]:
    """Trimmed Open-Meteo response - three hourly ticks for JHB."""
    return {
        "latitude": -26.2041,
        "longitude": 28.0473,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "precipitation": "mm",
            "wind_speed_10m": "km/h",
        },
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


def test_output_columns_match_contract(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert tuple(result.columns) == OUTPUT_COLUMNS


def test_row_count_matches_hourly_array(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert len(result) == len(sample_response["hourly"]["time"])


def test_coordinates_broadcast_to_every_row(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert (result["latitude"] == -26.2041).all()
    assert (result["longitude"] == 28.0473).all()


def test_forecast_timestamp_is_parsed(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert result["forecast_timestamp"].iloc[0] == pd.Timestamp("2026-04-15T00:00")
    assert result["forecast_timestamp"].iloc[2] == pd.Timestamp("2026-04-15T02:00")


def test_forecast_date_derived_from_timestamp(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert result["forecast_date"].iloc[0] == dt.date(2026, 4, 15)


def test_values_copied_verbatim(sample_response: Dict[str, Any]) -> None:
    result = transform_weather_pandas(sample_response)
    assert result["temperature_c"].tolist() == [13.6, 13.5, 13.1]
    assert result["precipitation_mm"].tolist() == [0.0, 0.0, 0.1]
    assert result["wind_speed_kmh"].tolist() == [8.2, 7.9, 7.5]


def test_missing_hourly_key_raises() -> None:
    bad = {
        "latitude": -26.0,
        "longitude": 28.0,
        "hourly": {"time": [], "temperature_2m": []},  # missing other fields
    }
    with pytest.raises(ValueError, match="missing keys"):
        transform_weather_pandas(bad)
