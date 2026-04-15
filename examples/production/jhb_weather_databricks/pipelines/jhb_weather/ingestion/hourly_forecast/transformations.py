"""Johannesburg hourly-forecast transformation.

Open-Meteo returns a single JSON document with parallel arrays under
``hourly`` (one value per hour, one array per variable). The REST reader
yields this as a single-row Spark DataFrame with nested array columns.
This module explodes those arrays into a tidy one-row-per-hour frame
suitable for a Delta table.

Two implementations are provided and must stay in lock-step:

* ``transform_weather_spark``  - PySpark path; runs in production.
* ``transform_weather_pandas`` - pure pandas path; exercised by the unit
  tests so the business logic can be validated without a SparkSession.
"""

from __future__ import annotations

from typing import Any, Dict

from ubunye.core.interfaces import Task

OUTPUT_COLUMNS = (
    "latitude",
    "longitude",
    "forecast_timestamp",
    "forecast_date",
    "temperature_c",
    "relative_humidity_pct",
    "precipitation_mm",
    "wind_speed_kmh",
)


def transform_weather_pandas(response: Dict[str, Any]) -> "Any":
    """Pandas implementation over the raw Open-Meteo response dict."""
    import pandas as pd

    hourly = response.get("hourly") or {}
    required = {"time", "temperature_2m", "relative_humidity_2m", "precipitation", "wind_speed_10m"}
    missing = required - set(hourly)
    if missing:
        raise ValueError(f"Open-Meteo hourly payload missing keys: {sorted(missing)}")

    df = pd.DataFrame(
        {
            "forecast_timestamp": pd.to_datetime(hourly["time"]),
            "temperature_c": hourly["temperature_2m"],
            "relative_humidity_pct": hourly["relative_humidity_2m"],
            "precipitation_mm": hourly["precipitation"],
            "wind_speed_kmh": hourly["wind_speed_10m"],
        }
    )
    df["latitude"] = response["latitude"]
    df["longitude"] = response["longitude"]
    df["forecast_date"] = df["forecast_timestamp"].dt.date
    return df[list(OUTPUT_COLUMNS)]


def transform_weather_spark(df: "Any") -> "Any":
    """Spark implementation - identical column shape to the pandas version.

    Input ``df`` is a single-row frame with nested fields:
        latitude:  double
        longitude: double
        hourly:    struct<time: array<string>,
                          temperature_2m: array<double>,
                          relative_humidity_2m: array<long>,
                          precipitation: array<double>,
                          wind_speed_10m: array<double>>
    """
    from pyspark.sql import functions as F

    zipped = F.arrays_zip(
        F.col("hourly.time").alias("time"),
        F.col("hourly.temperature_2m").alias("temperature_2m"),
        F.col("hourly.relative_humidity_2m").alias("relative_humidity_2m"),
        F.col("hourly.precipitation").alias("precipitation"),
        F.col("hourly.wind_speed_10m").alias("wind_speed_10m"),
    )

    return (
        df.select(F.col("latitude"), F.col("longitude"), F.explode(zipped).alias("r"))
        .select(
            F.col("latitude").cast("double").alias("latitude"),
            F.col("longitude").cast("double").alias("longitude"),
            F.to_timestamp(F.col("r.time")).alias("forecast_timestamp"),
            F.col("r.temperature_2m").cast("double").alias("temperature_c"),
            F.col("r.relative_humidity_2m").cast("double").alias("relative_humidity_pct"),
            F.col("r.precipitation").cast("double").alias("precipitation_mm"),
            F.col("r.wind_speed_10m").cast("double").alias("wind_speed_kmh"),
        )
        .withColumn("forecast_date", F.to_date("forecast_timestamp"))
        .select(*OUTPUT_COLUMNS)
    )


class JhbHourlyForecast(Task):
    """Ubunye Task: ingest JHB hourly forecast from Open-Meteo into Unity Catalog."""

    def transform(self, sources: Dict[str, Any]) -> Dict[str, Any]:
        forecast = sources["forecast"]
        return {"jhb_hourly_forecast": transform_weather_spark(forecast)}
