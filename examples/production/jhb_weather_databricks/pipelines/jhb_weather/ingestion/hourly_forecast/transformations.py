"""Johannesburg hourly-forecast transformation.

Open-Meteo returns a single JSON document with parallel arrays under
``hourly`` (one value per hour, one array per variable). The REST reader
yields this as a single-row Spark DataFrame with nested array columns.
This module explodes those arrays into a tidy one-row-per-hour frame
suitable for a Delta table.

Single implementation — Spark only. Tested via a local SparkSession
fixture in ``tests/test_transformations.py``.
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


def transform_weather(df: "Any") -> "Any":
    """Explode the Open-Meteo hourly arrays into a tidy DataFrame.

    Input schema (as produced by the ``rest_api`` reader):
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
        return {"jhb_hourly_forecast": transform_weather(forecast)}
