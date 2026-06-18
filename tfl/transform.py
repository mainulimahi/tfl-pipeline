import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)


def transform_line_status(records: list[dict]) -> pd.DataFrame:
    """
    Clean and transform raw line status records.
    Silver layer: typed, validated, enriched.
    """
    if not records:
        raise ValueError("No line status records to transform.")

    df = pd.DataFrame(records)

    # Parse timestamps
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)

    # Add date partition column for ClickHouse
    df["extracted_date"] = df["extracted_at"].dt.date

    # Clean text fields
    df["line_name"] = df["line_name"].str.strip().str.title()
    df["status_description"] = df["status_description"].str.strip()
    df["disruption_reason"] = df["disruption_reason"].str.strip().where(
        df["disruption_reason"].notna(), None
    )

    # Ensure correct types
    df["status_severity"] = df["status_severity"].astype(int)
    df["is_disrupted"] = df["is_disrupted"].astype(bool)

    # Add severity label
    severity_map = {
        10: "Good Service",
        9: "Minor Delays",
        8: "Severe Delays",
        7: "Reduced Service",
        6: "Bus Service",
        5: "Part Suspended",
        4: "Planned Closure",
        3: "Part Closure",
        2: "Suspended",
        1: "Closed",
        0: "Special Service",
        20: "Not Running",
    }
    df["severity_label"] = df["status_severity"].map(severity_map).fillna("Unknown")

    logger.info(f"Transformed {len(df)} line status records")
    return df[[
        "extracted_at", "extracted_date", "line_id", "line_name",
        "status_severity", "severity_label", "status_description",
        "disruption_reason", "is_disrupted"
    ]]


def transform_bike_points(records: list[dict]) -> pd.DataFrame:
    """
    Clean and transform raw bike point records.
    Silver layer: typed, validated, with occupancy metrics.
    """
    if not records:
        raise ValueError("No bike point records to transform.")

    df = pd.DataFrame(records)

    # Parse timestamps
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)
    df["extracted_date"] = df["extracted_at"].dt.date

    # Clean text
    df["name"] = df["name"].str.strip()

    # Extract numeric ID from bike_point_id (e.g. "BikePoints_123" → 123)
    df["station_id"] = df["bike_point_id"].str.extract(r"(\d+)$").astype(int)

    # Ensure numeric types
    df["nb_bikes"] = df["nb_bikes"].astype(int)
    df["nb_empty_docks"] = df["nb_empty_docks"].astype(int)
    df["nb_docks"] = df["nb_docks"].astype(int)
    df["lat"] = df["lat"].astype(float)
    df["lon"] = df["lon"].astype(float)

    # Add occupancy percentage
    df["occupancy_pct"] = (
        (df["nb_bikes"] / df["nb_docks"].replace(0, 1)) * 100
    ).round(2)

    # Add availability status
    df["availability_status"] = pd.cut(
        df["occupancy_pct"],
        bins=[-1, 0, 25, 75, 101],
        labels=["Empty", "Low", "Available", "Full"]
    ).astype(str)

    logger.info(f"Transformed {len(df)} bike point records")
    return df[[
        "extracted_at", "extracted_date", "station_id", "bike_point_id",
        "name", "lat", "lon", "nb_bikes", "nb_empty_docks", "nb_docks",
        "occupancy_pct", "availability_status"
    ]]


def transform_air_quality(records: list[dict]) -> pd.DataFrame:
    """
    Clean and transform raw air quality records.
    Silver layer: typed, with numeric severity scores.
    """
    if not records:
        raise ValueError("No air quality records to transform.")

    df = pd.DataFrame(records)

    # Parse timestamps
    df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)
    df["extracted_date"] = df["extracted_at"].dt.date

    # Clean text
    text_cols = ["forecast_type", "forecast_band", "forecast_summary",
                 "no2_band", "o3_band", "pm10_band", "pm25_band", "so2_band"]
    for col in text_cols:
        df[col] = df[col].str.strip()

    # Map band to numeric score (for dashboard sorting)
    band_score = {
        "Low": 1,
        "Moderate": 2,
        "High": 3,
        "Very High": 4
    }
    df["forecast_score"] = df["forecast_band"].map(band_score).fillna(0).astype(int)

    logger.info(f"Transformed {len(df)} air quality records")
    return df[[
        "extracted_at", "extracted_date", "forecast_type", "forecast_band",
        "forecast_score", "forecast_summary", "no2_band", "o3_band",
        "pm10_band", "pm25_band", "so2_band"
    ]]


def transform_all(raw_data: dict[str, list[dict]]) -> dict[str, pd.DataFrame]:
    """
    Transform all raw TfL data.
    Returns dict of cleaned DataFrames.
    """
    logger.info("Starting transformations...")

    transformed = {
        "line_status": transform_line_status(raw_data["line_status"]),
        "bike_points": transform_bike_points(raw_data["bike_points"]),
        "air_quality": transform_air_quality(raw_data["air_quality"]),
    }

    for name, df in transformed.items():
        logger.info(f"{name}: {len(df)} rows, {len(df.columns)} columns")

    return transformed