import logging
import os
from typing import Any

import pandas as pd
import clickhouse_connect

logger = logging.getLogger(__name__)


def get_client() -> Any:
    """Create and return a ClickHouse client using environment variables."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "tfl_pipeline"),
    )


def init_database() -> None:
    """Create database and all tables if they don't exist."""
    client = get_client()
    db = os.environ.get("CLICKHOUSE_DB", "tfl_pipeline")

    # Create database
    client.command(f"CREATE DATABASE IF NOT EXISTS {db}")
    logger.info(f"Database '{db}' ready")

    # Bronze: Line Status
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_line_status (
            extracted_at     DateTime64(6, 'UTC'),
            extracted_date   Date,
            line_id          String,
            line_name        String,
            status_severity  Int32,
            severity_label   String,
            status_description String,
            disruption_reason  Nullable(String),
            is_disrupted     Bool
        )
        ENGINE = MergeTree()
        ORDER BY (extracted_date, line_id)
    """)

    # Bronze: Bike Points
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_bike_points (
            extracted_at        DateTime64(6, 'UTC'),
            extracted_date      Date,
            station_id          Int32,
            bike_point_id       String,
            name                String,
            lat                 Float64,
            lon                 Float64,
            nb_bikes            Int32,
            nb_empty_docks      Int32,
            nb_docks            Int32,
            occupancy_pct       Float64,
            availability_status String
        )
        ENGINE = MergeTree()
        ORDER BY (extracted_date, station_id)
    """)

    # Bronze: Air Quality
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.bronze_air_quality (
            extracted_at     DateTime64(6, 'UTC'),
            extracted_date   Date,
            forecast_type    String,
            forecast_band    String,
            forecast_score   Int32,
            forecast_summary String,
            no2_band         String,
            o3_band          String,
            pm10_band        String,
            pm25_band        String,
            so2_band         String
        )
        ENGINE = MergeTree()
        ORDER BY (extracted_date, forecast_type)
    """)

    # Gold: Daily Line Summary
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold_daily_line_summary (
            summary_date         Date,
            line_id              String,
            line_name            String,
            total_snapshots      Int32,
            disrupted_snapshots  Int32,
            disruption_rate_pct  Float64,
            min_severity         Int32,
            max_severity         Int32,
            avg_severity         Float64
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date, line_id)
    """)

    # Gold: Hourly Line Summary (intraday granularity)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold_hourly_line_summary (
            summary_date         Date,
            summary_hour         UInt8,
            line_id              String,
            line_name            String,
            total_snapshots      Int32,
            disrupted_snapshots  Int32,
            disruption_rate_pct  Float64,
            avg_severity         Float64
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date, summary_hour, line_id)
    """)

    # Gold: Daily Bike Summary
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold_daily_bike_summary (
            summary_date        Date,
            station_id          Int32,
            name                String,
            lat                 Float64,
            lon                 Float64,
            avg_bikes           Float64,
            avg_empty_docks     Float64,
            avg_occupancy_pct   Float64,
            total_snapshots     Int32
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date, station_id)
    """)

    # Gold: Hourly Bike Summary (intraday granularity)
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold_hourly_bike_summary (
            summary_date        Date,
            summary_hour        UInt8,
            station_id          Int32,
            name                String,
            lat                 Float64,
            lon                 Float64,
            avg_bikes           Float64,
            avg_empty_docks     Float64,
            avg_occupancy_pct   Float64,
            total_snapshots     Int32
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date, summary_hour, station_id)
    """)

    # Gold: Daily Air Quality Summary
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db}.gold_daily_air_quality (
            summary_date    Date,
            forecast_type   String,
            forecast_band   String,
            forecast_score  Int32,
            no2_band        String,
            o3_band         String,
            pm10_band       String,
            pm25_band       String,
            so2_band        String
        )
        ENGINE = MergeTree()
        ORDER BY (summary_date, forecast_type)
    """)

    logger.info("All tables ready")
    client.close()


def load_dataframe(df: pd.DataFrame, table: str, extracted_date: str) -> None:
    """Load a pandas DataFrame into a ClickHouse table, replacing any existing rows for this date."""
    client = get_client()
    db = os.environ.get("CLICKHOUSE_DB", "tfl_pipeline")
    full_table = f"{db}.{table}"

    client.command(f"""
        ALTER TABLE {full_table}
        DELETE WHERE extracted_date = '{extracted_date}'
    """)
    client.insert_df(full_table, df)
    logger.info(f"Loaded {len(df)} rows into {full_table} for {extracted_date}")
    client.close()


def build_gold_tables(extracted_date: str) -> None:
    """
    Build Gold layer aggregations from Bronze tables.
    Runs SQL inside ClickHouse — ELT pattern.
    Idempotent: safe to call multiple times for the same date.

    Builds both daily and hourly grains for lines and bikes,
    daily-only for air quality (low intraday value at 2 rows/day).
    """
    client = get_client()
    db = os.environ.get("CLICKHOUSE_DB", "tfl_pipeline")

    # --- Line summary: daily ---
    client.command(f"""
        ALTER TABLE {db}.gold_daily_line_summary
        DELETE WHERE summary_date = '{extracted_date}'
    """)
    client.command(f"""
        INSERT INTO {db}.gold_daily_line_summary
        SELECT
            toDate(extracted_date)          AS summary_date,
            line_id,
            line_name,
            count(*)                        AS total_snapshots,
            countIf(is_disrupted = true)    AS disrupted_snapshots,
            round(
                countIf(is_disrupted = true) * 100.0 / count(*), 2
            )                               AS disruption_rate_pct,
            min(status_severity)            AS min_severity,
            max(status_severity)            AS max_severity,
            round(avg(status_severity), 2)  AS avg_severity
        FROM {db}.bronze_line_status
        WHERE extracted_date = '{extracted_date}'
        GROUP BY extracted_date, line_id, line_name
    """)
    logger.info(f"Rebuilt gold_daily_line_summary for {extracted_date}")

    # --- Line summary: hourly ---
    client.command(f"""
        ALTER TABLE {db}.gold_hourly_line_summary
        DELETE WHERE summary_date = '{extracted_date}'
    """)
    client.command(f"""
        INSERT INTO {db}.gold_hourly_line_summary
        SELECT
            toDate(extracted_at)            AS summary_date,
            toHour(extracted_at)            AS summary_hour,
            line_id,
            line_name,
            count(*)                        AS total_snapshots,
            countIf(is_disrupted = true)    AS disrupted_snapshots,
            round(
                countIf(is_disrupted = true) * 100.0 / count(*), 2
            )                               AS disruption_rate_pct,
            round(avg(status_severity), 2)  AS avg_severity
        FROM {db}.bronze_line_status
        WHERE extracted_date = '{extracted_date}'
        GROUP BY summary_date, summary_hour, line_id, line_name
    """)
    logger.info(f"Rebuilt gold_hourly_line_summary for {extracted_date}")

    # --- Bike summary: daily ---
    client.command(f"""
        ALTER TABLE {db}.gold_daily_bike_summary
        DELETE WHERE summary_date = '{extracted_date}'
    """)
    client.command(f"""
        INSERT INTO {db}.gold_daily_bike_summary
        SELECT
            toDate(extracted_date)      AS summary_date,
            station_id,
            name,
            lat,
            lon,
            round(avg(nb_bikes), 2)        AS avg_bikes,
            round(avg(nb_empty_docks), 2)  AS avg_empty_docks,
            round(avg(occupancy_pct), 2)   AS avg_occupancy_pct,
            count(*)                       AS total_snapshots
        FROM {db}.bronze_bike_points
        WHERE extracted_date = '{extracted_date}'
        GROUP BY extracted_date, station_id, name, lat, lon
    """)
    logger.info(f"Rebuilt gold_daily_bike_summary for {extracted_date}")

    # --- Bike summary: hourly ---
    client.command(f"""
        ALTER TABLE {db}.gold_hourly_bike_summary
        DELETE WHERE summary_date = '{extracted_date}'
    """)
    client.command(f"""
        INSERT INTO {db}.gold_hourly_bike_summary
        SELECT
            toDate(extracted_at)        AS summary_date,
            toHour(extracted_at)        AS summary_hour,
            station_id,
            name,
            lat,
            lon,
            round(avg(nb_bikes), 2)        AS avg_bikes,
            round(avg(nb_empty_docks), 2)  AS avg_empty_docks,
            round(avg(occupancy_pct), 2)   AS avg_occupancy_pct,
            count(*)                       AS total_snapshots
        FROM {db}.bronze_bike_points
        WHERE extracted_date = '{extracted_date}'
        GROUP BY summary_date, summary_hour, station_id, name, lat, lon
    """)
    logger.info(f"Rebuilt gold_hourly_bike_summary for {extracted_date}")

    # --- Air quality summary: daily only ---
    client.command(f"""
        ALTER TABLE {db}.gold_daily_air_quality
        DELETE WHERE summary_date = '{extracted_date}'
    """)
    client.command(f"""
        INSERT INTO {db}.gold_daily_air_quality
        SELECT
            toDate(extracted_date) AS summary_date,
            forecast_type,
            forecast_band,
            max(forecast_score)    AS forecast_score,
            any(no2_band)          AS no2_band,
            any(o3_band)           AS o3_band,
            any(pm10_band)         AS pm10_band,
            any(pm25_band)         AS pm25_band,
            any(so2_band)          AS so2_band
        FROM {db}.bronze_air_quality
        WHERE extracted_date = '{extracted_date}'
        GROUP BY extracted_date, forecast_type, forecast_band
    """)
    logger.info(f"Rebuilt gold_daily_air_quality for {extracted_date}")

    client.close()


def load_all(transformed: dict, extracted_date: str) -> None:
    """
    Load all transformed DataFrames into ClickHouse Bronze tables,
    then build Gold aggregations (daily + hourly grains).
    """
    logger.info("Initialising database and tables...")
    init_database()

    logger.info("Loading Bronze tables...")
    load_dataframe(transformed["line_status"], "bronze_line_status", extracted_date)
    load_dataframe(transformed["bike_points"], "bronze_bike_points", extracted_date)
    load_dataframe(transformed["air_quality"], "bronze_air_quality", extracted_date)

    logger.info("Building Gold tables...")
    build_gold_tables(extracted_date)

    logger.info("Load complete ✅")