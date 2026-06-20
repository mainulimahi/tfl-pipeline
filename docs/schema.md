# Database Schema

## Overview

The pipeline uses a single ClickHouse database (`tfl_pipeline`) with a two-layer medallion structure:

| Layer | Purpose |
|---|---|
| Bronze | Raw, cleaned snapshots — one row per entity per pipeline run |
| Gold | Daily aggregated summaries, built via SQL from Bronze |

All tables use the `MergeTree` engine, ClickHouse's standard storage engine for analytical workloads.

---

## Bronze Layer

### `bronze_line_status`

One row per tube line per pipeline run.

| Column | Type | Description |
|---|---|---|
| `extracted_at` | DateTime64(6, 'UTC') | Exact timestamp of this snapshot |
| `extracted_date` | Date | Date partition (derived from `extracted_at`) |
| `line_id` | String | TfL line identifier (e.g. `victoria`) |
| `line_name` | String | Display name (e.g. `Victoria`) |
| `status_severity` | Int32 | TfL severity code (0–20, 10 = Good Service) |
| `severity_label` | String | Human-readable severity (e.g. `Minor Delays`) |
| `status_description` | String | TfL's status description |
| `disruption_reason` | Nullable(String) | Free-text disruption reason, if any |
| `is_disrupted` | Bool | True if severity ≠ 10 (Good Service) |

**Ordering key:** `(extracted_date, line_id)`
**Typical row count per run:** 11–15 (one or more rows per line if multiple statuses apply)

---

### `bronze_bike_points`

One row per Santander Cycles docking station per pipeline run.

| Column | Type | Description |
|---|---|---|
| `extracted_at` | DateTime64(6, 'UTC') | Exact timestamp of this snapshot |
| `extracted_date` | Date | Date partition |
| `station_id` | Int32 | Numeric station ID extracted from `bike_point_id` |
| `bike_point_id` | String | TfL's full station identifier (e.g. `BikePoints_1`) |
| `name` | String | Station name and area |
| `lat` | Float64 | Latitude |
| `lon` | Float64 | Longitude |
| `nb_bikes` | Int32 | Bikes currently available |
| `nb_empty_docks` | Int32 | Empty docks available |
| `nb_docks` | Int32 | Total docks at the station |
| `occupancy_pct` | Float64 | `nb_bikes / nb_docks * 100`, rounded to 2dp |
| `availability_status` | String | `Empty`, `Low`, `Available`, or `Full` based on occupancy band |

**Ordering key:** `(extracted_date, station_id)`
**Typical row count per run:** ~795–800

---

### `bronze_air_quality`

One row per forecast type (Current / Future) per pipeline run.

| Column | Type | Description |
|---|---|---|
| `extracted_at` | DateTime64(6, 'UTC') | Exact timestamp of this snapshot |
| `extracted_date` | Date | Date partition |
| `forecast_type` | String | `Current` or `Future` |
| `forecast_band` | String | `Low`, `Moderate`, `High`, or `Very High` |
| `forecast_score` | Int32 | Numeric mapping of band (Low=1 → Very High=4) |
| `forecast_summary` | String | TfL's full text summary |
| `no2_band` | String | Nitrogen dioxide band |
| `o3_band` | String | Ozone band |
| `pm10_band` | String | PM10 particulate band |
| `pm25_band` | String | PM2.5 particulate band |
| `so2_band` | String | Sulphur dioxide band |

**Ordering key:** `(extracted_date, forecast_type)`
**Typical row count per run:** 2

---

## Gold Layer

Built via `INSERT INTO ... SELECT` SQL run directly inside ClickHouse after each Bronze load — this is the transformation step in the ELT pattern.

### `gold_daily_line_summary`

One row per tube line per day, aggregated across all snapshots for that day.

| Column | Type | Description |
|---|---|---|
| `summary_date` | Date | The day being summarized |
| `line_id` | String | TfL line identifier |
| `line_name` | String | Display name |
| `total_snapshots` | Int32 | Number of status records for this line on this day |
| `disrupted_snapshots` | Int32 | Number of those records marked disrupted |
| `disruption_rate_pct` | Float64 | `disrupted_snapshots / total_snapshots * 100` |
| `min_severity` | Int32 | Best (lowest disruption) status seen |
| `max_severity` | Int32 | Worst (highest disruption) status seen |
| `avg_severity` | Float64 | Average severity score across the day |

**Ordering key:** `(summary_date, line_id)`

---

### `gold_daily_bike_summary`

One row per bike station per day.

| Column | Type | Description |
|---|---|---|
| `summary_date` | Date | The day being summarized |
| `station_id` | Int32 | Numeric station ID |
| `name` | String | Station name |
| `lat` | Float64 | Latitude |
| `lon` | Float64 | Longitude |
| `avg_bikes` | Float64 | Average bikes available across the day's snapshots |
| `avg_empty_docks` | Float64 | Average empty docks |
| `avg_occupancy_pct` | Float64 | Average occupancy percentage |
| `total_snapshots` | Int32 | Number of snapshots aggregated |

**Ordering key:** `(summary_date, station_id)`

---

### `gold_daily_air_quality`

One row per forecast type per day.

| Column | Type | Description |
|---|---|---|
| `summary_date` | Date | The day being summarized |
| `forecast_type` | String | `Current` or `Future` |
| `forecast_band` | String | Air quality band for this forecast |
| `forecast_score` | Int32 | Max numeric score seen for this forecast type that day |
| `no2_band` | String | Representative NO2 band |
| `o3_band` | String | Representative O3 band |
| `pm10_band` | String | Representative PM10 band |
| `pm25_band` | String | Representative PM2.5 band |
| `so2_band` | String | Representative SO2 band |

**Ordering key:** `(summary_date, forecast_type)`

---

## Data Flow

```
TfL API (raw JSON)
       ↓
extract.py → list[dict] (3 datasets, timestamped)
       ↓
validate_raw (row counts, ranges, nulls)
       ↓
transform.py → pandas DataFrames (typed, cleaned)
       ↓
load.py
   ├── init_database()       creates DB + 6 tables if not exist
   ├── load_dataframe() × 3  inserts into Bronze tables
   └── build_gold_tables()   runs SQL to populate Gold tables
       ↓
validate_load (confirms Bronze + Gold row counts for today)
```

---

## Useful Queries

```sql
-- Total rows across all Bronze tables
SELECT
    (SELECT count() FROM bronze_line_status)  AS line_status_rows,
    (SELECT count() FROM bronze_bike_points)  AS bike_points_rows,
    (SELECT count() FROM bronze_air_quality)  AS air_quality_rows;

-- Disruption rate by line, most recent day
SELECT line_name, disruption_rate_pct, avg_severity
FROM gold_daily_line_summary
WHERE summary_date = (SELECT max(summary_date) FROM gold_daily_line_summary)
ORDER BY disruption_rate_pct DESC;

-- Busiest (highest avg occupancy) bike stations
SELECT name, avg_occupancy_pct, avg_bikes
FROM gold_daily_bike_summary
WHERE summary_date = (SELECT max(summary_date) FROM gold_daily_bike_summary)
ORDER BY avg_occupancy_pct DESC
LIMIT 10;

-- Air quality trend over the last 30 days
SELECT summary_date, forecast_type, forecast_band, forecast_score
FROM gold_daily_air_quality
WHERE summary_date >= today() - 30
ORDER BY summary_date DESC;

-- Pipeline run history (distinct extraction timestamps)
SELECT DISTINCT extracted_date, count() AS snapshots_that_day
FROM bronze_line_status
GROUP BY extracted_date
ORDER BY extracted_date DESC;
```

---

## Data Source

| Field | Value |
|---|---|
| Provider | Transport for London (TfL) |
| API | TfL Unified API |
| Base URL | `https://api.tfl.gov.uk` |
| Authentication | `app_key` query parameter |
| Rate limit | 500 requests/minute (free tier) |
| Update frequency | Real-time (line status, bike points), daily (air quality forecast) |
