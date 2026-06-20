# TfL Transport Intelligence Pipeline

An ELT data pipeline that extracts live London transport data from the Transport for London (TfL) Unified API, loads it into a ClickHouse OLAP warehouse, transforms it via SQL using a medallion architecture, and visualizes it through Apache Superset — fully orchestrated with Apache Airflow and deployed with zero-downtime CI/CD via GitHub Actions.

---

## Architecture

```
TfL Unified API
       ↓ (daily @ 6am UTC, automatic)
  Apache Airflow (Orchestration)
       ↓
  Bronze layer (raw snapshots)        ← ClickHouse
       ↓
  SQL transformation (in-warehouse)
       ↓
  Gold layer (daily aggregations)     ← ClickHouse
       ↓
  Apache Superset (Dashboards)
```

---

## Why ELT instead of ETL

Unlike a traditional ETL pattern (Extract → Transform → Load), this project uses **ELT** (Extract → Load → Transform):

- Raw data is loaded into ClickHouse first with minimal processing
- All cleaning, typing, and aggregation happens via SQL **inside** ClickHouse
- ClickHouse's columnar engine is purpose-built for these aggregations — faster than an external transform engine for this data volume
- This is the modern data warehouse pattern used by Snowflake, BigQuery, and dbt-based stacks

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.10.5 |
| Extraction | Python `requests` |
| Warehouse | ClickHouse (OLAP, columnar) |
| Transformation | SQL (in-warehouse, ELT) |
| Visualization | Apache Superset |
| Containerization | Docker Compose |
| Reverse Proxy | Nginx |
| CI/CD | GitHub Actions |
| Language | Python 3.11 |
| Data Source | TfL Unified API |

---

## Project Structure

```
tfl-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml          # CI/CD - deploys DAG + module to shared Airflow
├── dags/
│   └── tfl_pipeline.py         # Airflow DAG definition
├── tfl/
│   ├── __init__.py
│   ├── extract.py              # TfL API extraction logic
│   ├── transform.py            # Pandas cleaning/typing (pre-load)
│   └── load.py                 # ClickHouse loader + Gold table builder
├── docs/
│   ├── architecture.md
│   └── schema.md
├── tests/
│   ├── __init__.py
│   ├── test_extract.py
│   └── test_transform.py
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── requirements.txt
└── README.md
```

---

## Shared Airflow Instance

This pipeline runs on the **same Airflow instance** as a separate NYC payroll project, demonstrating a realistic multi-pipeline production setup:

- Airflow webserver/scheduler are shared infrastructure
- This repo's DAG and source code are mounted into the shared Airflow container via Docker volumes
- ClickHouse and Superset are dedicated to this project only
- Each project has its own GitHub repository and independent CI/CD pipeline
- DAGs are distinguishable in the Airflow UI via tags: `tfl`, `transport`, `clickhouse`, `london`

---

## Pipeline DAG

```
start → extract → validate_raw → transform → load → validate_load → end
```

| Task | Description |
|---|---|
| `extract` | Fetches tube line status, bike point availability, and air quality from TfL API |
| `validate_raw` | Checks row counts, severity ranges, and negative values before transformation |
| `transform` | Cleans and types raw data using pandas before loading |
| `load` | Inserts into ClickHouse Bronze tables, then builds Gold aggregations via SQL |
| `validate_load` | Queries ClickHouse to confirm Bronze and Gold tables were populated correctly |

---

## Data Sources

| Endpoint | Data | Update Frequency |
|---|---|---|
| `/Line/Mode/tube/Status` | Disruption status for all 11 London Underground lines | Every few minutes |
| `/BikePoint` | Live availability for ~800 Santander Cycles docking stations | Every few minutes |
| `/AirQuality` | London air quality forecast (current + next day) | Daily |

Unlike static datasets, TfL data genuinely changes on every pipeline run — disruptions, bike availability, and air quality forecasts are all live, time-sensitive data.

---

## Medallion Architecture

| Layer | Tables | Purpose |
|---|---|---|
| Bronze | `bronze_line_status`, `bronze_bike_points`, `bronze_air_quality` | Raw cleaned snapshots, one row per entity per run |
| Gold | `gold_daily_line_summary`, `gold_daily_bike_summary`, `gold_daily_air_quality` | Daily aggregated metrics for dashboards |

See [`docs/schema.md`](docs/schema.md) for full table definitions.

---

## Data Quality Checks

| Check | Stage | Rule |
|---|---|---|
| Minimum line count | `validate_raw` | At least 5 tube lines returned |
| Valid severity range | `validate_raw` | Status severity between 0 and 20 |
| Minimum bike points | `validate_raw` | At least 100 bike stations returned |
| No negative bike counts | `validate_raw` | `nb_bikes` and `nb_docks` must be ≥ 0 |
| Valid air quality bands | `validate_raw` | Forecast band must be one of: Low, Moderate, High, Very High |
| Bronze row counts | `validate_load` | Confirms minimum row thresholds landed in ClickHouse for today |
| Gold table population | `validate_load` | Confirms all 3 Gold tables have rows for today |

---

## Email Alerts

Automatic email alerts are sent on any task failure, including the exception message and a direct link to task logs in the Airflow UI.

---

## Getting Started (Local Development)

### Prerequisites

- Python 3.11+
- A TfL API key (free, register at the [TfL API Portal](https://api-portal.tfl.gov.uk))

### Setup

**1. Clone the repository:**
```bash
git clone https://github.com/mainulimahi/tfl-pipeline.git
cd tfl-pipeline
```

**2. Create a virtual environment:**
```bash
python -m venv venv
venv\Scripts\activate   # Windows
```

**3. Install dependencies:**
```bash
pip install -r requirements.txt
```

**4. Create your `.env` file:**
```bash
cp .env.example .env
# Fill in your TFL_APP_KEY and ClickHouse credentials
```

**5. Test extraction locally:**
```bash
python -c "from tfl.extract import fetch_all; print(fetch_all())"
```

---

## Deployment

This repo deploys into a shared Airflow instance hosted on a separate server. CI/CD on every push to `main`:

1. SSHs into the production server
2. Pulls the latest code into this repo's dedicated folder
3. Restarts only the Airflow scheduler to pick up DAG changes
4. Verifies the deployment without affecting other running services (ClickHouse, Superset, or the unrelated NYC payroll pipeline)

---

## Environment Variables

See `.env.example` for all required variables:

| Variable | Description |
|---|---|
| `TFL_APP_KEY` | TfL Unified API primary key |
| `CLICKHOUSE_HOST` | ClickHouse service hostname |
| `CLICKHOUSE_PORT` | ClickHouse HTTP port |
| `CLICKHOUSE_USER` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | ClickHouse password |
| `CLICKHOUSE_DB` | ClickHouse database name |
| `AIRFLOW__CORE__FERNET_KEY` | Shared Airflow encryption key |
| `AIRFLOW__WEBSERVER__SECRET_KEY` | Shared Airflow Flask secret key |
| `ALERT_EMAIL` | Email address to receive failure alerts |

---

## License

MIT
