# Architecture

## Overview

This pipeline follows an **ELT (Extract → Load → Transform)** pattern, using ClickHouse's SQL engine to perform transformations in-warehouse rather than in an external processing engine. It runs on a shared Apache Airflow instance alongside an unrelated project, demonstrating realistic multi-pipeline production infrastructure.

---

## High-Level Flow

```
┌─────────────────────┐
│   TfL Unified API    │
│  (3 live endpoints)  │
└──────────┬───────────┘
           │ Python requests
           ▼
┌─────────────────────┐
│   Airflow: extract   │  raw JSON → list[dict]
└──────────┬───────────┘
           │ XCom
           ▼
┌─────────────────────┐
│ Airflow: validate_raw│  row counts, ranges, nulls
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│  Airflow: transform   │  pandas cleaning + typing
└──────────┬───────────┘
           │ XCom
           ▼
┌─────────────────────┐
│    Airflow: load      │  insert into ClickHouse
└──────────┬───────────┘
           │
           ▼
┌─────────────────────────────────────┐
│         ClickHouse Warehouse          │
│  ┌─────────────┐    ┌──────────────┐ │
│  │   Bronze    │ →  │     Gold      │ │
│  │ raw tables  │ SQL│  daily marts  │ │
│  └─────────────┘    └──────────────┘ │
└──────────────┬────────────────────────┘
               │
               ▼
┌─────────────────────┐
│  Airflow: validate_   │  confirms row counts
│         load           │  in Bronze + Gold
└──────────┬───────────┘
           │
           ▼
┌─────────────────────┐
│   Apache Superset     │  dashboards
└───────────────────────┘
```

---

## Component Responsibilities

### Airflow (Orchestration)

Runs on a shared instance also hosting an unrelated NYC payroll pipeline. This project's DAG and Python module are mounted into the shared Airflow container via Docker volume mounts pointing at this repository's `dags/` and `tfl/` folders — no code from this project lives inside the other project's repository, and vice versa.

Schedule: `0 6 * * *` (daily at 6am UTC) — deliberately offset from the other pipeline's midnight schedule to avoid resource contention on the shared server.

### Extraction Layer (`tfl/extract.py`)

Pure Python, no Spark or heavy dependencies. Makes three sequential HTTP calls to the TfL Unified API:

1. `/Line/Mode/tube/Status` — all 11 tube lines in a single call
2. `/BikePoint` — all ~800 Santander Cycles docking stations in a single call
3. `/AirQuality` — current and forecast air quality bands

Each function returns a flat `list[dict]` ready for pandas conversion, with a shared `extracted_at` UTC timestamp applied across all three datasets for a given run.

### Transformation Layer (`tfl/transform.py`)

Lightweight pandas-based cleaning that happens **before** load — this is the one pre-load transform step, kept intentionally minimal:

- Type coercion (strings → int/float where needed)
- Text normalization (trim, title case)
- Derived columns: `occupancy_pct`, `availability_status`, `severity_label`, `forecast_score`

The bulk of transformation logic — aggregation, summarization — happens **after** load, inside ClickHouse via SQL. This is the defining characteristic of ELT versus ETL.

### Load Layer (`tfl/load.py`)

Two responsibilities:

1. **Bronze load** — inserts cleaned pandas DataFrames directly into ClickHouse `MergeTree` tables via `clickhouse-connect`
2. **Gold build** — runs `INSERT INTO ... SELECT` SQL statements inside ClickHouse to aggregate Bronze data into daily summary tables

This split means ClickHouse — not Python — does the heavy aggregation work, leveraging its columnar engine for speed.

### Warehouse (ClickHouse)

Self-hosted, open source, running as a Docker container dedicated to this project. Chosen over alternatives for this specific use case:

| Option considered | Why not used |
|---|---|
| DuckDB | Single-writer limitation conflicts with Airflow's task model |
| PostgreSQL | OLTP engine, not optimized for the kind of aggregation queries this project needs |
| ClickHouse | Columnar OLAP engine, native concurrent writes, native Superset connector |

Data model follows a simplified medallion architecture — Bronze (raw, append-only snapshots) and Gold (daily aggregates) — without an intermediate Silver layer, since the cleaning step is handled in the lightweight pandas transform stage instead.

### Visualization (Apache Superset)

Connects directly to ClickHouse's Gold tables. Open source, self-hosted, chosen over Grafana (used by the other project on this server) to demonstrate breadth across the open-source BI tooling landscape.

---

## Why This Differs From a Typical ETL Pipeline

| Aspect | Traditional ETL | This Project (ELT) |
|---|---|---|
| Transform location | External engine (e.g. Spark) | Inside the warehouse (SQL) |
| Data arrives in warehouse | After full transformation | As raw snapshots first |
| Aggregation engine | Application code | ClickHouse columnar engine |
| Schema flexibility | Must be defined before load | Can evolve Bronze independently of Gold |
| Best suited for | Complex multi-step business logic | High-volume analytical aggregation |

---

## Deployment Architecture

```
GitHub (tfl-pipeline repo)
        │ push to main
        ▼
  GitHub Actions CI/CD
        │ SSH
        ▼
  Production Server
  ├── Shared Airflow (webserver + scheduler)
  │     ↑ mounts this repo's dags/ and tfl/ folders
  ├── ClickHouse (dedicated to this project)
  ├── Superset (dedicated to this project)
  └── [unrelated NYC payroll project — isolated]
```

CI/CD restarts only the Airflow scheduler on deploy — ClickHouse, Superset, and the unrelated project's services are never touched by this pipeline's deployments, and vice versa.

---

## Design Decisions Log

| Decision | Reasoning |
|---|---|
| ELT over ETL | Matches modern data warehouse best practice; avoids unnecessary Spark overhead for this data volume |
| ClickHouse over DuckDB | DuckDB's single-writer model is incompatible with Airflow's concurrent task execution |
| Shared Airflow instance | Realistic multi-tenant production pattern; avoids duplicating orchestration infrastructure |
| 6am UTC schedule | Avoids resource contention with the other pipeline's midnight schedule on a shared server |
| Superset over Grafana | Demonstrates a different open-source BI tool than the other project on this server |
| No Silver layer | Pre-load pandas cleaning makes an intermediate Silver layer redundant for this dataset's complexity |
