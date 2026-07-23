"""
One-off migration: repartition existing Bronze/Gold tables by date.

Why: init_database() now creates tables with PARTITION BY toYYYYMMDD(<date
column>) so load_dataframe()/build_gold_tables() can use atomic DROP
PARTITION instead of an async ALTER ... DELETE mutation (see tfl/load.py).
CREATE TABLE IF NOT EXISTS does not retroactively add a PARTITION BY to a
table that already exists — ClickHouse's partition key is immutable after
creation. Tables created before this change must be swapped out.

This does NOT run automatically and is NOT called from the DAG. Run it
manually, once, against the real ClickHouse instance:

    python scripts/migrate_partition_by_date.py

Safe to re-run: each table swap is skipped if the target table already
reports the expected partition key. Old tables are renamed to
"<table>_pre_partition_backup" rather than dropped — remove them yourself
once you've confirmed the swap looks right.
"""
import logging
import os

from tfl.load import get_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# (table, date_column, order_by_columns)
TABLES = [
    ("bronze_line_status", "extracted_date", "(extracted_date, line_id)"),
    ("bronze_bike_points", "extracted_date", "(extracted_date, station_id)"),
    ("bronze_air_quality", "extracted_date", "(extracted_date, forecast_type)"),
    ("gold_daily_line_summary", "summary_date", "(summary_date, line_id)"),
    ("gold_hourly_line_summary", "summary_date", "(summary_date, summary_hour, line_id)"),
    ("gold_daily_bike_summary", "summary_date", "(summary_date, station_id)"),
    ("gold_hourly_bike_summary", "summary_date", "(summary_date, summary_hour, station_id)"),
    ("gold_daily_air_quality", "summary_date", "(summary_date, forecast_type)"),
]


def already_partitioned(client, db: str, table: str) -> bool:
    result = client.query(
        "SELECT partition_key FROM system.tables WHERE database = {db:String} AND name = {table:String}",
        parameters={"db": db, "table": table},
    )
    if not result.result_rows:
        raise RuntimeError(f"Table {db}.{table} does not exist — run init_database() first.")
    return bool(result.result_rows[0][0])


def migrate_table(client, db: str, table: str, date_col: str, order_by: str) -> None:
    if already_partitioned(client, db, table):
        logger.info(f"{table}: already partitioned, skipping")
        return

    new_table = f"{table}_new"
    backup_table = f"{table}_pre_partition_backup"

    logger.info(f"{table}: creating {new_table} with PARTITION BY toYYYYMMDD({date_col})")
    client.command(f"DROP TABLE IF EXISTS {db}.{new_table}")
    client.command(f"""
        CREATE TABLE {db}.{new_table} AS {db}.{table}
        ENGINE = MergeTree()
        PARTITION BY toYYYYMMDD({date_col})
        ORDER BY {order_by}
    """)

    logger.info(f"{table}: copying data into {new_table}")
    client.command(f"INSERT INTO {db}.{new_table} SELECT * FROM {db}.{table}")

    old_count = client.query(f"SELECT count(*) FROM {db}.{table}").first_row[0]
    new_count = client.query(f"SELECT count(*) FROM {db}.{new_table}").first_row[0]
    if old_count != new_count:
        raise RuntimeError(
            f"{table}: row count mismatch after copy ({old_count} vs {new_count}) — "
            f"aborting swap, {new_table} left in place for inspection."
        )

    logger.info(f"{table}: row counts match ({new_count}), swapping table names")
    client.command(f"DROP TABLE IF EXISTS {db}.{backup_table}")
    client.command(
        f"RENAME TABLE {db}.{table} TO {db}.{backup_table}, "
        f"{db}.{new_table} TO {db}.{table}"
    )
    logger.info(f"{table}: migrated. Old data preserved as {backup_table} — drop it once verified.")


def main() -> None:
    db = os.environ.get("CLICKHOUSE_DB", "tfl_pipeline")
    client = get_client()
    try:
        for table, date_col, order_by in TABLES:
            migrate_table(client, db, table, date_col, order_by)
    finally:
        client.close()
    logger.info("Migration complete.")


if __name__ == "__main__":
    main()
