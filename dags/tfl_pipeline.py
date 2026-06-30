import os
from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowFailException

from tfl.extract import fetch_all
from tfl.transform import transform_all
from tfl.load import load_all


def extract(**context) -> None:
    """Fetch all TfL data and push to XCom."""
    raw_data = fetch_all()

    # Validate all 3 endpoints returned data
    if not raw_data.get("line_status"):
        raise AirflowFailException("TfL API returned empty line status data.")
    if not raw_data.get("bike_points"):
        raise AirflowFailException("TfL API returned empty bike points data.")
    if not raw_data.get("air_quality"):
        raise AirflowFailException("TfL API returned empty air quality data.")

    print(f"Extracted — "
          f"line_status: {len(raw_data['line_status'])} rows, "
          f"bike_points: {len(raw_data['bike_points'])} rows, "
          f"air_quality: {len(raw_data['air_quality'])} rows")

    context["ti"].xcom_push(key="raw_data", value=raw_data)


def validate_raw(**context) -> None:
    """Validate raw extracted data before transformation."""
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")

    if not raw_data:
        raise AirflowFailException("No raw data found in XCom.")

    # Line status checks
    line_status = raw_data["line_status"]
    if len(line_status) < 5:
        raise AirflowFailException(
            f"Expected at least 5 tube lines, got {len(line_status)}."
        )

    severities = [r["status_severity"] for r in line_status]
    if any(s < 0 or s > 20 for s in severities):
        raise AirflowFailException("Invalid status severity values detected.")

    # Bike points checks
    bike_points = raw_data["bike_points"]
    if len(bike_points) < 100:
        raise AirflowFailException(
            f"Expected at least 100 bike points, got {len(bike_points)}."
        )

    negative_bikes = [b for b in bike_points if b["nb_bikes"] < 0 or b["nb_docks"] < 0]
    if negative_bikes:
        raise AirflowFailException(
            f"Found {len(negative_bikes)} bike points with negative values."
        )

    # Air quality checks
    air_quality = raw_data["air_quality"]
    if len(air_quality) == 0:
        raise AirflowFailException("Air quality data is empty.")

    valid_bands = {"Low", "Moderate", "High", "Very High"}
    invalid_bands = [
        r for r in air_quality
        if r["forecast_band"] not in valid_bands
    ]
    if invalid_bands:
        raise AirflowFailException(
            f"Found {len(invalid_bands)} records with invalid forecast bands."
        )

    print(f"Validation passed — "
          f"line_status: {len(line_status)} rows, "
          f"bike_points: {len(bike_points)} rows, "
          f"air_quality: {len(air_quality)} rows")


def transform(**context) -> None:
    """Transform raw data and push cleaned DataFrames to XCom."""
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract")

    transformed = transform_all(raw_data)

    if transformed["line_status"].empty:
        raise AirflowFailException("Transformed line_status is empty.")
    if transformed["bike_points"].empty:
        raise AirflowFailException("Transformed bike_points is empty.")
    if transformed["air_quality"].empty:
        raise AirflowFailException("Transformed air_quality is empty.")

    # Convert DataFrames to dict for XCom serialization
    # Convert all datetime/date objects to strings to avoid JSON serialization issues
    serializable = {}
    for name, df in transformed.items():
        # Convert datetime columns to ISO string format
        for col in df.select_dtypes(include=["datetime64[ns, UTC]", "datetime64[ns]", "object"]).columns:
            try:
                df[col] = df[col].astype(str)
            except Exception:
                pass
        serializable[name] = df.to_dict(orient="records")

    context["ti"].xcom_push(key="transformed_data", value=serializable)

    print(f"Transformed — "
          f"line_status: {len(transformed['line_status'])} rows, "
          f"bike_points: {len(transformed['bike_points'])} rows, "
          f"air_quality: {len(transformed['air_quality'])} rows")


def load(**context) -> None:
    """Load transformed data into ClickHouse Bronze + build Gold tables."""
    import pandas as pd

    transformed_raw = context["ti"].xcom_pull(
        key="transformed_data", task_ids="transform"
    )

    if not transformed_raw:
        raise AirflowFailException("No transformed data found in XCom.")

    # Reconstruct DataFrames from XCom dicts
    transformed = {}
    for name, records in transformed_raw.items():
        df = pd.DataFrame(records)

        # Convert string timestamps back to datetime objects for ClickHouse
        if "extracted_at" in df.columns:
            df["extracted_at"] = pd.to_datetime(df["extracted_at"], utc=True)
        if "extracted_date" in df.columns:
            df["extracted_date"] = pd.to_datetime(df["extracted_date"]).dt.date

        transformed[name] = df

    # Get today's date for Gold table partitioning
    extracted_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    load_all(transformed, extracted_date)

    print(f"Loaded all data into ClickHouse for {extracted_date}")

def validate_load(**context) -> None:
    """Validate data was successfully loaded into ClickHouse."""
    import clickhouse_connect

    client = clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", ""),
        database=os.environ.get("CLICKHOUSE_DB", "tfl_pipeline"),
    )

    db = os.environ.get("CLICKHOUSE_DB", "tfl_pipeline")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Check Bronze tables have data for today
    checks = [
        ("bronze_line_status", 5),
        ("bronze_bike_points", 100),
        ("bronze_air_quality", 1),
    ]

    for table, min_rows in checks:
        result = client.query(
            f"SELECT count(*) FROM {db}.{table} "
            f"WHERE extracted_date = '{today}'"
        )
        count = result.first_row[0]
        if count < min_rows:
            raise AirflowFailException(
                f"{table} has only {count} rows for {today} "
                f"(expected at least {min_rows})."
            )
        print(f"{table}: {count} rows for {today} ✅")

    # Check Gold tables built
    gold_checks = [
        "gold_daily_line_summary",
        "gold_daily_bike_summary",
        "gold_daily_air_quality",
    ]

    for table in gold_checks:
        result = client.query(
            f"SELECT count(*) FROM {db}.{table} "
            f"WHERE summary_date = '{today}'"
        )
        count = result.first_row[0]
        if count == 0:
            raise AirflowFailException(
                f"Gold table {table} has 0 rows for {today}."
            )
        print(f"{table}: {count} rows for {today} ✅")

    client.close()
    print(f"All validation checks passed for {today} ✅")


with DAG(
    dag_id="tfl_pipeline",
    schedule="0 */4 8-20 * * *",   # every 4 hours, 8am–8pm UTC — buffered away from NYC's midnight run
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tfl", "transport", "clickhouse", "london"],
    default_args={
        "email": [os.environ.get("ALERT_EMAIL")],
        "email_on_failure": True,
        "email_on_retry": False,
    },
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    validate_raw_task = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    validate_load_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load,
    )

    (
        start
        >> extract_task
        >> validate_raw_task
        >> transform_task
        >> load_task
        >> validate_load_task
        >> end
    )