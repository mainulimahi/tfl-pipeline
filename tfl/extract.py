import os
import logging
from datetime import datetime, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

# ── API Configuration ──────────────────────────────────────
TFL_BASE_URL = "https://api.tfl.gov.uk"
TFL_APP_KEY = os.environ.get("TFL_APP_KEY")

TUBE_LINES = [
    "bakerloo", "central", "circle", "district",
    "hammersmith-city", "jubilee", "metropolitan",
    "northern", "piccadilly", "victoria", "waterloo-city"
]


def _get_headers() -> dict:
    """Return headers with TfL API key."""
    if not TFL_APP_KEY:
        raise ValueError("TFL_APP_KEY environment variable is not set.")
    return {"app_key": TFL_APP_KEY}


def _make_request(endpoint: str) -> Any:
    """Make a GET request to TfL API with error handling."""
    url = f"{TFL_BASE_URL}{endpoint}"
    try:
        response = requests.get(url, headers=_get_headers(), timeout=30)
        response.raise_for_status()
        logger.info(f"Successfully fetched: {endpoint}")
        return response.json()
    except requests.exceptions.Timeout:
        raise RuntimeError(f"TfL API timeout for endpoint: {endpoint}")
    except requests.exceptions.HTTPError as e:
        raise RuntimeError(f"TfL API HTTP error {e.response.status_code}: {endpoint}")
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"TfL API request failed: {e}")


def fetch_line_status() -> list[dict]:
    """
    Fetch current status for all tube lines.
    Returns list of line status records with extracted fields.
    """
    raw = _make_request("/Line/Mode/tube/Status")
    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []

    for line in raw:
        line_id = line.get("id", "")
        line_name = line.get("name", "")

        statuses = line.get("lineStatuses", [])
        if not statuses:
            records.append({
                "extracted_at": extracted_at,
                "line_id": line_id,
                "line_name": line_name,
                "status_severity": 10,
                "status_description": "Good Service",
                "disruption_reason": None,
                "is_disrupted": False,
            })
        else:
            for status in statuses:
                disruption = status.get("disruption", {})
                records.append({
                    "extracted_at": extracted_at,
                    "line_id": line_id,
                    "line_name": line_name,
                    "status_severity": status.get("statusSeverity", 10),
                    "status_description": status.get("statusSeverityDescription", "Good Service"),
                    "disruption_reason": disruption.get("description") if disruption else None,
                    "is_disrupted": status.get("statusSeverity", 10) != 10,
                })

    logger.info(f"Extracted {len(records)} line status records")
    return records


def fetch_bike_points() -> list[dict]:
    """
    Fetch all Santander bike point locations and availability.
    Returns list of bike point records.
    """
    raw = _make_request("/BikePoint")
    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []

    for point in raw:
        # Extract additional properties from nested list
        props = {p["key"]: p.get("value") for p in point.get("additionalProperties", [])}

        records.append({
            "extracted_at": extracted_at,
            "bike_point_id": point.get("id", ""),
            "name": point.get("commonName", ""),
            "lat": point.get("lat"),
            "lon": point.get("lon"),
            "nb_bikes": int(props.get("NbBikes", 0) or 0),
            "nb_empty_docks": int(props.get("NbEmptyDocks", 0) or 0),
            "nb_docks": int(props.get("NbDocks", 0) or 0),
        })

    logger.info(f"Extracted {len(records)} bike point records")
    return records


def fetch_air_quality() -> list[dict]:
    """
    Fetch London air quality forecast.
    Returns list of air quality records.
    """
    raw = _make_request("/AirQuality")
    extracted_at = datetime.now(timezone.utc).isoformat()
    records = []

    forecasts = raw.get("currentForecast", [])
    for forecast in forecasts:
        records.append({
            "extracted_at": extracted_at,
            "forecast_type": forecast.get("forecastType", ""),
            "forecast_band": forecast.get("forecastBand", ""),
            "forecast_summary": forecast.get("forecastSummary", ""),
            "no2_band": forecast.get("nO2Band", ""),
            "o3_band": forecast.get("o3Band", ""),
            "pm10_band": forecast.get("pM10Band", ""),
            "pm25_band": forecast.get("pM25Band", ""),
            "so2_band": forecast.get("sO2Band", ""),
        })

    logger.info(f"Extracted {len(records)} air quality records")
    return records


def fetch_all() -> dict[str, list[dict]]:
    """
    Fetch all TfL data sources.
    Returns dict with all extracted data.
    """
    logger.info("Starting TfL data extraction...")

    data = {
        "line_status": fetch_line_status(),
        "bike_points": fetch_bike_points(),
        "air_quality": fetch_air_quality(),
    }

    logger.info(
        f"Extraction complete — "
        f"line_status: {len(data['line_status'])} rows, "
        f"bike_points: {len(data['bike_points'])} rows, "
        f"air_quality: {len(data['air_quality'])} rows"
    )

    return data


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    result = fetch_all()
    print(json.dumps(result, indent=2, default=str))