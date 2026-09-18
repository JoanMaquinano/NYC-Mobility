# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# STEP 1: API to monthly CSV files. Import this Python notebook and run all cells.
# Packages (openmeteo-requests, requests-cache, retry-requests) are pre-installed in the job environment.

import hashlib
import json
from datetime import datetime, timezone

# Install packages if missing (for interactive runs; job environment has them pre-installed)
try:
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry
except ModuleNotFoundError:
    import subprocess
    import sys

    subprocess.check_call(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "-q",
            "openmeteo-requests==1.7.5",
            "requests-cache==1.3.3",
            "retry-requests==2.0.0",
        ]
    )
    import openmeteo_requests
    import requests_cache
    from retry_requests import retry

import pandas as pd

# The live forecast endpoint cannot supply this past three-month window.
URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
LATITUDE, LONGITUDE = 40.7143, -74.006
START_DATE, END_DATE = "2026-03-01", "2026-05-31"
HOURLY_VARIABLES = [
    "temperature_2m",
    "apparent_temperature",
    "precipitation_probability",
    "rain",
    "weather_code",
    "cloud_cover",
    "visibility",
    "wind_speed_10m",
    "wind_gusts_10m",
]
params = {
    "latitude": LATITUDE,
    "longitude": LONGITUDE,
    "start_date": START_DATE,
    "end_date": END_DATE,
    "hourly": HOURLY_VARIABLES,
    "timezone": "UTC",
    "temperature_unit": "celsius",
    "wind_speed_unit": "kmh",
    "precipitation_unit": "mm",
}
# Memory caching avoids needing a writable .cache folder in Databricks.
cache_session = requests_cache.CachedSession(backend="memory", expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
captured = []


def remember_response(http_response, *args, **kwargs):
    captured.append(http_response)


retry_session.hooks["response"].append(remember_response)
openmeteo = openmeteo_requests.Client(session=retry_session)
responses = openmeteo.weather_api(URL, params=params, timeout=30)
if len(responses) != 1:
    raise ValueError("Expected one response for New York.")
response = responses[0]
hourly = response.Hourly()
if hourly is None or hourly.Interval() != 3600:
    raise ValueError("Missing hourly data or unexpected time interval.")
if hourly.VariablesLength() != len(HOURLY_VARIABLES):
    raise ValueError("The returned variable count differs from the request.")

# Values use the same order as the requested hourly variables.
hourly_data = {
    "date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left",
    )
}
for index, name in enumerate(HOURLY_VARIABLES):
    values = hourly.Variables(index).ValuesAsNumpy()
    if len(values) != len(hourly_data["date"]):
        raise ValueError(f"Time and value lengths differ for {name}.")
    hourly_data[name] = values
hourly_dataframe = pd.DataFrame(hourly_data)
expected_dates = pd.date_range(
    START_DATE, pd.Timestamp(END_DATE) + pd.Timedelta(days=1), freq="h", inclusive="left", tz="UTC"
)
# Fail before loading if timestamps are missing, duplicated, or out of order.
if not pd.DatetimeIndex(hourly_dataframe["date"]).equals(expected_dates):
    raise ValueError("The API did not return the complete requested UTC window.")
if response.UtcOffsetSeconds() != 0:
    raise ValueError("Expected UTC timestamps.")
http_response = captured[-1]
http_response.raise_for_status()
# This SDK returns binary FlatBuffers. Preserve those exact response bytes.
raw_bytes = http_response.content
raw_sha256 = hashlib.sha256(raw_bytes).hexdigest()
request_json = json.dumps({"endpoint": URL, "params": params}, sort_keys=True)
batch_id = hashlib.sha256((request_json + raw_sha256).encode()).hexdigest()
ingested_at = datetime.now(timezone.utc)
print("HTTP status:", http_response.status_code)
print("Requested coordinates:", LATITUDE, LONGITUDE)
print("Returned grid coordinates:", response.Latitude(), response.Longitude())
print("Elevation (m):", response.Elevation())
print("UTC offset (seconds):", response.UtcOffsetSeconds())
print("Expected hours:", len(expected_dates), "Actual hours:", len(hourly_dataframe))
print("Duplicate timestamps:", hourly_dataframe["date"].duplicated().sum())
print("Null counts:\n", hourly_dataframe.isna().sum())
print("Response SHA-256:", raw_sha256)
display(hourly_dataframe)


# COMMAND ----------

# Save three tabular CSV files in the requested Unity Catalog Volume.
# The Volume must already exist and you need WRITE VOLUME permission.
from pathlib import Path

WEATHER_FOLDER = Path("/Volumes/workspace/default/ftw-b12-de/groups/week-08/group-d/weather")
WEATHER_FOLDER.mkdir(parents=True, exist_ok=True)

# Save original binary evidence separately; CSV is a tabular representation.
evidence_folder = WEATHER_FOLDER / "api_evidence"
evidence_folder.mkdir(exist_ok=True)
(evidence_folder / f"{raw_sha256}.bin").write_bytes(raw_bytes)
(evidence_folder / f"{raw_sha256}.json").write_text(request_json, encoding="utf-8")

# Add metadata only. Do not clean or replace source measurements here.
hourly_dataframe["month"] = hourly_dataframe["date"].dt.strftime("%Y-%m")
hourly_dataframe["latitude"] = LATITUDE
hourly_dataframe["longitude"] = LONGITUDE
hourly_dataframe["source_series"] = "historical_forecast:best_match"
for month_number, month_name in [(3, "march"), (4, "april"), (5, "may")]:
    monthly = hourly_dataframe[hourly_dataframe["date"].dt.month == month_number]
    destination = WEATHER_FOLDER / f"weather_{month_name}_2026.csv"
    csv_text = monthly.to_csv(index=False, date_format="%Y-%m-%dT%H:%M:%SZ", na_rep="")
    # Freeze each landing file so a rerun cannot silently change an existing load.
    if destination.exists():
        if destination.read_text(encoding="utf-8") != csv_text:
            raise ValueError(
                f"{destination} already exists with different data. Review a new version before replacing it."
            )
        print("Unchanged; kept:", destination)
    else:
        with destination.open("x", encoding="utf-8", newline="") as handle:
            handle.write(csv_text)
    print(month_name, "rows:", len(monthly), "file:", destination)