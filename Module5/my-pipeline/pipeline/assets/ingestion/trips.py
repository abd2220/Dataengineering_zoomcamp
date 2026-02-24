"""@bruin

name: ingestion.trips
connection: duckdb-default

materialization:
  type: table
  strategy: append
image: python:3.11

columns:
  - name: VendorID
    type: integer
    description: TPEP/LPEP provider code (1=Creative Mobile, 2=VeriFone)
  - name: tpep_pickup_datetime
    type: timestamp
    description: Date and time when the meter was engaged
  - name: tpep_dropoff_datetime
    type: timestamp
    description: Date and time when the meter was disengaged
  - name: passenger_count
    type: float
    description: Number of passengers (driver-entered)
  - name: trip_distance
    type: float
    description: Elapsed trip distance in miles from the taximeter
  - name: RatecodeID
    type: float
    description: Rate code in effect at end of trip (1=Standard, 2=JFK, etc.)
  - name: store_and_fwd_flag
    type: string
    description: Whether trip record was held in memory before sending (Y/N)
  - name: PULocationID
    type: integer
    description: TLC Taxi Zone where the meter was engaged
    primary_key: true
  - name: DOLocationID
    type: integer
    description: TLC Taxi Zone where the meter was disengaged
    primary_key: true
  - name: payment_type
    type: integer
    description: Numeric code for how the passenger paid
  - name: fare_amount
    type: float
    description: Time-and-distance fare calculated by the meter
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax that is automatically triggered
  - name: tip_amount
    type: float
    description: Tip amount (auto-populated for credit card tips)
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid in trip
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed on hailed trips
  - name: total_amount
    type: float
    description: Total amount charged to passengers (excludes cash tips)
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge for trips in Manhattan
  - name: Airport_fee
    type: float
    description: Airport fee for pickups at LaGuardia and JFK
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
  - name: pickup_datetime
    type: timestamp
    description: Normalized pickup datetime used as incremental key
  - name: dropoff_datetime
    type: timestamp
    description: Normalized dropoff datetime
  - name: extracted_at
    type: timestamp
    description: Timestamp when this record was extracted

@bruin"""

import os
import json
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from dateutil.relativedelta import relativedelta


# Base URL for the NYC TLC trip data parquet files
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

# Column name mapping: yellow and green taxis use different column names for pickup/dropoff
PICKUP_COL = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
}
DROPOFF_COL = {
    "yellow": "tpep_dropoff_datetime",
    "green": "lpep_dropoff_datetime",
}


def _generate_months(start_date: str, end_date: str) -> list[tuple[int, int]]:
    """Generate a list of (year, month) tuples between start_date and end_date."""
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    return months


def _fetch_parquet(taxi_type: str, year: int, month: int) -> pd.DataFrame | None:
    """Download a single parquet file from the TLC endpoint and return as a DataFrame."""
    filename = f"{taxi_type}_tripdata_{year}-{month:02d}.parquet"
    url = f"{TLC_BASE_URL}{filename}"

    print(f"Fetching: {url}")
    response = requests.get(url, timeout=120)

    if response.status_code != 200:
        print(f"  ⚠ Skipping {filename} (HTTP {response.status_code})")
        return None

    df = pd.read_parquet(BytesIO(response.content))
    print(f"  ✓ Got {len(df):,} rows from {filename}")
    return df


def _normalize_columns(df: pd.DataFrame, taxi_type: str) -> pd.DataFrame:
    """Normalize column names across yellow/green taxi types."""
    pickup_col = PICKUP_COL[taxi_type]
    dropoff_col = DROPOFF_COL[taxi_type]

    # Rename the taxi-type-specific datetime columns to unified names
    rename_map = {}
    if pickup_col in df.columns:
        rename_map[pickup_col] = "pickup_datetime"
    if dropoff_col in df.columns:
        rename_map[dropoff_col] = "dropoff_datetime"

    df = df.rename(columns=rename_map)

    # Keep the original columns too for compatibility with the asset schema
    if taxi_type == "yellow":
        df["tpep_pickup_datetime"] = df["pickup_datetime"]
        df["tpep_dropoff_datetime"] = df["dropoff_datetime"]
    elif taxi_type == "green":
        df["tpep_pickup_datetime"] = df["pickup_datetime"]
        df["tpep_dropoff_datetime"] = df["dropoff_datetime"]

    # Add taxi_type column
    df["taxi_type"] = taxi_type

    return df


def materialize():
    """
    Fetch NYC taxi trip data from the TLC public endpoint.

    Uses Bruin runtime context:
    - BRUIN_START_DATE / BRUIN_END_DATE to determine which months to fetch
    - BRUIN_VARS to read the `taxi_types` pipeline variable
    """
    # Read date window from Bruin environment variables
    start_date = os.getenv("BRUIN_START_DATE", "2022-01-01")
    end_date = os.getenv("BRUIN_END_DATE", "2022-02-01")

    # Read pipeline variables (taxi_types)
    bruin_vars = json.loads(os.getenv("BRUIN_VARS", "{}"))
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    print(f"Date range: {start_date} → {end_date}")
    print(f"Taxi types: {taxi_types}")

    # Generate the list of (year, month) to fetch
    months = _generate_months(start_date, end_date)
    print(f"Months to fetch: {months}")

    # Fetch and concatenate all parquet files
    frames = []
    for taxi_type in taxi_types:
        for year, month in months:
            df = _fetch_parquet(taxi_type, year, month)
            if df is not None:
                df = _normalize_columns(df, taxi_type)
                frames.append(df)

    if not frames:
        print("No data fetched — returning empty DataFrame.")
        return pd.DataFrame()

    final_df = pd.concat(frames, ignore_index=True)

    # Add extracted_at timestamp for lineage/debugging
    final_df["extracted_at"] = datetime.utcnow()

    # Ensure consistent column set — fill missing columns with None
    expected_cols = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_distance", "RatecodeID",
        "store_and_fwd_flag", "PULocationID", "DOLocationID",
        "payment_type", "fare_amount", "extra", "mta_tax",
        "tip_amount", "tolls_amount", "improvement_surcharge",
        "total_amount", "congestion_surcharge", "Airport_fee",
        "taxi_type", "pickup_datetime", "dropoff_datetime", "extracted_at",
    ]
    for col in expected_cols:
        if col not in final_df.columns:
            final_df[col] = None

    # Select only the expected columns
    final_df = final_df[expected_cols]

    print(f"\n✓ Total rows to ingest: {len(final_df):,}")
    return final_df
