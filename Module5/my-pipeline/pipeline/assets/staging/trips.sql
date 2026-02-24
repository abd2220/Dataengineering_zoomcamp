/* @bruin

name: staging.trips
type: duckdb.sql

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

depends:
  - ingestion.trips
  - ingestion.payment_lookup

columns:
  - name: pickup_datetime
    type: timestamp
    description: When the trip started (meter engaged)
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: When the trip ended (meter disengaged)
    primary_key: true
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    description: TLC Taxi Zone where the meter was engaged
    primary_key: true
    checks:
      - name: not_null
  - name: do_location_id
    type: integer
    description: TLC Taxi Zone where the meter was disengaged
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: Time-and-distance fare calculated by the meter
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    checks:
      - name: not_null
      - name: accepted_values
        value:
          - yellow
          - green
  - name: passenger_count
    type: float
    description: Number of passengers (driver-entered)
  - name: trip_distance
    type: float
    description: Elapsed trip distance in miles
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: Numeric code for how the passenger paid
  - name: payment_type_name
    type: string
    description: Human-readable payment type from lookup table
  - name: tip_amount
    type: float
    description: Tip amount (auto-populated for credit card tips)
  - name: total_amount
    type: float
    description: Total amount charged to passengers
  - name: vendor_id
    type: integer
    description: TPEP/LPEP provider code
  - name: ratecode_id
    type: float
    description: Rate code in effect at end of trip
  - name: store_and_fwd_flag
    type: string
    description: Whether trip record was held in memory before sending (Y/N)
  - name: extra
    type: float
    description: Miscellaneous extras and surcharges
  - name: mta_tax
    type: float
    description: MTA tax automatically triggered
  - name: tolls_amount
    type: float
    description: Total amount of all tolls paid
  - name: improvement_surcharge
    type: float
    description: Improvement surcharge assessed on hailed trips
  - name: congestion_surcharge
    type: float
    description: Congestion surcharge for trips in Manhattan
  - name: extracted_at
    type: timestamp
    description: Timestamp when this record was extracted

custom_checks:
  - name: row_count_positive
    description: Ensures staging table is not empty after load
    value: 1
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM staging.trips

@bruin */

-- Staging: clean, deduplicate, and enrich raw trip data
-- Uses ROW_NUMBER to deduplicate on composite key, then JOINs payment lookup

WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pu_location_id, do_location_id, fare_amount, taxi_type
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime <  '{{ end_datetime }}'
      AND pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND pu_location_id IS NOT NULL
      AND do_location_id IS NOT NULL
)

SELECT
    d.pickup_datetime,
    d.dropoff_datetime,
    d.pu_location_id,
    d.do_location_id,
    d.fare_amount,
    d.taxi_type,
    d.passenger_count,
    d.trip_distance,
    d.payment_type,
    p.payment_type_name,
    d.tip_amount,
    d.total_amount,
    d.vendor_id,
    d.ratecode_id,
    d.store_and_fwd_flag,
    d.extra,
    d.mta_tax,
    d.tolls_amount,
    d.improvement_surcharge,
    d.congestion_surcharge,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.row_num = 1
