/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Pickup date used as incremental key (truncated to day)
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: Type of taxi (yellow or green)
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: Human-readable payment type
    primary_key: true
  - name: trip_count
    type: integer
    description: Total number of trips
    checks:
      - name: positive
  - name: total_passengers
    type: float
    description: Sum of passenger counts
  - name: avg_trip_distance
    type: float
    description: Average trip distance in miles
  - name: total_fare
    type: float
    description: Sum of fare amounts
  - name: total_tips
    type: float
    description: Sum of tip amounts
  - name: total_revenue
    type: float
    description: Sum of total amounts (fare + extras + taxes + tips + tolls)
  - name: avg_fare
    type: float
    description: Average fare amount per trip

custom_checks:
  - name: row_count_positive
    description: Ensures report table is not empty after load
    value: 1
    query: SELECT CASE WHEN COUNT(*) > 0 THEN 1 ELSE 0 END FROM reports.trips_report

@bruin */

-- Aggregate staging trips by date, taxi type, and payment type
SELECT
    pickup_datetime,
    taxi_type,
    COALESCE(payment_type_name, 'unknown') AS payment_type_name,
    COUNT(*)                               AS trip_count,
    SUM(passenger_count)                   AS total_passengers,
    AVG(trip_distance)                     AS avg_trip_distance,
    SUM(fare_amount)                       AS total_fare,
    SUM(tip_amount)                        AS total_tips,
    SUM(total_amount)                      AS total_revenue,
    AVG(fare_amount)                       AS avg_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime <  '{{ end_datetime }}'
GROUP BY pickup_datetime, taxi_type, COALESCE(payment_type_name, 'unknown')
