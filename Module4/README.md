# Module 4: Analytics Engineering

First loaded data in Big Query.

## dbt Project Overview: `taxi_rides_ny`

This project uses dbt to transform NYC Taxi trip data.

### Structure

*   **Staging**: Cleans and standardizes raw data.
    *   `stg_green_tripdata`: Staging for Green taxi data.
    *   `stg_yellow_tripdata`: Staging for Yellow taxi data.
    *   `stg_fhv_tripdata`: Staging for FHV (For-Hire Vehicle) data.
*   **Intermediate**:
    *   `int_trips_unioned`: Unions Green and Yellow taxi data.
*   **Marts**: Business-level models.
    *   `dim_zones`: Dimension table for taxi zones.
    *   `fct_trips`: Fact table for individual trips.
    *   `fct_monthly_zone_revenue`: Aggregated monthly revenue by zone.

## Homework SQL Queries

These queries are used to answer questions for the [Module 4 Homework](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/04-analytics-engineering/homework.md).

### Loading Data

#### Loading data for Green Taxi
```sql
LOAD DATA OVERWRITE `data-zoomcamp-485408.nytaxi.green_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://dezoomcamp_hw4_2026_yousri/green_tripdata_*.csv.gz'],
  compression = 'GZIP');
```

#### Loading data for Yellow Taxi
```sql
LOAD DATA OVERWRITE `data-zoomcamp-485408.nytaxi.yellow_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://dezoomcamp_hw4_2026_yousri/yellow_tripdata_*.csv.gz'],
  compression = 'GZIP');
```

#### Loading data for FHV
```sql
LOAD DATA OVERWRITE `data-zoomcamp-485408.nytaxi.fhv_tripdata`
FROM FILES (
  format = 'CSV',
  uris = ['gs://dezoomcamp_hw4_2026_yousri/fhv_tripdata_*.csv.gz'],
  compression = 'GZIP');
```

### Analysis Queries

#### Q4: Best Performing Zone for Green Taxis (2020)
```sql
SELECT 
    pickup_zone, 
    SUM(revenue_monthly_total_amount) AS total_revenue
FROM `data-zoomcamp-485408.dbt_nytaxi.fct_monthly_zone_revenue`
WHERE 
    service_type = 'Green'
    -- Extracting the year from the date column
    AND EXTRACT(YEAR FROM revenue_month) = 2020
GROUP BY 1
ORDER BY total_revenue DESC
LIMIT 1;
```

#### Q5: Partial Monthly Trips for Green Taxis (Oct 2019)
Using the `fct_monthly_zone_revenue` table, what is the total number of trips (total_monthly_trips) for Green taxis in October 2019?
```sql
SELECT
    SUM(total_monthly_trips) as total_trips
FROM `data-zoomcamp-485408.dbt_nytaxi.fct_monthly_zone_revenue`
WHERE service_type = 'Green' and EXTRACT(YEAR FROM revenue_month) = 2019 and EXTRACT(MONTH FROM revenue_month) = 10
```

#### Q6: Count of records in stg_fhv_tripdata
```sql
select
count(*)
from data-zoomcamp-485408.dbt_nytaxi.stg_fhv_tripdata
```