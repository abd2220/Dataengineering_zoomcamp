# Module 3 Homework: Data Warehousing & BigQuery

This document contains the SQL queries executed for the Module 3 homework.

## BigQuery Setup

**Creating external table referring to GCS path:**

```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dezoomcamp_hw3_2026_yousri/yellow_tripdata_2024-*.parquet']
);
```


**Creating regular table:**

```sql
-- Creating regular table
CREATE OR REPLACE TABLE data-zoomcamp-485408.nytaxi.yellow_taxi_2024_regular AS
SELECT * FROM data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external;
```

## Question 1: Counting Records

```sql
-- Check yellow trip data
SELECT count(*) FROM data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external limit 10;
```
## Question 2: Data Read Estimation

**Query for the External Table:**

```sql
-- Query for the External Table
SELECT COUNT(DISTINCT(PULocationID)) AS distinct_pu_locations
FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external`;
```

**Query for the Native Table:**

```sql
-- Query for the Native Table
SELECT COUNT(DISTINCT(PULocationID)) AS distinct_pu_locations
FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_regular`;
```

## Question 4: Counting Zero Fare Trips

```sql
SELECT count(*)
FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external`
WHERE fare_amount = 0;
```

## Question 5: Partitioning and Clustering

**Creating optimized table:**

```sql
CREATE OR REPLACE TABLE `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_optimized`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_external`;
```

## Question 6: Partition Benefits

**Query for the Native Table:**

```sql
SELECT DISTINCT(VendorID)
FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_regular`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```

**Query for the Optimized Table:**

```sql
SELECT DISTINCT(VendorID)
FROM `data-zoomcamp-485408.nytaxi.yellow_taxi_2024_optimized`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
```
