# Module 1 Homework: Docker & SQL & Terraform & GCP

This repository contains my solutions for the Data Engineering Zoomcamp Module 1 homework, covering Docker, PostgreSQL, SQL, and Terraform/GCP basics.

## Environment Setup

### Prerequisites
- Docker & Docker Compose
- Python 3.x with pandas and sqlalchemy
- Terraform (for GCP infrastructure)
- Google Cloud SDK (for Terraform deployment)

## Project Structure

```
Module1/
├── README.md                          # This file
├── docker-compose.yaml                # Docker services configuration
├── ingesting_data.ipynb               # Data ingestion notebook
├── module1.sql                        # SQL queries for homework
├── green_tripdata_2025-11.parquet     # Trip data
├── taxi_zone_lookup.csv               # Zone lookup data
├── pyproject.toml                     # Python project configuration
└── terraform/
    ├── main.tf                        # Terraform resource definitions
    └── variable.tf                    # Terraform variables
```

---


### Starting the Database Environment

```bash
docker-compose up -d
```

This starts:
- **PostgreSQL 17** on port `5433` (to avoid conflicts with local installations)
- **pgAdmin 4** on port `8080`

### Connecting to pgAdmin
- URL: http://localhost:8080
- Email: `pgadmin@pgadmin.com`
- Password: `pgadmin`

### Database Connection Details
- Host: `localhost`
- Port: `5433`
- Database: `ny_taxi`
- User: `postgres`
- Password: `postgres`

---

## Data Ingestion

The data is ingested using the `ingesting_data.ipynb` notebook which:
1. Reads the `green_tripdata_2025-11.parquet` file
2. Reads the `taxi_zone_lookup.csv` file
3. Loads both datasets into PostgreSQL tables (`trip_data` and `zones`)

---

## Homework Questions & SQL Answers 

### Question 3: Trip Segmentation Count

**During the period of October 1st 2019 (inclusive) and November 1st 2019 (exclusive), how many trips, respectively, happened:**
- Up to 1 mile
- In between 1 (exclusive) and 3 miles (inclusive)
- In between 3 (exclusive) and 7 miles (inclusive)
- In between 7 (exclusive) and 10 miles (inclusive)
- Over 10 miles

```sql
SELECT 
    COUNT(*)
FROM 
    trip_data
WHERE 
    lpep_pickup_datetime >= '2025-11-01 00:00:00' 
    AND lpep_pickup_datetime < '2025-12-01 00:00:00'
    AND trip_distance <= 1.0;
```
---

### Question 4: Longest trip for each day

**Which was the pick up day with the longest trip distance?**

```sql
SELECT 
    lpep_pickup_datetime, 
    trip_distance
FROM 
    trip_data
WHERE 
    trip_distance < 100
ORDER BY 
    trip_distance DESC
LIMIT 3;
```
---

### Question 5: Three biggest pickup zones

**Which were the top pickup locations with over 13,000 in total_amount (across all trips) for 2019-11-18?**

```sql
SELECT 
    z."Zone", 
    SUM(t.total_amount) AS total_revenue
FROM 
    trip_data AS t
INNER JOIN 
    zones AS z ON t."PULocationID" = z."LocationID"
WHERE 
    DATE(t.lpep_pickup_datetime) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_revenue DESC
LIMIT 1;
```
---

### Question 6: Largest tip

**For the passengers picked up in October of 2019 in the zone named "East Harlem North" which was the drop off zone that had the largest tip?**

```sql
SELECT 
    "Zone"
FROM 
    zones 
WHERE 
    "LocationID" = (
        SELECT
            t."DOLocationID"
        FROM trip_data t
        INNER JOIN zones z
            ON z."LocationID" = t."PULocationID"
        WHERE 
            z."Zone" = 'East Harlem North' 
            AND EXTRACT(MONTH FROM lpep_pickup_datetime) = 11
        ORDER BY t.tip_amount DESC
        LIMIT 1
    );
```
---

## Terraform Configuration

The `terraform/` directory contains infrastructure-as-code for GCP resources:

### Resources Created
- **Google Cloud Storage Bucket:** `data-zoomcamp-485408-demo-bucket`
- **BigQuery Dataset:** `demo_dataset`

### Terraform Commands

```bash
cd terraform

# Initialize Terraform
terraform init

# Preview changes
terraform plan

# Apply changes
terraform apply

# Destroy resources
terraform destroy
```

### Configuration Files
- `main.tf` - Resource definitions (GCS bucket, BigQuery dataset)
- `variable.tf` - Variable definitions with defaults

---