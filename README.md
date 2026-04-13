# CabPulse 360 — NYC Yellow Taxi Operations Analytics

A production-grade, end-to-end Data Engineering pipeline built on NYC Yellow Taxi trip data. The pipeline ingests raw CSV data, transforms it through Bronze → Silver → Gold layers, validates it with automated Data Quality checks, exports to a cloud data warehouse, and serves a live analytics dashboard.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Data Overview](#data-overview)
- [Pipeline Layers](#pipeline-layers)
  - [Bronze Layer](#bronze-layer)
  - [Silver Layer](#silver-layer)
  - [Gold Layer](#gold-layer)
  - [Data Quality Checks](#data-quality-checks)
  - [Export to S3](#export-to-s3)
- [Snowflake Setup](#snowflake-setup)
- [Streamlit Dashboard](#streamlit-dashboard)
- [Airflow Orchestration](#airflow-orchestration)
- [Quarantine Strategy](#quarantine-strategy)
- [S3 Folder Structure](#s3-folder-structure)
- [Databricks Catalog Structure](#databricks-catalog-structure)
- [Setup & Configuration](#setup--configuration)
- [How to Run](#how-to-run)
- [DQ Check Details](#dq-check-details)
- [SCD Type 2 — Vendor Name History](#scd-type-2--vendor-name-history)
- [Known Issues & Solutions](#known-issues--solutions)
- [Cost Estimate](#cost-estimate)
- [Team](#team)

---

## Project Overview

CabPulse 360 is a batch data engineering project that processes NYC Yellow Taxi trip records across 5 monthly batches (January–May 2019, ~9,240 records per full run). The project demonstrates a real-world Medallion Architecture pattern with automated orchestration, data quality enforcement, and a live business intelligence dashboard.

**Key highlights:**
- Medallion Architecture: Bronze → Silver → Gold
- Star schema data model with SCD Type 2 for vendor history
- 10 automated DQ checks with quarantine for bad data
- Full pipeline orchestration via Apache Airflow
- Live Streamlit dashboard inside Snowflake
- Handles dirty data (batch_04) gracefully without polluting the dashboard

---

## Architecture

```
Raw CSV Files (S3)
       │
       ▼
┌─────────────────────────────────────────────────┐
│              DATABRICKS (Serverless)             │
│                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐    │
│  │  Bronze  │ → │  Silver  │ → │   Gold   │    │
│  │ Raw + meta│  │ Cleaned  │   │Star schema│   │
│  └──────────┘   └──────────┘   └──────────┘    │
│                                      │           │
│                              ┌───────────────┐  │
│                              │  DQ Checks    │  │
│                              │  + Quarantine │  │
│                              └───────────────┘  │
└─────────────────────────────────────────────────┘
       │
       ▼ (parquet export via boto3)
┌─────────────────┐
│    AWS S3       │
│  gold_parquet/  │
└─────────────────┘
       │
       ▼ (COPY INTO via Snowflake stage)
┌─────────────────┐
│   SNOWFLAKE     │
│ STAGING + GOLD  │
└─────────────────┘
       │
       ▼
┌─────────────────┐
│   STREAMLIT     │
│  (in Snowflake) │
└─────────────────┘

All steps orchestrated by Apache Airflow (Astro CLI + Docker)
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| AWS S3 | Raw data storage and parquet export staging |
| Databricks (Serverless) | Data processing — Bronze, Silver, Gold, DQ |
| Apache Spark / PySpark | Distributed data transformation |
| Delta Lake | Managed table format in Databricks |
| Unity Catalog | Schema and volume management |
| boto3 | Python AWS SDK for S3 export |
| Snowflake | Cloud data warehouse |
| Streamlit (in Snowflake) | Analytics dashboard |
| Apache Airflow | Pipeline orchestration |
| Astro CLI + Docker | Local Airflow environment |
| Altair | Advanced chart visualizations in Streamlit |

---

## Project Structure

```
cabpulse360/
├── notebooks/
│   ├── 01_bronze.py               # Ingest raw CSVs → Bronze Delta tables
│   ├── 02_silver.py               # Clean + transform → Silver Delta tables
│   ├── 03_gold.py                 # Star schema → Gold Delta tables
│   ├── 04_dq_checks.py            # 10 DQ tests + quarantine logic
│   └── 05_export_gold_to_s3.py    # Export Gold tables as parquet to S3
│
├── dags/
│   └── cabpulse360_dag.py         # Airflow DAG — full pipeline orchestration
│
├── streamlit/
│   └── streamlit_app.py           # Streamlit dashboard code (run inside Snowflake)
│
├── snowflake/
│   └── setup.sql                  # Snowflake DB, schema, stage, tables, views
│
├── data/
│   ├── reference/
│   │   ├── vendors.csv
│   │   ├── taxi_zones.csv
│   │   ├── rate_codes.csv
│   │   ├── payment_types.csv
│   │   └── vendor_name_changes.csv
│   └── batch_data/
│       ├── batch_01/trips_batch_01.csv
│       ├── batch_02/trips_batch_02.csv
│       ├── batch_03/trips_batch_03.csv
│       ├── batch_04/trips_batch_04.csv   ← intentionally dirty data
│       ├── batch_05/trips_batch_05.csv
│       └── batch_06/trips_batch_06.csv   ← all rows fail DQ (tests exception)
│
└── README.md
```

---

## Data Overview

### Trip data schema

| Column | Type | Description |
|---|---|---|
| VENDOR_ID | int | Taxi vendor identifier |
| PICKUP_DATETIME | timestamp | Trip start datetime |
| DROPOFF_DATETIME | timestamp | Trip end datetime |
| PASSENGER_COUNT | int | Number of passengers |
| TRIP_DISTANCE | double | Trip distance in miles |
| PICKUP_LOCATION_ID | int | Pickup zone ID |
| DROPOFF_LOCATION_ID | int | Dropoff zone ID |
| RATE_CODE_ID | int | Rate code applied |
| PAYMENT_TYPE | int | Payment method |
| FARE_AMOUNT | double | Base fare |
| EXTRA | double | Extra charges |
| MTA_TAX | double | MTA tax |
| TIP_AMOUNT | double | Tip amount |
| TOLLS_AMOUNT | double | Toll charges |
| TOTAL_AMOUNT | double | Total charge |
| CONGESTION_SURCHARGE | double | Congestion surcharge |

### Reference data

| File | Rows | Description |
|---|---|---|
| vendors.csv | 4 | Vendor names and codes |
| taxi_zones.csv | 61 | Borough and zone names |
| rate_codes.csv | 6 | Rate code descriptions |
| payment_types.csv | 6 | Payment method names |
| vendor_name_changes.csv | 3 | SCD2 name change history |

### Batch summary

| Batch | Month | Approx rows | Notes |
|---|---|---|---|
| batch_01 | Jan 2019 | ~1,848 | Clean data |
| batch_02 | Feb 2019 | ~1,848 | Clean data |
| batch_03 | Mar 2019 | ~1,848 | Clean data |
| batch_04 | Apr 2019 | ~1,848 | Dirty data — DQ failures expected |
| batch_05 | May 2019 | ~1,848 | Clean data |
| batch_06 | Test | 500 | All rows fail every DQ check — tests exception |

---

## Pipeline Layers

### Bronze Layer

**Notebook:** `01_bronze.py`

Reads raw CSVs from S3 and writes them to Delta tables with metadata columns added. Reference data is overwritten on every run. Trip data is appended per batch.

**Metadata columns added:**
- `_batch_id` — identifies which batch the row came from
- `_loaded_at` — timestamp of when the row was loaded
- `_source_file` — full S3 path of the source file

**Tables created:**
- `cabpulse360_bronze_team2.trips`
- `cabpulse360_bronze_team2.vendors`
- `cabpulse360_bronze_team2.taxi_zones`
- `cabpulse360_bronze_team2.rate_codes`
- `cabpulse360_bronze_team2.payment_types`
- `cabpulse360_bronze_team2.vendor_name_changes`

---

### Silver Layer

**Notebook:** `02_silver.py`

Cleans each entity independently. No joins happen in Silver — each table is cleaned, cast to correct types, deduplicated, and written separately.

**Transformations applied:**
- Cast all columns to correct data types
- Deduplicate trips using a window function on VENDOR_ID + PICKUP_DATETIME + DROPOFF_DATETIME + PICKUP_LOCATION_ID + DROPOFF_LOCATION_ID
- Trim whitespace from string columns
- Derive new columns: `TRIP_DATE`, `PICKUP_HOUR`, `TRIP_DURATION_MIN`, `TIME_BLOCK`, `FARE_CATEGORY`
- Handle column renames for schema consistency (e.g. `PU_LOCATION_ID` → `PICKUP_LOCATION_ID`)

**Tables created:**
- `cabpulse360_silver_team2.trips`
- `cabpulse360_silver_team2.vendors`
- `cabpulse360_silver_team2.taxi_zones`
- `cabpulse360_silver_team2.rate_codes`
- `cabpulse360_silver_team2.payment_types`
- `cabpulse360_silver_team2.vendor_name_changes`

---

### Gold Layer

**Notebook:** `03_gold.py`

Builds a star schema from Silver tables. This is where all joins happen.

**Dimension tables:**

| Table | Source | Rows | Notes |
|---|---|---|---|
| dim_date | silver.trips | ~31 | Derived from TRIP_DATE — year, month, day, day name, week |
| dim_vendor | silver.vendors + silver.vendor_name_changes | 7 | SCD Type 2 — tracks name changes with effective/end dates |
| dim_taxi_zone | silver.taxi_zones | 61 | Borough + zone + service zone |
| dim_rate_code | silver.rate_codes | 6 | Rate code descriptions |
| dim_payment_type | silver.payment_types | 6 | Payment method names |
| dim_time_block | silver.trips | 6 | EARLY_MORNING, MORNING, AFTERNOON, EVENING, NIGHT, LATE_NIGHT |

**Fact tables:**

| Table | Description |
|---|---|
| fact_trips | Central fact — 1 row per trip, foreign keys to all 6 dimensions |
| fact_daily_zone_summary | Pre-aggregated trips + revenue by date and pickup zone |

**Additional tables:**

| Table | Description |
|---|---|
| dq_log | Audit log of every DQ test result per batch |
| quarantine_trips | Bad rows removed from Silver before Gold processing |

---

### Data Quality Checks

**Notebook:** `04_dq_checks.py`

Runs 10 automated checks on the Gold layer. Results are saved to `dq_log`. If any checks fail, the quarantine process runs automatically.

| # | Test Name | Rule | Expected |
|---|---|---|---|
| 1 | fact_trips_no_null_date_key | DATE_KEY must not be null | 0 bad rows |
| 2 | fact_trips_no_null_vendor_sk | VENDOR_SK must not be null | 0 bad rows |
| 3 | fact_trips_row_count_matches_silver | Gold row count = Silver row count | 0 difference |
| 4 | fact_trips_no_duplicates | No duplicate trips (vendor + pickup + dropoff + zone) | 0 duplicates |
| 5 | fact_trips_no_zero_fares | FARE_AMOUNT ≠ 0 when TOTAL_AMOUNT > 0 | 0 bad rows |
| 6 | fact_trips_no_impossible_distance | TRIP_DISTANCE ≤ 200 miles | 0 bad rows |
| 7 | fact_trips_no_zero_passengers | PASSENGER_COUNT > 0 | 0 bad rows |
| 8 | fact_trips_no_future_dates | TRIP_DATE ≤ 2019-12-31 | 0 bad rows |
| 9 | fact_trips_no_negative_fares | FARE_AMOUNT ≥ 0 | 0 bad rows |
| 10 | dim_vendor_scd2_has_current | Every vendor has at least 1 current record | 0 vendors without current |

**Quarantine logic:**

If any DQ checks fail:
1. Silver trips for the failing batch are split into bad rows and good rows
2. Bad rows are saved to `gold.quarantine_trips` for investigation
3. Clean rows are saved to `silver.trips_clean_{batch_id}`
4. If good_count > 0 → pipeline continues with clean data only
5. If good_count = 0 → exception is raised, pipeline stops completely

This ensures the dashboard always shows only validated, clean data.

**Batch 04 known failures:**

| Test | Bad Rows |
|---|---|
| Zero fares | ~88 |
| Impossible distances | ~65 |
| Zero passengers | ~59 |
| Future dates | ~64 |
| Negative fares | ~37 |
| Duplicates | ~28 |

---

### Export to S3

**Notebook:** `05_export_gold_to_s3.py`

Exports all 8 Gold tables as parquet files to S3 using boto3. Uses Unity Catalog Volumes as intermediate staging area to work around Serverless compute restrictions on DBFS and spark.conf.

**Export path:**
```
s3://aws-macro-project/Team2/gold_parquet/
├── dim_date.parquet
├── dim_vendor.parquet
├── dim_taxi_zone.parquet
├── dim_rate_code.parquet
├── dim_payment_type.parquet
├── dim_time_block.parquet
├── fact_trips.parquet
└── fact_daily_zone_summary.parquet
```

---

## Snowflake Setup

Run `snowflake/setup.sql` to create all required objects.

**Objects created:**
- Database: `CABPULSE360_DB`
- Schemas: `STAGING`, `GOLD`
- Warehouse: `CABPULSE360_WH` (XSMALL, auto-suspend 60s)
- File format: `parquet_format`
- External stage: `cabpulse360_stage` (points to S3 gold_parquet folder)
- 8 staging tables (schema inferred from parquet via INFER_SCHEMA)
- 8 Gold views (SELECT * from staging tables)

**Load command pattern:**
```sql
CREATE OR REPLACE TABLE STAGING.FACT_TRIPS
  USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
      LOCATION=>'@cabpulse360_stage/fact_trips.parquet',
      FILE_FORMAT=>'parquet_format'
    ))
  );

COPY INTO STAGING.FACT_TRIPS
  FROM @cabpulse360_stage/fact_trips.parquet
  FILE_FORMAT=parquet_format
  MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE;
```

**Expected row counts after all 5 batches:**

| Table | Rows |
|---|---|
| DIM_DATE | ~31 |
| DIM_VENDOR | 7 |
| DIM_TAXI_ZONE | 61 |
| DIM_RATE_CODE | 6 |
| DIM_PAYMENT_TYPE | 6 |
| DIM_TIME_BLOCK | 6 |
| FACT_TRIPS | ~9,240 |
| FACT_DAILY_ZONE_SUMMARY | ~1,000+ |

---

## Streamlit Dashboard

**File:** `streamlit/streamlit_app.py`

Run inside Snowflake: Projects → Streamlit → + Streamlit App

**Configuration:**
- Warehouse: `CABPULSE360_WH`
- Database: `CABPULSE360_DB`
- Schema: `GOLD`

**Dashboard sections:**

| Section | Description |
|---|---|
| KPI cards | Total trips, passengers, revenue, avg fare, avg distance, avg tip |
| Daily revenue | Line chart — revenue trend over time |
| Trips by borough | Bar chart — pickup borough breakdown |
| Payment types | Bar chart — credit card vs cash vs other |
| Fare categories | Bar chart — Low / Medium / High / Premium |
| Top 10 pickup zones | Horizontal bar chart with zone names inside bars (Altair) |
| SCD2 demo | Table showing CMT → CMT Digital vendor name history |
| Sample trips | Latest 50 trips with vendor name and zone names joined |

---

## Airflow Orchestration

**File:** `dags/cabpulse360_dag.py`

**Setup:**
```bash
# Install Astro CLI
brew install astro

# Initialize project
mkdir cabpulse360_airflow && cd cabpulse360_airflow
astro dev init

# Add to requirements.txt
apache-airflow-providers-databricks
apache-airflow-providers-snowflake
apache-airflow-providers-amazon
apache-airflow-providers-common-sql

# Start Airflow
astro dev start
```

**Airflow UI:** `http://localhost:8080` (admin / admin)

**Connections required:**

| Connection ID | Type | Key fields |
|---|---|---|
| databricks_default | Databricks | Host: workspace URL, Extra: {"token": "your_pat"} |
| aws_default | Amazon Web Services | Login: AWS_KEY, Password: AWS_SECRET |
| snowflake_default | Snowflake | Account: ORG-ACCOUNTNAME, Region: blank |

**DAG pipeline:**
```
run_bronze → run_silver → run_gold → run_dq → export_gold_to_s3 → refresh_snowflake
```

**Important notes:**
- Uses Serverless compute format with `environment_key: Default`
- `schedule=None` — triggered manually per batch
- Change `BATCH_ID` variable in the DAG for each new batch
- Community Edition does NOT support the Jobs API — use a real Databricks workspace

---

## Quarantine Strategy

The DQ notebook implements a production-grade quarantine pattern:

```
Batch arrives
     │
     ▼
10 DQ tests run
     │
     ├── All PASS → export → Snowflake → Dashboard (clean data)
     │
     └── Any FAIL
              │
              ▼
         Split silver trips
              │
              ├── Bad rows → quarantine_trips table (stored for investigation)
              │
              └── Good rows → trips_clean_{batch_id}
                       │
                       ├── good_count > 0 → pipeline continues with clean rows only
                       │
                       └── good_count = 0 → Exception raised → Pipeline STOPS
                                            Nothing reaches S3 or Snowflake
```

This ensures:
- Bad data is never silently ignored
- Every bad row is traceable with batch_id and quarantine_timestamp
- The dashboard always shows only validated data
- Full audit trail in dq_log for every batch run

---

## S3 Folder Structure

```
aws-macro-project/
└── Team2/
    ├── batch_data/
    │   ├── batch_01/trips_batch_01.csv
    │   ├── batch_02/trips_batch_02.csv
    │   ├── batch_03/trips_batch_03.csv
    │   ├── batch_04/trips_batch_04.csv
    │   ├── batch_05/trips_batch_05.csv
    │   └── batch_06/trips_batch_06.csv
    ├── reference_data/
    │   ├── vendors.csv
    │   ├── taxi_zones.csv
    │   ├── rate_codes.csv
    │   ├── payment_types.csv
    │   └── vendor_name_changes.csv
    └── gold_parquet/
        ├── dim_date.parquet
        ├── dim_vendor.parquet
        ├── dim_taxi_zone.parquet
        ├── dim_rate_code.parquet
        ├── dim_payment_type.parquet
        ├── dim_time_block.parquet
        ├── fact_trips.parquet
        └── fact_daily_zone_summary.parquet
```

---

## Databricks Catalog Structure

```
cabpluse360_team2/
├── cabpulse360_bronze_team2/
│   ├── trips
│   ├── vendors
│   ├── taxi_zones
│   ├── rate_codes
│   ├── payment_types
│   └── vendor_name_changes
│
├── cabpulse360_silver_team2/
│   ├── trips
│   ├── vendors
│   ├── taxi_zones
│   ├── rate_codes
│   ├── payment_types
│   ├── vendor_name_changes
│   └── trips_clean_{batch_id}     ← created when DQ fails
│
└── cabpulse360_gold_team2/
    ├── dim_date
    ├── dim_vendor
    ├── dim_taxi_zone
    ├── dim_rate_code
    ├── dim_payment_type
    ├── dim_time_block
    ├── fact_trips
    ├── fact_daily_zone_summary
    ├── dq_log                     ← DQ audit history
    └── quarantine_trips           ← bad rows storage
```

---

## Setup & Configuration

### Prerequisites

- AWS account with S3 access
- Databricks workspace (non-Community Edition for Airflow integration)
- Snowflake account
- Docker Desktop
- Astro CLI
- Python 3.9+

### Configuration variables

Update these in each notebook before running:

```python
catalog       = "cabpluse360_team2"
bronze_schema = "cabpulse360_bronze_team2"
silver_schema = "cabpulse360_silver_team2"
gold_schema   = "cabpulse360_gold_team2"

bucket        = "aws-macro-project"
aws_key       = "YOUR_AWS_KEY"
aws_secret    = "YOUR_AWS_SECRET"
region        = "us-east-1"
```

Update in DAG file:

```python
TEAM          = "team2"
BATCH_ID      = "batch_01"           # change per batch
NOTEBOOK_ROOT = "/Workspace/Shared/cabpulse360"
```

---

## How to Run

### Manual run (per batch)

1. Upload batch CSV to S3: `Team2/batch_data/batch_XX/trips_batch_XX.csv`
2. Run `01_bronze` notebook with `batch_id = batch_XX`
3. Run `02_silver` notebook
4. Run `03_gold` notebook
5. Run `04_dq_checks` notebook with `batch_id = batch_XX`
6. If DQ passes — run `05_export_gold_to_s3` notebook
7. Run Snowflake TRUNCATE + COPY INTO for all 8 tables

### Automated run via Airflow

1. Update `BATCH_ID = "batch_XX"` in `cabpulse360_dag.py`
2. Start Airflow: `astro dev start`
3. Open `http://localhost:8080`
4. Enable DAG `cabpulse360_team2_pipeline`
5. Click Trigger DAG
6. Monitor 6 tasks: run_bronze → run_silver → run_gold → run_dq → export_gold_to_s3 → refresh_snowflake

---

## DQ Check Details

### How dq_log works

Every time the DQ notebook runs, all 10 test results are appended to `gold.dq_log`:

```sql
SELECT * FROM cabpluse360_team2.cabpulse360_gold_team2.dq_log
ORDER BY run_timestamp DESC;
```

Sample output:

| test_name | result_value | status | batch_id | run_timestamp |
|---|---|---|---|---|
| fact_trips_no_null_date_key | 0 | PASS | batch_01 | 2026-04-11 15:50:26 |
| fact_trips_no_zero_fares | 88 | FAIL | batch_04 | 2026-04-11 16:30:12 |
| fact_trips_no_impossible_distance | 65 | FAIL | batch_04 | 2026-04-11 16:30:12 |

### How quarantine_trips works

Bad rows from failed batches are stored here with full context:

```sql
SELECT batch_id, quarantine_reason, quarantine_timestamp, COUNT(*) as bad_rows
FROM cabpluse360_team2.cabpulse360_gold_team2.quarantine_trips
GROUP BY 1, 2, 3
ORDER BY quarantine_timestamp DESC;
```

---

## SCD Type 2 — Vendor Name History

The `dim_vendor` table implements Slowly Changing Dimension Type 2 to track vendor name changes over time.

**How it works:**

When a vendor changes its name, instead of updating the existing row (which would lose history), SCD2 creates two rows:
- The old row gets an `END_DATE` and `IS_CURRENT = 0`
- A new row is created with the new name, `EFFECTIVE_DATE` = change date, and `IS_CURRENT = 1`

**Example — CMT vendor:**

| VENDOR_SK | VENDOR_ID | VENDOR_NAME | EFFECTIVE_DATE | END_DATE | IS_CURRENT |
|---|---|---|---|---|---|
| 1 | 1 | Creative Mobile Technologies | 1900-01-01 | 2019-06-30 | 0 |
| 2 | 1 | CMT Digital | 2019-07-01 | 9999-12-31 | 1 |

This means trips before July 2019 are linked to the old name, and trips after are linked to the new name — giving accurate historical reporting.

---

## Known Issues & Solutions

| Issue | Cause | Solution |
|---|---|---|
| 403 Invalid access token | Databricks Community Edition blocks Jobs API | Use real Databricks workspace |
| CONFIG_NOT_AVAILABLE fs.s3a | Serverless blocks spark.conf for credentials | Use boto3 with Unity Catalog Volumes |
| PERSIST TABLE not supported | Serverless blocks .cache() | Remove cache(), use direct write |
| DBFS_DISABLED | DBFS disabled by admin | Use /Volumes/ path instead of /tmp/ |
| PlanMetrics not JSON serializable | Spark metadata in toPandas() | Select only data columns before toPandas() |
| DELTA_FAILED_TO_MERGE_FIELDS | Type mismatch between batches | Use explicit float values (0.0 not 0) in CSV |
| Snowflake 404 Not Found | Wrong account locator format | Use CURRENT_ACCOUNT_LOCATOR() to get exact value |
| COPY INTO 0 rows | Stage credentials expired | Recreate stage with new AWS credentials |
| schedule_interval deprecated | New Airflow version | Use schedule=None instead |

---

## Cost Estimate

Estimated for 84 million rows (~6.5 GB raw CSV), daily batch processing, moderate dashboard usage.

| Service | Monthly | Yearly |
|---|---|---|
| AWS S3 (storage + requests) | $18 | $216 |
| Databricks Bronze | $120 | $1,440 |
| Databricks Silver | $95 | $1,140 |
| Databricks Gold | $145 | $1,740 |
| Databricks DQ + Export | $60 | $720 |
| Snowflake (compute + storage) | $280 | $3,360 |
| Streamlit in Snowflake | $0 | $0 |
| Airflow (Astronomer hosted) | $44 | $528 |
| AWS Data Transfer | $85 | $1,020 |
| **Total** | **$847** | **$10,164** |
| **With optimizations** | **~$510** | **~$6,100** |

**Top optimization strategies:**
- Switch Databricks from Serverless to Job Clusters (~35% compute saving)
- Keep S3 bucket and Snowflake in same AWS region (eliminates transfer costs)
- Enable Snowflake result caching for repeated dashboard queries
- Use Delta OPTIMIZE + ZORDER on TRIP_DATE for faster Gold queries
- Export only changed partitions incrementally instead of full table overwrite

---

## Team

**Team 2**

Built as part of a Data Engineering macro project.

| Role | Name |
|---|---|
| Data Engineer | Tejpal Singh |

---

*Built with Databricks · Snowflake · Apache Airflow · AWS S3 · Streamlit*
