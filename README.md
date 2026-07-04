# ✈️ Flight Analytics Data Platform (End-to-End Data Engineering Project)

---

## 🚀 Overview

A production-style end-to-end data engineering platform for aviation analytics built on the **OPDI Flight List** dataset.

The system implements a **medallion architecture** (`bronze → silver → gold`) and delivers analytics-ready datasets in **Snowflake**, orchestrated with **Airflow**, processed with **Spark**, modeled with **dbt**, and visualized in **Superset**.

It demonstrates data engineering skills across **ingestion, processing, warehousing, orchestration, and infrastructure-as-code (Terraform)**.

---

## 🎯 Key Objectives

- Build a scalable flight data ingestion pipeline
- Clean and standardize high-volume aviation datasets
- Design analytics-ready dimensional models in Snowflake
- Enable business intelligence dashboards in Superset
- Automate infrastructure provisioning via Terraform
- Ensure reproducibility using Docker-based local environment

---

## 🧱 System Architecture

```
OPDI Flight Dataset
        ↓
Airflow (Orchestration)
        ↓
MinIO (Raw Data Lake - Bronze)
        ↓
Spark (Data Cleaning - Silver)
        ↓
Snowflake (Analytics Warehouse - Gold)
        ↓
dbt (Modeling + Tests)
        ↓
Superset (BI Dashboards)
```

**Infrastructure layer:**

- Terraform → Snowflake provisioning (warehouse, roles, schema)
- Docker Compose → full local environment

---

## 🛠️ Tech Stack


| **Layer**              | **Technology**        |
| ---------------------- | --------------------- |
| Orchestration          | Apache Airflow        |
| Processing             | Apache Spark          |
| Data Lake              | MinIO (S3-compatible) |
| Warehouse              | Snowflake             |
| Transformation         | dbt                   |
| Infrastructure as Code | Terraform             |
| Visualization          | Apache Superset       |
| Containerization       | Docker Compose        |


---

## 📦 Data Pipeline

### 1. Ingestion Layer (Airflow) ✅

- Downloads monthly **OPDI parquet** datasets
- Generates ingestion URLs dynamically
- Stores raw files in **MinIO**
- Partitions data by: `year / month / day`

### 2. Data Lake (Bronze Layer) ✅

- Immutable raw flight data
- Stored in **parquet** format
- Partitioned for query efficiency
- Path: `s3://flight-data/raw/flights/year=YYYY/month=MM/day=DD/`

### 3. Processing Layer (Spark → Silver) ✅

- Schema normalization and type casting
- Timestamp standardization (`first_seen`, `last_seen`)
- Uppercasing of airport codes (`ADEP`, `ADES`)
- Removal of invalid records
- **Derived features:**
  - Flight duration (`seconds / minutes / hours`)
- Output stored as structured **parquet**

### 4. Analytics Layer (Snowflake → Gold via dbt) ✅

**Dimensional model:**

- **Fact Table:** `fact_flights`
- **Dimensions:**
  - `dim_airport`
  - `dim_aircraft`
  - `dim_airlines`
  - `dim_dates`

**Aggregated Models (BI-ready):**

- Yearly flight traffic
- Monthly seasonality metrics
- Aircraft activity trends
- Airport completeness (data quality)
- Average flight duration trends

---

## 📊 Analytics & Dashboards (Superset) ✅


| **Dataset**                  | **Visualization** | **Insight**                           |
| ---------------------------- | ----------------- | ------------------------------------- |
| `gold_yearly_flight_traffic` | Line Chart        | Long-term aviation demand trends      |
| `gold_monthly_traffic`       | Bar Chart         | Seasonality & monthly demand patterns |
| `gold_avg_duration_trend`    | Line Chart        | Operational efficiency over time      |
| `gold_airport_completeness`  | Line Chart        | Data quality & missing enrichment     |
| `gold_aircraft_trend`        | Line Chart        | Fleet activity & aircraft diversity   |


---

## 🧪 Data Quality & Validation

Implemented in **dbt**:

- `NOT NULL` constraints (critical fields)
- Arrival ≥ Departure validation
- Duration sanity checks (`0–48h` window)
- Deduplication strategies based on business keys

---

## ⚙️ Orchestration (Airflow DAGs)

**Pipeline execution flow:**

```
flight_opdi_ingestion_v3_schema_safe
spark_clean_flights
minio_to_snowflake_full_load
dbt_silver_models
dbt_tests
dbt_gold_models
```

---

## 🧠 Key Engineering Decisions

### Medallion Architecture

**Separation of concerns:**

- **Bronze** → Raw ingestion
- **Silver** → Cleaned, standardized dataset
- **Gold** → Analytics-ready models

### Partitioning Strategy

- Year / month / day partitioning in **MinIO**
- Enables efficient **Spark** reads

### Data Modeling Approach

- Star-schema design in **Snowflake**
- Pre-aggregated BI models for performance

---

## 🔐 Infrastructure as Code (Terraform)

Terraform provisions **Snowflake** resources:

- Database
- Warehouse
- Schemas (`silver`/`gold` separation)
- Roles and permissions

> **⚠️ Important: Authentication Setup**
>
> Snowflake is accessed via **key-pair authentication**.  
> You must generate an SSH key for Terraform:
>
> ```bash
> ssh-keygen -t rsa -b 2048 -m PEM -f snowflake_tf_key
> ```
>
> - **Public key** → Uploaded to Snowflake user
> - **Private key** → Used by Terraform provider
>
> *Without this setup, infrastructure provisioning will fail.*

---

## 📁 Project Structure

```
flight-analytics-project/
│
├── airflow/         # DAGs (ingestion + orchestration)
├── spark/           # Data cleaning jobs
├── dbt/             # Data models + tests
├── terraform/       # Snowflake infrastructure
├── superset/        # Dashboards
├── postgres/        # Airflow metadata DB
├── docker-compose.yml
└── README.md
```

---

## ▶️ How to Run Locally

1. **Clone repository**
  ```bash
   git clone <repo-url>
   cd flight-analytics-project
  ```
2. **Start environment**
  ```bash
   docker-compose up -d
  ```
3. **Provision Snowflake (Terraform)**
  ```bash
   cd terraform
   terraform init
   terraform apply
  ```
4. **Configure Airflow connections**
  - Snowflake connection
  - MinIO (S3) connection
5. **Execute pipeline**
  Trigger DAGs in order:
  - Ingestion DAG
  - Spark cleaning job
  - MinIO → Snowflake load
  - dbt silver models
  - dbt tests
  - dbt gold models

---

## 📈 Business Value

This platform enables:

- Aviation demand forecasting
- Airline activity analysis
- Airport traffic monitoring
- Data quality tracking in ingestion pipelines
- Seasonal trend analysis

---

## 🔮 Future Improvements

- Incremental ingestion (CDC-style pipelines)
- Data quality framework (**Great Expectations**)
- CI/CD with **GitHub Actions**
- Cost optimization in **Snowflake**
- Automated **Superset** dashboard provisioning

---

## 👤 Author

**Jurii**