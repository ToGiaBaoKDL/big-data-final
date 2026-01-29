# PaySim Fraud Detection Lakehouse

A Data Lakehouse pipeline for financial fraud detection using **Apache Spark**, **ClickHouse**, **dbt**, and **Airflow**.

## Architecture Overview

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Generator  │───▶│ dl-landing  │───▶│ dl-analytics│───▶│ ClickHouse  │
│  (PaySim)   │    │  (Bronze)   │    │  (Silver)   │    │    (DW)     │
└─────────────┘    └─────────────┘    └───────┬─────┘    └──────┬──────┘
                                              │                 │
                                              ▼                 ▼
                                    ┌─────────────────┐  ┌──────────────┐
                                    │ dl-datascience  │  │  dbt Models  │
                                    │ (ML Features)   │  │   (Marts)    │
                                    └─────────────────┘  └──────────────┘
```

## Data Flow & Storage

| Layer | Bucket | Content | Format |
|-------|--------|---------|--------|
| **Bronze** | `dl-landing-*` | Raw generated data | Parquet |
| **Silver** | `dl-analytics-*` | Cleaned + Feature engineered | Parquet |
| **Gold** | ClickHouse | Serving layer | MergeTree |
| **ML Features** | `dl-datascience-*` | Window features for training | Parquet |

## Pipeline Workflow

### 1. ETL Pipeline (`etl_paysim`) - Hourly
```
generate_data → process_data → validate → ingest_to_clickhouse → dbt_run
```
- Generates synthetic PaySim data
- Spark cleans and engineers features
- Ingests to ClickHouse
- dbt transforms to marts

### 2. ML Pipeline (`ml_extract_feature_paysim`) - Daily
```
spark_batch_features
```
- Triggered by ETL completion (TriggerDagRunOperator)
- Calculates window-based ML features (1h, 24h velocity)
- Saves to Feature Store

## dbt Models

### Staging
- `stg_paysim_txn` - Clean/rename source columns

### Core
- `fact_transactions` - Denormalized transaction fact
- `dim_users` - User aggregates

### Marts (Analytics Ready)
| Mart | Purpose |
|------|---------|
| `mart_fraud_analysis` | Wide table for BI dashboards |
| `mart_merchant_risk` | Merchant risk profiling |
| `mart_hourly_patterns` | Fraud rate by hour/day |
| `mart_user_risk_score` | User risk scoring (0-100) |
| `mart_transaction_type_analysis` | Stats by transaction type |
| `mart_anomaly_detection` | Investigation queue |

## Quick Start

### Prerequisites
- Docker & Docker Compose
- 4GB+ RAM available

### 1. Environment Setup
```bash
# Clone the repository
git clone https://github.com/ToGiaBaoKDL/big-data-final.git
cd big-data-final

# Create environment file from example
cp infrastructure/.env.example infrastructure/.env
```

### 2. Start Infrastructure
```bash
# Start all services in detached mode
docker compose -f infrastructure/docker-compose.yml up -d

# Check service health (wait 2-3 mins for 'healthy' status)
docker compose -f infrastructure/docker-compose.yml ps
```

### 3. Data Initialization
You can initialize the system with historical data using the `init` mode. This loads data from a CSV file (e.g., Kaggle PaySim dataset) into the Data Lake.

**Option A: Run inside config container (Recommended)**
```bash
# 1. Ensure your CSV file is available (e.g., in data/paysim.csv)
# 2. Exec into the Airflow Scheduler container
docker exec -it infrastructure-airflow-scheduler-1 bash

# 3. Run the init script
python3 /opt/airflow/processing/generators/generate_paysim.py init --file /opt/airflow/data/paysim.csv
```
*Note: Make sure `data/` volume is mounted or file is accessible.*

**Option B: Monthly/Hourly Generation**
- The system is designed to generate synthetic data automatically via Airflow.
- Simply enabling the `etl_paysim` DAG will start generating hourly data.

### 4. Running the Pipeline
The core logic is orchestrated by **Airflow**.

1.  **Access Airflow**: [http://localhost:8080](http://localhost:8080)
    *   User: `admin`
    *   Password: `admin_password`
2.  **Trigger ETL**:
    *   Find DAG `etl_paysim`.
    *   Toggle the **ON** switch.
    *   (Optional) Click "Trigger DAG" for an immediate run.
3.  **Check Results**:
    *   **MinIO** ([localhost:9001](http://localhost:9001)): Check buckets `dl-landing`, `dl-analytics`.
    *   **ClickHouse** ([localhost:8123](http://localhost:8123)): Query `warehouse.fact_transactions`.

### 5. Metabase Setup (BI Dashboard)
Metabase is used for visualizing the fraud insights.

1.  **Access Metabase**: [http://localhost:3000](http://localhost:3000)
2.  **Initial Setup**: Follow the on-screen setup (Account creation).
3.  **Add Data Source**:
    *   **Database Type**: ClickHouse (See note below*)
    *   **Name**: `Fraud Warehouse`
    *   **Host**: `clickhouse` (Service name in Docker network)
    *   **Port**: `8123`
    *   **Database Name**: `default` (or `warehouse` if configured)
    *   **Username**: `clickhouse_admin`
    *   **Password**: `clickhouse_password`
4.  **Explore**: Use the query builder or write SQL to analyze `warehouse.fact_transactions`.

*> Note: If ClickHouse is not listed, you may need to add the clickhouse driver manually or use a Metabase image that includes it.*

## Project Structure

```
├── infrastructure/         # Infrastructure-as-Code
│   ├── compose/            # Modular Docker Compose files
│   ├── docker/             # Custom Docker images (Airflow, Spark, etc.)
│   └── scripts/            # Initialization scripts (Postgres, MinIO)
│
├── orchestration/          # Airflow DAGs & Logic
│   ├── dags/               # ETL & ML Pipeline definitions
│   └── plugins/            # Shared utilities (Utils, Hooks)
│
├── processing/             # Data Processing (Spark/Python)
│   ├── generators/         # Synthetic Data Generator (PaySim)
│   └── spark/              # Spark Jobs (Cleaning, Feature Eng)
│
├── warehouse/              # Data Warehouse (ClickHouse & dbt)
│   ├── clickhouse/         # DDLs & Migrations
│   ├── dbt_clickhouse/     # dbt Project (Transformation Layer)
│   └── ingestion/          # Scripts to load data from S3 -> ClickHouse
│
├── data/                   # Local data directory (ignored by git)
```

## Development & Debugging

### Run dbt locally
Execute dbt commands from within the `warehouse/dbt_clickhouse` directory:
```bash
cd warehouse/dbt_clickhouse
dbt run --profiles-dir .   # Build models
dbt test --profiles-dir .  # Run data tests
```

### Accessing Interfaces
| Service | URL | Credentials (User/Pass) |
|---------|-----|-------------------------|
| **Airflow** | http://localhost:8080 | `admin` / `admin_password` |
| **Metabase** | http://localhost:3000 | *(Setup Required)* |
| **MinIO** | http://localhost:9001 | `minio_admin` / `minio_password` |
| **Spark Master** | http://localhost:9090 | - |
| **ClickHouse** | http://localhost:8123 | `clickhouse_admin` / `clickhouse_password` |
| **MLflow** | http://localhost:5000 | `admin` / `password` |
