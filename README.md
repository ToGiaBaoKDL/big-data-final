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
wait_for_etl → spark_batch_features
```
- Waits for ETL completion (ExternalTaskSensor)
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

### Setup
```bash
# 1. Clone and configure
cp infrastructure/.env.example infrastructure/.env

# 2. Start services
docker compose -f infrastructure/docker-compose.yml up -d

# 3. Wait for services to be healthy (~2-3 min)
docker compose -f infrastructure/docker-compose.yml ps
```

### Access Services

| Service | URL | User | Password |
|---------|-----|------|----------|
| **Airflow** | `localhost:8080` | `admin` | `admin_password` |
| **Metabase** | `localhost:3000` | (Setup) | (Setup) |
| **MinIO** | `localhost:9001` | `minio_admin` | `minio_password` |
| **Spark UI** | `localhost:9090` | - | - |
| **ClickHouse** | `localhost:8123` | `clickhouse_admin` | `clickhouse_password` |
| **MLflow** | `localhost:5000` | `admin` | `password` |

### Run Pipeline
1. Open Airflow at http://localhost:8080
2. Enable and trigger `etl_paysim` DAG
3. After 24h of data, enable `ml_extract_feature_paysim`

## Project Structure

```
├── infrastructure/          # Docker Compose & configs
│   ├── compose/            # Service YAML files
│   ├── docker/             # Dockerfiles
│   └── scripts/            # Init scripts
├── orchestration/          # Airflow
│   ├── dags/               # DAG definitions
│   └── plugins/            # Utilities
├── processing/             # Data processing
│   ├── generators/         # PaySim data generator
│   └── spark/              # Spark ETL scripts
└── warehouse/              # Serving layer
    ├── clickhouse/         # SQL migrations
    ├── dbt_clickhouse/     # dbt project
    └── ingestion/          # S3 → ClickHouse loaders
```

## Development

### Run dbt locally
```bash
cd warehouse/dbt_clickhouse
dbt run --profiles-dir .
dbt test --profiles-dir .
```

### Check source freshness
```bash
dbt source freshness --profiles-dir .
```
