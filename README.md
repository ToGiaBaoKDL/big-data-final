# PaySim Fraud Detection Lakehouse

A production-grade Data Lakehouse pipeline to detect financial fraud using **Apache Spark**, **ClickHouse**, **dbt**, and **Airflow**.

## Architecture
- **Ingestion**: Python Generator (Simulates Real-time Transactions) -> **MinIO** (Data Lake).
- **Processing**: **Apache Spark** (Bronze -> Silver Cleaning & Feature Eng).
- **Warehouse**: **ClickHouse** (Gold Layer/Serving).
- **Transformation**: **dbt** (Modeling: Fact/Dim/Marts).
- **Orchestration**: **Airflow**.
- **Visualization**: **Metabase**.
- **Monitoring**: **MLflow** (Experiment Tracking - Ready).

## Quick Start

### Prerequisites
- Docker & Docker Compose installed.
- 4GB+ RAM available for containers.

### Setup
Clone the repo and configure environment variables:
```bash
cp infrastructure/.env.example infrastructure/.env
```

### Run
Start the entire stack:
```bash
docker compose -f infrastructure/docker-compose.yml up -d
```
*Wait ~2-3 minutes for services (especially Airflow & Postgres) to become healthy.*

### Pipeline Trigger
1. Access **Airflow**: [http://localhost:8080](http://localhost:8080)
   - User: `admin` / Pass: `admin_password`
2. Enable and trigger the DAG **`etl_paysim`**.
   - This will: Generate Data -> Clean (Spark) -> Ingest (ClickHouse) -> Transform (dbt).

### Analytics
Access **Metabase**: [http://localhost:3000](http://localhost:3000)
- Setup Admin account.
- Add Database **ClickHouse**:
  - DIsplay Name: `PaySim DW`
  - Host: `clickhouse`
  - Port: `8123`
  - DB Name: `finance_dw`
  - User: `clickhouse_admin` / Pass: `clickhouse_password`
- Explore `mart_fraud_analysis` (All-in-one Fraud View) or `mart_merchant_risk`.

---

## Project Structure
```text
├── infrastructure/       # Docker Compose & Env configs (Airflow, Spark, etc.)
├── orchestration/        # Airflow DAGs & Plugins
├── processing/           # Spark Utils & Data Generators
├── warehouse/            # Serving Layer
│   ├── clickhouse/       # SQL Migrations
│   ├── dbt_clickhouse/   # dbt Project (Data Models)
│   └── ingestion/        # Python Loaders (S3 -> ClickHouse)
└── data/                 # Local mount for Data Lake (MinIO persistence)
```

## Services & Credentials
| Service | URL | User | Password |
|---------|-----|------|----------|
| **Airflow** | [localhost:8080](http://localhost:8080) | `admin` | `admin_password` |
| **Metabase** | [localhost:3000](http://localhost:3000) | (Setup yourself) | (Setup yourself) |
| **MinIO** | [localhost:9001](http://localhost:9001) | `minio_admin` | `minio_password` |
| **Spark Master** | [localhost:9090](http://localhost:9090) | - | - |
| **ClickHouse** | `localhost:8123` | `clickhouse_admin` | `clickhouse_password` |
| **MLflow** | [localhost:5000](http://localhost:5000) | `admin` | `password` (Basic Auth via Nginx if configured, else Open) |
| **Postgres** | `localhost:5432` | `postgres` | `postgres` (Root) |
