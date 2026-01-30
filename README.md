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

| Layer | Bucket | Content | Format | Partitioning |
|-------|--------|---------|--------|--------------|
| **Bronze** | `dl-landing-*` | Raw generated data | Parquet | `part_dt=YYYYMMDD/part_hour=HH` |
| **Silver** | `dl-analytics-*` | Cleaned + Feature engineered | Parquet | `part_dt=YYYYMMDD/part_hour=HH` |
| **Gold** | ClickHouse | Serving layer | ReplacingMergeTree | `part_dt`, `part_hour` |
| **ML Features** | `dl-datascience-*` | Window features for training | Parquet | `part_dt=YYYYMMDD` |

## Pipeline Workflow

### 1. ETL Pipeline (`etl_paysim`) - Hourly
```
generate_data → process_data → validate → ingest_to_clickhouse → dbt_run → (trigger ML at 23:00)
```
- Generates synthetic PaySim data
- Spark cleans and engineers features
- Ingests to ClickHouse
- dbt transforms to marts
- Triggers ML feature extraction at end of day (23:00)

### 2. ML Feature Extraction (`ml_extract_feature_paysim`) - Daily
```
spark_batch_features → validate_features → end
```
- Triggered by ETL at 23:00 daily
- Calculates window-based ML features (1h, 24h velocity)
- Saves to Feature Store (dl-datascience bucket)

### 3. ML Training Pipeline (`ml_train_paysim`) - On-Demand
```
validate_features → train_spark_ml → log_completion
```
- Trains fraud detection models using PySpark ML (Logistic Regression, Random Forest, GBT)
- Logs experiments to MLflow
- Registers models in MLflow Model Registry

### Manual Trigger with Parameters

All DAGs support manual trigger with custom parameters:

```bash
# ETL DAG - trigger for specific hour
airflow dags trigger etl_paysim --conf '{"execution_date": "2026-01-30T10:00:00+00:00"}'

# ML Feature Extraction - extract features for specific date
airflow dags trigger ml_extract_feature_paysim --conf '{
  "execution_date": "2026-01-30"
}'

# ML Training (Optional) - specific model and date
airflow dags trigger ml_train_paysim --conf '{
  "model_type": "xgb",
  "run_date": "2026-01-30",
  "use_spark": false,
  "register_model": true
}'

# Without conf: uses current time (scheduled runs use data_interval_start)
airflow dags trigger etl_paysim
```

**Model Types**: `xgb` (XGBoost), `rf` (Random Forest), `lr` (Logistic Regression)

## dbt Models

### Staging
- `stg_paysim_txn` - Clean/rename source columns

### Core
- `fact_transactions` - Denormalized transaction fact
- `dim_users` - User aggregates

### Marts (Analytics Ready)
| Mart | Purpose |
|------|---------|
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

**Option 1: Init from CSV (Kaggle PaySim Dataset)**

Place your CSV file in the `data/` directory, then run the init workflow:

```bash
# Step 1: Generate to Landing layer (runs LOCAL on host machine)
python3 processing/generators/generate_paysim.py init --file data/paysim.csv

# Step 2: Process to Analytics layer (uses SPARK cluster resources)
docker cp processing/spark/process_paysim.py infrastructure-spark-master-1:/opt/spark/jobs/
docker exec infrastructure-spark-master-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g --executor-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio_admin \
    --conf spark.hadoop.fs.s3a.secret.key=minio_password \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /opt/spark/jobs/process_paysim.py --mode init

# Step 3: Load to ClickHouse (runs LOCAL on host machine)
CLICKHOUSE_HOST=localhost CLICKHOUSE_PASSWORD=clickhouse_password \
    python3 warehouse/clickhouse/clickhouse_loader.py --mode init

# Step 4: Run dbt transformations (runs LOCAL on host machine)
cd warehouse/dbt_clickhouse
dbt run --profiles-dir .
cd ../..

# Step 5: Extract ML features (uses SPARK cluster resources)
docker cp processing/spark/extract_feature_paysim.py infrastructure-spark-master-1:/opt/spark/jobs/
docker exec infrastructure-spark-master-1 /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g --executor-memory 2g \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minio_admin \
    --conf spark.hadoop.fs.s3a.secret.key=minio_password \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    /opt/spark/jobs/extract_feature_paysim.py --mode init
```

**Option 2: Synthetic Data Generation (No CSV Required)**

Skip init and let the DAGs generate data automatically:

```bash
# 1. Access Airflow UI: http://localhost:8080 (admin/admin_password)
# 2. Toggle ON the etl_paysim DAG
# 3. It will generate hourly synthetic transactions automatically
```

**Verification:**

```bash
# Check MinIO buckets
open http://localhost:9001  # User: minio_admin, Pass: minio_password

# Query ClickHouse
docker exec infrastructure-clickhouse-1 clickhouse-client --query "
    SELECT count(*) as total_rows, 
           sum(isFraud) as frauds,
           100.0 * sum(isFraud) / count(*) as fraud_rate_pct
    FROM finance_dw.paysim_txn
"
```

```bash
# Access Airflow UI at http://localhost:8080 (admin/admin_password)
# Toggle ON the etl_paysim DAG
# It will start generating hourly synthetic transactions automatically
```

**After init, the DAGs handle incremental processing automatically.**

---

## Idempotency & Data Consistency

All pipeline components implement idempotency to ensure reprocessing the same data produces consistent results without duplicates:

### Generator (Landing Layer)
- **Strategy**: Delete-then-insert at file level
- **Implementation**: Checks if file exists, removes it before uploading new version
- **Safe to retry**: Yes, same step can be regenerated

### Spark Processing (Analytics Layer)  
- **Strategy**: Overwrite entire partition
- **Implementation**: `write.mode("overwrite")` replaces all files in partition
- **Safe to retry**: Yes, reprocessing replaces old data completely
- **Note**: One partition = one hour. Multiple batches in same hour will be merged by Spark's overwrite

### ClickHouse Loader (Warehouse Layer)
- **Strategy**: Delete-then-insert at partition level
- **Implementation**: `ALTER TABLE DELETE WHERE part_dt=X AND part_hour=Y` before INSERT
- **Safe to retry**: Yes, partition is cleared before loading
- **Init mode**: Truncates entire table before bulk load

### dbt Transformations (Mart Layer)
- **Strategy**: Delete+insert on unique_key
- **Implementation**: `incremental_strategy='delete+insert'` with `unique_key='txn_id'`
- **Safe to retry**: Yes, conflicts resolved by unique key
- **Incremental filter**: Only processes new partitions based on `max(part_dt || part_hour)`

**Result**: You can safely rerun any step without worrying about data duplication.

---

### 4. Running the Pipeline
The core logic is orchestrated by **Airflow**.

1.  **Access Airflow**: [http://localhost:8080](http://localhost:8080)
    *   User: `admin`
    *   Password: `admin_password`

2.  **Enable DAGs**:
    *   **`etl_paysim`**: Hourly ETL (generate → process → ingest → dbt)
    *   **`ml_extract_feature_paysim`**: Daily feature extraction (triggered at 23:00 by ETL)
    *   **`ml_train_paysim`**: ML training (optional, manual trigger only)
    
3.  **Trigger Options**:
    *   **Auto**: Toggle DAG ON for scheduled runs
    *   **Manual**: Click "Trigger DAG" with optional params (see [Manual Trigger](#manual-trigger-with-parameters))

4.  **Check Results**:
    *   **MinIO** ([localhost:9001](http://localhost:9001)): Check buckets `dl-landing`, `dl-analytics`, `dl-datascience`
    *   **ClickHouse** ([localhost:8123](http://localhost:8123)): Query `finance_dw.paysim_txn` or dbt marts
    *   **MLflow** ([localhost:5000](http://localhost:5000)): View experiments and model registry (after training)
    *   **Verify Data Quality**:
        ```sql
        -- Check schema in ClickHouse
        DESCRIBE finance_dw.paysim_txn;
        
        -- Verify partition format (part_hour should be "00", "01", etc.)
        SELECT part_dt, part_hour, count() FROM finance_dw.paysim_txn 
        GROUP BY part_dt, part_hour ORDER BY part_dt, part_hour LIMIT 10;
        
        -- Check fraud rate
        SELECT count(*) as total, sum(isFraud) as frauds, 
               100.0 * sum(isFraud) / count(*) as fraud_rate_pct
        FROM finance_dw.paysim_txn;
        ```

### 5. Metabase Setup (BI Dashboard)
Metabase is used for visualizing the fraud insights.

1.  **Access Metabase**: [http://localhost:3000](http://localhost:3000)
2.  **Initial Setup**: Follow the on-screen setup (Account creation).
3.  **Add Data Source**:
    *   **Database Type**: ClickHouse (See note below*)
    *   **Name**: `Fraud Warehouse`
    *   **Host**: `clickhouse` (Service name in Docker network)
    *   **Port**: `8123`
    *   **Database Name**: `finance_dw`
    *   **Username**: `clickhouse_admin`
    *   **Password**: `clickhouse_password`
4.  **Explore**: Use the query builder or write SQL to analyze:
    *   Raw data: `finance_dw.paysim_txn`
    *   dbt marts: `finance_dw.mart_fraud_analysis`, `finance_dw.mart_user_risk_score`, etc.


## Project Structure

```
├── infrastructure/         # Infrastructure-as-Code
│   ├── compose/            # Modular Docker Compose files
│   ├── docker/             # Custom Docker images (Airflow, Spark, MLflow)
│   ├── scripts/            # Initialization scripts (Postgres, MinIO)
│   └── secrets/            # Service credentials and keys
│
├── orchestration/          # Airflow DAGs & Orchestration
│   ├── dags/               # ETL, ML Feature, ML Training DAGs
│   └── plugins/            # Shared utilities (validation, time calc)
│
├── processing/             # Data Processing & ML
│   ├── generators/         # Synthetic Data Generator (PaySim)
│   ├── spark/              # Spark Jobs (ETL, Feature Engineering)
│   └── ml/                 # ML Training Scripts (PySpark ML)
│
├── warehouse/              # Data Warehouse Layer
│   ├── clickhouse/         # DDLs, Migrations, Loader Scripts
│   └── dbt_clickhouse/     # dbt Project (Transformation Layer)
│
├── data/                   # Local data directory (CSV files, ignored by git)
├── logs/                   # Airflow task logs (ignored by git)
```

## Accessing Interfaces
| Service | URL | Credentials (User/Pass) |
|---------|-----|-------------------------|
| **Airflow** | http://localhost:8080 | `admin` / `admin_password` |
| **Metabase** | http://localhost:3000 | *(Setup Required)* |
| **MinIO** | http://localhost:9001 | `minio_admin` / `minio_password` |
| **Spark Master** | http://localhost:9090 | - |
| **ClickHouse** | http://localhost:8123 | `clickhouse_admin` / `clickhouse_password` |
| **MLflow** | http://localhost:5000 | `admin` / `password` |
