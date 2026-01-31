"""
ML Training DAG for PaySim Fraud Detection.
Optional pipeline - disabled by default.

Features:
- Trains model using PySpark ML (LogisticRegression, RandomForest, GBT)
- Logs experiments to MLflow
- Registers model in MLflow Model Registry

Usage:
  airflow dags trigger ml_train_paysim --conf '{"model_type": "lr", "run_date": "2026-01-30"}'
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
import pendulum
import os
from datetime import timedelta

# Paths
ML_SCRIPT_PATH = "/opt/airflow/processing/ml"
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# Environment for MLflow + AWS (include AWS vars for remote logging)
MLFLOW_ENV = {
    # MLflow config
    "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"),
    # MinIO/S3 config  
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", os.getenv("MINIO_ROOT_USER", "minio_admin")),
    "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", os.getenv("MINIO_ROOT_PASSWORD", "minio_password")),
    "AWS_REGION_NAME": os.getenv("AWS_REGION_NAME", "us-east-1"),
    # MinIO legacy vars (for validate_minio.py)
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minio_admin"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
    "MINIO_BUCKET_DATASCIENCE": os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience-gii2ij"),
    # PySpark Python version alignment (Airflow=3.11, Spark=3.11)
    "PYSPARK_PYTHON": "python3.11",
    "PYSPARK_DRIVER_PYTHON": "python3.11",
    # Silence Git warning in MLflow
    "GIT_PYTHON_REFRESH": "quiet",
}

default_args = {
    'owner': 'ml-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=3),
}

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description="Train and register fraud detection model",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 12, 28, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=['mlops', 'training', 'mlflow'],
    params={
        'model_type': 'lr',      # Model type: lr, rf, gbt
        'run_date': None,        # Feature snapshot date (default: ds/execution_date)
        'register_model': True,  # Register model in MLflow registry
        'tune': False,           # Enable hyperparameter tuning (slower but better)
    },
    doc_md="""
    ## ML Training Pipeline (PySpark ML)
    
    **Manual Trigger:**
    ```bash
    airflow dags trigger ml_train_paysim --conf '{"model_type": "lr", "run_date": "2026-01-30", "tune": true}'
    ```
    
    **Model Types:**
    - `lr`: Logistic Regression (fast, good baseline)
    - `rf`: Random Forest (slower, better accuracy)
    - `gbt`: Gradient Boosted Trees (slowest, best accuracy)
    
    **Outputs:**
    - Model logged to MLflow: http://localhost:5000
    - Metrics: AUC-ROC, AUC-PR, Precision, Recall, F1
    - Model Registry: Auto-register if `register_model=True`
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # Validate features exist
    validate_features = BashOperator(
        task_id="validate_features",
        bash_command="""
        set -e
        
        RUN_DATE="{{ params.run_date or ds }}"
        
        echo "Validating Feature Store"
        echo "Run Date: $RUN_DATE"
        echo "Bucket: $MINIO_BUCKET_DATASCIENCE"
        
        # Check if specific date partition exists
        if [ -n "$RUN_DATE" ] && [ "$RUN_DATE" != "None" ]; then
            PREFIX="paysim_features/run_date=$RUN_DATE/"
            echo "Checking features for date: $RUN_DATE"
        else
            PREFIX="paysim_features/"
            echo "Checking latest features (no date specified)"
        fi
        
        echo "Prefix: $PREFIX"
        
        # Use python -u for unbuffered output
        python3 -u /opt/airflow/plugins/utils/validate_minio.py \
            --bucket $MINIO_BUCKET_DATASCIENCE \
            --prefix "$PREFIX" \
            --min-files 1
        
        VALIDATION_EXIT_CODE=$?
        
        if [ $VALIDATION_EXIT_CODE -eq 0 ]; then
            echo "Features VALIDATED for $RUN_DATE"
        else
            echo "ERROR: Features NOT FOUND for $RUN_DATE"
            echo "Hint: Run ml_extract_feature_paysim DAG first"
            exit 1
        fi
        """,
        env=MLFLOW_ENV,
        append_env=True,  # Preserve Airflow env vars for remote logging
    )

    # Train with PySpark ML
    train_spark_ml = BashOperator(
        task_id="train_spark_ml",
        bash_command=f"""
        set -e
        export PATH=$PATH:/home/airflow/.local/bin
        
        MODEL_TYPE="{{{{ params.model_type or 'lr' }}}}"
        RUN_DATE="{{{{ params.run_date or ds }}}}"
        REGISTER="{{{{ params.register_model }}}}"
        TUNE="{{{{ params.tune }}}}"
        BUCKET="${{MINIO_BUCKET_DATASCIENCE}}"
        
        echo "PySpark ML Training"
        echo "Model Type: $MODEL_TYPE"
        echo "Run Date: $RUN_DATE"
        echo "Tune Hyperparameters: $TUNE"
        echo "Register Model: $REGISTER"
        echo "Feature Bucket: $BUCKET"
        
        # Validate script exists
        echo "Checking train script..."
        if [ ! -f {ML_SCRIPT_PATH}/train_spark.py ]; then
            echo "ERROR: train_spark.py not found at {ML_SCRIPT_PATH}/"
            echo "Expected path: {ML_SCRIPT_PATH}/train_spark.py"
            ls -la {ML_SCRIPT_PATH}/ || echo "Directory does not exist"
            exit 1
        fi
        echo "Script found: {ML_SCRIPT_PATH}/train_spark.py"
        
        # Build arguments for train_spark.py
        echo "Building training arguments..."
        ARGS="--model $MODEL_TYPE --save-model"
        
        if [ -n "$RUN_DATE" ] && [ "$RUN_DATE" != "None" ]; then
            ARGS="$ARGS --run-date $RUN_DATE"
        fi
        
        if [ "$TUNE" = "True" ]; then
            ARGS="$ARGS --tune"
        fi
        
        if [ "$REGISTER" = "True" ]; then
            ARGS="$ARGS --register"
        fi
        
        echo "Final arguments: $ARGS"
        echo "Submitting Spark job..."
        
        spark-submit \\
            --master {SPARK_MASTER} \\
            --driver-memory 800m \\
            --executor-memory 1200m \\
            --conf spark.executor.memoryOverhead=300m \\
            --conf spark.sql.shuffle.partitions=20 \\
            --conf spark.sql.adaptive.enabled=true \\
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
            --conf spark.pyspark.python=python3 \\
            --conf spark.pyspark.driver.python=python3 \\
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=${{MINIO_ROOT_USER}} \
            --conf spark.hadoop.fs.s3a.secret.key=${{MINIO_ROOT_PASSWORD}} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.eventLog.enabled=true \
            --conf spark.eventLog.dir=s3a://${{MINIO_BUCKET_LOGS:-dl-logs-3e91b5}}/spark-events/ \
            {ML_SCRIPT_PATH}/train_spark.py $ARGS
        
        TRAIN_EXIT_CODE=$?

        if [ $TRAIN_EXIT_CODE -eq 0 ]; then
            echo "Training completed successfully!"
        else
            echo "Training failed with exit code: $TRAIN_EXIT_CODE"
            exit $TRAIN_EXIT_CODE
        fi
        """,
        env=MLFLOW_ENV,
        append_env=True,  # Preserve Airflow env vars for remote logging
    )

    # Log to MLflow that training finished
    log_completion = BashOperator(
        task_id="log_completion",
        bash_command="""
        echo "ML Training Pipeline Complete"
        echo "View experiments at: $MLFLOW_TRACKING_URI"
        echo "Model Registry: $MLFLOW_TRACKING_URI/#/models"
        """,
        env=MLFLOW_ENV,
        append_env=True,  # Preserve Airflow env vars for remote logging
    )

    # DAG flow
    start >> validate_features >> train_spark_ml >> log_completion
