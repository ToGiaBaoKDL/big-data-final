"""
ML Training DAG for PaySim Fraud Detection.
Triggered after feature extraction completes, or manually.

Features:
- Trains model using sklearn/XGBoost or PySpark ML
- Logs experiments to MLflow
- Registers model in MLflow Model Registry
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
import pendulum
import os
from datetime import timedelta

# Paths
ML_SCRIPT_PATH = "/opt/airflow/processing/ml"
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

# Environment for MLflow
MLFLOW_ENV = {
    "MLFLOW_TRACKING_URI": os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000"),
    "AWS_ENDPOINT_URL": os.getenv("AWS_ENDPOINT_URL", "http://minio:9000"),
    "MINIO_ROOT_USER": os.getenv("MINIO_ROOT_USER", "minio_admin"),
    "MINIO_ROOT_PASSWORD": os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
    "MINIO_BUCKET_DATASCIENCE": os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience-gii2ij"),
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
        'model_type': 'xgb',  # xgb, rf, lr
        'run_date': None,     # Feature snapshot date (default: execution_date)
        'use_spark': False,   # Use PySpark ML instead of sklearn
        'register_model': True,
    },
    doc_md="""
    ## ML Training Pipeline
    
    **Triggers:**
    - Manual: Trigger with params `{"model_type": "xgb", "run_date": "2026-01-30"}`
    - After feature extraction DAG
    
    **Model Types:**
    - `xgb`: XGBoost (recommended for fraud)
    - `rf`: Random Forest
    - `lr`: Logistic Regression
    
    **Outputs:**
    - Model logged to MLflow
    - Metrics tracked in MLflow experiments
    - Model registered in Model Registry (if enabled)
    """,
) as dag:

    start = EmptyOperator(task_id="start")

    # Validate features exist
    validate_features = BashOperator(
        task_id="validate_features",
        bash_command="""
        set -e
        
        RUN_DATE="{{ params.run_date or 'latest' }}"
        
        echo "Validating feature store..."
        python3 /opt/airflow/plugins/utils/validate_minio.py \
            --bucket $MINIO_BUCKET_DATASCIENCE \
            --prefix "paysim_features/"
        
        echo "Features validated!"
        """,
        env=MLFLOW_ENV,
    )

    # Branch: Spark ML or sklearn
    def choose_training_method(**context):
        use_spark = context['params'].get('use_spark', False)
        if use_spark:
            return 'train_spark_ml'
        return 'train_sklearn'

    branch_training = BranchPythonOperator(
        task_id="branch_training",
        python_callable=choose_training_method,
    )

    # Option 1: Train with sklearn/XGBoost
    train_sklearn = BashOperator(
        task_id="train_sklearn",
        bash_command=f"""
        set -e
        export PATH=$PATH:/home/airflow/.local/bin
        
        MODEL_TYPE="{{{{ params.model_type or 'xgb' }}}}"
        RUN_DATE="{{{{ params.run_date or ds }}}}"
        REGISTER="{{{{ params.register_model }}}}"
        BUCKET="${{MINIO_BUCKET_DATASCIENCE}}"
        
        echo "Training sklearn Model"
        echo "Model Type: $MODEL_TYPE"
        echo "Run Date: $RUN_DATE (feature snapshot date)"
        echo "Feature Store: s3a://$BUCKET/paysim_features"
        
        cd {ML_SCRIPT_PATH}
        
        ARGS="--model $MODEL_TYPE --save-model"
        ARGS="$ARGS --data-path s3a://$BUCKET/paysim_features"
        ARGS="$ARGS --run-date $RUN_DATE"  # Always pass run_date
        
        if [ "$REGISTER" = "True" ]; then
            ARGS="$ARGS --register"
        fi
        
        python train_sklearn.py $ARGS
        
        echo "Training complete!"
        """,
        env=MLFLOW_ENV,
    )

    # Option 2: Train with PySpark ML
    train_spark_ml = BashOperator(
        task_id="train_spark_ml",
        bash_command=f"""
        set -e
        export PATH=$PATH:/home/airflow/.local/bin
        
        MODEL_TYPE="{{{{ params.model_type or 'lr' }}}}"
        RUN_DATE="{{{{ params.run_date or ds }}}}"
        REGISTER="{{{{ params.register_model }}}}"
        
        echo "Training PySpark ML Model"
        echo "Model Type: $MODEL_TYPE"
        echo "Run Date: ${{RUN_DATE:-all}}"
        
        ARGS="--model $MODEL_TYPE --save-model --tune"
        
        if [ -n "$RUN_DATE" ] && [ "$RUN_DATE" != "None" ]; then
            ARGS="$ARGS --run-date $RUN_DATE"
        fi
        
        if [ "$REGISTER" = "True" ]; then
            ARGS="$ARGS --register"
        fi
        
        spark-submit \\
            --master {SPARK_MASTER} \\
            --driver-memory 4g \\
            --executor-memory 4g \\
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=${{MINIO_ROOT_USER}} \
            --conf spark.hadoop.fs.s3a.secret.key=${{MINIO_ROOT_PASSWORD}} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.eventLog.enabled=true \
            --conf spark.eventLog.dir=s3a://${{MINIO_BUCKET_LOGS:-dl-logs-3e91b5}}/spark-events/ \
            {ML_SCRIPT_PATH}/train_spark.py $ARGS
        
        echo "Training complete!"
        """,
        env=MLFLOW_ENV,
    )

    # Join point
    training_complete = EmptyOperator(
        task_id="training_complete",
        trigger_rule="one_success"
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
    )

    # DAG flow
    start >> validate_features >> branch_training
    branch_training >> train_sklearn >> training_complete
    branch_training >> train_spark_ml >> training_complete
    training_complete >> log_completion
