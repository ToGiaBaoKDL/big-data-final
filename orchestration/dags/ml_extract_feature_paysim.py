from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.decorators import task
import pendulum
import os
from datetime import timedelta

# Paths
FEATURE_SCRIPT_PATH = "/opt/airflow/processing/spark/extract_feature_paysim.py"
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
EXECUTION_DATE_TEMPLATE = """{{ (dag_run.conf.get('execution_date') if dag_run and dag_run.conf and dag_run.conf.get('execution_date') else ds) | string | trim }}"""

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description='Batch Feature Engineering for Fraud Detection (Triggered by ETL)',
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 12, 28, tz="UTC"),
    tags=['mlops', 'spark', 'feature_engineering'],
    catchup=False,
    max_active_runs=1,
    params={
        'execution_date': None,       # Override date: YYYY-MM-DD (default: ds/execution_date)
        'trigger_training': False,    # Auto trigger ML training after feature extraction
        'model_type': 'xgb',          # Model to train if trigger_training=True (xgb, rf, lr)
    },
    doc_md="""
    ## ML Feature Extraction Pipeline
    
    **Purpose:** Extract window-based features for fraud detection and save to Feature Store.
    
    **Modes:**
    - Triggered by ETL at 23:00 daily
    - Manual: `{"execution_date": "2026-01-30"}`
    
    **Params:**
    - `execution_date`: Feature snapshot date (default: execution_date/ds)
    - `trigger_training`: Set to true to auto-trigger ML training (default: False)
    - `model_type`: Model type for training if triggered (xgb, rf, lr)
    
    **Output:** Feature store in dl-datascience bucket
    """,
) as dag:
    
    extract_features = BashOperator(
        task_id='spark_batch_features',
        bash_command=f"""
        set -e
        export PATH=$PATH:/home/airflow/.local/bin
        
        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        
        echo "--------------------------------------------"
        echo "Running Batch Feature Extraction"
        echo "Execution Date: $EXEC_DATE"
        echo "--------------------------------------------"

        spark-submit \\
            --master {SPARK_MASTER} \\
            --driver-memory 1g \\
            --executor-memory 1g \\
            --conf spark.driver.maxResultSize=512m \\
            --conf spark.sql.shuffle.partitions=20 \\
            --conf spark.default.parallelism=20 \\
            --conf spark.memory.fraction=0.8 \\
            --conf spark.memory.storageFraction=0.3 \\
            --conf spark.sql.adaptive.enabled=true \\
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \\
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key=${{MINIO_ROOT_USER}} \
            --conf spark.hadoop.fs.s3a.secret.key=${{MINIO_ROOT_PASSWORD}} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
            --conf spark.hadoop.fs.s3a.multiobjectdelete.enable=true \
            --conf spark.hadoop.fs.s3a.buffer.dir=/tmp/spark-s3a \
            --conf spark.hadoop.fs.s3a.fast.upload=true \
            --conf spark.hadoop.fs.s3a.fast.upload.buffer=bytebuffer \
            --conf spark.eventLog.enabled=false \
            --conf spark.ui.showConsoleProgress=true \
            {FEATURE_SCRIPT_PATH} \
            --mode incremental \
            --execution_date $EXEC_DATE
        
        if [ $? -eq 0 ]; then
            echo "Feature Extraction Successful"
        else
            echo "Feature Extraction Failed"
            exit 1
        fi
        """,
    )

    validate_features = BashOperator(
        task_id='validate_features',
        bash_command=f"""
        set -e
        
        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        
        echo "--------------------------------------------"
        echo "Validating Feature Store"
        echo "Execution Date: $EXEC_DATE"
        echo "--------------------------------------------"
        
        python3 /opt/airflow/plugins/utils/validate_minio.py \
            --bucket {os.getenv('MINIO_BUCKET_DATASCIENCE', 'dl-datascience-gii2ij')} \
            --prefix "paysim_features/run_date=$EXEC_DATE/"
        """,
    )
    
    @task.branch(task_id='check_trigger_training')
    def check_trigger_training(**context):
        """Branch: trigger training or skip"""
        trigger_training = context['dag_run'].conf.get('trigger_training', False)
        if trigger_training:
            return 'trigger_ml_training'
        return 'skip_training'
    
    skip_training = EmptyOperator(task_id='skip_training')
    
    trigger_ml_training = TriggerDagRunOperator(
        task_id='trigger_ml_training',
        trigger_dag_id='ml_train_paysim',
        conf={
            'run_date': EXECUTION_DATE_TEMPLATE,
            'model_type': "{{ dag_run.conf.get('model_type', 'xgb') }}",
            'use_spark': False,
            'register_model': True,
        },
        wait_for_completion=False,
        poke_interval=60,
    )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')
    branch = check_trigger_training()
    
    extract_features >> validate_features >> branch
    branch >> trigger_ml_training >> end
    branch >> skip_training >> end
