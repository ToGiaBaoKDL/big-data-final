from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
from datetime import timedelta

# Paths
FEATURE_SCRIPT_PATH = "/opt/airflow/processing/spark/extract_feature_paysim.py"
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description='Batch Feature Engineering for Fraud Detection (Daily)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['mlops', 'spark', 'feature_engineering'],
    catchup=False,
    max_active_runs=1
) as dag:

    extract_features = BashOperator(
        task_id='spark_batch_features',
        bash_command=f"""
        set -e
        echo "--------------------------------------------"
        echo "Running Batch Feature Extraction"
        echo "Execution Date: {{{{ ds_nodash }}}}"
        echo "--------------------------------------------"

        spark-submit \
            --master {SPARK_MASTER} \
            --driver-memory 2g \
            --executor-memory 2g \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
            --conf spark.hadoop.fs.s3a.access.key={os.getenv("MINIO_ROOT_USER", "minio_admin")} \
            --conf spark.hadoop.fs.s3a.secret.key={os.getenv("MINIO_ROOT_PASSWORD", "minio_password")} \
            --conf spark.hadoop.fs.s3a.path.style.access=true \
            {FEATURE_SCRIPT_PATH} \
            --execution_date {{{{ ds }}}}
        
        if [ $? -eq 0 ]; then
            echo "Feature Extraction Successful"
        else
            echo "Feature Extraction Failed"
            exit 1
        fi
        """
    )

    extract_features
