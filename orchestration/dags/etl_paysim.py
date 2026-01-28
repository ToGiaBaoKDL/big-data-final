from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pendulum
import os

SIMULATION_START = "2025-12-29T00:00:00+00:00"
DAG_START_DATE = pendulum.datetime(2025, 12, 29, tz="UTC")

GENERATOR_PATH = os.getenv("PAYSIM_GENERATOR_PATH", "/opt/airflow/processing/generators/generate_paysim.py")
SPARK_PROCESSOR_PATH = os.getenv("PAYSIM_PROCESSOR_PATH", "/opt/airflow/processing/spark/process_paysim.py")
UTIL_SCRIPT_PATH = "/opt/airflow/plugins/utils/paysim_time_calc.py"

BASE_ROWS = int(os.getenv("PAYSIM_BASE_ROWS", "1000"))
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id=os.path.basename(__file__).replace(".py", ""),
    default_args=default_args,
    description='PaySim Hourly Pipeline',
    schedule_interval='@hourly',
    start_date=DAG_START_DATE,
    catchup=False,
    max_active_runs=1,
    tags=['paysim', 'spark', 'etl', 'hourly'],
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command=f"""
        set -e

        STEP=$(python3 {UTIL_SCRIPT_PATH} "{{{{ data_interval_start }}}}" step)
        
        echo "--------------------------------------------"
        echo "Generating PaySim data"
        echo "Step: $STEP"
        echo "Execution Date: {{{{ ds }}}}"
        echo "Logical Date: {{{{ data_interval_start }}}}"
        echo "--------------------------------------------"
        
        python {GENERATOR_PATH} generate --rows {BASE_ROWS} --step $STEP
        
        if [ $? -eq 0 ]; then
            echo "Generation successful: Step $STEP"
        else
            echo "Generation failed"
            exit 1
        fi
        """,
    )

    process_data = BashOperator(            
        task_id='process_data',
        bash_command=f"""
        set -e
        
        # Get all values in one call
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "{{{{ data_interval_start }}}}" all)
        
        echo "--------------------------------------------"
        echo "Processing PaySim data"
        echo "Step: $STEP"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "Execution Date: {{{{ ds }}}}"
        echo "--------------------------------------------"
        
        spark-submit \
            --master {SPARK_MASTER} \
            --driver-memory 2g \
            --executor-memory 2g \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
            --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
            {SPARK_PROCESSOR_PATH} \
            --part_dt $PART_DT \
            --part_hour $PART_HOUR
        
        if [ $? -eq 0 ]; then
            echo "Processing successful: part_dt=$PART_DT/part_hour=$PART_HOUR"
        else
            echo "Processing failed"
            exit 1
        fi
        """,
    )

    validate = BashOperator(
        task_id='validate_output',
        bash_command=f"""
        set -e
        
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "{{{{ data_interval_start }}}}" all)
        
        echo "--------------------------------------------"
        echo "Validating output"
        echo "Step: $STEP"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "--------------------------------------------"
        
        python3 /opt/airflow/plugins/utils/validate_minio.py \
            --bucket {os.getenv('MINIO_BUCKET_ANALYTICS', 'dl-analytics-8f42a1')} \
            --prefix ".warehouse/paysim_txn/part_dt=$PART_DT/part_hour=$PART_HOUR/"
        """,
    )

    ingest = BashOperator(
        task_id='ingest_to_clickhouse',
        bash_command=f"""
        set -e
        
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "{{{{ data_interval_start }}}}" all)
        
        echo "--------------------------------------------"
        echo "Ingesting to ClickHouse"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "--------------------------------------------"
        
        python3 /opt/airflow/warehouse/ingestion/clickhouse_loader.py \
            --part_dt $PART_DT \
            --part_hour $PART_HOUR
        """,
        env={
            "CLICKHOUSE_HOST": "clickhouse",
            "CLICKHOUSE_PORT": "8123"
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
        set -e
        echo "--------------------------------------------"
        echo "Running dbt transformations"
        echo "--------------------------------------------"
        
        cd /opt/airflow/warehouse/dbt_clickhouse
        dbt run --profiles-dir .
        """,
        env={
            "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "default"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", ""),
            "DBT_PROFILES_DIR": "/opt/airflow/warehouse/dbt_clickhouse"
        }
    )

    generate_data >> process_data >> validate >> ingest >> dbt_run