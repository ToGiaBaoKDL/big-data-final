from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import pendulum
import os

DAG_START_DATE = pendulum.datetime(2025, 12, 28, tz="UTC")
EXECUTION_DATE_TEMPLATE = """{{ (dag_run.conf.get('execution_date') if dag_run and dag_run.conf and dag_run.conf.get('execution_date') else data_interval_start) | string | trim }}"""

GENERATOR_PATH = os.getenv("PAYSIM_GENERATOR_PATH", "/opt/airflow/processing/generators/generate_paysim.py")
SPARK_PROCESSOR_PATH = os.getenv("PAYSIM_PROCESSOR_PATH", "/opt/airflow/processing/spark/process_paysim.py")
UTIL_SCRIPT_PATH = "/opt/airflow/plugins/utils/paysim_time_calc.py"

BASE_ROWS = int(os.getenv("PAYSIM_BASE_ROWS", "2000"))
SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")

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
    params={
        'execution_date': None,  # Override date: YYYY-MM-DDTHH:MM:SS+00:00 (default: data_interval_start)
    },
    doc_md="""
    ## PaySim ETL Pipeline
    
    **Modes:**
    - Scheduled: Runs hourly, uses `data_interval_start`
    - Manual: Trigger with `{"execution_date": "2026-01-30T10:00:00+00:00"}`
    
    **Flow:** generate → process → validate → ingest → dbt → (trigger ML at 23:00)
    """,
) as dag:

    generate_data = BashOperator(
        task_id='generate_data',
        bash_command=f"""
        set -e

        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        STEP=$(python3 {UTIL_SCRIPT_PATH} "$EXEC_DATE" step)
        
        echo "--------------------------------------------"
        echo "Generating PaySim data"
        echo "Step: $STEP"
        echo "Execution Date: {{{{ ds }}}}"
        echo "Logical Date: $EXEC_DATE"
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
        
        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "$EXEC_DATE" all)
        
        echo "--------------------------------------------"
        echo "Processing PaySim data"
        echo "Step: $STEP"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "Execution Date: {{{{ ds }}}}"
        echo "--------------------------------------------"
        
        spark-submit \
            --master {SPARK_MASTER} \
            --driver-memory 800m \
            --executor-memory 1g \
            --conf spark.driver.maxResultSize=400m \
            --conf spark.executor.memoryOverhead=300m \
            --conf spark.driver.memoryOverhead=256m \
            --conf spark.memory.fraction=0.7 \
            --conf spark.memory.storageFraction=0.3 \
            --conf spark.sql.shuffle.partitions=20 \
            --conf spark.default.parallelism=20 \
            --conf spark.sql.adaptive.enabled=true \
            --conf spark.sql.adaptive.coalescePartitions.enabled=true \
            --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
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
            --conf spark.sql.sources.partitionOverwriteMode=dynamic \
            {SPARK_PROCESSOR_PATH} \
            --mode incremental \
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
        
        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "$EXEC_DATE" all)
        
        echo "--------------------------------------------"
        echo "Validating output"
        echo "Step: $STEP"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "--------------------------------------------"
        
        python3 /opt/airflow/plugins/utils/validate_minio.py \
            --bucket {os.getenv('MINIO_BUCKET_ANALYTICS', 'dl-analytics-g4igm3')} \
            --prefix ".warehouse/paysim_txn/part_dt=$PART_DT/part_hour=$PART_HOUR/"
        """,
    )

    ingest = BashOperator(
        task_id='ingest_to_clickhouse',
        bash_command=f"""
        set -e
        
        EXEC_DATE="{EXECUTION_DATE_TEMPLATE}"
        read STEP PART_DT PART_HOUR <<< $(python3 {UTIL_SCRIPT_PATH} "$EXEC_DATE" all)
        
        echo "--------------------------------------------"
        echo "Ingesting to ClickHouse"
        echo "Partition: part_dt=$PART_DT, part_hour=$PART_HOUR"
        echo "--------------------------------------------"
        
        python3 /opt/airflow/warehouse/clickhouse/clickhouse_loader.py \
            --mode incremental \
            --part_dt $PART_DT \
            --part_hour $PART_HOUR
        """,
        env={
            "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "clickhouse_admin"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_password"),
        }
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f"""
        set -e
        export PATH=$PATH:/home/airflow/.local/bin
        echo "--------------------------------------------"
        echo "Running dbt transformations"
        echo "--------------------------------------------"
        
        cd /opt/airflow/warehouse/dbt_clickhouse
        dbt run --profiles-dir .
        """,
        env={
            "CLICKHOUSE_HOST": os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            "CLICKHOUSE_PORT": os.getenv("CLICKHOUSE_PORT", "8123"),
            "CLICKHOUSE_USER": os.getenv("CLICKHOUSE_USER", "clickhouse_admin"),
            "CLICKHOUSE_PASSWORD": os.getenv("CLICKHOUSE_PASSWORD", "clickhouse_password"),
            "DBT_PROFILES_DIR": "/opt/airflow/warehouse/dbt_clickhouse"
        }
    )

    @task.branch(task_id='check_end_of_day')
    def check_end_of_day(**context):
        logical_date = context.get('data_interval_start') or context.get('execution_date')
        if logical_date.hour == 23:
            return 'trigger_ml_pipeline'
        return 'end_pipeline'

    check_ml_trigger = check_end_of_day()

    trigger_ml = TriggerDagRunOperator(
        task_id='trigger_ml_pipeline',
        trigger_dag_id='ml_extract_feature_paysim',
        execution_date='{{ ds }}',
        wait_for_completion=False,
    )

    end_pipeline = EmptyOperator(task_id='end_pipeline')

    generate_data >> process_data >> validate >> ingest >> dbt_run >> check_ml_trigger
    check_ml_trigger >> trigger_ml
    check_ml_trigger >> end_pipeline