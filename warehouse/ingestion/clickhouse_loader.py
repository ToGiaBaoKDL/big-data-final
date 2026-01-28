import os
import argparse
import clickhouse_connect
import sys

def ingest_partition(host, port, user, password, bucket, table, part_dt, part_hour):
    """
    Ingests a specific partition from MinIO (S3) into ClickHouse.
    """
    print(f"Connecting to ClickHouse {host}:{port}...")
    client = clickhouse_connect.get_client(
        host=host, 
        port=port, 
        username=user, 
        password=password
    )
    
    # MinIO S3 Source Configuration
    s3_url = f"http://minio:9000/{bucket}/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/*.parquet"
    access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
    
    # 1. Idempotency: Drop existing partition first to prevent duplicates on Retry
    print(f"Dropping partition {part_dt} to ensure idempotency...")
    try:
        drop_query = f"ALTER TABLE {table} DROP PARTITION '{part_dt}'"
        client.command(drop_query)
    except Exception as e:
        print(f"Warning dropping partition (might not exist yet): {e}")

    # 2. Ingest
    query = f"""
    INSERT INTO {table}
    SELECT *
    FROM s3(
        '{s3_url}',
        '{access_key}',
        '{secret_key}',
        'Parquet'
    )
    SETTINGS input_format_allow_errors_ration=0.1
    """
    
    print(f"Executing Ingestion Query for {part_dt}/{part_hour}...")
    
    client.command(query)
    
    # Verify count
    count = client.command(f"SELECT count() FROM {table} WHERE part_dt='{part_dt}' AND part_hour='{part_hour}'")
    print(f"Ingestion Successful. Inserted {count} rows into {table}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part_dt", required=True)
    parser.add_argument("--part_hour", required=True)
    parser.add_argument("--table", default="finance_dw.paysim_txn")
    
    args = parser.parse_args()
    
    try:
        ingest_partition(
            host=os.getenv("CLICKHOUSE_HOST", "clickhouse"),
            port=int(os.getenv("CLICKHOUSE_PORT", 8123)),
            user=os.getenv("CLICKHOUSE_USER", "default"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            bucket=os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-8f42a1"),
            table=args.table,
            part_dt=args.part_dt,
            part_hour=args.part_hour
        )
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)
