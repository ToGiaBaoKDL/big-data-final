import os
import argparse
import clickhouse_connect
import sys


def ingest_partition(host, port, user, password, bucket, table, part_dt, part_hour):
    """
    Ingests a specific partition from MinIO (S3) into ClickHouse.
    Uses DELETE + INSERT for idempotency at hour level.
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
    
    # 1. FIXED: Delete only the specific hour, not the entire day partition
    print(f"Deleting existing data for {part_dt}/{part_hour} to ensure idempotency...")
    try:
        delete_query = f"""
            ALTER TABLE {table} DELETE 
            WHERE part_dt = '{part_dt}' AND part_hour = '{part_hour}'
        """
        client.command(delete_query)
        # Wait for mutation to complete
        client.command(f"OPTIMIZE TABLE {table} FINAL")
    except Exception as e:
        print(f"Warning during delete (might not exist yet): {e}")

    # 2. Ingest from S3
    query = f"""
    INSERT INTO {table}
    SELECT *
    FROM s3(
        '{s3_url}',
        '{access_key}',
        '{secret_key}',
        'Parquet'
    )
    SETTINGS input_format_allow_errors_ratio=0.1
    """
    
    print(f"Executing Ingestion Query for {part_dt}/{part_hour}...")
    client.command(query)
    
    # 3. Verify count
    count = client.command(f"SELECT count() FROM {table} WHERE part_dt='{part_dt}' AND part_hour='{part_hour}'")
    print(f"Ingestion Successful. Inserted {count} rows into {table}.")
    
    return count


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
            user=os.getenv("CLICKHOUSE_USER", "clickhouse_admin"),
            password=os.getenv("CLICKHOUSE_PASSWORD", ""),
            bucket=os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3"),
            table=args.table,
            part_dt=args.part_dt,
            part_hour=args.part_hour
        )
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
