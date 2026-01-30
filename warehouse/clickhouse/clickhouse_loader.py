import os
import argparse
import clickhouse_connect
import sys
import re
from collections import OrderedDict
from minio import Minio

# Global client for connection reuse
_client = None

def get_client(host, port, user, password):
    """Get or create a reusable ClickHouse client."""
    global _client
    if _client is None:
        print(f"Connecting to ClickHouse {host}:{port}...")
        _client = clickhouse_connect.get_client(
            host=host, 
            port=port, 
            username=user, 
            password=password,
            settings={
                # Memory optimization settings
                'max_memory_usage': '3600000000',  # 3.6GB per query
                'max_bytes_before_external_group_by': '500000000',  # 500MB before spill to disk
                'max_bytes_before_external_sort': '500000000',
            }
        )
    return _client


def ingest_partition(client, bucket, table, part_dt, part_hour):
    """
    Ingests a specific partition from MinIO (S3) into ClickHouse.
    Uses synchronous DELETE + INSERT for idempotency at hour level.
    """
    access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
    minio_host_for_clickhouse = os.getenv("MINIO_HOST_FOR_CLICKHOUSE", "minio:9000")
    
    if part_dt == "all":
        s3_url = f"http://{minio_host_for_clickhouse}/{bucket}/.warehouse/paysim_txn/**/*.parquet"
        print(f"BULK MODE: Ingesting ALL data from {s3_url}...")   
    else:
        s3_url = f"http://{minio_host_for_clickhouse}/{bucket}/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/*.parquet"
        print(f"S3 URL: {s3_url}")
        print(f"Deleting existing data for {part_dt}/{part_hour} to ensure idempotency...")
        try:
            delete_query = f"""
                DELETE FROM {table} 
                WHERE part_dt = '{part_dt}' AND part_hour = '{part_hour}'
            """
            client.command(delete_query)
            print(f"Deleted existing data for partition {part_dt}/{part_hour}")
        except Exception as e:
            print(f"Lightweight delete failed, using mutation with wait: {e}")
            try:
                alter_query = f"""
                    ALTER TABLE {table} DELETE 
                    WHERE part_dt = '{part_dt}' AND part_hour = '{part_hour}'
                    SETTINGS mutations_sync = 1
                """
                client.command(alter_query)
                print(f"Mutation delete completed for partition {part_dt}/{part_hour}")
            except Exception as e2:
                print(f"Warning during delete (table might be empty): {e2}")

    # Column list matching parquet file (without partition columns which are in path, not file)
    PARQUET_COLUMNS = """
        transaction_time,
        type,
        amount,
        user_id,
        oldbalanceOrg,
        newbalanceOrig,
        merchant_id,
        oldbalanceDest,
        newbalanceDest,
        isFraud,
        isFlaggedFraud,
        errorBalanceOrig,
        errorBalanceDest,
        is_transfer,
        is_cash_out,
        is_merchant_dest,
        hour_of_day,
        day_of_week,
        is_all_orig_balance,
        is_dest_zero_init,
        is_org_zero_init,
        is_errorBalanceOrig,
        is_errorBalanceDest,
        processed_at
    """.strip()
    
    # All columns for INSERT (including partition columns)
    ALL_COLUMNS = f"{PARQUET_COLUMNS}, part_dt, part_hour"

    # 2. Ingest from S3 with explicit column mapping
    # IMPORTANT: part_dt and part_hour are NOT in parquet files (Hive-style partitioning)
    # We extract them from the virtual column _path using regex
    # Path format: .../.warehouse/paysim_txn/part_dt=YYYYMMDD/part_hour=HH/file.parquet
    query = f"""
    INSERT INTO {table} ({ALL_COLUMNS})
    SELECT 
        {PARQUET_COLUMNS},
        -- Extract part_dt from path: ...part_dt=YYYYMMDD/...
        extractAllGroupsHorizontal(_path, 'part_dt=([0-9]+)')[1][1] AS part_dt,
        -- Extract part_hour from path: ...part_hour=HH/...
        extractAllGroupsHorizontal(_path, 'part_hour=([0-9]+)')[1][1] AS part_hour
    FROM s3(
        '{s3_url}',
        '{access_key}',
        '{secret_key}',
        'Parquet'
    )
    SETTINGS 
        input_format_allow_errors_ratio=0.1,
        max_memory_usage=300000000,
        max_threads=1,
        max_insert_threads=1,
        max_block_size=5000,
        min_insert_block_size_rows=5000,
        max_read_buffer_size=524288,
        input_format_parquet_max_block_size=5000
    """
    
    print(f"Executing Ingestion Query for {part_dt}/{part_hour}...")
    client.command(query)
    
    # Force memory release after each partition
    try:
        client.command("SYSTEM JEMALLOC PURGE")
    except Exception:
        pass  # Ignore if command not available
    
    # 3. Verify count
    if part_dt == "all":
        count = client.command(f"SELECT count() FROM {table}")
        print(f"Ingestion Successful (BULK MODE). Total {count} rows in {table}.")
    else:
        count = client.command(f"SELECT count() FROM {table} WHERE part_dt='{part_dt}' AND part_hour='{part_hour}'")
        print(f"Ingestion Successful. Inserted {count} rows into {table}.")
    
    return count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load data from MinIO to ClickHouse")
    parser.add_argument("--mode", choices=["init", "incremental"], default="incremental",
                        help="init: load all data, incremental: load specific partition")
    parser.add_argument("--part_dt", help="Partition Date YYYYMMDD (required for incremental)")
    parser.add_argument("--part_hour", help="Partition Hour HH (required for incremental)")
    parser.add_argument("--table", default="finance_dw.paysim_txn")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == "incremental" and (not args.part_dt or not args.part_hour):
        parser.error("--part_dt and --part_hour are required for incremental mode")
    
    try:
        # Get connection config
        host = os.getenv("CLICKHOUSE_HOST", "clickhouse")
        port = int(os.getenv("CLICKHOUSE_PORT", 8123))
        user = os.getenv("CLICKHOUSE_USER", "clickhouse_admin")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        bucket = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")
        access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
        
        # Get reusable client
        client = get_client(host, port, user, password)
        
        if args.mode == "init":
            print("INIT MODE: Loading data partition by partition to avoid OOM")
            
            # Truncate table for clean init (idempotency)
            print(f"TRUNCATING table {args.table} for clean init...")
            client.command(f"TRUNCATE TABLE {args.table}")
            
            # Get list of partitions from MinIO directly (avoid heavy S3 DISTINCT query)
            minio_endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", "").replace("https://", "")
            minio_client = Minio(
                minio_endpoint,
                access_key=access_key,
                secret_key=secret_key,
                secure=False
            )
            
            print(f"Listing partitions from MinIO bucket: {bucket}")
            partitions = OrderedDict()
            prefix = ".warehouse/paysim_txn/"
            
            # List objects and extract partition info
            objects = minio_client.list_objects(bucket, prefix=prefix, recursive=True)
            for obj in objects:
                if obj.object_name.endswith('.parquet'):
                    match = re.search(r'part_dt=(\d+)/part_hour=(\d+)', obj.object_name)
                    if match:
                        key = (match.group(1), match.group(2))
                        partitions[key] = True
            
            partitions = list(partitions.keys())
            print(f"Found {len(partitions)} unique partitions to load")
            
            total_rows = 0
            for i, (part_dt, part_hour) in enumerate(partitions):
                print(f"\n[{i+1}/{len(partitions)}] Loading partition {part_dt}/{part_hour}...")
                rows = ingest_partition(
                    client=client,
                    bucket=bucket, table=args.table,
                    part_dt=part_dt, part_hour=part_hour
                )
                total_rows += rows
            
            print(f"\n=== INIT COMPLETE: {total_rows} total rows loaded ===")
        else:
            ingest_partition(
                client=client,
                bucket=bucket,
                table=args.table,
                part_dt=args.part_dt,
                part_hour=args.part_hour
            )
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
