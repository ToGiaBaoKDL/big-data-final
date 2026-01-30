import os
import argparse
import clickhouse_connect
import sys
import re
from collections import OrderedDict

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
                'max_memory_usage': '2000000000',  # 2GB per query
                'max_bytes_before_external_group_by': '500000000',  # 500MB before spill to disk
                'max_bytes_before_external_sort': '500000000',
            }
        )
    return _client


def ingest_partition(client, bucket, table, part_dt, part_hour):
    """
    Ingests a specific partition from MinIO (S3) into ClickHouse.
    Uses DELETE + INSERT for idempotency at hour level.
    """
    access_key = os.getenv("MINIO_ROOT_USER", "minio_admin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
    
    if part_dt == "all":
        s3_url = f"http://minio:9000/{bucket}/.warehouse/paysim_txn/**/*.parquet"
        print(f"BULK MODE: Ingesting ALL data from {s3_url}...")   
    else:
        s3_url = f"http://minio:9000/{bucket}/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/*.parquet"
        print(f"S3 URL: {s3_url}")
        
        try:
            test_query = f"""
            SELECT count() 
            FROM s3(
                '{s3_url}',
                '{access_key}',
                '{secret_key}',
                'Parquet'
            )
            """
            test_count = client.command(test_query)
            print(f"Found {test_count} rows in S3 path")
            
            if test_count == 0:
                print(f"WARNING: No data found in {s3_url}")
                print(f"This might be due to:")
                print(f"  1. Path mismatch (check part_dt/part_hour format)")
                print(f"  2. Data not yet written by Spark")
                print(f"  3. Wrong bucket name")
                return 0
        except Exception as e:
            print(f"ERROR checking S3 path: {e}")
            print(f"Continuing with ingestion anyway...")
        
        print(f"Deleting existing data for {part_dt}/{part_hour} to ensure idempotency...")
        try:
            delete_query = f"""
                ALTER TABLE {table} DELETE 
                WHERE part_dt = '{part_dt}' AND part_hour = '{part_hour}'
            """
            client.command(delete_query)
        except Exception as e:
            print(f"Warning during delete (might not exist yet): {e}")

    # Column list matching ClickHouse table schema
    COLUMNS = """
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
        processed_at,
        part_dt,
        part_hour
    """.strip()

    # 2. Ingest from S3 with explicit column mapping
    query = f"""
    INSERT INTO {table} ({COLUMNS})
    SELECT 
        {COLUMNS}
    FROM s3(
        '{s3_url}',
        '{access_key}',
        '{secret_key}',
        'Parquet'
    )
    SETTINGS 
        input_format_allow_errors_ratio=0.1,
        max_memory_usage=1000000000,
        max_threads=2
    """
    
    print(f"Executing Ingestion Query for {part_dt}/{part_hour}...")
    client.command(query)
    
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
            
            # Get list of unique partitions from S3
            list_query = f"""
            SELECT DISTINCT 
                _path
            FROM s3(
                'http://minio:9000/{bucket}/.warehouse/paysim_txn/**/*.parquet',
                '{access_key}',
                '{secret_key}',
                'Parquet'
            )
            LIMIT 1000
            """
            
            # Extract unique part_dt/part_hour combos from paths
            result = client.query(list_query)
            partitions = OrderedDict()
            for row in result.result_rows:
                path = row[0]
                match = re.search(r'part_dt=(\d+)/part_hour=(\d+)', path)
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
