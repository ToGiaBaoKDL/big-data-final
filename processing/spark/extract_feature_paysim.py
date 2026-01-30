from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, ByteType
import argparse
import os
import sys
import traceback
from datetime import datetime, timedelta


# Env Config
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

# Buckets
BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")
BUCKET_DATASCIENCE = os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience-gii2ij")

# Explicit Schema for robustness
# NOTE: part_dt/part_hour are NOT included - they come from directory path (Hive-style partitioning)
# Spark infers them as IntegerType from path pattern: /part_dt=20251228/part_hour=00/
ANALYTICS_SCHEMA = StructType([
    StructField("transaction_time", TimestampType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("user_id", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", ByteType(), True),
    StructField("isFlaggedFraud", ByteType(), True),
    StructField("errorBalanceOrig", DoubleType(), True),
    StructField("errorBalanceDest", DoubleType(), True),
    StructField("is_errorBalanceOrig", ByteType(), True),
    StructField("is_errorBalanceDest", ByteType(), True),
    StructField("is_transfer", ByteType(), True),
    StructField("is_cash_out", ByteType(), True),
    StructField("is_merchant_dest", ByteType(), True),
    StructField("hour_of_day", ByteType(), True),
    StructField("day_of_week", ByteType(), True),
    StructField("is_all_orig_balance", ByteType(), True),
    StructField("is_dest_zero_init", ByteType(), True),
    StructField("is_org_zero_init", ByteType(), True),
    StructField("processed_at", TimestampType(), True),
])

# Base path for partition discovery
ANALYTICS_BASE_PATH = f"s3a://{BUCKET_ANALYTICS}/.warehouse/paysim_txn"


def create_spark_session():
    """
    Creates SparkSession with configs.
    When running via spark-submit, configs from CLI take precedence.
    When running standalone (local testing), configs from code are used.
    """
    builder = SparkSession.builder.appName("BatchFeatureEngineering")
    is_spark_submit = any('spark-submit' in arg for arg in sys.argv) or \
                      'PYSPARK_SUBMIT_ARGS' in os.environ
    
    if not is_spark_submit:
        # Local/standalone mode - set configs
        print("Running in LOCAL mode - applying S3A configs from code")
        builder = builder \
            .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
            .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
            .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
            .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    else:
        print("Running via SPARK-SUBMIT - using configs from command line")
    
    spark = builder.getOrCreate()
    return spark


def process_batch_features(spark, execution_date):
    """Reads history with rolling window, calculates Window Features for ML."""
    
    # Determine date range for reading
    # NOTE: part_dt is INTEGER (e.g., 20251228) inferred from path by Spark
    if execution_date and execution_date != "init":
        exec_dt = datetime.strptime(execution_date, "%Y-%m-%d")
        cutoff_part_dt = int(exec_dt.strftime("%Y%m%d"))  # Integer for partition filter
        
        # Use centralized config for training window
        start_dt = exec_dt - timedelta(days=30)  # 30-day rolling window
        start_part_dt = int(start_dt.strftime("%Y%m%d"))  # Integer for partition filter
        
        # Build path pattern for partition pruning
        input_path = f"{ANALYTICS_BASE_PATH}/part_dt=*/part_hour=*"
        print(f"Reading from: {input_path}")
        print(f"ROLLING WINDOW: {start_part_dt} <= part_dt <= {cutoff_part_dt} ({30} days)")
        print(f"  → Production best practice: prevents concept drift")
    else:
        # Init mode - read all data
        input_path = f"{ANALYTICS_BASE_PATH}/part_dt=*/part_hour=*"
        print(f"INIT MODE: Reading all available data from: {input_path}")
        start_part_dt = None
        cutoff_part_dt = None
    
    try:
        # Use explicit schema for data columns, basePath for partition discovery
        # basePath tells Spark where the partition columns start in the path
        df = spark.read.schema(ANALYTICS_SCHEMA) \
            .option("basePath", ANALYTICS_BASE_PATH) \
            .parquet(input_path)
        
        # Apply rolling window filter (Spark's partition pruning optimizes this)
        # part_dt is INTEGER type (inferred from path), so use integer comparison
        if start_part_dt and cutoff_part_dt:
            df = df.filter(
                (F.col("part_dt") >= start_part_dt) & 
                (F.col("part_dt") <= cutoff_part_dt)
            )
            
            # Verify partition count
            distinct_partitions = df.select("part_dt").distinct().collect()
            partition_count = len(distinct_partitions)
            print(f"PARTITION VERIFICATION: Found {partition_count} distinct part_dt values")
            if partition_count > 0:
                dates = sorted([r["part_dt"] for r in distinct_partitions])
                print(f"  → Actual date range: {dates[0]} to {dates[-1]}")
            if partition_count < 30:
                print(f"  → WARNING: Expected up to {30} days, got {partition_count}")
                print(f"  → This is OK if data doesn't span full window yet")
            print(f"  → Filter range: {start_part_dt} to {cutoff_part_dt}")
        
        if execution_date == "init":
            print("INIT MODE: Optimizing for large dataset processing")
            sample_count = df.limit(100).count()
            if sample_count == 0:
                print("WARNING: No data found in analytics layer. Exiting.")
                return
            print(f"Data exists (sample: {sample_count} rows). Processing full dataset...")
            df = df.persist(StorageLevel.DISK_ONLY)
        else:
            row_count = df.count()
            print(f"Loaded {row_count} rows from analytics layer (filtered)")
            if row_count == 0:
                print("WARNING: No data found in analytics layer. Exiting.")
                return
            print("Using DISK_ONLY storage for 30-day rolling window to prevent OOM")
            df = df.persist(StorageLevel.DISK_ONLY)
    except Exception as e:
        print(f"Error reading data: {e}")
        raise 

    # User-based (Origin) windows
    w_orig = Window.partitionBy("user_id").orderBy("transaction_time")
    w_orig_past = Window.partitionBy("user_id").orderBy("transaction_time").rowsBetween(Window.unboundedPreceding, -1)
    
    # Destination-based windows  
    w_dest = Window.partitionBy("merchant_id").orderBy("transaction_time")
    
    # Convert timestamp to seconds for range-based windows
    df = df.withColumn("ts_seconds", F.col("transaction_time").cast("long"))
    
    # Range-based windows for destination (EXCLUSIVE of current row: -3600 to -1)
    w_dest_1h = Window.partitionBy("merchant_id").orderBy("ts_seconds").rangeBetween(-3600, -1)
    w_dest_24h = Window.partitionBy("merchant_id").orderBy("ts_seconds").rangeBetween(-86400, -1)
    w_dest_past = Window.partitionBy("merchant_id").orderBy("ts_seconds").rowsBetween(Window.unboundedPreceding, -1)
    
    # Range-based windows for origin user (EXCLUSIVE of current row)
    w_orig_1h = Window.partitionBy("user_id").orderBy("ts_seconds").rangeBetween(-3600, -1)
    w_orig_24h = Window.partitionBy("user_id").orderBy("ts_seconds").rangeBetween(-86400, -1)

    print("Calculating Window Features (split into stages to manage memory)...")
    
    # Stage 1: Destination features
    print("  Stage 1/3: Calculating destination window features...")
    df = df \
        .withColumn("dest_prev_tx_time", F.lag("transaction_time").over(w_dest)) \
        .withColumn("dest_delta_seconds", F.col("ts_seconds") - F.col("dest_prev_tx_time").cast("long")) \
        .withColumn("dest_msg_count_1h", F.count("*").over(w_dest_1h)) \
        .withColumn("dest_amount_sum_1h", F.sum("amount").over(w_dest_1h)) \
        .withColumn("dest_msg_count_24h", F.count("*").over(w_dest_24h)) \
        .withColumn("dest_amount_sum_24h", F.sum("amount").over(w_dest_24h)) \
        .withColumn("dest_count_past", F.count("*").over(w_dest_past)) \
        .withColumn("dest_sum_amount_past", F.sum("amount").over(w_dest_past)) \
        .withColumn("dest_history_fraud_count", F.sum("isFraud").over(w_dest_past))
    
    # Stage 2: Origin features
    print("  Stage 2/3: Calculating origin window features...")
    df = df \
        .withColumn("orig_prev_tx_time", F.lag("transaction_time").over(w_orig)) \
        .withColumn("orig_delta_seconds", F.col("ts_seconds") - F.col("orig_prev_tx_time").cast("long")) \
        .withColumn("orig_msg_count_1h", F.count("*").over(w_orig_1h)) \
        .withColumn("orig_amount_sum_1h", F.sum("amount").over(w_orig_1h)) \
        .withColumn("orig_msg_count_24h", F.count("*").over(w_orig_24h)) \
        .withColumn("orig_amount_sum_24h", F.sum("amount").over(w_orig_24h)) \
        .withColumn("orig_prev_amount", F.lag("amount").over(w_orig)) \
        .withColumn("orig_count_past", F.count("*").over(w_orig_past)) \
        .withColumn("orig_sum_amount_past", F.sum("amount").over(w_orig_past)) \
        .withColumn("orig_avg_amount_past", F.avg("amount").over(w_orig_past)) \
        .withColumn("orig_history_fraud_count", F.sum("isFraud").over(w_orig_past))
    
    # Stage 3: Simple features
    print("  Stage 3/3: Calculating pattern features...")
    features_df = df \
        .withColumn("is_night_txn", F.when((F.col("hour_of_day") >= 22) | (F.col("hour_of_day") <= 6), 1).otherwise(0)) \
        .withColumn("is_round_amount", F.when(F.col("amount") % 1000 == 0, 1).otherwise(0)) \
        .withColumn("amount_log", F.log1p("amount"))

    # Select final columns for Feature Store
    final_df = features_df.select(
        "transaction_time", "user_id", "merchant_id", "amount", "type", 
        F.col("isFraud").alias("is_fraud"),
        "hour_of_day", "day_of_week",
        "is_transfer", "is_cash_out",
        # Time delta features
        "orig_delta_seconds", "dest_delta_seconds",
        # Destination window features
        "dest_msg_count_1h", "dest_amount_sum_1h",
        "dest_msg_count_24h", "dest_amount_sum_24h",
        "dest_count_past", "dest_sum_amount_past", "dest_history_fraud_count",
        # Origin window features
        "orig_msg_count_1h", "orig_amount_sum_1h",
        "orig_msg_count_24h", "orig_amount_sum_24h",
        "orig_prev_amount", "orig_count_past", "orig_sum_amount_past", 
        "orig_avg_amount_past", "orig_history_fraud_count",
        # Pattern & derived features
        "is_night_txn", "is_round_amount", "amount_log"
    )

    # Unpersist cached data
    df.unpersist()

    snapshot_date = execution_date if execution_date else datetime.now().strftime("%Y-%m-%d")
    output_path = f"s3a://{BUCKET_DATASCIENCE}/paysim_features/run_date={snapshot_date}"
    
    print(f"Saving Feature Snapshot to: {output_path}")
    
    # Optimize write for bulk vs incremental
    if execution_date == "init":
        print("INIT MODE: Optimizing write with repartition...")
        final_df = final_df.repartition(50)
        final_df.write.mode("overwrite").parquet(output_path)
        print("Batch Feature Engineering Complete (bulk mode).")
    else:
        # Incremental mode - use repartition for 30-day window
        output_count = final_df.count()
        print(f"Total rows: {output_count:,}")
        print(f"Repartitioning to 10 files for 30-day window write...")
        final_df = final_df.repartition(10)
        final_df.write.mode("overwrite").parquet(output_path)
        print(f"Batch Feature Engineering Complete. Wrote {output_count:,} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["init", "incremental"], default="incremental",
                        help="init: first time full extraction, incremental: daily update")
    parser.add_argument("--execution_date", help="Snapshot Date YYYY-MM-DD")
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        
        if args.mode == "init":
            print("INIT MODE: Full Feature Extraction")
            process_batch_features(spark, args.execution_date or "init")
        else:
            process_batch_features(spark, args.execution_date)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        traceback.print_exc()
        raise
    finally:
        if spark:
            print("Graceful shutdown: stopping SparkSession...")
            try:
                # Give executors time to finish cleanup
                import time
                time.sleep(2)
                spark.stop()
                print("SparkSession stopped successfully.")
            except Exception as stop_err:
                print(f"Warning during shutdown (can be ignored): {stop_err}")
