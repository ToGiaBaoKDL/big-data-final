from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, current_timestamp, when, lit, lpad
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType, LongType
import os
import sys
import argparse
import traceback

# Env Config
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

# Buckets
BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "dl-landing-8f42a1")
BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")


def create_spark_session():
    """
    Creates SparkSession with configs.
    When running via spark-submit, configs from CLI take precedence.
    When running standalone (local testing), configs from code are used.
    """
    builder = SparkSession.builder.appName("PaySimProcessor")    
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
    
    builder = builder.config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    
    spark = builder.getOrCreate()
    return spark


def process_partition(spark, part_dt, part_hour):
    """
    Reads a specific partition from Landing, Clean & Transform, Write to Analytics.
    If part_dt="all", reads recursively and preserves partition structure.
    
    IMPORTANT: Uses Hive-style partitioning consistently:
    - Partition columns are in PATH, not in parquet files
    - Uses basePath for reading to infer partition columns
    - Uses partitionBy for writing (removes columns from file, puts in path)
    """
    # Base path for partition discovery (Hive-style)
    landing_base_path = f"s3a://{BUCKET_LANDING}/.warehouse/paysim_txn"
    analytics_base_path = f"s3a://{BUCKET_ANALYTICS}/.warehouse/paysim_txn"
    
    if part_dt == "all":
        input_path = f"{landing_base_path}/part_dt=*/part_hour=*"
        output_path = analytics_base_path
        print("Running in BULK MODE (ALL partitions)")
    else:
        input_path = f"{landing_base_path}/part_dt={part_dt}/part_hour={part_hour}"
        output_path = analytics_base_path
        print(f"Running in INCREMENTAL MODE (partition: {part_dt}/{part_hour})")
    
    print(f"Reading from: {input_path}")
    
    try:
        # Use basePath to infer partition columns from path (Hive-style partitioning)
        # Partition columns (part_dt, part_hour) are NOT in parquet files, only in directory structure
        df = spark.read.option("basePath", landing_base_path).parquet(input_path)
        
        if part_dt != "all":
            row_count = df.count()
            print(f"Loaded {row_count} rows from landing")
            if row_count == 0:
                raise ValueError(f"Empty partition: {input_path}")
        else:
            print("BULK MODE: Skipping expensive count operation")
            # Sample check instead
            sample_count = df.limit(10).count()
            if sample_count == 0:
                raise ValueError(f"No data found in {input_path}")
            print(f"Data exists (sample: {sample_count} rows)")
    except Exception as e:
        print(f"ERROR: Failed to read partition: {input_path}")
        raise

    # --- 1. Bronze to Silver (Cleaning) ---
    print("Applying Cleaning logic...")
    
    # Define selection list
    select_cols = [
        col("transaction_time").cast("timestamp"),
        trim(col("type")).alias("type"),
        col("amount").cast("double"),
        col("nameOrig").alias("user_id"),
        col("oldbalanceOrg").cast("double"),
        col("newbalanceOrig").cast("double"),
        col("nameDest").alias("merchant_id"),
        col("oldbalanceDest").cast("double"),
        col("newbalanceDest").cast("double"),
        col("isFraud").cast("byte"),
        col("isFlaggedFraud").cast("byte")
    ]
    
    # Add partition columns (inferred from path by Spark with basePath option)
    # These are IntegerType by default from Hive-style partitioning
    if "part_dt" in df.columns:
        select_cols.append(col("part_dt"))
    if "part_hour" in df.columns:
        select_cols.append(col("part_hour"))
    
    df_clean = df.select(*select_cols)
    
    # Conditional deduplication
    if part_dt == "all":
        print("BULK MODE: Skipping dropDuplicates (assume source is clean)")
        # For bulk init, trust the source data or handle duplicates at ingestion time
    else:
        df_clean = df_clean.dropDuplicates()
    
    df_clean = df_clean.withColumn("processed_at", current_timestamp())

    # --- 2. Silver to Feature (Feature Engineering) ---
    print("Applying Feature Engineering logic...")
    
    # Balance Errors
    df_features = df_clean.withColumn("errorBalanceOrig", (col("oldbalanceOrg") - col("amount")) - col("newbalanceOrig")) \
                          .withColumn("errorBalanceDest", (col("oldbalanceDest") + col("amount")) - col("newbalanceDest"))
    
    # Balance Error Flags (use byte for storage optimization)
    df_features = df_features.withColumn("is_errorBalanceOrig", when(col("errorBalanceOrig") != 0, 1).otherwise(0).cast("byte")) \
                             .withColumn("is_errorBalanceDest", when(col("errorBalanceDest") != 0, 1).otherwise(0).cast("byte"))

    # One-hot encoding (use byte for storage optimization)
    df_features = df_features.withColumn("is_transfer", when(col("type") == "TRANSFER", 1).otherwise(0).cast("byte")) \
                             .withColumn("is_cash_out", when(col("type") == "CASH_OUT", 1).otherwise(0).cast("byte"))

    # Merchant Flag (use byte for storage optimization)
    df_features = df_features.withColumn("is_merchant_dest", when(col("merchant_id").startswith("M"), 1).otherwise(0).cast("byte"))

    # Time Features (use byte for storage optimization)
    df_features = df_features.withColumn("hour_of_day", F.hour("transaction_time").cast("byte")) \
                             .withColumn("day_of_week", F.dayofweek("transaction_time").cast("byte"))

    # "Emptied Account": Amount equals old balance (use byte)
    df_features = df_features.withColumn("is_all_orig_balance", when(col("amount") == col("oldbalanceOrg"), 1).otherwise(0).cast("byte"))
    
    # "Zero Init Dest": Destination had 0 balance before (use byte)
    df_features = df_features.withColumn("is_dest_zero_init", when(col("oldbalanceDest") == 0, 1).otherwise(0).cast("byte"))
    
    # "Zero Init Orig": Origin had 0 balance before (use byte)
    df_features = df_features.withColumn("is_org_zero_init", when(col("oldbalanceOrg") == 0, 1).otherwise(0).cast("byte"))

    # --- 3. Prepare partition columns for write ---
    # Spark inferred part_dt and part_hour as IntegerType from path
    # We need to convert them to String for consistent partitionBy() behavior
    if part_dt != "all":
        # Incremental mode: add partition columns as strings (from arguments)
        df_features = df_features.withColumn("part_dt", lit(part_dt)) \
                                 .withColumn("part_hour", lit(part_hour))
    else:
        # Bulk mode: convert existing integer partition columns to string with zero-padding
        df_features = df_features.withColumn("part_dt", col("part_dt").cast("string")) \
                                 .withColumn("part_hour", lpad(col("part_hour").cast("string"), 2, "0"))

    # --- 4. Write to Analytics (always use partitionBy for Hive-style consistency) ---
    print(f"Writing to: {output_path}")
    
    if part_dt == "all":
        # Bulk mode: repartition for efficient write
        print("Repartitioning for optimized bulk write...")
        df_features = df_features.repartition(200, "part_dt", "part_hour")
        print("Writing with partitionBy (Hive-style: columns in path, not in file)...")
        df_features.write.mode("overwrite").partitionBy("part_dt", "part_hour").parquet(output_path)
        print("Processing Complete (bulk mode).")
    else:
        # Incremental mode: write to specific partition
        # partitionBy ensures partition columns are in PATH, not in parquet file
        output_count = df_features.count()
        print(f"Writing with partitionBy to specific partition (Hive-style)...")
        df_features.write.mode("overwrite").partitionBy("part_dt", "part_hour").parquet(output_path)
        print(f"Processing Complete. Wrote {output_count} rows to partition {part_dt}/{part_hour}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["init", "incremental"], default="incremental",
                        help="init: process all partitions, incremental: process specific partition")
    parser.add_argument("--part_dt", help="Partition Date YYYYMMDD (required for incremental)")
    parser.add_argument("--part_hour", help="Partition Hour HH (required for incremental)")
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == "incremental" and (not args.part_dt or not args.part_hour):
        parser.error("--part_dt and --part_hour are required for incremental mode")
    
    spark = None
    try:
        spark = create_spark_session()
        
        if args.mode == "init":
            print("INIT MODE: Processing ALL partitions")
            process_partition(spark, "all", "all")
        else:
            process_partition(spark, args.part_dt, args.part_hour)
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
