from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import argparse
import os
from datetime import datetime

# Env Config
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

# Buckets
BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")
BUCKET_DATASCIENCE = os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience-gii2ij")

# Explicit Schema for robustness
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
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
    StructField("errorBalanceOrig", DoubleType(), True),
    StructField("errorBalanceDest", DoubleType(), True),
    StructField("is_errorBalanceOrig", IntegerType(), True),
    StructField("is_errorBalanceDest", IntegerType(), True),
    StructField("is_transfer", IntegerType(), True),
    StructField("is_cash_out", IntegerType(), True),
    StructField("is_merchant_dest", IntegerType(), True),
    StructField("hour_of_day", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("is_all_orig_balance", IntegerType(), True),
    StructField("is_dest_zero_init", IntegerType(), True),
    StructField("is_org_zero_init", IntegerType(), True),
    StructField("processed_at", TimestampType(), True),
    StructField("part_dt", StringType(), True),
    StructField("part_hour", StringType(), True),
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("BatchFeatureEngineering") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def process_batch_features(spark, execution_date):
    """
    Reads FULL history, calculates Window Features, filters for 'execution_date' (optional snapshot strategy),
    and saves to Feature Store.
    """
    input_path = f"s3a://{BUCKET_ANALYTICS}/.warehouse/paysim_txn/part_dt=*/part_hour=*"
    print(f"Reading full history from: {input_path}")
    
    try:
        # Use explicit schema for robustness
        df = spark.read.schema(ANALYTICS_SCHEMA).parquet(input_path)
        row_count = df.count()
        print(f"Loaded {row_count} rows from analytics layer")
        
        if row_count == 0:
            print("WARNING: No data found in analytics layer. Exiting.")
            return
    except Exception as e:
        print(f"Error reading data: {e}")
        raise  # Fail explicitly instead of silent return

    # Window specs for user-based features
    w_orig = Window.partitionBy("user_id").orderBy("transaction_time")
    w_dest = Window.partitionBy("merchant_id").orderBy("transaction_time")
    
    # Convert timestamp to seconds for range-based windows
    df = df.withColumn("ts_seconds", F.col("transaction_time").cast("long"))
    
    w_dest_1h = Window.partitionBy("merchant_id").orderBy("ts_seconds").rangeBetween(-3600, 0)
    w_dest_24h = Window.partitionBy("merchant_id").orderBy("ts_seconds").rangeBetween(-86400, 0)
    w_dest_past = Window.partitionBy("merchant_id").orderBy("ts_seconds").rowsBetween(Window.unboundedPreceding, -1)

    print("Calculating Window Features...")
    
    # Calculate IQR for outlier detection
    try:
        q1, q3 = df.approxQuantile("amount", [0.25, 0.75], 0.05)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        print(f"Outlier bounds: [{lower_bound:.2f}, {upper_bound:.2f}]")
    except Exception:
        lower_bound, upper_bound = 0.0, 999999999.9
        print("WARNING: Could not calculate IQR, using default bounds")

    features_df = df \
        .withColumn("orig_prev_tx_time", F.lag("transaction_time").over(w_orig)) \
        .withColumn("orig_delta_seconds", F.col("ts_seconds") - F.col("orig_prev_tx_time").cast("long")) \
        .withColumn("dest_prev_tx_time", F.lag("transaction_time").over(w_dest)) \
        .withColumn("dest_delta_seconds", F.col("ts_seconds") - F.col("dest_prev_tx_time").cast("long")) \
        .withColumn("dest_msg_count_1h", F.count("*").over(w_dest_1h)) \
        .withColumn("dest_amount_sum_1h", F.sum("amount").over(w_dest_1h)) \
        .withColumn("dest_msg_count_24h", F.count("*").over(w_dest_24h)) \
        .withColumn("dest_amount_sum_24h", F.sum("amount").over(w_dest_24h)) \
        .withColumn("dest_count_past", F.count("*").over(w_dest_past)) \
        .withColumn("dest_sum_amount_past", F.sum("amount").over(w_dest_past)) \
        .withColumn("dest_history_fraud_count", F.sum("isFraud").over(w_dest.rowsBetween(Window.unboundedPreceding, -1))) \
        .withColumn("is_amount_outlier", F.when((F.col("amount") < lower_bound) | (F.col("amount") > upper_bound), 1).otherwise(0))

    # Select final columns for Feature Store
    final_df = features_df.select(
        "transaction_time", "user_id", "merchant_id", "amount", "type", 
        F.col("isFraud").alias("is_fraud"),  # Rename for consistency
        "hour_of_day", "day_of_week", "is_all_orig_balance", "is_dest_zero_init", "is_org_zero_init",
        "is_transfer", "is_cash_out", "errorBalanceOrig", "errorBalanceDest",
        "is_errorBalanceOrig", "is_errorBalanceDest",
        "orig_delta_seconds", "dest_delta_seconds",
        "dest_msg_count_1h", "dest_amount_sum_1h",
        "dest_msg_count_24h", "dest_amount_sum_24h",
        "dest_count_past", "dest_sum_amount_past", "dest_history_fraud_count",
        "is_amount_outlier",
        "part_dt", "part_hour"
    )

    snapshot_date = execution_date if execution_date else datetime.now().strftime("%Y-%m-%d")
    output_path = f"s3a://{BUCKET_DATASCIENCE}/paysim_features/run_date={snapshot_date}"
    
    print(f"Saving Feature Snapshot to: {output_path}")
    final_df.write.mode("overwrite").partitionBy("part_dt", "part_hour").parquet(output_path)
    
    # Log final stats
    output_count = final_df.count()
    print(f"Batch Feature Engineering Complete. Wrote {output_count} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", help="Snapshot Date YYYY-MM-DD")
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        process_batch_features(spark, args.execution_date)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark:
            spark.stop()
