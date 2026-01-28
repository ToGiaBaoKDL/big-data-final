from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse
import os
from datetime import datetime

# Env Config
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

# Buckets
BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")
BUCKET_DATASCIENCE = os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience")

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
        df = spark.read.parquet(input_path)
    except Exception as e:
        print(f"Error reading data: {e}")
        return

    w_orig = Window.partitionBy("user_id").orderBy("transaction_time")
    w_dest = Window.partitionBy("merchant_id").orderBy("transaction_time")
    
    df = df.withColumn("ts_seconds", F.col("transaction_time").cast("long"))
    
    w_dest_past = Window.partitionBy("merchant_id").orderBy("ts_seconds").rowsBetween(Window.unboundedPreceding, -1)

    print("Calculating Window Features...")
    
    try:
        q1, q3 = df.approxQuantile("amount", [0.25, 0.75], 0.05)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
    except Exception:
        lower_bound, upper_bound = 0.0, 999999999.9

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
        "is_fraud",
        "hour_of_day", "day_of_week", "is_all_orig_balance", "is_dest_zero_init", "is_org_zero_init",
        "is_transfer", "is_cash_out", "errorBalanceOrig", "errorBalanceDest",
        F.when(F.col("errorBalanceOrig") != 0, 1).otherwise(0).alias("is_errorBalanceOrig"),
        F.when(F.col("errorBalanceDest") != 0, 1).otherwise(0).alias("is_errorBalanceDest"),
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
    print("Batch Feature Engineering Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution_date", help="Snapshot Date YYYYMMDD")
    args = parser.parse_args()
    
    spark = create_spark_session()
    process_batch_features(spark, args.execution_date)
    spark.stop()
