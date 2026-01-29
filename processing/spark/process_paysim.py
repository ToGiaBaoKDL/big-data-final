from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
import argparse
import os

# Env Config
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

# Buckets
BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "dl-landing-8f42a1")
BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")

# Explicit Schema for Landing Data (Bronze)
LANDING_SCHEMA = StructType([
    StructField("transaction_time", TimestampType(), True),
    StructField("type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("nameOrig", StringType(), True),
    StructField("oldbalanceOrg", DoubleType(), True),
    StructField("newbalanceOrig", DoubleType(), True),
    StructField("nameDest", StringType(), True),
    StructField("oldbalanceDest", DoubleType(), True),
    StructField("newbalanceDest", DoubleType(), True),
    StructField("isFraud", IntegerType(), True),
    StructField("isFlaggedFraud", IntegerType(), True),
])


def create_spark_session():
    spark = SparkSession.builder \
        .appName("PaySimProcessor") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def process_partition(spark, part_dt, part_hour):
    """
    Reads a specific partition from Landing, Clean & Transform, Write to Analytics.
    """
    input_path = f"s3a://{BUCKET_LANDING}/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}"
    output_path = f"s3a://{BUCKET_ANALYTICS}/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}"
    
    print(f"Reading from: {input_path}")
    
    try:
        df = spark.read.schema(LANDING_SCHEMA).parquet(input_path)
        row_count = df.count()
        print(f"Loaded {row_count} rows from landing")
        if row_count == 0:
            raise ValueError(f"Empty partition: {input_path}")
    except Exception as e:
        print(f"ERROR: Failed to read partition: {input_path}")
        raise

    # --- 1. Bronze to Silver (Cleaning) ---
    print("Applying Cleaning logic...")
    df_clean = df.select(
        col("transaction_time").cast("timestamp"),
        trim(col("type")).alias("type"),
        col("amount").cast("double"),
        col("nameOrig").alias("user_id"),
        col("oldbalanceOrg").cast("double"),
        col("newbalanceOrig").cast("double"),
        col("nameDest").alias("merchant_id"),
        col("oldbalanceDest").cast("double"),
        col("newbalanceDest").cast("double"),
        col("isFraud").cast("int"),
        col("isFlaggedFraud").cast("int")
    )
    
    df_clean = df_clean.dropDuplicates()
    df_clean = df_clean.withColumn("processed_at", current_timestamp())

    # --- 2. Silver to Feature (Feature Engineering) ---
    print("Applying Feature Engineering logic...")
    
    # Balance Errors
    df_features = df_clean.withColumn("errorBalanceOrig", (col("oldbalanceOrg") - col("amount")) - col("newbalanceOrig")) \
                          .withColumn("errorBalanceDest", (col("oldbalanceDest") + col("amount")) - col("newbalanceDest"))
    
    # Balance Error Flags
    df_features = df_features.withColumn("is_errorBalanceOrig", when(col("errorBalanceOrig") != 0, 1).otherwise(0)) \
                             .withColumn("is_errorBalanceDest", when(col("errorBalanceDest") != 0, 1).otherwise(0))

    # One-hot encoding
    df_features = df_features.withColumn("is_transfer", when(col("type") == "TRANSFER", 1).otherwise(0)) \
                             .withColumn("is_cash_out", when(col("type") == "CASH_OUT", 1).otherwise(0))

    # Merchant Flag
    df_features = df_features.withColumn("is_merchant_dest", when(col("merchant_id").startswith("M"), 1).otherwise(0))

    # Time Features (Stateless)
    df_features = df_features.withColumn("hour_of_day", F.hour("transaction_time")) \
                             .withColumn("day_of_week", F.dayofweek("transaction_time"))

    # "Emptied Account": Amount equals old balance (User drained everything)
    df_features = df_features.withColumn("is_all_orig_balance", when(col("amount") == col("oldbalanceOrg"), 1).otherwise(0))
    
    # "Zero Init Dest": Destination had 0 balance before (New account?)
    df_features = df_features.withColumn("is_dest_zero_init", when(col("oldbalanceDest") == 0, 1).otherwise(0))
    
    # "Zero Init Orig": Origin had 0 balance before (Unusual?)
    df_features = df_features.withColumn("is_org_zero_init", when(col("oldbalanceOrg") == 0, 1).otherwise(0))

    # --- 3. Add partition columns ---
    df_features = df_features.withColumn("part_dt", lit(part_dt)) \
                             .withColumn("part_hour", lit(part_hour))

    # --- 4. Write to Analytics (Partitioned by dt/hour) ---
    print(f"Writing to: {output_path}")
    
    output_count = df_features.count()
    df_features.write.mode("overwrite").parquet(output_path)
    
    print(f"Processing Complete. Wrote {output_count} rows.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--part_dt", required=True, help="Partition Date YYYYMMDD")
    parser.add_argument("--part_hour", required=True, help="Partition Hour HH")
    
    args = parser.parse_args()
    
    spark = create_spark_session()
    try:
        process_partition(spark, args.part_dt, args.part_hour)
    finally:
        spark.stop()
