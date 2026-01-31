import os

# MinIO buckets
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")

BUCKET_ANALYTICS = os.getenv("MINIO_BUCKET_ANALYTICS", "dl-analytics-g4igm3")
BUCKET_DATASCIENCE = os.getenv("MINIO_BUCKET_DATASCIENCE", "dl-datascience-gii2ij")
BUCKET_MLFLOW = os.getenv("MINIO_BUCKET_MLFLOW", "dl-mlflow-7c0d3e")

# Feature store path
FEATURE_STORE_PATH = f"s3a://{BUCKET_DATASCIENCE}/paysim_features"

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow-server:5000")
MLFLOW_EXPERIMENT_NAME = "paysim_fraud_detection"
MLFLOW_MODEL_NAME = "paysim_fraud_model"

TARGET_COL = "is_fraud"
TRAIN_RATIO = 0.8
RANDOM_SEED = 42

# Training window configuration
# Options: 30 (fast iteration), 90 (balanced), 180 (stable patterns)
TRAINING_WINDOW_DAYS = 30 

# Features for ML training
FEATURE_COLS = [
    # Base amount features
    "amount",
    "amount_log",
    
    # Time features
    "hour_of_day",
    "day_of_week",
    "is_night_txn",
    
    # Transaction type
    "is_transfer",
    "is_cash_out",
    
    # Time delta features (velocity)
    "orig_delta_seconds",
    "dest_delta_seconds",
    
    # Destination window features (velocity)
    "dest_msg_count_1h",
    "dest_amount_sum_1h",
    "dest_msg_count_24h",
    "dest_amount_sum_24h",
    "dest_count_past",
    "dest_sum_amount_past",
    
    # Origin window features (velocity)
    "orig_msg_count_1h",
    "orig_amount_sum_1h",
    "orig_msg_count_24h",
    "orig_amount_sum_24h",
    "orig_prev_amount",
    "orig_count_past",
    "orig_sum_amount_past",
    "orig_avg_amount_past",
    
    # Pattern features
    "is_round_amount"
]

# Columns to drop from input
DROP_COLS = [
    "user_id",
    "merchant_id", 
    "transaction_time",
    "type",
    "part_dt",
    "part_hour"
]

XGBOOST_PARAMS = {
    "max_depth": 6,
    "n_estimators": 200,
    "learning_rate": 0.05,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "eval_metric": "aucpr",
    "random_state": RANDOM_SEED,
    "n_jobs": -1
}

# For PySpark ML Pipeline
SPARK_LR_PARAMS = {
    "regParam": [0.01, 0.1],
    "elasticNetParam": [0.0, 0.5, 1.0],
    "maxIter": 100
}
