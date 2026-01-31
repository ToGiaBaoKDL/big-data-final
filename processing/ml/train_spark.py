"""
PySpark ML Training for PaySim Fraud Detection.
Uses PySpark ML Pipeline with MLflow tracking.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, ChiSqSelector
from pyspark.ml.classification import LogisticRegression, GBTClassifier, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import MulticlassMetrics

import mlflow
import mlflow.spark
import os
import argparse
from datetime import datetime

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    FEATURE_STORE_PATH, TARGET_COL, FEATURE_COLS, TRAIN_RATIO, RANDOM_SEED,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME, MLFLOW_MODEL_NAME,
    SPARK_LR_PARAMS
)


def create_spark_session():
    """Create Spark session with S3 (MinIO) support."""
    spark = SparkSession.builder \
        .appName("PaySimFraudTraining") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark


def load_features(spark, run_date=None):
    """Load features from feature store."""
    if run_date:
        path = f"{FEATURE_STORE_PATH}/run_date={run_date}"
    else:
        # Load latest or all
        path = f"{FEATURE_STORE_PATH}/run_date=*"
    
    print(f"Loading features from: {path}")
    df = spark.read.parquet(path)
    
    # Filter only fraud-eligible transactions
    df = df.filter(col("type").isin("CASH_OUT", "TRANSFER"))
    
    row_count = df.count()
    fraud_count = df.filter(col(TARGET_COL) == 1).count()
    print(f"Loaded {row_count:,} rows, {fraud_count:,} frauds ({fraud_count/row_count*100:.4f}%)")
    
    return df


def prepare_data(df):
    """Prepare data for training with class weighting."""
    # Calculate class weights
    counts = df.groupBy(TARGET_COL).count().collect()
    count_dict = {row[TARGET_COL]: row['count'] for row in counts}
    fraud_count = count_dict.get(1, 1)
    non_fraud_count = count_dict.get(0, 1)
    weight_ratio = non_fraud_count / fraud_count
    
    print(f"Class imbalance ratio: 1:{weight_ratio:.0f}")
    
    # Add weight column
    df = df.withColumn(
        "weight", 
        when(col(TARGET_COL) == 1, weight_ratio).otherwise(1.0)
    )
    
    # Handle nulls
    df = df.fillna(0)
    
    return df, weight_ratio


def split_data_by_time(df, train_ratio):
    """Time-based split to avoid data leakage."""
    # Convert timestamp to numeric for approxQuantile (Spark doesn't support quantile on TimestampType)
    df_with_ts = df.withColumn("_ts_numeric", col("transaction_time").cast("long"))
    threshold = df_with_ts.stat.approxQuantile("_ts_numeric", [train_ratio], 0.01)[0]
    
    train_df = df.filter(col("transaction_time").cast("long") <= threshold)
    test_df = df.filter(col("transaction_time").cast("long") > threshold)
    
    print(f"Train: {train_df.count():,} rows")
    print(f"Test: {test_df.count():,} rows")
    
    return train_df, test_df


def build_pipeline(feature_cols, model_type="lr"):
    """Build ML pipeline with feature assembly and model."""
    stages = []
    
    # 1. Vector Assembler
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="raw_features",
        handleInvalid="keep"
    )
    stages.append(assembler)
    
    # 2. Standard Scaler
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=False
    )
    stages.append(scaler)
    
    # 3. Model
    if model_type == "lr":
        model = LogisticRegression(
            labelCol=TARGET_COL,
            featuresCol="scaled_features",
            weightCol="weight",
            maxIter=100
        )
    elif model_type == "rf":
        model = RandomForestClassifier(
            labelCol=TARGET_COL,
            featuresCol="scaled_features",
            weightCol="weight",
            numTrees=100,
            maxDepth=10,
            seed=RANDOM_SEED
        )
    elif model_type == "gbt":
        model = GBTClassifier(
            labelCol=TARGET_COL,
            featuresCol="scaled_features",
            weightCol="weight",
            maxIter=50,
            maxDepth=5,
            seed=RANDOM_SEED
        )
    else:
        raise ValueError(f"Unknown model type: {model_type}")
    
    stages.append(model)
    
    return Pipeline(stages=stages), model


def tune_model(pipeline, model, train_df, model_type="lr"):
    """Hyperparameter tuning with cross-validation."""
    evaluator = BinaryClassificationEvaluator(
        labelCol=TARGET_COL,
        metricName="areaUnderPR"  # PR-AUC is better for imbalanced
    )
    
    if model_type == "lr":
        param_grid = ParamGridBuilder() \
            .addGrid(model.regParam, SPARK_LR_PARAMS["regParam"]) \
            .addGrid(model.elasticNetParam, SPARK_LR_PARAMS["elasticNetParam"]) \
            .build()
    else:
        param_grid = ParamGridBuilder().build()
    
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
        seed=RANDOM_SEED
    )
    
    print("Starting cross-validation...")
    cv_model = cv.fit(train_df)
    
    return cv_model.bestModel


def evaluate_model(model, test_df):
    """Evaluate model and return metrics."""
    predictions = model.transform(test_df)
    
    # AUC metrics
    evaluator_roc = BinaryClassificationEvaluator(labelCol=TARGET_COL, metricName="areaUnderROC")
    evaluator_pr = BinaryClassificationEvaluator(labelCol=TARGET_COL, metricName="areaUnderPR")
    
    auc_roc = evaluator_roc.evaluate(predictions)
    auc_pr = evaluator_pr.evaluate(predictions)
    
    # Detailed metrics using MulticlassMetrics
    pred_and_label = predictions.select(
        col("prediction").cast("double"),
        col(TARGET_COL).cast("double")
    ).rdd
    
    metrics = MulticlassMetrics(pred_and_label)
    
    # Per-class metrics
    precision_fraud = metrics.precision(1.0)
    recall_fraud = metrics.recall(1.0)
    f1_fraud = metrics.fMeasure(1.0)
    
    precision_normal = metrics.precision(0.0)
    recall_normal = metrics.recall(0.0)
    f1_normal = metrics.fMeasure(0.0)
    
    # Weighted metrics (accounts for class imbalance)
    f1_weighted = metrics.weightedFMeasure()
    precision_weighted = metrics.weightedPrecision
    recall_weighted = metrics.weightedRecall
    
    # Confusion matrix
    predictions.groupBy(TARGET_COL, "prediction").count().show()
    
    results = {
        "auc_roc": auc_roc,
        "auc_pr": auc_pr,
        "accuracy": metrics.accuracy,
        # Fraud class
        "precision_fraud": precision_fraud,
        "recall_fraud": recall_fraud,
        "f1_fraud": f1_fraud,
        # Normal class
        "precision_normal": precision_normal,
        "recall_normal": recall_normal,
        "f1_normal": f1_normal,
        # Weighted metrics
        "f1_weighted": f1_weighted,
        "precision_weighted": precision_weighted,
        "recall_weighted": recall_weighted,
        "macro_recall": (recall_fraud + recall_normal) / 2
    }
    
    print("MODEL EVALUATION RESULTS")
    for k, v in results.items():
        print(f"  {k}: {v:.4f}")
    
    return results, predictions


def train_with_mlflow(spark, args):
    """Main training function with MLflow tracking."""
    
    # Setup MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    with mlflow.start_run(run_name=f"train_{args.model}_{datetime.now().strftime('%Y%m%d_%H%M')}"):
        # Log parameters
        mlflow.log_param("model_type", args.model)
        mlflow.log_param("run_date", args.run_date or "all")
        mlflow.log_param("train_ratio", TRAIN_RATIO)
        mlflow.log_param("num_features", len(FEATURE_COLS))
        
        # 1. Load data
        print("\nLoading Features...")
        df = load_features(spark, args.run_date)
        
        # 2. Prepare data
        print("\nPreparing Data...")
        df, weight_ratio = prepare_data(df)
        mlflow.log_param("class_weight_ratio", weight_ratio)
        
        # Filter to available features only
        available_features = [c for c in FEATURE_COLS if c in df.columns]
        print(f"Using {len(available_features)} features")
        mlflow.log_param("features_used", len(available_features))
        
        # 3. Split data
        print("\nSplitting Data...")
        train_df, test_df = split_data_by_time(df, TRAIN_RATIO)
        mlflow.log_param("train_size", train_df.count())
        mlflow.log_param("test_size", test_df.count())
        
        # 4. Build pipeline
        print(f"\nBuilding Pipeline ({args.model})...")
        pipeline, model_obj = build_pipeline(available_features, args.model)
        
        # 5. Train/Tune
        if args.tune:
            print("\nTuning Hyperparameters...")
            best_model = tune_model(pipeline, model_obj, train_df, args.model)
        else:
            print("\nTraining Model...")
            best_model = pipeline.fit(train_df)
        
        # 6. Evaluate
        print("\nEvaluating Model...")
        metrics, predictions = evaluate_model(best_model, test_df)
        
        # Log metrics to MLflow
        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)
        
        # 7. Save model
        if args.save_model:
            print("\nSaving Model to MLflow...")
            mlflow.spark.log_model(
                best_model,
                artifact_path="spark_model",
                registered_model_name=MLFLOW_MODEL_NAME if args.register else None
            )
            print(f"Model logged to MLflow")
        
        # Log feature list
        with open("/tmp/features.txt", "w") as f:
            f.write("\n".join(available_features))
        mlflow.log_artifact("/tmp/features.txt")
        
        print("\nTraining Complete!")
        print(f"MLflow Run ID: {mlflow.active_run().info.run_id}")
        
        return best_model, metrics


def main():
    parser = argparse.ArgumentParser(description="Train Fraud Detection Model")
    parser.add_argument("--model", default="lr", choices=["lr", "rf", "gbt"],
                        help="Model type: lr=LogisticRegression, rf=RandomForest, gbt=GradientBoosting")
    parser.add_argument("--run-date", help="Feature snapshot date (YYYY-MM-DD)")
    parser.add_argument("--tune", action="store_true", help="Enable hyperparameter tuning")
    parser.add_argument("--save-model", action="store_true", help="Save model to MLflow")
    parser.add_argument("--register", action="store_true", help="Register model in MLflow Model Registry")
    
    args = parser.parse_args()
    
    spark = None
    try:
        spark = create_spark_session()
        train_with_mlflow(spark, args)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    main()
