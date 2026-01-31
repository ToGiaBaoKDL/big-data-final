"""
PySpark ML Training for PaySim Fraud Detection.
Uses PySpark ML Pipeline with MLflow tracking.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when, lit
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression, GBTClassifier, RandomForestClassifier
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

import mlflow
import mlflow.spark
import os
import argparse
from datetime import datetime
import json
import tempfile

from config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
    FEATURE_STORE_PATH, TARGET_COL, FEATURE_COLS, TRAIN_RATIO, RANDOM_SEED,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME, MLFLOW_MODEL_NAME,
    SPARK_LR_PARAMS
)

# Configure MLflow S3 endpoint for MinIO (required for artifact storage)
os.environ["MLFLOW_S3_ENDPOINT_URL"] = MINIO_ENDPOINT
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY


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
    """Evaluate model and return metrics using DataFrame API (memory-efficient)."""
    predictions = model.transform(test_df)
    
    # Cache predictions for multiple evaluations
    predictions.cache()
    
    # AUC metrics using built-in evaluators
    evaluator_roc = BinaryClassificationEvaluator(labelCol=TARGET_COL, metricName="areaUnderROC")
    evaluator_pr = BinaryClassificationEvaluator(labelCol=TARGET_COL, metricName="areaUnderPR")
    
    auc_roc = evaluator_roc.evaluate(predictions)
    auc_pr = evaluator_pr.evaluate(predictions)
    
    # Confusion matrix using DataFrame API (avoids expensive RDD conversion)
    confusion = predictions.groupBy(TARGET_COL, "prediction").count()
    confusion.show()
    
    # Collect confusion matrix values
    cm_rows = confusion.collect()
    cm = {}
    for row in cm_rows:
        key = (int(row[TARGET_COL]), int(row["prediction"]))
        cm[key] = row["count"]
    
    # Extract values (TP, TN, FP, FN)
    tp = cm.get((1, 1), 0)  # True Positive: actual=1, pred=1
    tn = cm.get((0, 0), 0)  # True Negative: actual=0, pred=0
    fp = cm.get((0, 1), 0)  # False Positive: actual=0, pred=1
    fn = cm.get((1, 0), 0)  # False Negative: actual=1, pred=0
    
    total = tp + tn + fp + fn
    
    # Calculate metrics manually
    accuracy = (tp + tn) / total if total > 0 else 0.0
    
    # Fraud class (positive)
    precision_fraud = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall_fraud = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    f1_fraud = 2 * precision_fraud * recall_fraud / (precision_fraud + recall_fraud) if (precision_fraud + recall_fraud) > 0 else 0.0
    
    # Normal class (negative)
    precision_normal = tn / (tn + fn) if (tn + fn) > 0 else 0.0
    recall_normal = tn / (tn + fp) if (tn + fp) > 0 else 0.0
    f1_normal = 2 * precision_normal * recall_normal / (precision_normal + recall_normal) if (precision_normal + recall_normal) > 0 else 0.0
    
    # Weighted metrics (by support)
    fraud_support = tp + fn
    normal_support = tn + fp
    total_support = fraud_support + normal_support
    
    precision_weighted = (precision_fraud * fraud_support + precision_normal * normal_support) / total_support if total_support > 0 else 0.0
    recall_weighted = (recall_fraud * fraud_support + recall_normal * normal_support) / total_support if total_support > 0 else 0.0
    f1_weighted = (f1_fraud * fraud_support + f1_normal * normal_support) / total_support if total_support > 0 else 0.0
    
    # Unpersist cached predictions
    predictions.unpersist()
    
    results = {
        "auc_roc": auc_roc,
        "auc_pr": auc_pr,
        "accuracy": accuracy,
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
        "macro_recall": (recall_fraud + recall_normal) / 2,
        # Raw counts
        "true_positives": tp,
        "true_negatives": tn,
        "false_positives": fp,
        "false_negatives": fn
    }
    
    print("MODEL EVALUATION RESULTS")
    print(f"  Confusion Matrix: TP={tp:,}, TN={tn:,}, FP={fp:,}, FN={fn:,}")
    for k, v in results.items():
        if isinstance(v, float):
            print(f"  {k}: {v:.4f}")
    
    return results, predictions


def train_with_mlflow(spark, args):
    """Main training function with MLflow tracking."""
    
    # Setup MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    # Build descriptive run name: {model}_{data_scope}_{timestamp}
    data_scope = args.run_date if args.run_date else "all_dates"
    run_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    run_name = f"{args.model}_{data_scope}_{run_timestamp}"
    
    # Prepare tags for easy filtering/search in MLflow UI
    tags = {
        # Model identification
        "model_type": args.model,
        "model_family": "spark_ml",
        
        # Data lineage
        "data_source": "paysim_features",
        "data_run_date": args.run_date or "all",
        "feature_store_path": FEATURE_STORE_PATH,
        
        # Training metadata
        "training_mode": "tuned" if args.tune else "default",
        "environment": os.getenv("ENVIRONMENT", "development"),
        
        # Versioning
        "code_version": os.getenv("GIT_COMMIT", "unknown"),
        "spark_version": spark.version,
        "mlflow_version": mlflow.__version__,
    }
    
    with mlflow.start_run(run_name=run_name, tags=tags):
        # Log parameters - grouped by category
        # Data parameters
        mlflow.log_param("data.run_date", args.run_date or "all")
        mlflow.log_param("data.feature_path", FEATURE_STORE_PATH)
        mlflow.log_param("data.train_ratio", TRAIN_RATIO)
        
        # Model parameters
        mlflow.log_param("model.type", args.model)
        mlflow.log_param("model.num_features", len(FEATURE_COLS))
        mlflow.log_param("model.tuning_enabled", args.tune)
        
        # 1. Load data
        print("\nLoading Features...")
        df = load_features(spark, args.run_date)
        
        # 2. Prepare data
        print("\nPreparing Data...")
        df, weight_ratio = prepare_data(df)
        mlflow.log_param("data.class_weight_ratio", round(weight_ratio, 2))
        
        # Filter to available features only
        available_features = [c for c in FEATURE_COLS if c in df.columns]
        print(f"Using {len(available_features)} features")
        mlflow.log_param("model.features_available", len(available_features))
        
        # 3. Split data
        print("\nSplitting Data...")
        train_df, test_df = split_data_by_time(df, TRAIN_RATIO)
        train_count = train_df.count()
        test_count = test_df.count()
        mlflow.log_param("data.train_size", train_count)
        mlflow.log_param("data.test_size", test_count)
        
        # Log data statistics as metrics
        mlflow.log_metric("data_train_rows", train_count)
        mlflow.log_metric("data_test_rows", test_count)
        mlflow.log_metric("data_total_rows", train_count + test_count)
        
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
        
        # 7. Save model with proper artifact organization
        if args.save_model:
            print("\nSaving Model to MLflow...")
            
            # Specify pip requirements explicitly to avoid inference warnings
            pip_requirements = [
                f"pyspark=={spark.version}",
                f"mlflow=={mlflow.__version__}"
            ]
            
            # Build model signature description
            model_description = f"""
## PaySim Fraud Detection Model

### Model Info
- **Type**: {args.model.upper()} ({"Logistic Regression" if args.model == "lr" else "Random Forest" if args.model == "rf" else "Gradient Boosted Trees"})
- **Training Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
- **Data Snapshot**: {args.run_date or "All available data"}

### Performance Metrics
- AUC-ROC: {metrics['auc_roc']:.4f}
- AUC-PR: {metrics['auc_pr']:.4f}
- Recall (Fraud): {metrics['recall_fraud']:.4f}
- Precision (Fraud): {metrics['precision_fraud']:.4f}

### Features
{len(available_features)} features used. See `features.txt` artifact for full list.
"""
            
            # Log model with metadata
            mlflow.spark.log_model(
                best_model,
                artifact_path="model",  # Simplified path
                registered_model_name=MLFLOW_MODEL_NAME if args.register else None,
                pip_requirements=pip_requirements
            )
            
            with tempfile.TemporaryDirectory() as tmpdir:
                # 1. Feature list
                features_path = os.path.join(tmpdir, "features.json")
                with open(features_path, "w") as f:
                    json.dump({
                        "features": available_features,
                        "count": len(available_features),
                        "target": TARGET_COL
                    }, f, indent=2)
                mlflow.log_artifact(features_path, artifact_path="metadata")
                
                # 2. Training config
                config_path = os.path.join(tmpdir, "training_config.json")
                with open(config_path, "w") as f:
                    json.dump({
                        "model_type": args.model,
                        "data_run_date": args.run_date,
                        "train_ratio": TRAIN_RATIO,
                        "class_weight_ratio": round(weight_ratio, 2),
                        "tuning_enabled": args.tune,
                        "train_size": train_count,
                        "test_size": test_count,
                        "feature_store_path": FEATURE_STORE_PATH
                    }, f, indent=2)
                mlflow.log_artifact(config_path, artifact_path="metadata")
                
                # 3. Model description (for Model Registry)
                desc_path = os.path.join(tmpdir, "README.md")
                with open(desc_path, "w") as f:
                    f.write(model_description)
                mlflow.log_artifact(desc_path, artifact_path="metadata")
            
            print(f"Model and artifacts logged to MLflow")
        
        # Legacy: Also log plain text features for backward compatibility
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
