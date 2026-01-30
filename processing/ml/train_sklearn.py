"""
Scikit-learn/XGBoost ML Training for PaySim Fraud Detection.

Usage:
    python train_sklearn.py --model xgb --data-path s3a://dl-datascience/paysim_features --save-model --register
"""
import os
import argparse
from datetime import datetime
import numpy as np
import polars as pl
import s3fs

import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    roc_auc_score, average_precision_score,
    precision_score, recall_score, f1_score, confusion_matrix
)

import mlflow
import mlflow.xgboost
import mlflow.sklearn

from config import (
    FEATURE_STORE_PATH, TARGET_COL, FEATURE_COLS, DROP_COLS,
    TRAIN_RATIO, RANDOM_SEED, XGBOOST_PARAMS,
    MLFLOW_TRACKING_URI, MLFLOW_EXPERIMENT_NAME, MLFLOW_MODEL_NAME,
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, BUCKET_DATASCIENCE
)


def get_s3_filesystem():
    """Get S3 filesystem for MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
        use_ssl=False
    )


def load_features_polars(path: str, run_date: str = None) -> pl.DataFrame:
    """
    Load features from parquet using Polars (faster than pandas).
    Supports both S3 and local paths.
    """
    # Normalize path
    if path.startswith("s3a://"):
        path = path.replace("s3a://", "s3://")
    
    if path.startswith("s3://"):
        fs = get_s3_filesystem()
        s3_path = path.replace("s3://", "")
        
        if run_date:
            full_path = f"{s3_path}/run_date={run_date}"
        else:
            # Find latest run_date
            runs = sorted(fs.glob(f"{s3_path}/run_date=*"))
            if not runs:
                raise FileNotFoundError(f"No feature snapshots in {path}")
            full_path = runs[-1]
        
        print(f"Loading from MinIO: s3://{full_path}")
        
        # List all parquet files
        parquet_files = fs.glob(f"{full_path}/**/*.parquet")
        if not parquet_files:
            parquet_files = fs.glob(f"{full_path}/*.parquet")
        
        # Load with Polars using fsspec
        dfs = []
        for pf in parquet_files:
            with fs.open(pf, 'rb') as f:
                dfs.append(pl.read_parquet(f))
        
        df = pl.concat(dfs) if len(dfs) > 1 else dfs[0]
    
    else:
        # Local filesystem
        import glob as glob_module
        
        if run_date:
            full_path = f"{path}/run_date={run_date}"
        else:
            runs = sorted(glob_module.glob(f"{path}/run_date=*"))
            if not runs:
                raise FileNotFoundError(f"No feature snapshots in {path}")
            full_path = runs[-1]
        
        print(f"Loading from local: {full_path}")
        df = pl.read_parquet(full_path)
    
    # Filter fraud-eligible only (TRANSFER/CASH_OUT)
    if "type" in df.columns:
        df = df.filter(pl.col("type").is_in(["CASH_OUT", "TRANSFER"]))
    
    print(f"Loaded {len(df):,} rows")
    fraud_rate = df[TARGET_COL].mean()
    print(f"Fraud rate: {fraud_rate*100:.4f}%")
    
    return df


def prepare_features(df: pl.DataFrame):
    """
    Prepare X, y from Polars DataFrame.
    CRITICAL: For temporal features, must use time-based split to prevent leakage.
    """
    # Get available feature columns
    available_features = [c for c in FEATURE_COLS if c in df.columns]
    print(f"Using {len(available_features)} of {len(FEATURE_COLS)} features")
    
    if "transaction_time" in df.columns:
        df = df.sort("transaction_time")
        print("✓ Sorted by transaction_time (temporal ordering preserved)")
        
        # Also return transaction_time for temporal split
        temporal_col = df["transaction_time"].to_numpy()
    else:
        temporal_col = None
        print("WARNING: No transaction_time column - cannot do temporal split")
    
    # Select features and convert to numpy (for sklearn)
    X = df.select(available_features).fill_null(0).fill_nan(0).to_numpy()
    y = df[TARGET_COL].cast(pl.Int32).to_numpy()
    
    # Handle inf values
    X = np.nan_to_num(X, nan=0.0, posinf=999999.0, neginf=-999999.0)
    
    return X, y, available_features, temporal_col


def get_model(model_type, class_weight_ratio):
    """Get model by type."""
    if model_type == "xgb":
        params = XGBOOST_PARAMS.copy()
        params["scale_pos_weight"] = class_weight_ratio
        return xgb.XGBClassifier(**params)
    
    elif model_type == "rf":
        return RandomForestClassifier(
            n_estimators=100,
            max_depth=10,
            class_weight="balanced",
            random_state=RANDOM_SEED,
            n_jobs=-1
        )
    
    elif model_type == "lr":
        return LogisticRegression(
            max_iter=1000,
            class_weight="balanced",
            random_state=RANDOM_SEED
        )
    
    else:
        raise ValueError(f"Unknown model: {model_type}")


def evaluate_model(model, X_test, y_test):
    """
    Comprehensive evaluation for fraud detection.
    Focus on metrics relevant to imbalanced classification.
    """
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]
    
    # Fraud detection metrics (focus on recall and precision balance)
    metrics = {
        "auc_roc": roc_auc_score(y_test, y_prob),
        "auc_pr": average_precision_score(y_test, y_prob),  # Better for imbalanced
        "precision_fraud": precision_score(y_test, y_pred, zero_division=0),
        "recall_fraud": recall_score(y_test, y_pred, zero_division=0),
        "f1_fraud": f1_score(y_test, y_pred, zero_division=0),
        # Business metrics
        "fraud_detection_rate": recall_score(y_test, y_pred, zero_division=0),  # Same as recall
        "false_positive_rate": 0.0  # Will calculate below
    }
    
    # Confusion matrix
    cm = confusion_matrix(y_test, y_pred)
    tn, fp, fn, tp = cm.ravel()
    
    # Calculate FPR
    if (fp + tn) > 0:
        metrics["false_positive_rate"] = fp / (fp + tn)
    
    print("MODEL EVALUATION - FRAUD DETECTION")
    print(f"\nPerformance Metrics:")
    print(f"  AUC-ROC:     {metrics['auc_roc']:.4f}")
    print(f"  AUC-PR:      {metrics['auc_pr']:.4f} (Primary metric for imbalanced)")
    print(f"  Precision:   {metrics['precision_fraud']:.4f} (How many flagged are actual fraud)")
    print(f"  Recall:      {metrics['recall_fraud']:.4f} (How many frauds caught)")
    print(f"  F1-Score:    {metrics['f1_fraud']:.4f}")
    
    print(f"\nConfusion Matrix:")
    print(f"              Predicted")
    print(f"              Normal    Fraud")
    print(f"  Actual Normal  {tn:>7,}  {fp:>6,}")
    print(f"  Actual Fraud   {fn:>7,}  {tp:>6,}")
    
    print(f"\nBusiness Impact:")
    print(f"  Frauds Caught:     {tp:,}/{tp+fn:,} ({metrics['recall_fraud']*100:.1f}%)")
    print(f"  Frauds Missed:     {fn:,} ({fn/(tp+fn)*100:.1f}%)")
    print(f"  False Alarms:      {fp:,} ({metrics['false_positive_rate']*100:.4f}% of normal)")
    print(f"  Legitimate Users:  {tn:,} (not affected)")
    
    # Alert if performance is concerning
    if metrics['recall_fraud'] < 0.7:
        print(f"\nWARNING: Recall < 70% - Many frauds going undetected!")
    if metrics['false_positive_rate'] > 0.01:
        print(f"\nWARNING: FPR > 1% - Too many false alarms!")
    
    return metrics


def train_with_mlflow(args):
    """Main training with MLflow."""
    
    # Setup MLflow
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT_NAME)
    
    run_name = f"{args.model}_{datetime.now().strftime('%Y%m%d_%H%M')}"
    
    with mlflow.start_run(run_name=run_name):
        # Log params
        mlflow.log_param("model_type", args.model)
        mlflow.log_param("data_path", args.data_path)
        mlflow.log_param("run_date", args.run_date or "latest")
        
        # 1. Load data with Polars (faster)
        print("\nLoading Features with Polars...")
        df = load_features_polars(args.data_path, args.run_date)
        
        # 2. Prepare features
        print("\nPreparing Features...")
        X, y, feature_names, temporal_col = prepare_features(df)
        mlflow.log_param("num_features", len(feature_names))
        
        # Class weight
        class_weight_ratio = (y == 0).sum() / max((y == 1).sum(), 1)
        mlflow.log_param("class_weight_ratio", class_weight_ratio)
        
        # 3. Temporal Split 
        if temporal_col is not None:
            # Temporal split: train on older, test on newer
            split_idx = int(len(X) * TRAIN_RATIO)
            X_train, X_test = X[:split_idx], X[split_idx:]
            y_train, y_test = y[:split_idx], y[split_idx:]
            
            mlflow.log_param("split_method", "temporal")
            print(f"    Split at index {split_idx} (time-ordered)")
        else:
            # Fallback to stratified random
            print("Falling back to stratified random split (no temporal column)")
            X_train, X_test, y_train, y_test = train_test_split(
                X, y,
                test_size=1-TRAIN_RATIO,
                random_state=RANDOM_SEED,
                stratify=y,
                shuffle=True
            )
            mlflow.log_param("split_method", "stratified_random")
        
        mlflow.log_param("train_size", len(X_train))
        mlflow.log_param("test_size", len(X_test))
        
        print(f"Train: {len(X_train):,} ({y_train.mean()*100:.4f}% fraud)")
        print(f"Test:  {len(X_test):,} ({y_test.mean()*100:.4f}% fraud)")
        
        # 4. Train
        print(f"\nTraining {args.model.upper()}...")
        model = get_model(args.model, class_weight_ratio)
        
        if args.model == "xgb":
            # Split training into train/validation for early stopping
            # Use TEMPORAL split to prevent data leakage (consistent with main split)
            val_split_idx = int(len(X_train) * 0.8)
            X_tr, X_val = X_train[:val_split_idx], X_train[val_split_idx:]
            y_tr, y_val = y_train[:val_split_idx], y_train[val_split_idx:]
            print(f"    XGBoost temporal validation split: train {len(X_tr)}, val {len(X_val)}")
            model.fit(
                X_tr, y_tr,
                eval_set=[(X_val, y_val)],
                early_stopping_rounds=20,
                verbose=False
            )
        else:
            model.fit(X_train, y_train)
        
        # 5. Evaluate
        print("\nEvaluating...")
        metrics = evaluate_model(model, X_test, y_test)
        
        # Log metrics
        for k, v in metrics.items():
            mlflow.log_metric(k, v)
        
        # 6. Feature importance (use Polars)
        if hasattr(model, "feature_importances_"):
            importance = pl.DataFrame({
                "feature": feature_names,
                "importance": model.feature_importances_.tolist()
            }).sort("importance", descending=True)
            
            print("\nTop 10 Features:")
            for row in importance.head(10).iter_rows(named=True):
                print(f"  {row['feature']:<35} {row['importance']:.4f}")
            
            # Save importance
            importance.write_csv("/tmp/feature_importance.csv")
            mlflow.log_artifact("/tmp/feature_importance.csv")
        
        # 7. Save model
        if args.save_model:
            print("\nSaving Model...")
            
            if args.model == "xgb":
                mlflow.xgboost.log_model(
                    model,
                    artifact_path="model",
                    registered_model_name=MLFLOW_MODEL_NAME if args.register else None
                )
            else:
                mlflow.sklearn.log_model(
                    model,
                    artifact_path="model",
                    registered_model_name=MLFLOW_MODEL_NAME if args.register else None
                )
        
        # Save feature list
        with open("/tmp/features.txt", "w") as f:
            f.write("\n".join(feature_names))
        mlflow.log_artifact("/tmp/features.txt")
        
        print("\nTraining Complete!")
        print(f"MLflow Run ID: {mlflow.active_run().info.run_id}")
        
        return model, metrics


def main():
    parser = argparse.ArgumentParser(description="Train Fraud Detection Model (sklearn/XGBoost)")
    parser.add_argument("--model", default="xgb", choices=["xgb", "rf", "lr"])
    parser.add_argument("--data-path", required=True, help="Path to feature parquet files")
    parser.add_argument("--run-date", help="Feature snapshot date")
    parser.add_argument("--save-model", action="store_true")
    parser.add_argument("--register", action="store_true", help="Register in Model Registry")
    
    args = parser.parse_args()
    train_with_mlflow(args)


if __name__ == "__main__":
    main()
