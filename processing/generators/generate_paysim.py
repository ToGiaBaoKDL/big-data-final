import os
import shutil
import random
import io
from datetime import datetime, timedelta
import polars as pl
import numpy as np
from faker import Faker
from minio import Minio
import argparse

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "dl-landing-8f42a1")

# Constants
START_DATE_SIMULATION = datetime(2025, 12, 29, 0, 0, 0)
FAKE = Faker()

# Transaction Type Distribution (Based on real PaySim data)
TXN_TYPES = ["PAYMENT", "CASH_OUT", "CASH_IN", "TRANSFER", "DEBIT"]
TXN_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

# Fraud Configuration
FRAUD_BASE_RATE = 0.0013  # 0.13% - realistic fraud rate
FRAUD_NIGHT_MULTIPLIER = 3.0  # Fraud tăng 3x vào đêm (22h-6h)
FRAUD_TYPES = ["TRANSFER", "CASH_OUT"]  # Chỉ có 2 loại này có fraud

# Fraud Pattern Distribution
FRAUD_PATTERN_EMPTY_ACCOUNT = 0.60  # 60%: Empty toàn bộ account
FRAUD_PATTERN_LARGE_ROUND = 0.20    # 20%: Large round amounts (100k, 500k, 1M)
FRAUD_PATTERN_JUST_BELOW = 0.15     # 15%: Just below flagging threshold
FRAUD_PATTERN_NORMAL_LOOKING = 0.05 # 5%: Normal-looking amounts

# Mule accounts (fraud destinations reused across frauds)
MULE_ACCOUNTS = [FAKE.bothify(text='C#########') for _ in range(50)]  # 50 mule accounts

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def step_to_timestamp(step: int) -> datetime:
    """Maps an integer step (hour) to a simulation datetime."""
    return START_DATE_SIMULATION + timedelta(hours=step - 1)

def add_partition_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Adds part_dt (YYYYMMDD) and part_hour (HH) based on step/timestamp."""
    return df.with_columns([
        pl.col("step").map_elements(lambda s: step_to_timestamp(s), return_dtype=pl.Datetime).alias("transaction_time")
    ]).with_columns([
        pl.col("transaction_time").dt.strftime("%Y%m%d").alias("part_dt"),
        pl.col("transaction_time").dt.strftime("%H").alias("part_hour")
    ])

def upload_local_directory_to_minio(client, local_path, bucket, prefix):
    """Recursively uploads a local directory to MinIO."""
    for root, _, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, local_path)
            object_name = f"{prefix}/{rel_path}"
            
            print(f"Uploading {object_name}...")
            client.fput_object(bucket, object_name, file_path)

def process_initial_load(source_file):
    """
    Mode Init: Reads CSV, adds partitions, saves as Parquet, uploads to MinIO.
    Target: .warehouse/paysim_txn/part_dt=.../part_hour=...
    """
    print(f"Processing Initial Data from {source_file}...")
    
    # Read CSV
    df = pl.read_csv(source_file)
    
    # Add partition columns
    df_processed = add_partition_cols(df)
    
    # Write partitioned parquet locally first
    temp_dir = "temp_paysim_init"
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        
    print("Writing partitioned parquet locally...")
    df_processed.write_parquet(
        temp_dir,
        use_pyarrow=True,
        partition_by=["part_dt", "part_hour"]
    )
    
    # Upload to MinIO
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_LANDING):
        client.make_bucket(BUCKET_LANDING)

    print("Uploading to MinIO...")
    upload_local_directory_to_minio(client, temp_dir, BUCKET_LANDING, ".warehouse/paysim_txn")
    
    # Cleanup
    shutil.rmtree(temp_dir)
    print("Initial Load Complete.")

def get_fraud_rate_for_hour(hour: int) -> float:
    """
    Returns fraud rate based on time of day.
    Night hours (22-6) have higher fraud rate.
    """
    if 22 <= hour or hour <= 6:
        return FRAUD_BASE_RATE * FRAUD_NIGHT_MULTIPLIER
    return FRAUD_BASE_RATE

def generate_step_data(step: int, base_rows: int):
    """
    Generates realistic PaySim data for a single step (hour).
    Uses vectorized operations for better performance.
    """
    ts = step_to_timestamp(step)
    hour = ts.hour
    
    # Realistic hourly variation based on time of day
    # Peak hours (9-17): +30% to +50%
    # Night hours (0-6): -40% to -20%
    # Evening (18-23): -10% to +10%
    # Morning (7-8): +10% to +30%
    if 9 <= hour <= 17:
        multiplier = random.uniform(1.30, 1.50)
    elif 0 <= hour <= 6:
        multiplier = random.uniform(0.60, 0.80)
    elif 18 <= hour <= 23:
        multiplier = random.uniform(0.90, 1.10)
    else:  # 7-8
        multiplier = random.uniform(1.10, 1.30)
    
    # Add random daily variation (+/- 10%)
    daily_variation = random.uniform(0.90, 1.10)
    
    n_rows = int(base_rows * multiplier * daily_variation)
    print(f"  > Generating {n_rows} rows for Step {step} (Hour {hour:02d})...")
    
    part_dt = ts.strftime("%Y%m%d")
    part_hour = ts.strftime("%H")
    
    # Generate transaction types
    types_array = np.random.choice(TXN_TYPES, size=n_rows, p=TXN_WEIGHTS)
    
    # Generate amounts based on transaction type
    amounts = np.zeros(n_rows)
    for i, txn_type in enumerate(TXN_TYPES):
        mask = types_array == txn_type
        count = mask.sum()
        if count > 0:
            if txn_type == "TRANSFER":
                # TRANSFER: higher amounts, log-normal distribution
                amounts[mask] = np.random.lognormal(mean=8, sigma=2, size=count)
                amounts[mask] = np.clip(amounts[mask], 10, 10000000)
            elif txn_type == "CASH_OUT":
                # CASH_OUT: medium to high
                amounts[mask] = np.random.lognormal(mean=7, sigma=1.5, size=count)
                amounts[mask] = np.clip(amounts[mask], 10, 1000000)
            else:
                # PAYMENT, CASH_IN, DEBIT: lower amounts
                amounts[mask] = np.random.lognormal(mean=6, sigma=1.2, size=count)
                amounts[mask] = np.clip(amounts[mask], 10, 50000)
    
    amounts = np.round(amounts, 2)
    
    # Generate account names
    name_orig = np.array([FAKE.bothify(text='C#########') for _ in range(n_rows)])
    name_dest = np.array([
        FAKE.bothify(text='M#########') if types_array[i] in ["PAYMENT", "DEBIT"]
        else FAKE.bothify(text='C#########')
        for i in range(n_rows)
    ])
    
    # Generate balances - vectorized
    old_balance_orig = np.zeros(n_rows)
    new_balance_orig = np.zeros(n_rows)
    old_balance_dest = np.zeros(n_rows)
    new_balance_dest = np.zeros(n_rows)
    
    for i, txn_type in enumerate(TXN_TYPES):
        mask = types_array == txn_type
        count = mask.sum()
        if count == 0:
            continue
            
        if txn_type in ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT"]:
            # Outgoing: ensure sufficient balance (with some variance)
            old_balance_orig[mask] = amounts[mask] * np.random.uniform(1.1, 5.0, size=count)
            new_balance_orig[mask] = old_balance_orig[mask] - amounts[mask]
        else:  # CASH_IN
            # Incoming: random starting balance
            old_balance_orig[mask] = np.random.uniform(0, 50000, size=count)
            new_balance_orig[mask] = old_balance_orig[mask] + amounts[mask]
        
        # Destination balances
        old_balance_dest[mask] = np.random.uniform(0, 100000, size=count)
        
        if txn_type in ["CASH_IN", "TRANSFER"]:
            # Dest receives money
            new_balance_dest[mask] = old_balance_dest[mask] + amounts[mask]
        elif txn_type == "CASH_OUT":
            # Merchant pays out cash (balance decreases)
            new_balance_dest[mask] = old_balance_dest[mask] - amounts[mask]
            # Ensure non-negative
            new_balance_dest[mask] = np.maximum(new_balance_dest[mask], 0)
        else:
            # PAYMENT, DEBIT: dest balance unchanged
            new_balance_dest[mask] = old_balance_dest[mask]
    
    # Round balances
    old_balance_orig = np.round(old_balance_orig, 2)
    new_balance_orig = np.round(new_balance_orig, 2)
    old_balance_dest = np.round(old_balance_dest, 2)
    new_balance_dest = np.round(new_balance_dest, 2)
    
    # Initialize fraud columns (use int8 for storage optimization)
    is_fraud = np.zeros(n_rows, dtype=np.int8)
    is_flagged_fraud = np.zeros(n_rows, dtype=np.int8)
    
    # Inject fraud - only for TRANSFER and CASH_OUT
    fraud_rate = get_fraud_rate_for_hour(hour)
    fraud_eligible = np.isin(types_array, FRAUD_TYPES)
    fraud_candidates = np.where(fraud_eligible)[0]
    
    if len(fraud_candidates) > 0:
        n_frauds = int(len(fraud_candidates) * fraud_rate)
        fraud_indices = np.random.choice(fraud_candidates, size=min(n_frauds, len(fraud_candidates)), replace=False)
        
        for idx in fraud_indices:
            is_fraud[idx] = 1
            txn_type = types_array[idx]
            
            # Determine fraud pattern
            pattern_roll = random.random()
            
            if pattern_roll < FRAUD_PATTERN_EMPTY_ACCOUNT:
                # Pattern 1: Empty account (most common)
                amounts[idx] = old_balance_orig[idx]
                new_balance_orig[idx] = 0.0
                
            elif pattern_roll < FRAUD_PATTERN_EMPTY_ACCOUNT + FRAUD_PATTERN_LARGE_ROUND:
                # Pattern 2: Large round amounts
                round_amounts = [50000, 100000, 250000, 500000, 1000000]
                amounts[idx] = random.choice(round_amounts)
                # Ensure sufficient balance
                if amounts[idx] > old_balance_orig[idx]:
                    old_balance_orig[idx] = amounts[idx] * random.uniform(1.1, 1.5)
                new_balance_orig[idx] = old_balance_orig[idx] - amounts[idx]
                
            elif pattern_roll < FRAUD_PATTERN_EMPTY_ACCOUNT + FRAUD_PATTERN_LARGE_ROUND + FRAUD_PATTERN_JUST_BELOW:
                # Pattern 3: Just below flagging threshold (199k for TRANSFER)
                if txn_type == "TRANSFER":
                    amounts[idx] = random.uniform(180000, 199999)
                else:  # CASH_OUT
                    amounts[idx] = random.uniform(150000, 250000)
                # Ensure sufficient balance
                if amounts[idx] > old_balance_orig[idx]:
                    old_balance_orig[idx] = amounts[idx] * random.uniform(1.1, 1.5)
                new_balance_orig[idx] = old_balance_orig[idx] - amounts[idx]
                
            else:
                # Pattern 4: Normal-looking amounts (harder to detect)
                amounts[idx] = round(random.uniform(5000, 50000), 2)
                if amounts[idx] > old_balance_orig[idx]:
                    old_balance_orig[idx] = amounts[idx] * random.uniform(1.5, 3.0)
                new_balance_orig[idx] = old_balance_orig[idx] - amounts[idx]
            
            # Round amounts
            amounts[idx] = round(amounts[idx], 2)
            old_balance_orig[idx] = round(old_balance_orig[idx], 2)
            new_balance_orig[idx] = round(new_balance_orig[idx], 2)
            
            # Destination: Use mule accounts (80% reuse, 20% new)
            if random.random() < 0.80:
                name_dest[idx] = random.choice(MULE_ACCOUNTS)
            else:
                name_dest[idx] = FAKE.bothify(text='C#########')
            
            # Dest receives the fraud amount
            new_balance_dest[idx] = old_balance_dest[idx] + amounts[idx]
            new_balance_dest[idx] = round(new_balance_dest[idx], 2)
            
            # PaySim flagging rule: TRANSFER > 200,000
            if txn_type == "TRANSFER" and amounts[idx] > 200000:
                is_flagged_fraud[idx] = 1
    
    # Create DataFrame
    df = pl.DataFrame({
        "step": np.full(n_rows, step),
        "type": types_array,
        "amount": amounts,
        "nameOrig": name_orig,
        "oldbalanceOrg": old_balance_orig,
        "newbalanceOrig": new_balance_orig,
        "nameDest": name_dest,
        "oldbalanceDest": old_balance_dest,
        "newbalanceDest": new_balance_dest,
        "isFraud": is_fraud,
        "isFlaggedFraud": is_flagged_fraud,
        "transaction_time": [ts] * n_rows,
        "part_dt": [part_dt] * n_rows,
        "part_hour": [part_hour] * n_rows
    })
    
    return df

def generate_incremental_batch(base_rows: int, start_step: int):
    """
    Generates 1 hour of data (1 step) for the given step.
    """
    print(f"Generating data for Step {start_step}...")
    
    client = get_minio_client()
    
    # Generate for single step
    df = generate_step_data(start_step, base_rows)
    
    # Infer partition from first row
    part_dt = df["part_dt"][0]
    part_hour = df["part_hour"][0]
    
    # Filename
    filename = f"paysim_step{start_step}.parquet"
    object_name = f".warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/{filename}"
    
    # Buffer & Upload
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    file_size = buffer.getbuffer().nbytes
    
    print(f"  -> Uploading {object_name} ({file_size/1024:.1f} KB)...")
    client.put_object(
        BUCKET_LANDING,
        object_name,
        buffer,
        file_size,
        content_type="application/octet-stream"
    )
    
    print("Generation Complete.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PaySim Data Generator for MinIO/Datalake")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # Init mode
    p_init = subparsers.add_parser("init", help="Initial load from CSV")
    p_init.add_argument("--file", required=True, help="Path to PaySim CSV file")

    # Generate mode
    p_gen = subparsers.add_parser("generate", help="Generate incremental data")
    p_gen.add_argument("--rows", type=int, default=1000, help="Base rows per hour")
    p_gen.add_argument("--step", type=int, required=True, help="Step (hour) to generate")
    p_gen.add_argument("--local", action="store_true", help="Write to local directory instead of MinIO")

    args = parser.parse_args()

    try:
        if args.mode == "init":
            process_initial_load(args.file)
        elif args.mode == "generate":
            if args.local:
                steps_to_generate = 1
                start_step = args.step
                print(f"Generating LOCAL data for Step {start_step}...")
                
                df = generate_step_data(start_step, args.rows)
                
                part_dt = df["part_dt"][0]
                part_hour = df["part_hour"][0]
                
                filename = f"paysim_step{start_step}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
                local_path = f"sample_data/dl-landing/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}"
                os.makedirs(local_path, exist_ok=True)
                
                full_path = os.path.join(local_path, filename)
                df.write_parquet(full_path)
                print(f"Saved locally to: {full_path}")
            else:
                generate_incremental_batch(args.rows, args.step)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()
        exit(1)