import os
import shutil
import random
import io
from datetime import datetime, timedelta
from collections import defaultdict
import polars as pl
import numpy as np
from faker import Faker
from minio import Minio
import argparse
import concurrent.futures

# --- Configuration ---
MINIO_ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio_password")
BUCKET_LANDING = os.getenv("MINIO_BUCKET_LANDING", "dl-landing-8f42a1")

# Constants
START_DATE_SIMULATION = datetime(2025, 12, 28, 0, 0, 0)
FAKE = Faker()

# PaySim Config
TXN_TYPES = ["PAYMENT", "CASH_OUT", "CASH_IN", "TRANSFER", "DEBIT"]
TXN_WEIGHTS = [0.35, 0.30, 0.20, 0.10, 0.05]

# Fraud Configuration
FRAUD_BASE_RATE = 0.0013
FRAUD_NIGHT_MULTIPLIER = 3.0
FRAUD_TYPES = ["TRANSFER", "CASH_OUT"]

# Fraud Pattern Distribution
FRAUD_PATTERN_EMPTY = 0.45
FRAUD_PATTERN_LARGE = 0.15
FRAUD_PATTERN_JUST_BELOW = 0.15
FRAUD_PATTERN_BURST = 0.15
FRAUD_PATTERN_NORMAL = 0.10

# Customer Segments
CUSTOMER_SEGMENTS = {
    'high_value': 0.05,
    'business': 0.10,
    'regular': 0.60,
    'low_income': 0.20,
    'dormant': 0.05
}

# Activity rates by segment (transactions per hour)
SEGMENT_ACTIVITY_RATES = {
    'high_value': 1.0,
    'business': 1.2,    # More active
    'regular': 1.0,
    'low_income': 0.8,  # Less active
    'dormant': 0.05     # Very rare (5% of normal)
}

# --- Data Pools ---
N_CUSTOMERS = 15000
N_MERCHANTS = 1000
CUSTOMER_POOL = [f"C{np.random.randint(10**8, 10**9)}" for _ in range(N_CUSTOMERS)]
MERCHANT_POOL = [f"M{np.random.randint(10**8, 10**9)}" for _ in range(N_MERCHANTS)]
MULE_ACCOUNTS = [f"C{np.random.randint(10**8, 10**9)}" for _ in range(50)]

# Assign segments to customers (persistent)
CUSTOMER_SEGMENTS_MAP = {}
segment_keys = list(CUSTOMER_SEGMENTS.keys())
segment_weights = list(CUSTOMER_SEGMENTS.values())
for cust in CUSTOMER_POOL:
    CUSTOMER_SEGMENTS_MAP[cust] = np.random.choice(segment_keys, p=segment_weights)

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def step_to_base_timestamp(step: int) -> datetime:
    """Trả về thời điểm bắt đầu của step."""
    return START_DATE_SIMULATION + timedelta(hours=step - 1)

def generate_diverse_timestamps(step: int, n_rows: int) -> tuple:
    """
    Tạo timestamps phân bố trong 1 giờ với clustering effect.
    
    Timestamps sẽ:
    - 70% random lẻ trong giờ (giây ngẫu nhiên từ 0-3599)
    - 30% cluster vào phút tròn (:00, :15, :30, :45) để giống thực tế
    
    Ví dụ step=1 (giờ 00:00-00:59):
    - Transaction 1: 00:03:27 (random)
    - Transaction 2: 00:15:42 (cluster vào :15)
    - Transaction 3: 00:47:13 (random)
    - Transaction 4: 00:30:08 (cluster vào :30)
    
    Returns: (timestamps_array, part_dts_array, part_hours_array)
    """
    base_time = step_to_base_timestamp(step)
    
    # Generate random seconds (0-3599) - giây ngẫu nhiên trong 1 giờ
    random_offsets = np.random.uniform(0, 3600, size=n_rows)
    
    # Clustering: 30% vào phút tròn (:00, :15, :30, :45) - simulate real user behavior
    cluster_mask = np.random.random(n_rows) < 0.30
    cluster_minutes = np.random.choice([0, 15, 30, 45], size=cluster_mask.sum())
    random_offsets[cluster_mask] = cluster_minutes * 60 + np.random.uniform(0, 60, size=cluster_mask.sum())
    
    # Convert to timestamps - vectorized
    times = np.array([base_time + timedelta(seconds=float(off)) for off in random_offsets])
    part_dts = np.array([t.strftime("%Y%m%d") for t in times])
    part_hours = np.array([t.strftime("%H") for t in times])
    
    return times, part_dts, part_hours

def get_segment_balance_range(segment: str) -> tuple:
    """Trả về (min, max) balance cho từng segment."""
    if segment == 'high_value':
        return (100000, 10000000)
    elif segment == 'business':
        return (50000, 2000000)
    elif segment == 'low_income':
        return (100, 10000)
    elif segment == 'dormant':
        return (0, 5000)
    else:  # regular
        return (1000, 100000)

def initialize_balance(account: str) -> float:
    """Khởi tạo balance dựa trên segment của account."""
    if account.startswith('M'):  # Merchant
        return np.random.lognormal(mean=10, sigma=2)
    
    segment = CUSTOMER_SEGMENTS_MAP.get(account, 'regular')
    min_bal, max_bal = get_segment_balance_range(segment)
    
    balance = np.random.lognormal(mean=8, sigma=2)
    balance = np.clip(balance, min_bal, max_bal)
    return balance

def get_amount_by_segment_and_type(txn_type: str, segment: str, n: int) -> np.ndarray:
    """Generate amounts dựa trên segment và transaction type."""
    if segment == 'high_value':
        if txn_type == "TRANSFER":
            amounts = np.random.lognormal(mean=10, sigma=2, size=n)
            amounts = np.clip(amounts, 50000, 50000000)
        elif txn_type == "CASH_OUT":
            amounts = np.random.lognormal(mean=9, sigma=1.8, size=n)
            amounts = np.clip(amounts, 10000, 5000000)
        else:
            amounts = np.random.lognormal(mean=7.5, sigma=1.5, size=n)
            amounts = np.clip(amounts, 1000, 500000)
    
    elif segment == 'business':
        if txn_type == "TRANSFER":
            amounts = np.random.lognormal(mean=9, sigma=1.8, size=n)
            amounts = np.clip(amounts, 10000, 10000000)
        elif txn_type == "CASH_OUT":
            amounts = np.random.lognormal(mean=8, sigma=1.6, size=n)
            amounts = np.clip(amounts, 5000, 2000000)
        else:
            amounts = np.random.lognormal(mean=7, sigma=1.4, size=n)
            amounts = np.clip(amounts, 500, 200000)
    
    elif segment == 'low_income':
        if txn_type == "TRANSFER":
            amounts = np.random.lognormal(mean=6, sigma=1.2, size=n)
            amounts = np.clip(amounts, 100, 50000)
        elif txn_type == "CASH_OUT":
            amounts = np.random.lognormal(mean=5.5, sigma=1.1, size=n)
            amounts = np.clip(amounts, 50, 20000)
        else:
            amounts = np.random.lognormal(mean=5, sigma=1.0, size=n)
            amounts = np.clip(amounts, 20, 5000)
    
    else:  # regular, dormant
        if txn_type == "TRANSFER":
            amounts = np.random.lognormal(mean=8, sigma=2, size=n)
            amounts = np.clip(amounts, 100, 10000000)
        elif txn_type == "CASH_OUT":
            amounts = np.random.lognormal(mean=7, sigma=1.5, size=n)
            amounts = np.clip(amounts, 50, 1000000)
        else:
            amounts = np.random.lognormal(mean=6, sigma=1.2, size=n)
            amounts = np.clip(amounts, 50, 50000)
    
    return amounts

def upload_file(client, bucket, object_name, file_path):
    """Helper function for single file upload."""
    try:
        client.fput_object(bucket, object_name, file_path)
        print(f"Uploaded {object_name}")
    except Exception as e:
        print(f"Failed to upload {object_name}: {e}")

def upload_local_directory_to_minio(client, local_path, bucket, prefix, max_workers=8):
    """Recursively uploads a local directory to MinIO in parallel."""
    files_to_upload = []
    
    for root, _, files in os.walk(local_path):
        for file in files:
            file_path = os.path.join(root, file)
            rel_path = os.path.relpath(file_path, local_path)
            object_name = f"{prefix}/{rel_path}"
            files_to_upload.append((bucket, object_name, file_path))
    
    print(f"Uploading {len(files_to_upload)} files with {max_workers} threads...")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all upload tasks
        futures = [
            executor.submit(upload_file, client, bucket, obj, path)
            for bucket, obj, path in files_to_upload
        ]
        
        # Wait for completion
        for future in concurrent.futures.as_completed(futures):
            # We can aggregate results here if needed
            try:
                future.result()
            except Exception as e:
                print(f"Upload task failed: {e}")

def process_initial_load(source_file):
    """
    Mode Init: Đọc CSV, thêm varied timestamps, partition và upload trực tiếp lên MinIO.
    Ensures consistent schema with incremental mode.
    """
    print(f"Processing Initial Data from {source_file}...")
    
    df = pl.read_csv(source_file)
    
    if 'step' not in df.columns:
        raise ValueError("CSV must contain 'step' column")

    df = df.with_columns([
        pl.col("isFraud").cast(pl.Int8),
        pl.col("isFlaggedFraud").cast(pl.Int8),
        pl.col("amount").cast(pl.Float64),
        pl.col("oldbalanceOrg").cast(pl.Float64),
        pl.col("newbalanceOrig").cast(pl.Float64),
        pl.col("oldbalanceDest").cast(pl.Float64),
        pl.col("newbalanceDest").cast(pl.Float64),
    ])
    
    print(f"Total rows: {len(df):,}")
    print(f"Unique steps: {df['step'].n_unique()}")
    print("\nAdding varied transaction timestamps and uploading...")
    
    # Get MinIO client
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_LANDING):
        print(f"Creating bucket: {BUCKET_LANDING}")
        client.make_bucket(BUCKET_LANDING)
    
    steps = sorted(df["step"].unique().to_list())
    total_uploaded = 0
    
    for i, step_val in enumerate(steps, 1):
        step_df = df.filter(pl.col("step") == step_val)
        n_step_rows = len(step_df)
        
        print(f"\n[{i}/{len(steps)}] Processing step {step_val}: {n_step_rows:,} rows")
        
        # Generate varied timestamps
        times, part_dts, part_hours = generate_diverse_timestamps(step_val, n_step_rows)
        
        # Convert Python datetimes to ISO strings for Polars compatibility
        times_str = [t.strftime("%Y-%m-%d %H:%M:%S") for t in times]
        
        step_df = step_df.with_columns([
            pl.Series("transaction_time", times_str).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S"),
            pl.Series("part_dt", part_dts),
            pl.Series("part_hour", part_hours)
        ])
        
        # Get unique partitions and upload each
        unique_partitions = step_df.select(["part_dt", "part_hour"]).unique()
        
        partition_count = 0
        for row in unique_partitions.iter_rows(named=True):
            part_dt = row["part_dt"]
            part_hour = row["part_hour"]
            
            partition_df = step_df.filter(
                (pl.col("part_dt") == part_dt) & (pl.col("part_hour") == part_hour)
            )
            
            # Convert to parquet in memory
            buffer = io.BytesIO()
            partition_df.write_parquet(buffer)
            buffer.seek(0)
            file_size = buffer.getbuffer().nbytes
            
            # Upload to MinIO
            filename = f"init_step{step_val}_{part_dt}_{part_hour}.parquet"
            object_name = f".warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/{filename}"
            
            client.put_object(
                BUCKET_LANDING,
                object_name,
                buffer,
                file_size,
                content_type="application/octet-stream"
            )
            
            partition_count += 1
            total_uploaded += len(partition_df)
        
        print(f"  Step {step_val}: {partition_count} partitions, {n_step_rows:,} rows uploaded")
    
    print(f"Initial Load Complete!")
    print(f"  Total rows: {total_uploaded:,}")
    print(f"  Steps: {len(steps)}")
    print(f"  Bucket: {BUCKET_LANDING}")


def get_fraud_rate_for_hour(hour: int) -> float:
    """Fraud cao hơn vào ban đêm (22h-6h)."""
    if 22 <= hour or hour <= 6:
        return FRAUD_BASE_RATE * FRAUD_NIGHT_MULTIPLIER
    return FRAUD_BASE_RATE

def generate_step_data(step: int, base_rows: int):
    """
    Generate data với:
    1. Varied timestamps
    2. Weighted account selection (dormant accounts ít giao dịch)
    3. Balance tính ĐÚNG theo thứ tự thời gian
    4. Fraud patterns đa dạng
    """
    base_time = step_to_base_timestamp(step)
    hour = base_time.hour
    
    # Hourly variation
    if 9 <= hour <= 17:
        multiplier = random.uniform(1.30, 1.50)
    elif 0 <= hour <= 6:
        multiplier = random.uniform(0.60, 0.80)
    elif 18 <= hour <= 23:
        multiplier = random.uniform(0.90, 1.10)
    else:
        multiplier = random.uniform(1.10, 1.30)
    
    daily_variation = random.uniform(0.90, 1.10)
    n_rows = int(base_rows * multiplier * daily_variation)
    
    print(f"  > Generating {n_rows} rows for Step {step} (Hour {hour:02d})...")
    
    # 1. Generate timestamps
    trans_times, part_dts, part_hours = generate_diverse_timestamps(step, n_rows)
    
    # 2. Generate accounts với WEIGHTED selection (dormant ít hơn)
    # Create weighted customer pool
    customer_weights = np.array([
        SEGMENT_ACTIVITY_RATES[CUSTOMER_SEGMENTS_MAP.get(c, 'regular')] 
        for c in CUSTOMER_POOL
    ])
    customer_weights = customer_weights / customer_weights.sum()
    
    name_orig = np.random.choice(CUSTOMER_POOL, size=n_rows, replace=True, p=customer_weights)
    
    # 3. Generate transaction types
    types_array = np.random.choice(TXN_TYPES, size=n_rows, p=TXN_WEIGHTS)
    
    # 4. Generate destinations
    name_dest = np.empty(n_rows, dtype=object)
    for txn_type in TXN_TYPES:
        mask = types_array == txn_type
        count = mask.sum()
        if count > 0:
            if txn_type in ["PAYMENT", "DEBIT"]:
                name_dest[mask] = np.random.choice(MERCHANT_POOL, size=count, replace=True)
            else:
                name_dest[mask] = np.random.choice(CUSTOMER_POOL, size=count, replace=True, p=customer_weights)
    
    # 5. Get segments
    segments = np.array([CUSTOMER_SEGMENTS_MAP.get(acc, 'regular') for acc in name_orig])
    
    # 6. Generate amounts dựa trên segment - VECTORIZED
    amounts = np.zeros(n_rows)
    for txn_type in TXN_TYPES:
        for segment in CUSTOMER_SEGMENTS.keys():
            mask = (types_array == txn_type) & (segments == segment)
            count = mask.sum()
            if count > 0:
                amounts[mask] = get_amount_by_segment_and_type(txn_type, segment, count)
    
    amounts = np.round(amounts, 2)
    
    # 7. Inject Fraud - VECTORIZED approach
    is_fraud = np.zeros(n_rows, dtype=np.int8)
    is_flagged_fraud = np.zeros(n_rows, dtype=np.int8)
    fraud_pattern_type = np.full(n_rows, '', dtype=object)
    
    fraud_rate = get_fraud_rate_for_hour(hour)
    fraud_eligible = np.isin(types_array, FRAUD_TYPES)
    fraud_candidates = np.where(fraud_eligible)[0]
    
    burst_fraud_accounts = set()
    
    if len(fraud_candidates) > 0:
        n_frauds = max(1, int(len(fraud_candidates) * fraud_rate))
        fraud_indices = np.random.choice(fraud_candidates, size=min(n_frauds, len(fraud_candidates)), replace=False)
        
        for idx in fraud_indices:
            is_fraud[idx] = 1
            txn_type = types_array[idx]
            
            pattern_roll = random.random()
            
            if pattern_roll < FRAUD_PATTERN_EMPTY:
                fraud_pattern_type[idx] = 'empty'
                
            elif pattern_roll < FRAUD_PATTERN_EMPTY + FRAUD_PATTERN_LARGE:
                fraud_pattern_type[idx] = 'large_round'
                round_amounts = [50000, 100000, 250000, 500000, 1000000, 2000000]
                amounts[idx] = random.choice(round_amounts)
                
            elif pattern_roll < FRAUD_PATTERN_EMPTY + FRAUD_PATTERN_LARGE + FRAUD_PATTERN_JUST_BELOW:
                fraud_pattern_type[idx] = 'just_below'
                if txn_type == "TRANSFER":
                    amounts[idx] = random.uniform(180000, 199999)
                else:
                    amounts[idx] = random.uniform(150000, 250000)
                    
            elif pattern_roll < FRAUD_PATTERN_EMPTY + FRAUD_PATTERN_LARGE + FRAUD_PATTERN_JUST_BELOW + FRAUD_PATTERN_BURST:
                fraud_pattern_type[idx] = 'burst'
                amounts[idx] = round(random.uniform(10000, 30000), 2)
                burst_fraud_accounts.add(name_orig[idx])
                
            else:
                fraud_pattern_type[idx] = 'normal'
                amounts[idx] = round(random.uniform(5000, 50000), 2)
            
            amounts[idx] = round(amounts[idx], 2)
            
            # Use mule accounts
            if random.random() < 0.85:
                name_dest[idx] = random.choice(MULE_ACCOUNTS)
            
            # Flagging rule
            if txn_type == "TRANSFER" and amounts[idx] > 200000:
                is_flagged_fraud[idx] = 1
    
    # Add burst fraud
    if burst_fraud_accounts:
        for burst_acc in list(burst_fraud_accounts)[:3]:
            # Find other ELIGIBLE transactions from same account
            same_acc_indices = np.where(
                (name_orig == burst_acc) & 
                (is_fraud == 0) & 
                fraud_eligible
            )[0]
            
            n_additional = min(random.randint(2, 5), len(same_acc_indices))
            if n_additional > 0:
                additional_fraud_idx = np.random.choice(same_acc_indices, size=n_additional, replace=False)
                for idx in additional_fraud_idx:
                    is_fraud[idx] = 1
                    fraud_pattern_type[idx] = 'burst_additional'
                    amounts[idx] = round(random.uniform(10000, 30000), 2)
                    if random.random() < 0.85:
                        name_dest[idx] = random.choice(MULE_ACCOUNTS)
    
    # 8. Sort ALL arrays by timestamp BEFORE balance calculation
    time_values = np.array([t.timestamp() for t in trans_times])
    sorted_indices = np.argsort(time_values)
    
    # Reorder ALL arrays
    trans_times = trans_times[sorted_indices]
    part_dts = part_dts[sorted_indices]
    part_hours = part_hours[sorted_indices]
    name_orig = name_orig[sorted_indices]
    name_dest = name_dest[sorted_indices]
    types_array = types_array[sorted_indices]
    segments = segments[sorted_indices]
    amounts = amounts[sorted_indices]
    is_fraud = is_fraud[sorted_indices]
    is_flagged_fraud = is_flagged_fraud[sorted_indices]
    fraud_pattern_type = fraud_pattern_type[sorted_indices]
    
    # 9. Calculate balances - VECTORIZED where possible
    balance_cache = {}
    
    old_balance_orig = np.zeros(n_rows)
    new_balance_orig = np.zeros(n_rows)
    old_balance_dest = np.zeros(n_rows)
    new_balance_dest = np.zeros(n_rows)
    
    # Process in order (already sorted)
    for i in range(n_rows):
        acc_orig = name_orig[i]
        acc_dest = name_dest[i]
        amt = amounts[i]
        txn_type = types_array[i]
        
        # Initialize balance if needed
        if acc_orig not in balance_cache:
            balance_cache[acc_orig] = initialize_balance(acc_orig)
        
        ob_orig = balance_cache[acc_orig]
        
        # Empty account fraud pattern
        if is_fraud[i] and fraud_pattern_type[i] == 'empty':
            amt = ob_orig
            amounts[i] = round(amt, 2)
        
        # Calculate new balance for origin
        if txn_type in ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT"]:
            nb_orig = max(0, ob_orig - amt)
        else:  # CASH_IN
            nb_orig = ob_orig + amt
        
        old_balance_orig[i] = round(ob_orig, 2)
        new_balance_orig[i] = round(nb_orig, 2)
        balance_cache[acc_orig] = nb_orig
        
        # Destination balance
        if acc_dest not in balance_cache:
            balance_cache[acc_dest] = initialize_balance(acc_dest)
        
        ob_dest = balance_cache[acc_dest]
        
        if txn_type in ["CASH_IN", "TRANSFER"]:
            nb_dest = ob_dest + amt
        elif txn_type == "CASH_OUT":
            nb_dest = max(0, ob_dest - amt)
        else:
            nb_dest = ob_dest
        
        old_balance_dest[i] = round(ob_dest, 2)
        new_balance_dest[i] = round(nb_dest, 2)
        balance_cache[acc_dest] = nb_dest
    
    # 10. Create DataFrame - ALL arrays are already sorted by time
    df = pl.DataFrame({
        "step": np.full(n_rows, step, dtype=np.int32),
        "type": types_array.tolist(),
        "amount": amounts.astype(np.float64),  # Float64 for Spark/ClickHouse
        "nameOrig": name_orig.tolist(),
        "oldbalanceOrg": old_balance_orig.astype(np.float64),
        "newbalanceOrig": new_balance_orig.astype(np.float64),
        "nameDest": name_dest.tolist(),
        "oldbalanceDest": old_balance_dest.astype(np.float64),
        "newbalanceDest": new_balance_dest.astype(np.float64),
        "isFraud": is_fraud.astype(np.int8),  # Int8 for UInt8 in ClickHouse
        "isFlaggedFraud": is_flagged_fraud.astype(np.int8),
        "transaction_time": trans_times.tolist(),
        "part_dt": part_dts.tolist(),
        "part_hour": part_hours.tolist()
    })
    
    return df

def generate_incremental_batch(base_rows: int, start_step: int):
    """Generates 1 step và upload lên MinIO."""
    print(f"Generating data for Step {start_step}...")
    
    client = get_minio_client()
    df = generate_step_data(start_step, base_rows)
    
    part_dt = df["part_dt"][0]
    part_hour = df["part_hour"][0]
    
    filename = f"paysim_step{start_step}.parquet"
    object_name = f".warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}/{filename}"
    
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

    p_init = subparsers.add_parser("init", help="Initial load từ CSV")
    p_init.add_argument("--file", required=True, help="Path to PaySim CSV file")

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
                df = generate_step_data(args.step, args.rows)
                part_dt = df["part_dt"][0]
                part_hour = df["part_hour"][0]
                
                local_path = f"sample_data/dl-landing/.warehouse/paysim_txn/part_dt={part_dt}/part_hour={part_hour}"
                os.makedirs(local_path, exist_ok=True)
                
                filename = f"paysim_step{args.step}.parquet"
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