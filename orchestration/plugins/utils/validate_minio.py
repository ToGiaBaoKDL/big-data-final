import os
import sys
import argparse
import traceback
from minio import Minio


def validate_minio_path(bucket: str, prefix: str, min_files: int = 1, min_size_kb: float = 0.0) -> bool:
    """
    Validates that objects exist in the specified MinIO path.
    
    Args:
        bucket: S3 bucket name
        prefix: Object prefix path
        min_files: Minimum number of files expected (default: 1)
        min_size_kb: Minimum total size in KB (default: 0)
    
    Returns:
        True if validation passes, False otherwise
    """
    print(f"Validating path: s3://{bucket}/{prefix}")
    
    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", "").replace("https://", "")
    
    client = Minio(
        endpoint,
        access_key=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
        secure=False
    )
    
    # Check bucket exists
    if not client.bucket_exists(bucket):
        print(f"ERROR: Bucket '{bucket}' does not exist!")
        return False
    
    # List objects recursively
    objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    
    # Filter only parquet files (ignore directories and _SUCCESS files)
    data_files = [obj for obj in objects 
                  if not obj.is_dir 
                  and not obj.object_name.endswith('_SUCCESS')
                  and not obj.object_name.endswith('.crc')]
    
    if not data_files:
        print(f"ERROR: No data files found in s3://{bucket}/{prefix}")
        print("  Hint: Check if the path is correct and data was written successfully.")
        return False
    
    # Calculate total size
    total_size = sum((obj.size or 0) for obj in data_files)
    total_size_kb = total_size / 1024
    
    # Validation results
    print(f"  Found {len(data_files)} data file(s)")
    print(f"  Total Size: {total_size_kb:.2f} KB ({total_size:,} bytes)")
    
    # Show sample files
    if len(data_files) <= 5:
        for obj in data_files:
            print(f"    - {obj.object_name} ({obj.size:,} bytes)")
    else:
        print(f"    First 3 files:")
        for obj in data_files[:3]:
            print(f"      - {obj.object_name} ({obj.size:,} bytes)")
        print(f"    ... and {len(data_files) - 3} more files")
    
    # Validate minimum file count
    if len(data_files) < min_files:
        print(f"ERROR: Expected at least {min_files} files, found {len(data_files)}")
        return False
    
    # Validate minimum size
    if total_size_kb < min_size_kb:
        print(f"ERROR: Expected at least {min_size_kb} KB, got {total_size_kb:.2f} KB")
        return False
    
    # Validate not empty
    if total_size == 0:
        print("ERROR: Total size is 0 bytes. Dataset is empty.")
        return False
    
    print("  Validation PASSED ")
    return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Validate data exists in MinIO path")
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--prefix", required=True, help="Object prefix path")
    parser.add_argument("--min-files", type=int, default=1, help="Minimum number of files expected")
    parser.add_argument("--min-size-kb", type=float, default=0.0, help="Minimum total size in KB")
    
    args = parser.parse_args()
    
    try:
        success = validate_minio_path(
            args.bucket, 
            args.prefix,
            min_files=args.min_files,
            min_size_kb=args.min_size_kb
        )
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)
