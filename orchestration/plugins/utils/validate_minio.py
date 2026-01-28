import os
import sys
import argparse
from minio import Minio

def validate_minio_path(bucket, prefix):
    """
    Checks if objects exist in the key path.
    """
    print(f"Validating path: s3://{bucket}/{prefix}")
    
    client = Minio(
        os.getenv("AWS_ENDPOINT_URL", "http://localhost:9000").replace("http://", ""),
        access_key=os.getenv("MINIO_ROOT_USER", "minio_admin"),
        secret_key=os.getenv("MINIO_ROOT_PASSWORD", "minio_password"),
        secure=False
    )
    
    if not client.bucket_exists(bucket):
        print(f"Bucket {bucket} does not exist!")
        return False
        
    objects = list(client.list_objects(bucket, prefix=prefix))
    
    if not objects:
        print(f"No files found in {bucket}/{prefix}")
        return False
    
    total_size = sum(obj.size for obj in objects)
    print(f"Found {len(objects)} files.")
    print(f"   Total Size: {total_size/1024:.2f} KB")
    return True

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    
    args = parser.parse_args()
    
    try:
        success = validate_minio_path(args.bucket, args.prefix)
        if not success:
            sys.exit(1)
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        sys.exit(1)
