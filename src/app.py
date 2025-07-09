import boto3
import os
import tempfile
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure S3 client with retry settings for VPC environment
s3 = boto3.client("s3", config=Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=50
))

MAX_WORKERS = int(os.environ.get("MAX_WORKERS", "64"))

def list_keys(source_bucket, source_prefix):
    """List S3 keys with improved pagination and error handling"""
    print(f"Attempting to list objects in bucket: {source_bucket}, prefix: {source_prefix}")
    
    try:
        # Test connectivity first
        s3.head_bucket(Bucket=source_bucket)
        print("Successfully connected to S3 bucket")
    except ClientError as e:
        print(f"Failed to connect to bucket {source_bucket}: {e}")
        raise
    
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=source_bucket, 
        Prefix=source_prefix,
        PaginationConfig={
            'PageSize': 1000 # list_objects_v2 limit
        }
    )
    
    try:
        for page_num, page in enumerate(page_iterator):
            contents = page.get("Contents", [])
            print(f"Processing page {page_num + 1} with {len(contents)} objects...")
            for obj in contents:
                yield obj["Key"], obj["Size"]
    except ClientError as e:
        print(f"Error listing objects: {e}")
        raise

def download_file(source_bucket, key, local_path):
    with open(local_path, "wb") as f:
        s3.download_fileobj(source_bucket, key, f)

def lambda_handler(event, context):
    # Extract configuration from environment variables with event overrides
    source_bucket = event.get("source_bucket") or os.environ.get("DEFAULT_SOURCE_BUCKET")
    target_bucket = event.get("target_bucket") or os.environ.get("DEFAULT_TARGET_BUCKET")
    source_prefix = event.get("source_prefix") or os.environ.get("DEFAULT_SOURCE_PREFIX", "logs/")
    target_prefix = event.get("target_prefix") or os.environ.get("DEFAULT_TARGET_PREFIX", "compressed/")
    
    if not source_bucket or not target_bucket:
        raise ValueError("source_bucket and target_bucket must be provided via event or environment variables")
    
    print(f"Using source_bucket: {source_bucket}, target_bucket: {target_bucket}")
    print(f"Using source_prefix: {source_prefix}, target_prefix: {target_prefix}")
    
    start_total = time.time()
    with tempfile.TemporaryDirectory() as tmpdir:
        logdir = os.path.join(tmpdir, "logs")
        os.makedirs(logdir)

        print("Listing S3 keys...")
        start_list = time.time()
        key_size_pairs = []
        try:
            for key, size in list_keys(source_bucket, source_prefix):
                key_size_pairs.append((key, size))
                if len(key_size_pairs) % 100 == 0:
                    print(f"Listed {len(key_size_pairs)} keys so far...")
        except Exception as e:
            print(f"Failed to list keys: {e}")
            raise
            
        keys = [k for k, _ in key_size_pairs]
        sizes = [s for _, s in key_size_pairs]
        end_list = time.time()
        print(f"Listed {len(keys)} keys in {end_list - start_list:.2f} seconds.")

        total_files = len(keys)
        total_bytes_downloaded = 0

        def task(idx_key):
            idx, key = idx_key
            rel_path = key[len(source_prefix):]
            local_file = os.path.join(logdir, rel_path)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            download_file(source_bucket, key, local_file)
            return sizes[idx]

        print("Downloading files...")
        start_dl = time.time()
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            downloaded_sizes = list(executor.map(task, enumerate(keys)))
        total_bytes_downloaded = sum(downloaded_sizes)
        end_dl = time.time()
        print(f"Downloaded all files in {end_dl - start_dl:.2f} seconds.")
        print(f"Files downloaded: {total_files}, total size: {total_bytes_downloaded/1024/1024:.2f} MB")

        # Step 1: Create .zip with no compression (-0)
        print("Creating .zip archive...")
        start_zip = time.time()
        archive_zip = os.path.join(tmpdir, "archive.zip")
        subprocess.run([
            "/opt/bin/zip", "-r", "-0", "-q", archive_zip, "logs"
        ], cwd=tmpdir, check=True)
        end_zip = time.time()
        print(f"Created .zip archive in {end_zip - start_zip:.2f} seconds.")

        # Step 2: Compress with zstd -3 to .zip.zst
        print("Compressing with zstd...")
        start_zst = time.time()
        archive_zst = archive_zip + ".zst"
        subprocess.run([
            "/opt/bin/zstd", "-3", archive_zip, "-o", archive_zst
        ], check=True)
        end_zst = time.time()
        print(f"Compressed with zstd in {end_zst - start_zst:.2f} seconds.")

        # Step 3: Upload to S3
        print("Uploading to S3...")
        start_upload = time.time()
        final_key = os.path.join(target_prefix, "archive.zip.zst")
        archive_zst_size = os.path.getsize(archive_zst)
        with open(archive_zst, "rb") as f:
            s3.upload_fileobj(f, target_bucket, final_key)
        end_upload = time.time()
        print(f"Uploaded to S3 in {end_upload - start_upload:.2f} seconds.")
        print(f"Files compressed: {total_files}, uploaded size: {archive_zst_size/1024/1024:.2f} MB")

    end_total = time.time()
    print(f"Total time: {end_total - start_total:.2f} seconds.")

    return {
        "statusCode": 200,
        "body": (
            f"Uploaded {final_key}, "
            f"files: {total_files}, "
            f"downloaded: {total_bytes_downloaded/1024/1024:.2f} MB, "
            f"uploaded: {archive_zst_size/1024/1024:.2f} MB"
        )
    }
