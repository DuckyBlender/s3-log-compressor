import os
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Configuration constants
S3_BUCKET = "s3-log-compressor-sourcebucket-rkeoqdxsxu2w"
S3_TARGET_FOLDER = "mock-logs/"  # Target folder in S3
LOCAL_LOGS_DIR = "mock_logs"
MAX_WORKERS = 100

def upload_file_to_s3(s3_client, local_file_path, bucket, s3_key):
    """Upload a single file to S3"""
    try:
        s3_client.upload_file(local_file_path, bucket, s3_key)
        return True
    except ClientError as e:
        print(f"Error uploading {local_file_path}: {e}")
        return False

def upload_mock_logs():
    """Upload all mock log files to S3"""
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Check if local directory exists
    if not os.path.exists(LOCAL_LOGS_DIR):
        print(f"Directory {LOCAL_LOGS_DIR} does not exist. Run mock.py first.")
        return
    
    # Get all JSON files from the mock_logs directory
    log_files = [f for f in os.listdir(LOCAL_LOGS_DIR) if f.endswith('.json')]
    
    if not log_files:
        print(f"No JSON files found in {LOCAL_LOGS_DIR}")
        return
    
    print(f"Found {len(log_files)} files to upload...")
    
    successful_uploads = 0
    failed_uploads = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_file = {
            executor.submit(
                upload_file_to_s3, 
                s3_client, 
                os.path.join(LOCAL_LOGS_DIR, filename), 
                S3_BUCKET, 
                f"{S3_TARGET_FOLDER}{filename}"
            ): filename 
            for filename in log_files
        }
        
        for future in tqdm(as_completed(future_to_file), total=len(log_files), desc="Uploading files"):
            if future.result():
                successful_uploads += 1
            else:
                failed_uploads += 1
    
    print(f"\nUpload complete!")
    print(f"Successful uploads: {successful_uploads}")
    print(f"Failed uploads: {failed_uploads}")
    print(f"Files uploaded to: s3://{S3_BUCKET}/{S3_TARGET_FOLDER}")

if __name__ == "__main__":
    upload_mock_logs()
