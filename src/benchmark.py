import boto3
import os
import tempfile
import time
import json
from concurrent.futures import ThreadPoolExecutor
from botocore.exceptions import ClientError
from botocore.config import Config

# Configure S3 client with retry settings for VPC environment
s3 = boto3.client("s3", config=Config(
    retries={'max_attempts': 10, 'mode': 'adaptive'},
    max_pool_connections=200
))

def list_keys(source_bucket, source_prefix):
    """List S3 keys with improved pagination and error handling"""
    paginator = s3.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(
        Bucket=source_bucket, 
        Prefix=source_prefix,
        PaginationConfig={'PageSize': 1000}
    )
    
    for page in page_iterator:
        contents = page.get("Contents", [])
        for obj in contents:
            yield obj["Key"], obj["Size"]

def download_file(source_bucket, key, local_path):
    with open(local_path, "wb") as f:
        s3.download_fileobj(source_bucket, key, f)

def benchmark_downloads(source_bucket, source_prefix, keys, sizes, max_workers):
    """Benchmark downloads with specific number of workers"""
    with tempfile.TemporaryDirectory() as tmpdir:
        logdir = os.path.join(tmpdir, "logs")
        os.makedirs(logdir)
        
        def task(idx_key):
            idx, key = idx_key
            rel_path = key[len(source_prefix):]
            local_file = os.path.join(logdir, rel_path)
            os.makedirs(os.path.dirname(local_file), exist_ok=True)
            download_file(source_bucket, key, local_file)
            return sizes[idx]
        
        start_time = time.time()
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            downloaded_sizes = list(executor.map(task, enumerate(keys)))
        end_time = time.time()
        
        total_bytes = sum(downloaded_sizes)
        duration = end_time - start_time
        
        return {
            'workers': max_workers,
            'duration': duration,
            'total_bytes': total_bytes,
            'files_count': len(keys),
            'kbps': (total_bytes / 1024) / duration if duration > 0 else 0
        }

def lambda_handler(event, context):
    # Extract configuration from environment variables with event overrides
    source_bucket = event.get("source_bucket") or os.environ.get("DEFAULT_SOURCE_BUCKET")
    source_prefix = event.get("source_prefix") or os.environ.get("DEFAULT_SOURCE_PREFIX", "logs/")
    max_files = event.get("max_files", 1000)  # Increased limit for benchmarking
    
    if not source_bucket:
        raise ValueError("source_bucket must be provided via event or environment variables")
    
    print(f"BENCHMARK_START,bucket={source_bucket},prefix={source_prefix},max_files={max_files}")
    
    # List and collect files
    print("PHASE,listing_files")
    start_list = time.time()
    key_size_pairs = []
    for key, size in list_keys(source_bucket, source_prefix):
        key_size_pairs.append((key, size))
        if len(key_size_pairs) >= max_files:
            break
    
    keys = [k for k, _ in key_size_pairs]
    sizes = [s for _, s in key_size_pairs]
    end_list = time.time()
    
    total_bytes = sum(sizes)
    print(f"LISTING_COMPLETE,files={len(keys)},total_kb={total_bytes/1024:.2f},duration={end_list-start_list:.2f}")
    
    # Test different worker counts
    worker_counts = [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024]
    results = []
    
    for workers in worker_counts:
        if workers > len(keys):  # Skip if more workers than files
            print(f"SKIP,workers={workers},reason=more_workers_than_files")
            continue
            
        print(f"PHASE,downloading,workers={workers}")
        try:
            result = benchmark_downloads(source_bucket, source_prefix, keys, sizes, workers)
            results.append(result)
            print(f"RESULT,{workers},{result['duration']:.3f},{result['kbps']:.2f},{result['files_count']},{result['total_bytes']}")
        except Exception as e:
            print(f"ERROR,workers={workers},error={str(e)}")
            # Don't break, continue with next worker count
    
    # Find optimal worker count after all tests are complete
    if results:
        best_result = min(results, key=lambda x: x['duration'])
        print(f"OPTIMAL,workers={best_result['workers']},duration={best_result['duration']:.3f},kbps={best_result['kbps']:.2f}")
    
    print("BENCHMARK_COMPLETE")
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "results": results,
            "optimal_workers": best_result['workers'] if results else None,
            "files_tested": len(keys),
            "total_kb": total_bytes / 1024
        })
    }
