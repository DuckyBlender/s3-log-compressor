# S3 Log Compressor

A simple AWS Lambda function that downloads multiple files from S3 and creates a single zip archive. Perfect for archiving log files or consolidating multiple files into a single downloadable package. The zip uses zstd for compression (mainly for paths and metadata)

## Features

- **Batch Processing**: Process thousands of files from S3 manifest
- **Efficient Zipping**: Create zip archives with progress logging
- **Cross-Bucket Support**: Works with files from multiple S3 buckets
- **Optional Cleanup**: Delete source files after successful archiving
- **Progress Tracking**: Logs progress every 10,000 files processed and 50,000 files listed
- **KMS Encryption**: Supports server-side encryption with KMS keys
- **Concurrent Downloads**: Configurable worker threads for parallel processing
- **Flexible Path Structure**: Option to include or exclude S3 bucket names in zip paths

## Event Structure

```json
{
  "input_s3_manifest_url": "s3://bucket/manifest.txt",
  "output_s3_url": "s3://bucket/archive.zip",
  "delete_source_files": false,
  "include_s3_name": true
}
```

### Parameters

- `input_s3_manifest_url`: S3 URL to a text file containing a list of files to archive
- `output_s3_url`: S3 URL where the final zip archive will be stored
- `delete_source_files`: Whether to delete source files after successful archiving (default: false)
- `include_s3_name`: Whether to include S3 bucket names in the zip archive paths (default: true)

When `include_s3_name` is `true`, files will be stored in the zip with paths like `bucket-name/path/to/file.txt`. When `false`, files will be stored with just their S3 key path like `path/to/file.txt`.

## Prerequisites
(for building and just lambda deployment)
- [Rust](https://rustup.rs/) (rustup)
- [Cargo Lambda](https://www.cargo-lambda.info/guide/getting-started.html)
(for full deployment)
- [AWS CLI](https://aws.amazon.com/cli/)
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)

## Building

To build just the binary:
```bash
cargo lambda build --release --arm64
```
The binary will be located in `target/lambda/s3_log_compressor`.

## Deployment

To deploy the entire stack:
```bash
sam build && sam deploy
```

## How It Works

1. Downloads a manifest file from S3 containing a list of files to archive, by default with 256 workers (this is optimal, more will cause os error 24 "Too many open files")
2. Checks accessibility of all buckets mentioned in the manifest
3. Lists all files to be processed and calculates total size
4. Downloads files concurrently and adds them to a zip archive
5. Uploads the final zip file to the specified S3 location
6. Optionally deletes source files if requested

## Environment Variables

- `MAX_WORKERS`: Number of concurrent download workers (optional, default: 256)
- `KMS_KEY_ID`: KMS key ID for server-side encryption (optional)
