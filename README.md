# S3 Log Compressor

A simple AWS Lambda function that downloads multiple files from S3 and creates a single zip archive, or retrieves a single file from an archive. Perfect for archiving log files or consolidating multiple files into a single downloadable package. The zip uses zstd for compression (mainly for paths and metadata)

## Features

- **Dual-Mode Operation**: Supports both compressing files into an archive and decompressing a single file from an archive.
- **Batch Processing**: Process thousands of files from an S3 manifest.
- **Asynchronous Cleanup**: Optionally deletes source files asynchronously after successful archiving.
- **Robust Validation**: Halts on any file download failure to ensure archive integrity.
- **Optimized Manifest Parsing**: Reduces S3 API calls by intelligently distinguishing files from directories in the manifest.
- **Concurrent Downloads**: Configurable worker threads for parallel processing.
- **KMS Encryption**: Supports server-side encryption with KMS keys.

## Orchestrator Lambda

The orchestrator lambda is responsible for processing a large S3 inventory export. It filters the inventory for a specific day, groups the files by AWS account and region, generates a manifest for each group, and then invokes the `s3-log-compressor` lambda to compress the files for each group.

### Orchestrator Event Structure

```json
{
  "inventory_s3_url": "s3://your-source-bucket/path/to/inventory.csv",
  "output_manifest_s3_prefix": "s3://your-target-bucket/manifests/",
  "date_to_process": "2024-12-25"
}
```

- `inventory_s3_url`: The S3 URL of the inventory export file (in CSV format).
- `output_manifest_s3_prefix`: The S3 prefix where the generated manifest files will be stored.
- `date_to_process`: The date (in `YYYY-MM-DD` format) to filter the inventory by.

## Event Structure

The `operation` field determines the function's behavior.

### Compress Operation

```json
{
  "operation": "compress",
  "input_s3_manifest_url": "s3://bucket/manifest.txt",
  "output_s3_url": "s3://bucket/archive.zip",
  "delete_source_files": false,
  "include_s3_name": true,
  "max_workers": 256
}
```

### Decompress Operation

```json
{
  "operation": "decompress",
  "source_s3_url": "s3://bucket/archive.zip",
  "file_to_extract": "path/in/zip/to/file.txt"
}
```

### Parameters

- `operation`: `compress` or `decompress`.
- `input_s3_manifest_url`: (Compress) S3 URL to a text file containing a list of files/directories to archive.
- `output_s3_url`: (Compress) S3 URL where the final zip archive will be stored.
- `delete_source_files`: (Compress) Whether to delete source files after successful archiving (default: `false`).
- `include_s3_name`: (Compress) Whether to include S3 bucket names in the zip archive paths (default: `true`).
- `max_workers`: (Compress) Maximum number of concurrent workers for downloading files (default: 256).
- `source_s3_url`: (Decompress) S3 URL of the source zip archive.
- `file_to_extract`: (Decompress) The full path of the file to extract from the archive.

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

### Compression
1. Downloads a manifest file from S3 containing a list of files and directories to archive.
2. Checks accessibility of all buckets mentioned in the manifest.
3. Lists all files to be processed, intelligently exploring directories as needed.
4. Downloads files concurrently and adds them to a zip archive. If any download fails, the process aborts.
5. Uploads the final zip file to the specified S3 location.
6. Optionally starts an asynchronous process to delete source files.

### Decompression
1. Downloads the specified zip archive from S3.
2. Extracts the requested file from the archive in memory.
3. Returns the file content as a base64-encoded string.

## Technical Details

The compression engine uses a few key Rust concepts to work safely and efficiently:

- **`Arc<Mutex<...>>`**: To handle many concurrent file downloads, the core `ZipWriter` is wrapped in an `Arc` (Atomic Reference Counter) and a `Mutex` (Mutual Exclusion lock).
    - `Arc` allows multiple download tasks to safely share ownership of the writer.
    - `Mutex` ensures that only one task can write to the zip file at a time, preventing data corruption.
- **`ZipWriter`**: This is a utility from the `zip` crate that handles the low-level details of creating a valid `.zip` archive structure.
- **`BufWriter`**: To improve performance, file writes are sent through a `BufWriter`. It acts as an in-memory buffer, collecting smaller writes into a single larger, more efficient write to the filesystem, reducing I/O overhead.

## Environment Variables

- `KMS_KEY_ID`: KMS key ID for server-side encryption (optional)
