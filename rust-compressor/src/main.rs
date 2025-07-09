use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client,primitives::ByteStream};
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write, Cursor};
use std::path::Path;
use std::time::Instant;
use tempfile::tempdir;
use tracing::{info, error, Level};
use walkdir::WalkDir;
use zip::read::ZipArchive;
use zip::write::{FileOptions, ZipWriter};
use zstd::stream::{decode_all, encode_all};
use base64::{engine::general_purpose::STANDARD, Engine as _};

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize structured logging for CloudWatch
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .json()
        .init();

    let func = service_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

// Main Lambda handler - routes to compression or decompression based on event payload
async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let operation = event["operation"].as_str().unwrap_or("compress");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    // Route to appropriate handler based on operation type
    let result = if operation == "compress" {
        handle_compression(&client, &event).await
    } else if operation == "decompress" {
        handle_decompression(&client, &event).await
    } else {
        Ok(serde_json::json!({ "status": "error", "message": "Invalid operation" }))
    };

    // Convert any errors to JSON responses for easier debugging
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            let error_message = format!("{:#?}", e);
            error!("Operation failed: {}", error_message);
            Ok(serde_json::json!({ "status": "error", "message": error_message }))
        }
    }
}

// Compress multiple files from S3 into a single zstd-compressed archive
async fn handle_compression(client: &Client, event: &Value) -> Result<Value, Error> {
    // Extract configuration from event or environment variables
    let source_bucket = event["source_bucket"].as_str().or(env::var("DEFAULT_SOURCE_BUCKET").ok().as_deref()).unwrap_or_default().to_string();
    let target_bucket = event["target_bucket"].as_str().or(env::var("DEFAULT_TARGET_BUCKET").ok().as_deref()).unwrap_or_default().to_string();
    let source_prefix = event["source_prefix"].as_str().or(env::var("DEFAULT_SOURCE_PREFIX").ok().as_deref()).unwrap_or("logs/").to_string();
    let target_prefix = event["target_prefix"].as_str().or(env::var("DEFAULT_TARGET_PREFIX").ok().as_deref()).unwrap_or("compressed/").to_string();
    let max_workers: usize = env::var("MAX_WORKERS").unwrap_or_else(|_| "512".to_string()).parse()?;
    let kms_key_id = env::var("KMS_KEY_ID").ok();

    info!(source_bucket, target_bucket, source_prefix, target_prefix, "Configuration");

    // Create temporary directory for processing
    let tmp_dir = tempdir()?;
    let log_dir = tmp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;

    // Step 1: List all files to compress
    let (keys, total_original_size) = list_keys(&client, &source_bucket, &source_prefix).await?;
    info!(
        "Listed {} keys with a total size of {} bytes",
        keys.len(),
        total_original_size
    );

    // Step 2: Download all files concurrently
    let start_dl = Instant::now();
    download_files(&client, &source_bucket, &keys, &log_dir, &source_prefix, max_workers).await?;
    info!("Downloaded all files in {:.2?}", start_dl.elapsed());

    // Recalculate total size from downloaded files
    let total_original_size = WalkDir::new(&log_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum::<u64>();
    info!(total_original_size_bytes = total_original_size, "Calculated total size of original files");

    // Step 3: Create uncompressed zip archive
    let start_zip = Instant::now();
    let archive_zip_path = tmp_dir.path().join("archive.zip");
    zip_directory(&log_dir, &archive_zip_path, zip::CompressionMethod::Stored)?;
    info!("Created .zip archive in {:.2?}", start_zip.elapsed());

    // Step 4: Compress zip file with zstd
    let start_zst = Instant::now();
    let archive_zst_path = tmp_dir.path().join("archive.zip.zst");
    let mut zip_file = File::open(&archive_zip_path)?;
    let mut zip_data = Vec::new();
    zip_file.read_to_end(&mut zip_data)?;
    let compressed_data = encode_all(&zip_data[..], 3)?;
    let mut zst_file = File::create(&archive_zst_path)?;
    zst_file.write_all(&compressed_data)?;
    info!("Compressed with zstd in {:.2?}", start_zst.elapsed());

    // Calculate compression metrics
    let compressed_size = fs::metadata(&archive_zst_path)?.len();
    info!(compressed_size_bytes = compressed_size, "Calculated final compressed size");

    if total_original_size > 0 {
        let ratio = compressed_size as f64 / total_original_size as f64;
        info!(compression_ratio = format!("{:.4}", ratio), "Calculated compression ratio (compressed/original)");
    }

    // Step 5: Upload compressed archive to S3
    let start_upload = Instant::now();
    let final_key = format!("{}archive.zip.zst", target_prefix);
    let stream = ByteStream::from_path(&archive_zst_path).await?;
    
    let mut put_object_request = client.put_object().bucket(&target_bucket).key(&final_key).body(stream);
    if let Some(key_id) = kms_key_id {
        put_object_request = put_object_request
            .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
            .ssekms_key_id(key_id);
    }
    
    put_object_request.send().await?;
    info!("Uploaded to S3 in {:.2?}", start_upload.elapsed());

    Ok(serde_json::json!({ "status": "ok", "output_key": final_key }))
}

// Decompress archive and extract a specific file, returning it as base64
async fn handle_decompression(client: &Client, event: &Value) -> Result<Value, Error> {
    let source_bucket = event["source_bucket"].as_str().ok_or("source_bucket not specified")?.to_string();
    let source_key = event["source_key"].as_str().ok_or("source_key not specified")?.to_string();
    let file_to_extract = event["file_to_extract"].as_str().ok_or("file_to_extract not specified")?.to_string();

    info!(source_bucket, source_key, file_to_extract, "Decompression Configuration");

    // Step 1: Download compressed archive from S3
    let start_dl = Instant::now();
    let mut object = client.get_object().bucket(&source_bucket).key(&source_key).send().await?;
    let mut zst_data = Vec::new();
    while let Some(bytes) = object.body.next().await {
        zst_data.extend_from_slice(&bytes?);
    }
    info!("Downloaded {} bytes in {:.2?}", zst_data.len(), start_dl.elapsed());

    // Step 2: Decompress zstd data back to zip
    let start_decompress = Instant::now();
    let zip_data = decode_all(&zst_data[..])?;
    info!("Decompressed to {} bytes in {:.2?}", zip_data.len(), start_decompress.elapsed());

    // Step 3: Extract specific file from zip archive
    let start_unzip = Instant::now();
    let mut archive = ZipArchive::new(Cursor::new(zip_data))?;
    let mut file = archive.by_name(&file_to_extract)?;
    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content)?;
    info!("Extracted '{}' ({} bytes) in {:.2?}", file_to_extract, file_content.len(), start_unzip.elapsed());

    // Step 4: Encode file content as base64 for JSON response
    let encoded_content = STANDARD.encode(&file_content);

    Ok(serde_json::json!({
        "status": "ok",
        "file_name": file_to_extract,
        "file_content_base64": encoded_content
    }))
}

// List all S3 objects under a given prefix and return keys with total size
async fn list_keys(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<(Vec<String>, u64), Error> {
    let mut keys = Vec::new();
    let mut total_size: u64 = 0;
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();
    while let Some(result) = stream.next().await {
        for obj in result?.contents() {
            keys.push(obj.key().unwrap().to_string());
            total_size += obj.size().unwrap_or(0) as u64;
        }
    }
    Ok((keys, total_size))
}

// Download multiple S3 objects concurrently to local directory
async fn download_files(client: &Client, bucket: &str, keys: &[String], local_dir: &Path, prefix: &str, max_workers: usize) -> Result<(), Error> {
    stream::iter(keys)
        .map(|key| {
            let client = client.clone();
            let bucket = bucket.to_string();
            let key = key.clone();
            let local_dir = local_dir.to_path_buf();
            let prefix = prefix.to_string();
            async move {
                let rel_path = key.strip_prefix(&prefix).unwrap();
                let local_path = local_dir.join(rel_path);
                if let Some(parent) = local_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let mut file = File::create(&local_path)?;
                let mut object = client.get_object().bucket(&bucket).key(&key).send().await?;
                while let Some(bytes) = object.body.next().await {
                    file.write_all(&bytes?)?;
                }
                Ok::<(), Error>(())
            }
        })
        .buffer_unordered(max_workers)
        .for_each(|_| async {})
        .await;
    Ok(())
}

// Create a zip archive from a directory with specified compression method
fn zip_directory(src_dir: &Path, dst_file: &Path, method: zip::CompressionMethod) -> zip::result::ZipResult<()> {
    let file = File::create(dst_file)?;
    let mut zip = ZipWriter::new(file);
    let options = FileOptions::<()>::default().compression_method(method);

    let walkdir = WalkDir::new(src_dir);
    let it = walkdir.into_iter().filter_map(|e| e.ok());

    for entry in it {
        let path = entry.path();
        let name = path.strip_prefix(src_dir).unwrap();
        if path.is_file() {
            zip.start_file(name.to_str().unwrap(), options)?;
            let mut f = File::open(path)?;
            let mut buffer = Vec::new();
            f.read_to_end(&mut buffer)?;
            zip.write_all(&buffer)?;
        } else if !name.as_os_str().is_empty() {
            zip.add_directory(name.to_str().unwrap(), options)?;
        }
    }
    zip.finish()?;
    Ok(())
}
