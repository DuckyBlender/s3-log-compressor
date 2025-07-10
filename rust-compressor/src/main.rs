use aws_config::BehaviorVersion;
use aws_sdk_s3::{primitives::ByteStream, Client};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet, HashMap};
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;
use tracing::{error, info, Level};
use zip::read::ZipArchive;
use zip::write::{FileOptions, ZipWriter};

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

    // Create a fresh config and client for each invocation to avoid connection pool issues
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

    // Explicitly drop the client to release connection pool
    drop(client);

    // Force a yield to allow cleanup
    tokio::task::yield_now().await;

    // Convert any errors to JSON responses for easier debugging
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            let error_message = format!("{:#?}", e);
            error!("Operation failed: {}", error_message);
            // To ensure a non-200 response, we should return an Err from the handler.
            // The lambda runtime will serialize this into an error response.
            Err(e)
        }
    }
}

// Compress multiple files from S3 into a single zstd-compressed archive
async fn handle_compression(client: &Client, event: &Value) -> Result<Value, Error> {
    let operation_start = Instant::now();
    // Extract configuration from event
    let input_s3_manifest_url = event["input_s3_manifest_url"]
        .as_str()
        .ok_or("'input_s3_manifest_url' not specified")?
        .to_string();

    let output_s3_url = event["output_s3_url"]
        .as_str()
        .ok_or("'output_s3_url' not specified")?
        .to_string();

    let delete_source_files = event["delete_source_files"].as_bool().unwrap_or(false);
    let compression_level = event["compression_level"].as_i64().unwrap_or(3) as i32;
    if !(1..=22).contains(&compression_level) {
        return Err("Invalid compression level, must be between 1 and 22".into());
    }

    let (target_bucket, final_key) = parse_s3_url(&output_s3_url)?;

    let max_workers: usize = env::var("MAX_WORKERS")
        .unwrap_or_else(|_| "256".to_string())
        .parse()?;

    let kms_key_id = env::var("KMS_KEY_ID").ok();

    info!(
        input_s3_manifest_url,
        output_s3_url, delete_source_files, compression_level, "Configuration"
    );

    // Create temporary directory for processing
    let tmp_dir = tempdir()?;

    // Step 1: Download and parse the manifest file to get the list of S3 inputs
    let inputs = download_and_parse_manifest(client, &input_s3_manifest_url).await?;
    info!("Parsed {} inputs from manifest file", inputs.len());

    // Step 2: Check bucket accessibility before proceeding
    if let Err(e) = check_bucket_accessibility(client, &inputs).await {
        error!("Bucket accessibility check failed: {}", e);
        return Ok(serde_json::json!({ "status": "error", "message": e.to_string() }));
    }
    info!("All source buckets are accessible.");

    // Step 3: List all files to compress from the S3 inputs
    let start_list = Instant::now();
    let (files_to_process, s3_metadata_size) = list_files_from_inputs(client, &inputs).await?;
    let list_files_duration = start_list.elapsed();
    let files_processed = files_to_process.len();
    info!(
        "Listed {} keys with a total S3 metadata size of {} bytes in {:.2?}",
        files_processed, s3_metadata_size, list_files_duration
    );

    if files_to_process.is_empty() {
        info!("No files to process. Exiting.");
        return Ok(serde_json::json!({ "status": "ok", "message": "No files to process" }));
    }

    // Step 3: Download files and create zip archive
    let start_zip = Instant::now();
    let downloaded_size = download_and_create_zip(client, &files_to_process, &tmp_dir, max_workers).await?;
    let zip_duration = start_zip.elapsed();
    info!("Downloaded and zipped all files in {:.2?}", zip_duration);

    info!(
        downloaded_size_bytes = downloaded_size,
        "Calculated total size of downloaded files"
    );

    // Add validation step to ensure downloaded bytes match S3 metadata
    if s3_metadata_size != downloaded_size {
        let error_message = format!(
            "Mismatch between S3 metadata size ({}) and downloaded size ({}).",
            s3_metadata_size, downloaded_size
        );
        error!("{}", error_message);
        return Err(error_message.into());
    }

    // Step 4: Compress zip file with zstd
    let start_compress = Instant::now();
    let archive_zst_path = compress_zip_to_zstd(&tmp_dir, compression_level).await?;
    let compress_duration = start_compress.elapsed();
    info!("Compressed with zstd in {:.2?}", compress_duration);

    // Calculate compression metrics
    let compressed_size = fs::metadata(&archive_zst_path)?.len();
    info!(
        compressed_size_bytes = compressed_size,
        "Calculated final compressed size"
    );

    if downloaded_size > 0 {
        let ratio = compressed_size as f64 / downloaded_size as f64;
        info!(
            compression_ratio = format!("{:.4}", ratio),
            "Calculated compression ratio (compressed/downloaded)"
        );
    }

    // Step 7: Upload compressed archive to S3
    let start_upload = Instant::now();
    // Use ByteStream::from_path for efficient streaming without loading into memory
    let stream = ByteStream::from_path(&archive_zst_path).await?;

    let mut put_object_request = client
        .put_object()
        .bucket(&target_bucket)
        .key(&final_key)
        .body(stream);
    if let Some(key_id) = kms_key_id {
        put_object_request = put_object_request
            .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
            .ssekms_key_id(key_id);
    }

    put_object_request.send().await?;
    let upload_duration = start_upload.elapsed();
    info!("Uploaded to S3 in {:.2?}", upload_duration);

    // Step 8: Delete original files from source bucket if requested
    let mut delete_duration = None;
    if delete_source_files {
        let start_delete = Instant::now();
        if !files_to_process.is_empty() {
            // Group files by bucket
            let mut files_by_bucket: HashMap<String, Vec<String>> =
                HashMap::new();
            for (bucket, key) in files_to_process {
                files_by_bucket.entry(bucket).or_default().push(key);
            }

            let delete_futs = files_by_bucket.into_iter().map(|(bucket, keys)| {
                let client = client.clone();
                async move {
                    let chunks = keys.chunks(1000); // S3 delete_objects has a limit of 1000 keys per request
                    let delete_futs_for_bucket = chunks.map(|chunk| {
                        let client = client.clone();
                        let bucket = bucket.clone();
                        let chunk_keys: Vec<String> = chunk.iter().map(|s| s.to_string()).collect();

                        async move {
                            let object_identifiers = chunk_keys
                                .into_iter()
                                .map(|key| {
                                    aws_sdk_s3::types::ObjectIdentifier::builder()
                                        .key(key)
                                        .build()
                                        .unwrap()
                                })
                                .collect::<Vec<_>>();

                            let delete_request = aws_sdk_s3::types::Delete::builder()
                                .set_objects(Some(object_identifiers))
                                .build()?;

                            client
                                .delete_objects()
                                .bucket(&bucket)
                                .delete(delete_request)
                                .send()
                                .await
                        }
                    });
                    // Wait for all delete futures for this bucket
                    futures::future::join_all(delete_futs_for_bucket).await
                }
            });

            let results_per_bucket = futures::future::join_all(delete_futs).await;
            let mut total_success_count = 0;
            for bucket_results in results_per_bucket {
                for result in bucket_results {
                    match result {
                        Ok(output) => {
                            let deleted_count = output.deleted().len();
                            total_success_count += deleted_count;
                            info!("Successfully deleted {} objects", deleted_count);
                        }
                        Err(e) => {
                            error!("Failed to delete a batch of objects: {}", e);
                        }
                    }
                }
            }
            let elapsed = start_delete.elapsed();
            delete_duration = Some(elapsed);
            info!(
                "Deleted {} original files in {:.2?}",
                total_success_count, elapsed
            );
        }
    } else {
        info!("Skipping deletion of source files as 'delete_source_files' is false.");
    }

    let total_runtime = operation_start.elapsed();
    let total_runtime_secs = total_runtime.as_secs_f64();

    let mut time_metrics = serde_json::json!({
        "total_runtime": format!("{:.4}s", total_runtime_secs),
        "list_files_time": format!("{:.4}s", list_files_duration.as_secs_f64()),
        "download_and_zip_time": format!("{:.4}s", zip_duration.as_secs_f64()),
        "zstd_compress_time": format!("{:.4}s", compress_duration.as_secs_f64()),
        "upload_time": format!("{:.4}s", upload_duration.as_secs_f64()),
    });

    if let Some(d) = delete_duration {
        time_metrics["delete_time"] = serde_json::json!(format!("{:.4}s", d.as_secs_f64()));
    }

    let data_metrics = serde_json::json!({
        "files_processed": files_processed,
        "s3_metadata_size_bytes": s3_metadata_size,
        "downloaded_size_bytes": downloaded_size,
        "compressed_size_bytes": compressed_size,
        "compression_ratio": if downloaded_size > 0 {
            format!("{:.4}", compressed_size as f64 / downloaded_size as f64)
        } else {
            "N/A".to_string()
        }
    });

    Ok(serde_json::json!({
        "status": "ok",
        "output_key": final_key,
        "metrics": {
            "time": time_metrics,
            "data": data_metrics
        }
    }))
}

// Helper to check accessibility of all buckets in the manifest
async fn check_bucket_accessibility(client: &Client, inputs: &[String]) -> Result<(), Error> {
    let mut unique_buckets = HashSet::new();
    for s3_url in inputs {
        let (bucket, _) = parse_s3_url(s3_url)?;
        unique_buckets.insert(bucket);
    }

    let check_futs = unique_buckets.into_iter().map(|bucket| {
        let client = client.clone();
        async move {
            match client.head_bucket().bucket(&bucket).send().await {
                Ok(_) => Ok(None),
                Err(_e) => Ok(Some(bucket)),
            }
        }
    });

    let results: Vec<Result<Option<String>, Error>> = futures::future::join_all(check_futs).await;

    let inaccessible_buckets: BTreeSet<String> = results
        .into_iter()
        .filter_map(|res| res.ok().flatten())
        .collect();

    if !inaccessible_buckets.is_empty() {
        let bucket_list: Vec<String> = inaccessible_buckets.into_iter().collect();
        let error_message = format!(
            "Access denied for the following buckets: {}",
            bucket_list.join(", ")
        );
        return Err(error_message.into());
    }

    Ok(())
}

// Decompress archive and extract a specific file, returning it as base64
async fn handle_decompression(client: &Client, event: &Value) -> Result<Value, Error> {
    let operation_start = Instant::now();
    let source_s3_url = event["source_s3_url"]
        .as_str()
        .ok_or("source_s3_url not specified")?
        .to_string();
    let file_to_extract = event["file_to_extract"]
        .as_str()
        .ok_or("file_to_extract not specified")?
        .to_string();

    let (source_bucket, source_key) = parse_s3_url(&source_s3_url)?;

    info!(
        source_s3_url,
        file_to_extract, "Decompression Configuration"
    );

    // Step 1: Download compressed archive from S3
    let start_download = Instant::now();
    let tmp_dir = tempfile::tempdir()?;
    let compressed_data = download_compressed_archive(client, &source_bucket, &source_key).await?;
    let download_duration = start_download.elapsed();
    info!("Downloaded {} bytes in {:.2?}", compressed_data.len(), download_duration);

    // Step 2: Decompress zstd archive
    let start_decompress = Instant::now();
    let zip_path = decompress_zstd_to_temp(&tmp_dir, &compressed_data).await?;
    let decompress_duration = start_decompress.elapsed();
    info!("Decompressed archive in {:.2?}", decompress_duration);

    // Step 3: Extract specific file from zip
    let start_extract = Instant::now();
    let file_content = extract_file_from_zip(&zip_path, &file_to_extract).await?;
    let extract_duration = start_extract.elapsed();
    info!(
        "Extracted '{}' ({} bytes) in {:.2?}",
        file_to_extract,
        file_content.len(),
        extract_duration
    );

    // Step 3: Encode file content as base64 for JSON response
    let encoded_content = STANDARD.encode(&file_content);

    let total_runtime = operation_start.elapsed();
    let total_runtime_secs = total_runtime.as_secs_f64();

    Ok(serde_json::json!({
        "status": "ok",
        "file_name": file_to_extract,
        "file_content_base64": encoded_content,
        "metrics": {
            "total_runtime": format!("{:.4}s", total_runtime_secs),
            "download_time": format!("{:.4}s", download_duration.as_secs_f64()),
            "decompress_time": format!("{:.4}s", decompress_duration.as_secs_f64()),
            "extract_time": format!("{:.4}s", extract_duration.as_secs_f64())
        }
    }))
}

// Helper to download and parse a manifest file from S3
async fn download_and_parse_manifest(
    client: &Client,
    manifest_url: &str,
) -> Result<Vec<String>, Error> {
    let (bucket, key) = parse_s3_url(manifest_url)?;
    let object_result = client.get_object().bucket(&bucket).key(&key).send().await;

    let mut object = match object_result {
        Ok(o) => o,
        Err(e) => {
            let service_error = e.into_service_error();
            if service_error.is_no_such_key() {
                return Err(format!("Manifest file not found at {}", manifest_url).into());
            }
            return Err(format!(
                "Failed to download manifest from {}: {}",
                manifest_url, service_error
            )
            .into());
        }
    };

    let mut content = Vec::new();
    while let Some(bytes) = object.body.next().await {
        content.extend_from_slice(&bytes?);
    }

    let content_str = String::from_utf8(content)?;
    let paths = content_str
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    Ok(paths)
}

// Helper to parse S3 URLs like "s3://bucket-name/key/path"
fn parse_s3_url(s3_url: &str) -> Result<(String, String), Error> {
    let stripped_url = s3_url
        .strip_prefix("s3://")
        .ok_or("Invalid S3 URL format")?;
    let mut parts = stripped_url.splitn(2, '/');
    let bucket = parts.next().ok_or("Missing bucket in S3 URL")?.to_string();
    let key = parts.next().unwrap_or("").to_string();
    Ok((bucket, key))
}

// List all S3 objects from a list of S3 URLs (files or prefixes)
async fn list_files_from_inputs(
    client: &Client,
    inputs: &[String],
) -> Result<(Vec<(String, String)>, u64), Error> {
    let mut files = Vec::new();
    let mut total_size: u64 = 0;

    for s3_url in inputs {
        let (bucket, prefix) = parse_s3_url(s3_url)?;

        if s3_url.ends_with('/') {
            // It's a prefix (directory)
            let mut stream = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&prefix)
                .into_paginator()
                .send();
            while let Some(result) = stream.next().await {
                for obj in result?.contents() {
                    if !obj.key().unwrap_or("").ends_with('/') {
                        // Exclude "directory" objects
                        let key = obj.key().unwrap().to_string();
                        total_size += obj.size().unwrap_or(0) as u64;
                        files.push((bucket.clone(), key));
                    }
                }
            }
        } else {
            // It's a single file
            let object = client
                .head_object()
                .bucket(&bucket)
                .key(&prefix)
                .send()
                .await?;
            total_size += object.content_length().unwrap_or(0) as u64;
            files.push((bucket, prefix));
        }
    }
    Ok((files, total_size))
}

// Download files from S3 and create a zip archive using temporary files
async fn download_and_create_zip(
    client: &Client,
    files_to_process: &[(String, String)],
    tmp_dir: &tempfile::TempDir,
    max_workers: usize,
) -> Result<u64, Error> {
    // Create zip file in the temporary directory
    let zip_path = tmp_dir.path().join("archive.zip");
    let zip_file = File::create(&zip_path)?;
    let zip_writer = Arc::new(Mutex::new(ZipWriter::new(zip_file)));
    let total_downloaded_size = Arc::new(Mutex::new(0u64));

    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Stored);

    stream::iter(files_to_process)
        .map(|(bucket, key)| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let zip_writer = Arc::clone(&zip_writer);
            let total_downloaded_size = Arc::clone(&total_downloaded_size);

            async move {
                let object_result = client.get_object().bucket(&bucket).key(&key).send().await?;
                let mut object = object_result;
                let mut content = Vec::new();
                while let Some(bytes) = object.body.next().await {
                    content.extend_from_slice(&bytes?);
                }

                let zip_path = Path::new(&bucket).join(&key).to_str().unwrap().to_string();

                let mut size_guard = total_downloaded_size.lock().unwrap();
                *size_guard += content.len() as u64;
                let mut writer = zip_writer.lock().unwrap();
                writer.start_file(zip_path, options)?;
                writer.write_all(&content)?;

                Ok::<(), Error>(())
            }
        })
        .buffer_unordered(max_workers)
        .for_each(|result| async {
            if let Err(e) = result {
                error!("Failed to download and zip file: {}", e);
            }
        })
        .await;

    // Finish the zip file
    let final_zip_writer = Arc::try_unwrap(zip_writer)
        .expect("Arc unwrap failed")
        .into_inner()
        .unwrap();
    final_zip_writer.finish()?;

    let final_size = *total_downloaded_size.lock().unwrap();
    Ok(final_size)
}

// Compress zip file to zstd format using streaming compression
async fn compress_zip_to_zstd(
    tmp_dir: &tempfile::TempDir,
    compression_level: i32,
) -> Result<std::path::PathBuf, Error> {
    let zip_path = tmp_dir.path().join("archive.zip");
    let zst_path = tmp_dir.path().join("archive.zip.zst");

    // Open the zip file for reading
    let zip_file = File::open(&zip_path)?;
    let compressed_file = File::create(&zst_path)?;

    // Stream compress the zip file to zstd
    let mut encoder = zstd::stream::Encoder::new(compressed_file, compression_level)?;
    std::io::copy(&mut std::io::BufReader::new(zip_file), &mut encoder)?;
    encoder.finish()?;

    // Zip will be automatically deleted after this function ends
    Ok(zst_path)
}

// Download compressed archive from S3
async fn download_compressed_archive(
    client: &Client,
    bucket: &str,
    key: &str,
) -> Result<Vec<u8>, Error> {
    let object_result = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await;

    let mut object = match object_result {
        Ok(o) => o,
        Err(e) => {
            let service_error = e.into_service_error();
            if service_error.is_no_such_key() {
                return Err(format!("Source archive not found at s3://{}/{}", bucket, key).into());
            }
            return Err(format!(
                "Failed to download archive from s3://{}/{}: {}",
                bucket, key, service_error
            )
            .into());
        }
    };

    let mut compressed_data = Vec::new();
    while let Some(bytes) = object.body.next().await {
        let chunk = bytes?;
        compressed_data.extend_from_slice(&chunk);
    }

    Ok(compressed_data)
}

// Decompress zstd data to a temporary zip file
async fn decompress_zstd_to_temp(
    tmp_dir: &tempfile::TempDir,
    compressed_data: &[u8],
) -> Result<std::path::PathBuf, Error> {
    let zip_path = tmp_dir.path().join("decompressed.zip");
    
    // Decompress zstd data and write to temporary file
    let decompressed_data = zstd::decode_all(compressed_data)?;
    let mut zip_file = File::create(&zip_path)?;
    zip_file.write_all(&decompressed_data)?;
    zip_file.flush()?;
    
    info!("Decompressed {} bytes to temporary file", decompressed_data.len());
    Ok(zip_path)
}

// Extract a specific file from a zip archive
async fn extract_file_from_zip(
    zip_path: &std::path::Path,
    file_to_extract: &str,
) -> Result<Vec<u8>, Error> {
    let zip_file = File::open(zip_path)?;
    let mut archive = ZipArchive::new(zip_file)?;
    let mut file = archive.by_name(file_to_extract)?;
    let mut file_content = Vec::new();
    file.read_to_end(&mut file_content)?;
    
    Ok(file_content)
}
