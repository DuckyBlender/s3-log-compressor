use aws_config::BehaviorVersion;
use aws_sdk_s3::types::ServerSideEncryption;
use aws_sdk_s3::{primitives::ByteStream, Client};
use base64::{engine::general_purpose, Engine as _};
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::env;
use std::fs::File as StdFile;
use std::io::{BufWriter, Read, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;
use tokio::fs;
use tracing::{error, info, Level};
use zip::write::{FileOptions, ZipWriter};

const COMPRESSION_METHOD: zip::CompressionMethod = zip::CompressionMethod::Zstd;

#[tokio::main]
async fn main() -> Result<(), Error> {
    // Initialize structured logging for CloudWatch
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let func = service_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

// Main Lambda handler
async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    // Create a fresh config and client for each invocation to avoid connection pool issues
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let operation = event["operation"].as_str().unwrap_or("compress");

    let result = match operation {
        "compress" => handle_compress_operation(&client, &event).await,
        "decompress" => handle_decompress_operation(&client, &event).await,
        _ => Err(Error::from("Invalid operation specified")),
    };

    // Explicitly drop the client to release connection pool
    drop(client);

    // Force a yield to allow cleanup
    tokio::task::yield_now().await;

    // Convert any errors to JSON responses for easier debugging
    match result {
        Ok(value) => Ok(value),
        Err(e) => {
            error!("Error processing event: {}", e);
            Ok(serde_json::json!({
                "statusCode": 500,
                "body": serde_json::json!({
                    "error": e.to_string()
                })
            }))
        }
    }
}

async fn handle_decompress_operation(client: &Client, event: &Value) -> Result<Value, Error> {
    let source_s3_url = event["source_s3_url"]
        .as_str()
        .ok_or_else(|| Error::from("Missing source_s3_url"))?;
    let file_to_extract = event["file_to_extract"]
        .as_str()
        .ok_or_else(|| Error::from("Missing file_to_extract"))?;

    let (bucket, key) = parse_s3_url(source_s3_url)?;

    info!(
        "Starting decompression for file {} from archive {}",
        file_to_extract, source_s3_url
    );

    // Check storage class to prevent trying to access from deep archive
    let head_object_output = client
        .head_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await?;
    if let Some(storage_class) = head_object_output.storage_class {
        if storage_class.as_str() == "DEEP_ARCHIVE" {
            return Err(Error::from(
                "Object is in Deep Archive. Restore it before decompressing.",
            ));
        }
    }

    let object = client.get_object().bucket(bucket).key(key).send().await?;
    let body_bytes = object.body.collect().await?.into_bytes();
    let reader = std::io::Cursor::new(body_bytes);

    let mut archive = zip::ZipArchive::new(reader)?;

    let mut file = archive.by_name(file_to_extract)?;
    let mut contents = Vec::new();
    file.read_to_end(&mut contents)?;

    let encoded_contents = general_purpose::STANDARD.encode(&contents);

    Ok(serde_json::json!({
        "file_content_base64": encoded_contents
    }))
}

// Create a zip archive from multiple files from S3
async fn handle_compress_operation(client: &Client, event: &Value) -> Result<Value, Error> {
    let operation_start = Instant::now();
    // Extract configuration from event
    let input_s3_manifest_url = event["input_s3_manifest_url"]
        .as_str()
        .ok_or_else(|| Error::from("Missing input_s3_manifest_url"))?;

    let output_s3_url = event["output_s3_url"]
        .as_str()
        .ok_or_else(|| Error::from("Missing output_s3_url"))?;

    let delete_source_files = event["delete_source_files"].as_bool().unwrap_or(false);
    let include_s3_name = event["include_s3_name"].as_bool().unwrap_or(true);
    let max_workers: usize = event["max_workers"]
        .as_u64()
        .unwrap_or(256)
        .try_into()
        .unwrap_or(256);

    let (target_bucket, final_key) = parse_s3_url(output_s3_url)?;

    let kms_key_id = env::var("KMS_KEY_ID").ok();

    info!(
        "Starting zip creation from manifest: {} to {}",
        input_s3_manifest_url, output_s3_url
    );

    // Create temporary directory for processing
    let tmp_dir = tempdir()?;

    // Step 1: Download and parse the manifest file to get the list of S3 inputs
    let inputs = download_and_parse_manifest(client, input_s3_manifest_url).await?;
    info!("Parsed {} inputs from manifest file", inputs.len());

    // Step 2: Check bucket accessibility before proceeding
    check_bucket_accessibility(client, &inputs).await?;
    info!("All source buckets are accessible.");

    // Step 3: List all files to zip from the S3 inputs
    let start_list = Instant::now();
    let files_to_process = list_files_from_inputs(client, &inputs).await?;
    let list_files_duration = start_list.elapsed();
    let files_processed = files_to_process.len();
    info!(
        "Listed {} files to process in {:.2?}.",
        files_processed, list_files_duration,
    );

    if files_to_process.is_empty() {
        return Err(Error::from("No files to process."));
    }

    // Step 4: Download files and create zip archive
    let start_zip = Instant::now();
    let (downloaded_size, successful_downloads, failed_downloads) = download_and_create_zip(
        client,
        &files_to_process,
        &tmp_dir,
        max_workers,
        include_s3_name,
    )
    .await?;
    let zip_duration = start_zip.elapsed();
    info!("Downloaded and zipped all files in {:.2?}", zip_duration);

    info!(
        "Download stats: {} successful, {} failed.",
        successful_downloads, failed_downloads
    );

    if failed_downloads > 0 {
        return Err(Error::from(format!(
            "Failed to download {failed_downloads} files. Aborting."
        )));
    }

    // Step 5: Get zip file path and calculate metrics
    let archive_path = tmp_dir.path().join("archive.zip");

    // Calculate zip metrics
    let zipped_size = fs::metadata(&archive_path).await?.len();
    info!(
        "Zip archive created at: {}. Size: {} bytes",
        archive_path.display(),
        zipped_size
    );

    let mut compression_ratio = None;
    if downloaded_size > 0 {
        let ratio = zipped_size as f64 / downloaded_size as f64;
        compression_ratio = Some(ratio);
        info!("Compression ratio: {:.4}", ratio);
    }

    // Step 6: Upload zip archive to S3
    let start_upload = Instant::now();
    // Use ByteStream::from_path for efficient streaming without loading into memory
    let stream = ByteStream::from_path(&archive_path).await?;

    let mut put_object_request = client
        .put_object()
        .bucket(&target_bucket)
        .key(&final_key)
        .body(stream);
    if let Some(key_id) = kms_key_id {
        put_object_request = put_object_request
            .server_side_encryption(ServerSideEncryption::AwsKms)
            .ssekms_key_id(key_id);
    }
    put_object_request.send().await?;
    let upload_duration = start_upload.elapsed();
    info!("Uploaded to S3 in {:.2?}", upload_duration);

    // Step 7: Delete original files from source bucket if requested
    if delete_source_files {
        info!("Starting asynchronous deletion of source files.");
        let client_clone = client.clone();
        let files_to_delete = files_to_process.clone();
        tokio::spawn(async move {
            match delete_files_in_batches(&client_clone, &files_to_delete).await {
                Ok((success, fail)) => info!(
                    "Async deletion completed: {} successful, {} failed.",
                    success, fail
                ),
                Err(e) => error!("Async deletion failed: {}", e),
            }
        });
    } else {
        info!("Skipping deletion of source files.");
    }

    let total_runtime = operation_start.elapsed();
    let total_runtime_secs = total_runtime.as_secs_f64();

    let time_metrics = serde_json::json!({
        "total_runtime": format!("{:.4}s", total_runtime_secs),
        "list_files_time": format!("{:.4}s", list_files_duration.as_secs_f64()),
        "zip_creation_time": format!("{:.4}s", zip_duration.as_secs_f64()),
        "upload_time": format!("{:.4}s", upload_duration.as_secs_f64()),
    });

    let mut data_metrics = serde_json::json!({
        "files_processed": files_processed,
        "successful_downloads": successful_downloads,
        "failed_downloads": failed_downloads,
        "total_uncompressed_bytes": downloaded_size,
        "total_compressed_bytes": zipped_size,
    });

    if let Some(ratio) = compression_ratio {
        data_metrics["compression_ratio"] = serde_json::json!(ratio);
    }

    Ok(serde_json::json!({
        "status": "success",
        "output_s3_url": output_s3_url,
        "metrics": {
            "time": time_metrics,
            "data": data_metrics,
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
                Err(_) => Ok(Some(bucket)),
            }
        }
    });

    let results: Vec<Result<Option<String>, Error>> = futures::future::join_all(check_futs).await;

    let inaccessible_buckets: BTreeSet<String> = results
        .into_iter()
        .filter_map(Result::ok)
        .flatten()
        .collect();

    if !inaccessible_buckets.is_empty() {
        let bucket_list = inaccessible_buckets
            .into_iter()
            .collect::<Vec<String>>()
            .join(", ");
        return Err(Error::from(format!(
            "The following buckets are not accessible: {bucket_list}"
        )));
    }

    Ok(())
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
        Err(e) => return Err(Error::from(format!("Failed to get manifest from S3: {e}"))),
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
) -> Result<Vec<(String, String)>, Error> {
    let mut files = Vec::new();

    for s3_url in inputs {
        let (bucket, key) = parse_s3_url(s3_url)?;
        if s3_url.ends_with('/') {
            // It's a directory, list objects
            let mut stream = client
                .list_objects_v2()
                .bucket(&bucket)
                .prefix(&key)
                .into_paginator()
                .send();

            while let Some(result) = stream.next().await {
                match result {
                    Ok(output) => {
                        for object in output.contents() {
                            if !object.key().unwrap_or("").ends_with('/') {
                                files.push((bucket.clone(), object.key().unwrap().to_string()));
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to list objects for prefix {}: {}", s3_url, e);
                        return Err(e.into());
                    }
                }
            }
        } else {
            // It's a file
            files.push((bucket, key));
        }
    }
    Ok(files)
}

// Download files from S3 and create a zip archive using temporary files
async fn download_and_create_zip(
    client: &Client,
    files_to_process: &[(String, String)],
    tmp_dir: &tempfile::TempDir,
    max_workers: usize,
    include_s3_name: bool,
) -> Result<(u64, usize, usize), Error> {
    let zip_path = tmp_dir.path().join("archive.zip");
    let zip_file = StdFile::create(&zip_path)?;
    let writer = Arc::new(Mutex::new(ZipWriter::new(BufWriter::new(zip_file))));

    let total_size = Arc::new(Mutex::new(0u64));
    let successful_downloads = Arc::new(Mutex::new(0usize));
    let failed_downloads = Arc::new(Mutex::new(0usize));

    let download_futs = files_to_process.iter().map(|(bucket, key)| {
        let client = client.clone();
        let writer = Arc::clone(&writer);
        let total_size = Arc::clone(&total_size);
        let successful_downloads = Arc::clone(&successful_downloads);
        let failed_downloads = Arc::clone(&failed_downloads);
        let bucket = bucket.clone();
        let key = key.clone();

        async move {
            let object_result = client.get_object().bucket(&bucket).key(&key).send().await;

            match object_result {
                Ok(mut object) => {
                    let mut byte_vec = Vec::new();
                    let mut downloaded_size = 0;
                    while let Some(bytes_result) = object.body.next().await {
                        match bytes_result {
                            Ok(bytes) => {
                                downloaded_size += bytes.len();
                                byte_vec.extend_from_slice(&bytes);
                            }
                            Err(e) => {
                                error!(
                                    "Failed to download chunk for s3://{}/{}: {}",
                                    bucket, key, e
                                );
                                *failed_downloads.lock().unwrap() += 1;
                                return Err(Error::from(e.to_string()));
                            }
                        }
                    }

                    let zip_path = if include_s3_name {
                        format!("{bucket}/{key}")
                    } else {
                        Path::new(&key)
                            .file_name()
                            .unwrap()
                            .to_str()
                            .unwrap()
                            .to_string()
                    };

                    let options =
                        FileOptions::<()>::default().compression_method(COMPRESSION_METHOD);
                    let write_result = tokio::task::spawn_blocking(move || {
                        let mut writer_guard = writer.lock().unwrap();
                        writer_guard.start_file(zip_path, options)?;
                        writer_guard.write_all(&byte_vec)?;
                        Ok::<(), std::io::Error>(())
                    })
                    .await;

                    match write_result {
                        Ok(Ok(_)) => {
                            *total_size.lock().unwrap() += downloaded_size as u64;
                            *successful_downloads.lock().unwrap() += 1;
                            Ok(())
                        }
                        Ok(Err(e)) => {
                            error!(
                                "Failed to write to zip file for s3://{}/{}: {}",
                                bucket, key, e
                            );
                            *failed_downloads.lock().unwrap() += 1;
                            Err(Error::from(e.to_string()))
                        }
                        Err(e) => {
                            error!(
                                "Failed to write to zip file task panicked for s3://{}/{}: {}",
                                bucket, key, e
                            );
                            *failed_downloads.lock().unwrap() += 1;
                            Err(Error::from(e.to_string()))
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get object s3://{}/{}: {}", bucket, key, e);
                    *failed_downloads.lock().unwrap() += 1;
                    Err(Error::from(e.to_string()))
                }
            }
        }
    });

    let stream = stream::iter(download_futs).buffer_unordered(max_workers);
    let results: Vec<_> = stream.collect().await;

    // Check for any hard errors from the download futures
    for result in results {
        if let Err(e) = result {
            // A failure in one download should fail the whole process if it's a critical error
            // The current logic inside the future just increments a counter.
            // To ensure we stop, we can check the failed_downloads count after.
            error!("A download task failed: {}", e);
        }
    }

    let final_writer = Arc::try_unwrap(writer)
        .expect("Writer lock should not be held")
        .into_inner()
        .unwrap();

    tokio::task::spawn_blocking(move || {
        final_writer.finish()?;
        Ok::<(), std::io::Error>(())
    })
    .await??;

    let total_size = *total_size.lock().unwrap();
    let successful_downloads = *successful_downloads.lock().unwrap();
    let failed_downloads = *failed_downloads.lock().unwrap();

    Ok((total_size, successful_downloads, failed_downloads))
}

// Helper function to delete files in batches
async fn delete_files_in_batches(
    client: &Client,
    files_to_process: &[(String, String)],
) -> Result<(usize, usize), Error> {
    let mut success_count = 0;
    let mut failed_count = 0;

    let mut files_by_bucket: HashMap<String, Vec<aws_sdk_s3::types::ObjectIdentifier>> =
        HashMap::new();
    for (bucket, key) in files_to_process {
        let identifier = aws_sdk_s3::types::ObjectIdentifier::builder()
            .key(key)
            .build()
            .map_err(|e| Error::from(format!("Failed to build object identifier: {e}")))?;
        files_by_bucket
            .entry(bucket.clone())
            .or_default()
            .push(identifier);
    }

    for (bucket, objects) in files_by_bucket {
        for chunk in objects.chunks(1000) {
            let delete_request = aws_sdk_s3::types::Delete::builder()
                .set_objects(Some(chunk.to_vec()))
                .quiet(true)
                .build()
                .map_err(|e| Error::from(format!("Failed to build delete request: {e}")))?;

            match client
                .delete_objects()
                .bucket(&bucket)
                .delete(delete_request)
                .send()
                .await
            {
                Ok(output) => {
                    let deleted_count = chunk.len();
                    if let Some(errors) = output.errors {
                        let failed_to_delete = errors.len();
                        failed_count += failed_to_delete;
                        success_count += deleted_count.saturating_sub(failed_to_delete);
                        for error in errors {
                            error!(
                                "Failed to delete object {} from bucket {}: {}",
                                error.key.unwrap_or_default(),
                                bucket,
                                error.message.unwrap_or_default()
                            );
                        }
                    } else {
                        success_count += deleted_count;
                    }
                }
                Err(e) => {
                    error!("Failed to delete objects from bucket {}: {}", bucket, e);
                    failed_count += chunk.len();
                }
            }
        }
    }

    Ok((success_count, failed_count))
}
