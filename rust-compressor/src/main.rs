use aws_config::BehaviorVersion;
use aws_sdk_s3::{primitives::ByteStream, Client};
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::collections::{BTreeSet, HashSet, HashMap};
use std::env;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tempfile::tempdir;
use tokio::fs::{self, File};
use tracing::{error, info, Level};
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

// Main Lambda handler
async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    // Create a fresh config and client for each invocation to avoid connection pool issues
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    // Call the zip creation handler
    let result = handle_zip_creation(&client, &event).await;

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

// Create a zip archive from multiple files from S3
async fn handle_zip_creation(client: &Client, event: &Value) -> Result<Value, Error> {
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
    
    let include_s3_name = event["include_s3_name"].as_bool().unwrap_or(true);

    let (target_bucket, final_key) = parse_s3_url(&output_s3_url)?;

    let max_workers: usize = env::var("MAX_WORKERS")
        .unwrap_or_else(|_| "256".to_string())
        .parse()?;

    let kms_key_id = env::var("KMS_KEY_ID").ok();

    info!(
        input_s3_manifest_url,
        output_s3_url, delete_source_files, include_s3_name, "Configuration"
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

    // Step 3: List all files to zip from the S3 inputs
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

    // Step 4: Download files and create zip archive
    let start_zip = Instant::now();
    let downloaded_size =
        download_and_create_zip(client, &files_to_process, &tmp_dir, max_workers, include_s3_name).await?;
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

    // Step 5: Get zip file path and calculate metrics
    let archive_path = tmp_dir.path().join("archive.zip");

    // Calculate zip metrics
    let zipped_size = fs::metadata(&archive_path).await?.len();
    info!(
        zipped_size_bytes = zipped_size,
        "Calculated final zipped size"
    );

    if downloaded_size > 0 {
        let ratio = zipped_size as f64 / downloaded_size as f64;
        info!(
            compression_ratio = format!("{:.4}", ratio),
            "Calculated compression ratio (zipped/downloaded)"
        );
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
            .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::AwsKms)
            .ssekms_key_id(key_id);
    }

    put_object_request.send().await?;
    let upload_duration = start_upload.elapsed();
    info!("Uploaded to S3 in {:.2?}", upload_duration);

    // Step 7: Delete original files from source bucket if requested
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
        "upload_time": format!("{:.4}s", upload_duration.as_secs_f64()),
    });

    if let Some(d) = delete_duration {
        time_metrics["delete_time"] = serde_json::json!(format!("{:.4}s", d.as_secs_f64()));
    }

    let data_metrics = serde_json::json!({
        "files_processed": files_processed,
        "s3_metadata_size_bytes": s3_metadata_size,
        "downloaded_size_bytes": downloaded_size,
        "zipped_size_bytes": zipped_size,
        "compression_ratio": if downloaded_size > 0 {
            format!("{:.4}", zipped_size as f64 / downloaded_size as f64)
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
                        
                        // Log progress every 50,000 files discovered
                        if files.len() % 50000 == 0 {
                            info!("Discovered {} files so far...", files.len());
                        }
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
    include_s3_name: bool,
) -> Result<u64, Error> {
    // Create zip file in the temporary directory
    let zip_path = tmp_dir.path().join("archive.zip");
    let zip_file = File::create(&zip_path).await?;
    let zip_writer = Arc::new(Mutex::new(ZipWriter::new(zip_file.into_std().await)));
    let total_downloaded_size = Arc::new(Mutex::new(0u64));
    let files_processed_count = Arc::new(Mutex::new(0u64));

    let options = FileOptions::<()>::default().compression_method(zip::CompressionMethod::Stored);
    let total_files = files_to_process.len();

    stream::iter(files_to_process)
        .map(|(bucket, key)| {
            let client = client.clone();
            let bucket = bucket.clone();
            let key = key.clone();
            let zip_writer = Arc::clone(&zip_writer);
            let total_downloaded_size = Arc::clone(&total_downloaded_size);
            let files_processed_count = Arc::clone(&files_processed_count);

            async move {
                let object_result = client.get_object().bucket(&bucket).key(&key).send().await?;
                let mut object = object_result;
                let mut content = Vec::new();
                while let Some(bytes) = object.body.next().await {
                    content.extend_from_slice(&bytes?);
                }

                let zip_path = if include_s3_name {
                    Path::new(&bucket).join(&key).to_str().unwrap().to_string()
                } else {
                    key.clone()
                };

                let mut size_guard = total_downloaded_size.lock().unwrap();
                *size_guard += content.len() as u64;
                let mut writer = zip_writer.lock().unwrap();
                writer.start_file(zip_path, options)?;
                writer.write_all(&content)?;

                let mut count_guard = files_processed_count.lock().unwrap();
                *count_guard += 1;
                if *count_guard % 10000 == 0 {
                    let percentage = (*count_guard as f64 / total_files as f64 * 100.0) as u32;
                    info!("Downloaded and zipped {}/{} ({}%) files...", *count_guard, total_files, percentage);
                }

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
