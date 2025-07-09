use aws_config::BehaviorVersion;
use aws_sdk_s3::{Client,primitives::ByteStream};
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::Value;
use std::env;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;
use tempfile::tempdir;
use walkdir::WalkDir;
use tracing::{info, Level};
use zip::write::{FileOptions, ZipWriter};
use zstd::stream::encode_all;

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .json()
        .init();

    let func = service_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();

    let source_bucket = event["source_bucket"].as_str().or(env::var("DEFAULT_SOURCE_BUCKET").ok().as_deref()).unwrap_or_default().to_string();
    let target_bucket = event["target_bucket"].as_str().or(env::var("DEFAULT_TARGET_BUCKET").ok().as_deref()).unwrap_or_default().to_string();
    let source_prefix = event["source_prefix"].as_str().or(env::var("DEFAULT_SOURCE_PREFIX").ok().as_deref()).unwrap_or("logs/").to_string();
    let target_prefix = event["target_prefix"].as_str().or(env::var("DEFAULT_TARGET_PREFIX").ok().as_deref()).unwrap_or("compressed/").to_string();
    let max_workers: usize = env::var("MAX_WORKERS").unwrap_or_else(|_| "1024".to_string()).parse()?;

    info!(source_bucket, target_bucket, source_prefix, target_prefix, "Configuration");

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    let tmp_dir = tempdir()?;
    let log_dir = tmp_dir.path().join("logs");
    fs::create_dir_all(&log_dir)?;

    // List files
    let (keys, total_original_size) = list_keys(&client, &source_bucket, &source_prefix).await?;
    info!(
        "Listed {} keys with a total size of {} bytes",
        keys.len(),
        total_original_size
    );

    // Download files
    let start_dl = Instant::now();
    download_files(&client, &source_bucket, &keys, &log_dir, &source_prefix, max_workers).await?;
    info!("Downloaded all files in {:.2?}", start_dl.elapsed());

    let total_original_size = WalkDir::new(&log_dir)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum::<u64>();
    info!(total_original_size_bytes = total_original_size, "Calculated total size of original files");

    // Create zip
    let start_zip = Instant::now();
    let archive_zip_path = tmp_dir.path().join("archive.zip");
    zip_directory(&log_dir, &archive_zip_path, zip::CompressionMethod::Stored)?;
    info!("Created .zip archive in {:.2?}", start_zip.elapsed());

    // Compress with zstd
    let start_zst = Instant::now();
    let archive_zst_path = tmp_dir.path().join("archive.zip.zst");
    let mut zip_file = File::open(&archive_zip_path)?;
    let mut zip_data = Vec::new();
    zip_file.read_to_end(&mut zip_data)?;
    let compressed_data = encode_all(&zip_data[..], 3)?;
    let mut zst_file = File::create(&archive_zst_path)?;
    zst_file.write_all(&compressed_data)?;
    info!("Compressed with zstd in {:.2?}", start_zst.elapsed());

    let compressed_size = fs::metadata(&archive_zst_path)?.len();
    info!(compressed_size_bytes = compressed_size, "Calculated final compressed size");

    if total_original_size > 0 {
        let ratio = compressed_size as f64 / total_original_size as f64;
        info!(compression_ratio = format!("{:.4}", ratio), "Calculated compression ratio (compressed/original)");
    }

    // Upload to S3
    let start_upload = Instant::now();
    let final_key = format!("{}archive.zip.zst", target_prefix);
    let stream = ByteStream::from_path(&archive_zst_path).await?;
    client.put_object().bucket(&target_bucket).key(&final_key).body(stream).send().await?;
    info!("Uploaded to S3 in {:.2?}", start_upload.elapsed());

    Ok(serde_json::json!({ "status": "ok", "output_key": final_key }))
}

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
