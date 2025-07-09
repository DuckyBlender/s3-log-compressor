use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use futures::stream::{self, StreamExt};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::env;
use std::time::{Instant};
use tokio::io::AsyncWriteExt;

#[derive(Deserialize, Debug)]
struct Request {
    source_bucket: Option<String>,
    source_prefix: Option<String>,
    max_files: Option<usize>,
}

#[derive(Serialize, Debug)]
struct Response {
    results: Vec<BenchmarkResult>,
    optimal_workers: Option<usize>,
    files_tested: usize,
    total_kb: f64,
}

#[derive(Serialize, Debug, Clone)]
struct BenchmarkResult {
    workers: usize,
    duration: f64,
    total_bytes: u64,
    files_count: usize,
    kbps: f64,
}

async fn list_keys(client: &Client, bucket: &str, prefix: &str) -> Result<Vec<(String, i64)>, Error> {
    let mut keys = Vec::new();
    let mut stream = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .into_paginator()
        .send();

    while let Some(result) = stream.next().await {
        match result {
            Ok(output) => {
                for object in output.contents() {
                    keys.push((object.key().unwrap_or("").to_string(), object.size().unwrap_or(0)));
                }
            }
            Err(e) => {
                tracing::error!("Error listing objects: {:?}", e);
                return Err(Box::new(e));
            }
        }
    }
    Ok(keys)
}

async fn download_file(client: &Client, bucket: &str, key: &str) -> Result<u64, Error> {
    let mut object = client.get_object().bucket(bucket).key(key).send().await?;
    let mut sink = tokio::io::sink();
    let mut bytes_downloaded = 0;
    while let Some(bytes) = object.body.next().await {
        let bytes = bytes?;
        bytes_downloaded += bytes.len() as u64;
        sink.write_all(&bytes).await?;
    }
    Ok(bytes_downloaded)
}

async fn benchmark_downloads(
    client: &Client,
    source_bucket: &str,
    keys: &[(String, i64)],
    max_workers: usize,
) -> Result<BenchmarkResult, Error> {
    let start_time = Instant::now();

    let bodies = stream::iter(keys)
        .map(|(key, _)| {
            let client = client.clone();
            let source_bucket = source_bucket.to_string();
            let key = key.clone();
            tokio::spawn(async move {
                download_file(&client, &source_bucket, &key).await
            })
        })
        .buffer_unordered(max_workers);

    let downloaded_sizes: Vec<Result<u64, Error>> = bodies
        .map(|res| match res {
            Ok(Ok(size)) => Ok(size),
            Ok(Err(e)) => Err(e),
            Err(e) => Ok(Err(Box::new(e))?),
        })
        .collect()
        .await;

    let end_time = Instant::now();
    let duration = end_time.duration_since(start_time);

    let total_bytes = downloaded_sizes.iter().filter_map(|r| r.as_ref().ok()).sum();

    Ok(BenchmarkResult {
        workers: max_workers,
        duration: duration.as_secs_f64(),
        total_bytes,
        files_count: keys.len(),
        kbps: (total_bytes as f64 / 1024.0) / duration.as_secs_f64(),
    })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .json()
        .init();

    let func = service_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event, _context) = event.into_parts();
    let request: Request = serde_json::from_value(event)?;

    let source_bucket = request.source_bucket.or_else(|| env::var("DEFAULT_SOURCE_BUCKET").ok()).unwrap();
    let source_prefix = request.source_prefix.or_else(|| env::var("DEFAULT_SOURCE_PREFIX").ok()).unwrap_or_else(|| "logs/".to_string());
    let max_files = request.max_files.unwrap_or(1000);

    tracing::info!(
        "BENCHMARK_START,bucket={},prefix={},max_files={}",
        source_bucket,
        source_prefix,
        max_files
    );

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let client = Client::new(&config);

    tracing::info!("PHASE,listing_files");
    let start_list = Instant::now();
    let mut key_size_pairs = list_keys(&client, &source_bucket, &source_prefix).await?;
    key_size_pairs.truncate(max_files);
    let end_list = Instant::now();

    let total_bytes: i64 = key_size_pairs.iter().map(|(_, size)| size).sum();
    tracing::info!(
        "LISTING_COMPLETE,files={},total_kb={:.2},duration={:.2}",
        key_size_pairs.len(),
        total_bytes as f64 / 1024.0,
        end_list.duration_since(start_list).as_secs_f64()
    );

    let worker_counts = vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024];
    let mut results = Vec::new();

    for &workers in &worker_counts {
        if workers > key_size_pairs.len() {
            tracing::info!("SKIP,workers={},reason=more_workers_than_files", workers);
            continue;
        }

        tracing::info!("PHASE,downloading,workers={}", workers);
        match benchmark_downloads(&client, &source_bucket, &key_size_pairs, workers).await {
            Ok(result) => {
                tracing::info!(
                    "RESULT,{},{:.3},{:.2},{},{}",
                    workers,
                    result.duration,
                    result.kbps,
                    result.files_count,
                    result.total_bytes
                );
                results.push(result);
            }
            Err(e) => {
                tracing::error!("ERROR,workers={},error={:?}", workers, e);
            }
        }
    }

    let best_result = results.iter().min_by(|a, b| a.duration.partial_cmp(&b.duration).unwrap());

    if let Some(best) = best_result {
        tracing::info!(
            "OPTIMAL,workers={},duration={:.3},kbps={:.2}",
            best.workers,
            best.duration,
            best.kbps
        );
    }

    tracing::info!("BENCHMARK_COMPLETE");

    let response = Response {
        results: results.clone(),
        optimal_workers: best_result.map(|b| b.workers),
        files_tested: key_size_pairs.len(),
        total_kb: total_bytes as f64 / 1024.0,
    };

    Ok(serde_json::to_value(response)?)
}
