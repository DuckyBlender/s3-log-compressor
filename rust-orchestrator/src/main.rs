use aws_config::BehaviorVersion;
use aws_sdk_lambda::Client as LambdaClient;
use aws_sdk_lambda::primitives::Blob;
use aws_sdk_s3::Client as S3Client;
use lambda_runtime::{Error, LambdaEvent, service_fn};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::env;
use tracing::{Level, info};

#[derive(Deserialize)]
struct OrchestratorEvent {
    inventory_s3_url: String,
    output_manifest_s3_prefix: String,
    date_to_process: String, // YYYY-MM-DD
}

#[derive(Serialize)]
struct CompressorEvent {
    operation: String,
    input_s3_manifest_url: String,
    output_s3_url: String,
    delete_source_files: bool,
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_target(false)
        .without_time()
        .init();

    let func = service_fn(handler);
    lambda_runtime::run(func).await?;
    Ok(())
}

async fn handler(event: LambdaEvent<Value>) -> Result<Value, Error> {
    let (event_value, _context) = event.into_parts();
    let event: OrchestratorEvent = serde_json::from_value(event_value)?;

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = S3Client::new(&config);
    let lambda_client = LambdaClient::new(&config);

    info!("Starting orchestration for date: {}", event.date_to_process);

    let (inventory_bucket, inventory_key) = parse_s3_url(&event.inventory_s3_url)?;

    let inventory_object = s3_client
        .get_object()
        .bucket(inventory_bucket)
        .key(inventory_key)
        .send()
        .await?;

    let body_bytes = inventory_object.body.collect().await?.into_bytes();
    let mut rdr = csv::Reader::from_reader(body_bytes.as_ref());

    let mut files_by_group: HashMap<(String, String), Vec<String>> = HashMap::new();

    for result in rdr.records() {
        let record = result?;
        let bucket = record.get(0).unwrap_or_default();
        let key = record.get(1).unwrap_or_default();
        let last_modified = record.get(3).unwrap_or_default();

        if last_modified.starts_with(&event.date_to_process) {
            if let Some((account, region)) = extract_account_region_from_key(key) {
                let s3_path = format!("s3://{bucket}/{key}");
                files_by_group
                    .entry((account, region))
                    .or_default()
                    .push(s3_path);
            }
        }
    }

    info!(
        "Found {} groups to process for date {}",
        files_by_group.len(),
        event.date_to_process
    );

    let (manifest_bucket, manifest_prefix) = parse_s3_url(&event.output_manifest_s3_prefix)?;

    for ((account, region), files) in files_by_group {
        let manifest_content = files.join("\n");
        let manifest_key = format!(
            "{}/{}/{}-{}-manifest.txt",
            manifest_prefix, event.date_to_process, account, region
        );

        s3_client
            .put_object()
            .bucket(&manifest_bucket)
            .key(&manifest_key)
            .body(manifest_content.into_bytes().into())
            .send()
            .await?;

        let manifest_url = format!("s3://{manifest_bucket}/{manifest_key}");
        let output_zip_url = format!(
            "s3://{}/compressed-logs/{}/{}-{}.zip",
            manifest_bucket, event.date_to_process, account, region
        );

        let compressor_payload = CompressorEvent {
            operation: "compress".to_string(),
            input_s3_manifest_url: manifest_url,
            output_s3_url: output_zip_url,
            delete_source_files: false,
        };

        let payload_str = serde_json::to_string(&compressor_payload)?;
        let payload_blob = Blob::new(payload_str.as_bytes());

        let compressor_arn = env::var("COMPRESSOR_FUNCTION_ARN")?;

        info!("Invoking compressor for group {}-{}", account, region);
        lambda_client
            .invoke()
            .function_name(compressor_arn)
            .payload(payload_blob)
            .invocation_type(aws_sdk_lambda::types::InvocationType::Event)
            .send()
            .await?;
    }

    Ok(serde_json::json!({ "status": "success" }))
}

fn parse_s3_url(s3_url: &str) -> Result<(String, String), Error> {
    let stripped_url = s3_url
        .strip_prefix("s3://")
        .ok_or("Invalid S3 URL format")?;
    let mut parts = stripped_url.splitn(2, '/');
    let bucket = parts.next().ok_or("Missing bucket in S3 URL")?.to_string();
    let key = parts.next().unwrap_or("").to_string();
    Ok((bucket, key))
}

fn extract_account_region_from_key(key: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = key.split('/').collect();
    if parts.len() > 3 && parts[0] == "AWSLogs" {
        let account = parts[1].to_string();
        let region = parts[3].to_string();
        return Some((account, region));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url_valid() {
        let url = "s3://my-bucket/my-key/is/here";
        let (bucket, key) = parse_s3_url(url).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "my-key/is/here");
    }

    #[test]
    fn test_parse_s3_url_no_key() {
        let url = "s3://my-bucket/";
        let (bucket, key) = parse_s3_url(url).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");
    }

    #[test]
    fn test_parse_s3_url_no_key_no_slash() {
        let url = "s3://my-bucket";
        let (bucket, key) = parse_s3_url(url).unwrap();
        assert_eq!(bucket, "my-bucket");
        assert_eq!(key, "");
    }

    #[test]
    fn test_parse_s3_url_invalid() {
        let url = "http://my-bucket/my-key";
        assert!(parse_s3_url(url).is_err());
    }

    #[test]
    fn test_extract_account_region_from_key_valid() {
        let key = "AWSLogs/123456789012/CloudTrail/us-east-1/2025/07/14/some_file.json.gz";
        let (account, region) = extract_account_region_from_key(key).unwrap();
        assert_eq!(account, "123456789012");
        assert_eq!(region, "us-east-1");
    }

    #[test]
    fn test_extract_account_region_from_key_invalid() {
        let key = "some/other/path";
        assert!(extract_account_region_from_key(key).is_none());
    }

    #[test]
    fn test_extract_account_region_from_key_short() {
        let key = "AWSLogs/12345";
        assert!(extract_account_region_from_key(key).is_none());
    }
}
