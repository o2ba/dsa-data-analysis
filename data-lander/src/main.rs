use aws_sdk_s3::Client;
use dotenvy::dotenv;
use log::{info, warn};

mod converter_uploader;
mod downloader;
mod platform_collector;
mod unzipper;
mod utils;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logger
    env_logger::init();
    
    // Load envs
    dotenv().ok();
    
    println!("Starting data-lander application...");

    // Debug environment variables
    println!("Environment variables:");
    for (key, value) in std::env::vars() {
        if key.starts_with("AWS_") || key.starts_with("S3_") || key == "URL" {
            if key.contains("SECRET") || key.contains("KEY") {
                println!("  {}: [REDACTED]", key);
            } else {
                println!("  {}: {}", key, value);
            }
        }
    }

    // Get the URL
    let url = match std::env::var("URL") {
        Ok(url) => {
            println!("URL found: {}", url);
            url
        }
        Err(e) => {
            eprintln!("URL environment variable not set: {}", e);
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };

    // Initialize S3 config
    println!("Initializing S3 config...");
    let s3_config = utils::get_s3_config().await;
    let s3_client = Client::from_conf(s3_config);

    let s3_bucket: String = match std::env::var("S3_BUCKET_NAME") {
        Ok(bucket) => {
            println!("S3 bucket found: {}", bucket);
            bucket
        }
        Err(e) => {
            eprintln!("S3_BUCKET_NAME environment variable not set: {}", e);
            return Err(Box::new(e) as Box<dyn std::error::Error>);
        }
    };

    // Create a prefix based on the URL
    let temp_file = downloader::download_zip_to_temp(&url).await?;

    info!(
        "File size: {:.2} MB downloaded to {:?}",
        utils::get_file_size(&temp_file).unwrap_or(u64::MIN),
        &temp_file.path()
    );

    // Unzip and stream into a temporary directory
    let extract_dir = tempfile::tempdir()?;
    let extracted_files = unzipper::streamed_unzip(&temp_file, extract_dir.path())?;

    // Process each extracted file
    for file_path in &extracted_files {
        info!("Processing: {:?}", file_path);

        if file_path.extension().and_then(|s| s.to_str()) == Some("csv") {
            // This function will filter the CSV by ALLOWED_VLOPS and upload it to S3 as Parquet
            // Handles -> Conversion, Filtering, and Uploading
            // SRP nightmare, but it works
            converter_uploader::convert_filter_and_upload_direct(
                file_path,
                &s3_client,
                &s3_bucket,
                utils::get_s3_prefix(&url)?.as_str(),
            )
            .await
            .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })?;
        } else {
            warn!("Non non-CSV file found, skipping...: {:?}", file_path);
        }
    }

    println!("✅ Data processing completed successfully!");
    println!("Processed {} files total", extracted_files.len());
    
    // Check if this should be a one-time job or keep running
    if std::env::var("KEEP_ALIVE").unwrap_or_default() == "true" {
        println!("KEEP_ALIVE=true, entering sleep mode...");
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(3600)).await; // Sleep 1 hour
            println!("Still alive... (sleeping)");
        }
    } else {
        println!("Job completed, exiting normally.");
    }

    Ok(())
}
