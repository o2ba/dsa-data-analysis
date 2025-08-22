use aws_sdk_s3::Client;
use dotenvy::dotenv;
use log::{info, warn};

mod converter_uploader;
mod downloader;
mod unzipper;
mod utils;

// Hardcoding them because we can't really change these after the fact
const ALLOWED_VLOPS: [&str; 11] = [
    "Facebook",
    "Discord Netherlands B.V.",
    "Google Maps",
    "Instagram",
    "Kleinanzeigen",
    "Leboncoin",
    "LinkedIn",
    "Reddit",
    "Telegram",
    "TikTok",
    "X",
];

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load envs
    dotenv().ok();

    // Get the URL
    let url = std::env::var("URL").expect("msg: URL environment variable not set");

    // Initialize S3 config
    let s3_client = Client::from_conf(utils::get_s3_config().await);

    let s3_bucket: String =
        std::env::var("S3_BUCKET_NAME").expect("msg: S3_BUCKET_NAME environment variable not set");

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

    Ok(())
}
