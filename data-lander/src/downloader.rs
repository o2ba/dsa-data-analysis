use reqwest;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use std::time::Duration;
use tracing::{info, error, instrument};

#[instrument(skip_all, fields(url = %url))]
pub async fn download_zip_to_temp(
    url: &str,
) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    info!("Starting ZIP download");
    
    // Puts a temp file in ephemeral storage on ECS Fargate
    // When temp_file goes out of scope, it will be deleted
    let temp_file = NamedTempFile::with_suffix(".zip")?;
    let response = create_client().get(url).send().await?;
    
    validate_response(&response)?;

    // Stream the response to a temporary file
    stream_to_file(response, &temp_file).await?;
    
    info!(
        temp_path = ?temp_file.path(), 
        size_bytes = temp_file.as_file().metadata()?.len(),
        "Download completed"
    );

    // Returns ownership of file
    Ok(temp_file)
}

fn create_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .expect("Failed to create HTTP client")
}

fn validate_response(
    response: &reqwest::Response
) -> Result<(), Box<dyn std::error::Error>> {
    if !response.status().is_success() {
        error!(status = %response.status(), "HTTP request failed");
        return Err(format!("HTTP error: {}", response.status()).into());
    }
    Ok(())
}


async fn stream_to_file(
    response: reqwest::Response,
    temp_file: &NamedTempFile,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut file = tokio::fs::File::from_std(temp_file.reopen()?);
    let bytes = response.bytes().await?;
    
    info!(size_mb = bytes.len() / (1024 * 1024), "Downloaded ZIP");
    
    file.write_all(&bytes).await?;
    Ok(())
}