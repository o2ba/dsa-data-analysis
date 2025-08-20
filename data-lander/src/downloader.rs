use reqwest;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tracing::{info, error, instrument};

#[instrument(skip_all, fields(url = %url))]
pub async fn download_zip_to_temp(
    url: &str,
) -> Result<NamedTempFile, Box<dyn std::error::Error>> {
    info!("Starting ZIP download");
    
    let temp_file = NamedTempFile::with_suffix(".zip")?;
    let response = create_client().get(url).send().await?;
    
    validate_response(&response)?;
    stream_to_file(response, &temp_file).await?;
    
    info!(temp_path = ?temp_file.path(), "Download completed");
    Ok(temp_file)
}

fn create_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
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
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    
    use futures_util::StreamExt;
    
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        file.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        
        if should_log_progress(downloaded) {
            info!(downloaded_bytes = downloaded, "Download progress");
        }
    }
    
    file.flush().await?;
    Ok(())
}

fn should_log_progress(downloaded: u64) -> bool {
    downloaded % (50 * 1024 * 1024) == 0 // Log every 50MB
}