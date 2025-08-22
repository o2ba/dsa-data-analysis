use polars::prelude::*;
use std::path::Path;
use aws_sdk_s3::{primitives::ByteStream, Client};
use std::io::Cursor;
use log::{info, warn};
use std::fmt;
use aws_sdk_s3::error::SdkError;

/// Custom error type for CSV processing operations that implements Send + Sync
#[derive(Debug)]
pub enum CsvProcessingError {
    PolarsError(PolarsError),
    IoError(std::io::Error),
    InvalidPath(String),
    EmptyDataFrame(String),
    ParquetWriteError(String),
}

impl fmt::Display for CsvProcessingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CsvProcessingError::PolarsError(e) => write!(f, "Polars operation failed: {}", e),
            CsvProcessingError::IoError(e) => write!(f, "I/O operation failed: {}", e),
            CsvProcessingError::InvalidPath(path) => write!(f, "Invalid file path: {}", path),
            CsvProcessingError::EmptyDataFrame(msg) => write!(f, "Empty DataFrame: {}", msg),
            CsvProcessingError::ParquetWriteError(msg) => write!(f, "Parquet write failed: {}", msg),
        }
    }
}

impl std::error::Error for CsvProcessingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            CsvProcessingError::PolarsError(e) => Some(e),
            CsvProcessingError::IoError(e) => Some(e),
            _ => None,
        }
    }
}

// Ensure Send + Sync traits are implemented
unsafe impl Send for CsvProcessingError {}
unsafe impl Sync for CsvProcessingError {}

impl From<PolarsError> for CsvProcessingError {
    fn from(error: PolarsError) -> Self {
        CsvProcessingError::PolarsError(error)
    }
}

impl From<std::io::Error> for CsvProcessingError {
    fn from(error: std::io::Error) -> Self {
        CsvProcessingError::IoError(error)
    }
}

/// Synchronous function to process CSV file and return parquet bytes with row count
/// This function handles all CPU-intensive Polars operations in a thread-safe manner
fn process_csv_to_parquet_bytes(
    csv_path: &Path,
    allowed_platforms: &[&str]
) -> Result<Option<(Vec<u8>, u64)>, CsvProcessingError> {
    // Validate file path
    if !csv_path.exists() {
        return Err(CsvProcessingError::InvalidPath(
            format!("CSV file does not exist: {}", csv_path.display())
        ));
    }

    // Read CSV and apply filters - all synchronous operations
    let filtered_df = LazyCsvReader::new(csv_path)
        .with_has_header(true)
        .finish()
        .map_err(|e| CsvProcessingError::PolarsError(e))?
        .filter(
            col("platform_name").is_in(lit(Series::new(
                PlSmallStr::from_static("platforms"), 
                allowed_platforms
            )), false)
        )
        .collect()
        .map_err(|e| CsvProcessingError::PolarsError(e))?;

    // Handle empty DataFrame case by returning None
    if filtered_df.height() == 0 {
        let msg = format!("No rows after filtering for file: {}", csv_path.display());
        warn!("{}", msg);
        return Ok(None);
    }

    // Get row count before writing to buffer
    let row_count = filtered_df.height() as u64;

    // Write parquet to memory buffer - synchronous
    let mut buffer = Vec::new();
    let mut cursor = Cursor::new(&mut buffer);
    
    ParquetWriter::new(&mut cursor)
        .finish(&mut filtered_df.clone())
        .map_err(|e| CsvProcessingError::ParquetWriteError(
            format!("Failed to write parquet for {}: {}", csv_path.display(), e)
        ))?;

    Ok(Some((buffer, row_count)))
}

/// Custom error type for the async orchestrator function
#[derive(Debug)]
pub enum UploadError {
    CsvProcessing(CsvProcessingError),
    TaskJoin(String),
    S3Upload(String),
    InvalidFilePath(String),
}

impl fmt::Display for UploadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UploadError::CsvProcessing(e) => write!(f, "CSV processing error: {}", e),
            UploadError::TaskJoin(msg) => write!(f, "Task join error: {}", msg),
            UploadError::S3Upload(msg) => write!(f, "S3 upload error: {}", msg),
            UploadError::InvalidFilePath(msg) => write!(f, "Invalid file path: {}", msg),
        }
    }
}

impl std::error::Error for UploadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            UploadError::CsvProcessing(e) => Some(e),
            _ => None,
        }
    }
}

impl From<CsvProcessingError> for UploadError {
    fn from(error: CsvProcessingError) -> Self {
        UploadError::CsvProcessing(error)
    }
}

// Ensure Send + Sync traits are implemented for UploadError
unsafe impl Send for UploadError {}
unsafe impl Sync for UploadError {}

pub async fn convert_filter_and_upload_direct(
    csv_path: &Path,
    s3_client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Option<u64>, UploadError> {
    
    // Create the allowed platforms list
    let allowed_platforms = &[
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

    // Process CSV to parquet bytes using spawn_blocking to avoid runtime conflicts
    let csv_path_owned = csv_path.to_owned();
    let allowed_platforms_owned: Vec<String> = allowed_platforms.iter().map(|s| s.to_string()).collect();
    
    let join_handle = tokio::task::spawn_blocking(move || {
        let allowed_platforms_refs: Vec<&str> = allowed_platforms_owned.iter().map(|s| s.as_str()).collect();
        process_csv_to_parquet_bytes(&csv_path_owned, &allowed_platforms_refs)
    });
    
    // Handle the JoinHandle result properly and propagate errors with context
    let result_option = join_handle.await
        .map_err(|join_err| UploadError::TaskJoin(
            format!("Failed to join CSV processing task for {}: {}", 
                   csv_path.display(), join_err)
        ))?
        .map_err(|csv_err| UploadError::CsvProcessing(csv_err))?;
    
    let (buffer, row_count) = match result_option {
        Some((buf, count)) => (buf, count),
        None => return Ok(None), // No data after filtering
    };

    // Upload to S3 - this is the only async operation
    let file_name = csv_path
        .file_stem()
        .ok_or_else(|| UploadError::InvalidFilePath(
            format!("Cannot extract file name from path: {}", csv_path.display())
        ))?
        .to_string_lossy();
    
    let s3_key = format!("{}{}.parquet", prefix, file_name);
    
    // Log upload attempt for debugging
    info!("Attempting to upload {} bytes to s3://{}/{}", 
          buffer.len(), bucket, s3_key);
    
    // Log the current AWS region configuration for debugging
    let current_region = std::env::var("S3_REGION").unwrap_or_else(|_| "not set".to_string());
    info!("Using AWS region: {}", current_region);
    
    // Log AWS credential configuration status (without exposing actual values)
    let aws_access_key_set = std::env::var("AWS_ACCESS_KEY_ID").is_ok();
    let aws_secret_key_set = std::env::var("AWS_SECRET_ACCESS_KEY").is_ok();
    let aws_profile_set = std::env::var("AWS_PROFILE").is_ok();
    
    info!("AWS credentials status - Access Key: {}, Secret Key: {}, Profile: {}", 
          if aws_access_key_set { "SET" } else { "NOT SET" },
          if aws_secret_key_set { "SET" } else { "NOT SET" },
          if aws_profile_set { "SET" } else { "NOT SET" });
    
    s3_client
        .put_object()
        .bucket(bucket)
        .key(&s3_key)
        .body(ByteStream::from(buffer.clone()))
        .send()
        .await
        .map_err(|s3_err| {
            let detailed_error = match &s3_err {
                SdkError::ServiceError(service_err) => {
                    let status_code = service_err.raw().status().as_u16();
                    let error_msg = format!("S3 Service Error: {} (HTTP {})", 
                                          service_err.err(), status_code);
                    
                    // Add common troubleshooting hints based on status code
                    match status_code {
                        301 => format!("{} - REGION MISMATCH: Bucket '{}' exists in a different AWS region. Check S3_REGION environment variable", error_msg, bucket),
                        403 => format!("{} - ACCESS DENIED: Check 1) AWS credentials are configured, 2) IAM user/role has s3:PutObject permission for bucket '{}', 3) Bucket policy allows your AWS account", error_msg, bucket),
                        404 => format!("{} - Bucket '{}' may not exist or be accessible", error_msg, bucket),
                        400 => format!("{} - Invalid request parameters", error_msg),
                        500..=599 => format!("{} - AWS server error, retry may help", error_msg),
                        _ => error_msg
                    }
                },
                SdkError::TimeoutError(_) => "S3 request timed out - check network connectivity".to_string(),
                SdkError::ResponseError(resp_err) => {
                    format!("S3 Response Error: {:?} - check network connectivity", resp_err)
                },
                SdkError::DispatchFailure(dispatch_err) => {
                    format!("S3 Dispatch Failure: {:?} - check AWS configuration", dispatch_err)
                },
                SdkError::ConstructionFailure(construct_err) => {
                    format!("S3 Construction Failure: {:?} - check request parameters", construct_err)
                },
                _ => format!("Unknown S3 Error: {} - check AWS configuration and connectivity", s3_err)
            };
            
            UploadError::S3Upload(
                format!("Failed to upload {} to s3://{}/{}: {}", 
                       file_name, bucket, s3_key, detailed_error)
            )
        })?;

    info!("Processed and uploaded {} ({} rows) to s3://{}/{}", 
          file_name, row_count, bucket, s3_key);
    
    Ok(Some(row_count))
}