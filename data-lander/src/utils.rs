use aws_config::Region;
use regex::Regex;
use tempfile::NamedTempFile;

// S3 Utils

pub fn get_s3_prefix(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let is_light_variant = is_light_variant(url)?;
    let date = get_date_from_url(url)?;

    let prefix = if is_light_variant {
        format!("global-light/{:04}-{:02}-{:02}/", date[0], date[1], date[2])
    } else {
        format!("global-full/{:04}-{:02}-{:02}/", date[0], date[1], date[2])
    };
    Ok(prefix)
}

pub async fn get_s3_config() -> aws_sdk_s3::Config {
    let s3_region: String =
        std::env::var("S3_REGION").expect("msg: S3_REGION environment variable not set");

    // Load the default AWS configuration (includes credentials from ~/.aws/credentials)
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(Region::new(s3_region))
        .load()
        .await;

    aws_sdk_s3::Config::from(&aws_config)
}

// File utils

pub fn get_file_size(file: &NamedTempFile) -> Result<u64, Box<dyn std::error::Error>> {
    let metadata = file.as_file().metadata()?;
    Ok(metadata.len())
}

// Regex/String utils

fn get_date_from_url(url: &str) -> Result<[u16; 3], Box<dyn std::error::Error>> {
    // Regex extracts after 'global-'
    let re = Regex::new(r"global-(\d{4}-\d{2}-\d{2})")?;

    if let Some(caps) = re.captures(url) {
        let date_str = caps.get(1).ok_or("Date not found in URL")?.as_str();
        let parts: Vec<&str> = date_str.split('-').collect();

        if parts.len() == 3 {
            let year: u16 = parts[0].parse()?;
            let month: u16 = parts[1].parse()?;
            let day: u16 = parts[2].parse()?;
            return Ok([year, month, day]);
        } else {
            return Err("Invalid date format in URL".into());
        }
    } else {
        return Err("No date found in URL".into());
    }
}

fn is_light_variant(url: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // Check if the URL contains 'light' in the filename
    let re = Regex::new(r"global-\d{4}-\d{2}-\d{2}-(light|full)\.zip")?;

    if re.is_match(url) {
        Ok(url.contains("-light"))
    } else {
        Err("Invalid URL format".into())
    }
}
