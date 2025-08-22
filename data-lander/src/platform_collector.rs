use polars::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use log::{info, warn, error};
use regex::Regex;
use crate::converter_uploader::CsvProcessingError;

/// Platform data collector that groups CSV data by platform for consolidated processing
pub struct PlatformDataCollector {
    /// HashMap storing DataFrames grouped by platform name
    platform_data: HashMap<String, Vec<DataFrame>>,
    /// List of allowed platforms for filtering
    allowed_platforms: Vec<String>,
    /// Regex for platform name sanitization
    sanitization_regex: Regex,
}

impl PlatformDataCollector {
    /// Create a new PlatformDataCollector with specified allowed platforms
    pub fn new(allowed_platforms: Vec<String>) -> Self {
        // Create regex for sanitizing platform names for S3 compatibility
        // This regex will match any character that's not alphanumeric, hyphen, or underscore
        let sanitization_regex = Regex::new(r"[^a-zA-Z0-9\-_]")
            .expect("Failed to compile sanitization regex");
        
        Self {
            platform_data: HashMap::new(),
            allowed_platforms,
            sanitization_regex,
        }
    }

    /// Add CSV data to the collector, grouping by platform_name
    pub fn add_csv_data(&mut self, csv_path: &Path) -> Result<(), CsvProcessingError> {
        // Validate file path
        if !csv_path.exists() {
            return Err(CsvProcessingError::InvalidPath(
                format!("CSV file does not exist: {}", csv_path.display())
            ));
        }

        info!("Processing CSV file for platform grouping: {:?}", csv_path);

        // Read CSV file
        let df = LazyCsvReader::new(csv_path)
            .with_has_header(true)
            .finish()
            .map_err(CsvProcessingError::PolarsError)?
            .collect()
            .map_err(CsvProcessingError::PolarsError)?;

        // Check if DataFrame is empty
        if df.height() == 0 {
            warn!("Empty CSV file, skipping: {:?}", csv_path);
            return Ok(());
        }

        // Check if platform_name column exists
        let platform_name_col = PlSmallStr::from_static("platform_name");
        if !df.get_column_names().contains(&platform_name_col) {
            error!("CSV file missing 'platform_name' column: {:?}", csv_path);
            return Err(CsvProcessingError::InvalidPath(
                format!("Missing 'platform_name' column in file: {}", csv_path.display())
            ));
        }

        // Filter by allowed platforms
        let allowed_platforms_series = Series::new(
            PlSmallStr::from_static("platforms"), 
            &self.allowed_platforms
        );

        let filtered_df = df
            .lazy()
            .filter(
                col("platform_name").is_in(lit(allowed_platforms_series), false)
            )
            .collect()
            .map_err(CsvProcessingError::PolarsError)?;

        if filtered_df.height() == 0 {
            warn!("No rows match allowed platforms in file: {:?}", csv_path);
            return Ok(());
        }

        // Group data by platform_name
        self.group_dataframe_by_platform(filtered_df, csv_path)?;

        Ok(())
    }

    /// Internal method to group DataFrame by platform and add to collection
    fn group_dataframe_by_platform(&mut self, df: DataFrame, csv_path: &Path) -> Result<(), CsvProcessingError> {
        // Get unique platform names from the DataFrame
        let platform_names = df
            .column("platform_name")
            .map_err(CsvProcessingError::PolarsError)?
            .unique()
            .map_err(CsvProcessingError::PolarsError)?;

        // Process each platform
        for platform_value in platform_names.iter() {
            let platform_name = match platform_value.get_str() {
                Ok(Some(name)) => name,
                Ok(None) => {
                    warn!("Null platform name found in file: {:?}", csv_path);
                    continue;
                }
                Err(e) => {
                    error!("Error extracting platform name from file {:?}: {}", csv_path, e);
                    continue;
                }
            };

            // Sanitize platform name for S3 compatibility
            let sanitized_platform = self.sanitize_platform_name(platform_name);
            
            // Filter DataFrame for this specific platform
            let platform_df = df
                .lazy()
                .filter(col("platform_name").eq(lit(platform_name)))
                .collect()
                .map_err(CsvProcessingError::PolarsError)?;

            if platform_df.height() > 0 {
                info!("Found {} rows for platform '{}' in file: {:?}", 
                      platform_df.height(), sanitized_platform, csv_path);
                
                // Add to platform data collection
                self.platform_data
                    .entry(sanitized_platform.clone())
                    .or_insert_with(Vec::new)
                    .push(platform_df);
            }
        }

        Ok(())
    }

    /// Sanitize platform name for S3 path compatibility
    pub fn sanitize_platform_name(&self, platform_name: &str) -> String {
        // Convert to lowercase and replace invalid characters with hyphens
        let sanitized = self.sanitization_regex
            .replace_all(&platform_name.to_lowercase(), "-")
            .to_string();
        
        // Remove leading/trailing hyphens and collapse multiple hyphens
        let cleaned = sanitized
            .trim_matches('-')
            .split('-')
            .filter(|s| !s.is_empty())
            .collect::<Vec<&str>>()
            .join("-");

        // Handle edge case of empty result
        if cleaned.is_empty() {
            warn!("Platform name '{}' resulted in empty sanitized name, using 'unknown'", platform_name);
            "unknown".to_string()
        } else {
            cleaned
        }
    }

    /// Get list of all platforms that have collected data
    pub fn get_platforms(&self) -> Vec<String> {
        self.platform_data.keys().cloned().collect()
    }

    /// Get the DataFrames for a specific platform
    pub fn get_platform_data(&self, platform: &str) -> Option<&Vec<DataFrame>> {
        self.platform_data.get(platform)
    }

    /// Consolidate all DataFrames for a specific platform into a single DataFrame
    pub fn consolidate_platform_data(&self, platform: &str) -> Result<DataFrame, CsvProcessingError> {
        let dataframes = self.platform_data.get(platform)
            .ok_or_else(|| CsvProcessingError::InvalidPath(
                format!("No data found for platform: {}", platform)
            ))?;

        if dataframes.is_empty() {
            return Err(CsvProcessingError::EmptyDataFrame(
                format!("No DataFrames available for platform: {}", platform)
            ));
        }

        info!("Consolidating {} DataFrames for platform: {}", dataframes.len(), platform);

        // If only one DataFrame, return a clone
        if dataframes.len() == 1 {
            return Ok(dataframes[0].clone());
        }

        // Concatenate multiple DataFrames
        let mut consolidated = dataframes[0].clone();
        for df in &dataframes[1..] {
            consolidated = consolidated
                .lazy()
                .with_columns([
                    // Ensure all columns are present in both DataFrames
                    col("*")
                ])
                .collect()
                .map_err(CsvProcessingError::PolarsError)?
                .vstack(df)
                .map_err(CsvProcessingError::PolarsError)?;
        }

        info!("Consolidated platform '{}' data: {} total rows", 
              platform, consolidated.height());

        Ok(consolidated)
    }

    /// Get total number of platforms with data
    pub fn platform_count(&self) -> usize {
        self.platform_data.len()
    }

    /// Get total number of DataFrames across all platforms
    pub fn total_dataframe_count(&self) -> usize {
        self.platform_data.values().map(|v| v.len()).sum()
    }

    /// Check if a specific platform has data
    pub fn has_platform_data(&self, platform: &str) -> bool {
        self.platform_data.contains_key(platform)
    }

    /// Get summary statistics for collected data
    pub fn get_summary(&self) -> HashMap<String, usize> {
        let mut summary = HashMap::new();
        
        for (platform, dataframes) in &self.platform_data {
            let total_rows: usize = dataframes.iter()
                .map(|df| df.height())
                .sum();
            summary.insert(platform.clone(), total_rows);
        }
        
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_platform_name_sanitization() {
        let allowed_platforms = vec!["Facebook".to_string(), "Google".to_string()];
        let collector = PlatformDataCollector::new(allowed_platforms);

        // Test various platform name sanitizations
        assert_eq!(collector.sanitize_platform_name("Facebook"), "facebook");
        assert_eq!(collector.sanitize_platform_name("Google Maps"), "google-maps");
        assert_eq!(collector.sanitize_platform_name("Discord Netherlands B.V."), "discord-netherlands-b-v");
        assert_eq!(collector.sanitize_platform_name("X"), "x");
        assert_eq!(collector.sanitize_platform_name("TikTok"), "tiktok");
        assert_eq!(collector.sanitize_platform_name(""), "unknown");
        assert_eq!(collector.sanitize_platform_name("!!!"), "unknown");
        assert_eq!(collector.sanitize_platform_name("Test-Platform_123"), "test-platform-123");
    }

    #[test]
    fn test_new_collector() {
        let allowed_platforms = vec!["Facebook".to_string(), "Google".to_string()];
        let collector = PlatformDataCollector::new(allowed_platforms.clone());

        assert_eq!(collector.allowed_platforms, allowed_platforms);
        assert_eq!(collector.platform_count(), 0);
        assert_eq!(collector.total_dataframe_count(), 0);
    }

    #[test]
    fn test_empty_csv_handling() {
        let allowed_platforms = vec!["Facebook".to_string()];
        let mut collector = PlatformDataCollector::new(allowed_platforms);

        // Create empty CSV file
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "platform_name,data").unwrap();
        temp_file.flush().unwrap();

        // Should handle empty file gracefully
        let result = collector.add_csv_data(temp_file.path());
        assert!(result.is_ok());
        assert_eq!(collector.platform_count(), 0);
    }
}