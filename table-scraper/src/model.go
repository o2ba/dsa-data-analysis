package main

type DailyRecord struct {
	Date                string  `json:"date"`
	StatementsOfReasons int64   `json:"statements_of_reasons"`
	FullCSVSizeMB       float64 `json:"full_csv_size_mb"`
	FullZipSizeMB       float64 `json:"full_zip_size_mb"`
	FullZipURL          string  `json:"full_zip_url"`
	FullSha1URL         string  `json:"full_sha1_url"`
	LightCSVSizeMB      float64 `json:"light_csv_size_mb"`
	LightZipSizeMB      float64 `json:"light_zip_size_mb"`
	LightZipURL         string  `json:"light_zip_url"`
	LightSha1URL        string  `json:"light_sha1_url"`
}

type Summary struct {
	TotalRecords                     int            `json:"total_records"`
	TotalStatements                  int64          `json:"total_statements"`
	TotalFullCSVSizeTB               float64        `json:"total_full_csv_size_tb"`
	TotalFullZipSizeTB               float64        `json:"total_full_zip_size_tb"`
	TotalFullParquetConservativeSize float64        `json:"total_full_parquet_conservative_size"`
	TotalFullParquetAggressiveSize   float64        `json:"total_full_parquet_aggressive_size"`
	DateRange                        string         `json:"date_range"`
	ScrapedAt                        string         `json:"scraped_at"`
	SamplingStrategy                 string         `json:"sampling_strategy,omitempty"`
	S3StandardCosts                  S3StorageCosts `json:"eu_central_s3_standard_storage_costs,omitempty"`
	S3DeepGlacierCosts               S3StorageCosts `json:"eu_central_s3_deep_glacier_storage_costs,omitempty"`
}

type S3StorageCosts struct {
	StorageFullCSV             float64 `json:"s3_storage_full_csv"`
	StorageFullZip             float64 `json:"s3_storage_full_zip"`
	StorageParquetConservative float64 `json:"s3_storage_parquet_conservative"`
	StorageParquetAggressive   float64 `json:"s3_storage_parquet_aggressive"`
}

type SampledSummaries struct {
	EveryDay   Summary `json:"every_day"`
	Every2Days Summary `json:"every_2_days"`
	Every4Days Summary `json:"every_4_days"`
	Every6Days Summary `json:"every_6_days"`
}

type DataExport struct {
	Summaries SampledSummaries `json:"summaries"`
	Records   []DailyRecord    `json:"records"`
}
