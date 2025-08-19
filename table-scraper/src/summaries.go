package main

import (
	"fmt"
	"time"
)

func generateSummary(records []DailyRecord) Summary {
	if len(records) == 0 {
		return Summary{}
	}

	var totalStatements int64
	var totalFullCSV, totalFullZip, totalLightCSV, totalLightZip float64

	earliestDate := records[0].Date
	latestDate := records[0].Date

	for _, record := range records {
		totalStatements += record.StatementsOfReasons
		totalFullCSV += record.FullCSVSizeMB
		totalFullZip += record.FullZipSizeMB
		totalLightCSV += record.LightCSVSizeMB
		totalLightZip += record.LightZipSizeMB

		if record.Date < earliestDate {
			earliestDate = record.Date
		}
		if record.Date > latestDate {
			latestDate = record.Date
		}
	}

	dateRange := fmt.Sprintf("%s to %s", earliestDate, latestDate)
	if earliestDate == latestDate {
		dateRange = earliestDate
	}

	var totalFullCSVSizeTB = mbToTb(totalFullCSV)
	var totalFullZipSizeTB = mbToTb(totalFullZip)
	var totalFullParquetConservativeSize = totalFullCSVSizeTB * parquetReductionFactorConservative
	var totalFullParquetAggressiveSize = totalFullCSVSizeTB * parquetReductionFactorAggressive

	return Summary{
		TotalRecords:                     len(records),
		TotalStatements:                  totalStatements,
		TotalFullCSVSizeTB:               totalFullCSVSizeTB,
		TotalFullZipSizeTB:               totalFullZipSizeTB,
		TotalFullParquetConservativeSize: totalFullParquetConservativeSize,
		TotalFullParquetAggressiveSize:   totalFullParquetAggressiveSize,
		S3StandardCosts: getS3StorageCosts(
			totalFullCSVSizeTB,
			totalFullZipSizeTB,
			euCentralS3StroageStandardPerGB,
			parquetReductionFactorConservative,
			parquetReductionFactorAggressive,
		),
		S3DeepGlacierCosts: getS3StorageCosts(
			totalFullCSVSizeTB,
			totalFullZipSizeTB,
			euCentralS3StroageGlacierDeepArchivePerGB,
			parquetReductionFactorConservative,
			parquetReductionFactorAggressive,
		),
		DateRange: dateRange,
		ScrapedAt: time.Now().UTC().Format(time.RFC3339),
	}
}

func mbToTb(mb float64) float64 {
	return mb / 1024 / 1024
}

func generateSampledSummaries(records []DailyRecord) SampledSummaries {
	return SampledSummaries{
		EveryDay: generateSampledSummary(records, 1),
		// N should not be divisible by 7 so we don't sample the same weekday
		Every2Days: generateSampledSummary(records, 2),
		Every4Days: generateSampledSummary(records, 4),
		Every6Days: generateSampledSummary(records, 6),
	}
}

func generateSampledSummary(records []DailyRecord, interval int) Summary {
	if len(records) == 0 {
		return Summary{}
	}

	// Sort records by date to ensure consistent sampling
	sortedRecords := make([]DailyRecord, len(records))
	copy(sortedRecords, records)

	// Simple sort by date string (works for YYYY-MM-DD format)
	for i := 0; i < len(sortedRecords)-1; i++ {
		for j := i + 1; j < len(sortedRecords); j++ {
			if sortedRecords[i].Date > sortedRecords[j].Date {
				sortedRecords[i], sortedRecords[j] = sortedRecords[j], sortedRecords[i]
			}
		}
	}

	// Sample every N days starting from the first day (index 0)
	var sampledRecords []DailyRecord
	for i := 0; i < len(sortedRecords); i += interval {
		sampledRecords = append(sampledRecords, sortedRecords[i])
	}

	// Generate summary for sampled records
	summary := generateSummary(sampledRecords)
	summary.SamplingStrategy = fmt.Sprintf("Every %d days from first day", interval)

	return summary
}
