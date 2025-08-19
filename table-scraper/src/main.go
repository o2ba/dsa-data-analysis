package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"
)

const (
	baseURL     = "https://transparency.dsa.ec.europa.eu/explore-data/download?page="
	scrapeDelay = 500
	// Technically for first 50 TB only, but for now max data size is still below
	euCentralS3StroageStandardPerGB           = 0.0245
	euCentralS3StroageGlacierDeepArchivePerGB = 0.00099
	parquetReductionFactorConservative        = 0.2
	parquetReductionFactorAggressive          = 0.1
)

func main() {
	records, err := scrapeData()
	if err != nil {
		log.Fatal(err)
	}

	// Generate summaries
	summaries := generateSampledSummaries(records)

	// Create export structure
	export := DataExport{
		Summaries: summaries,
		Records:   records,
	}

	// Convert to JSON
	jsonData, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		log.Fatal("Error marshaling to JSON:", err)
	}

	// Write to file
	filename := fmt.Sprintf("data/output/dsa_data_export_%s.json",
		time.Now().Format("2006-01-02_15-04-05"))

	err = os.WriteFile(filename, jsonData, 0644)
	if err != nil {
		log.Fatal("Error writing JSON file:", err)
	}

	fmt.Printf("Data exported to %s\n", filename)
}
