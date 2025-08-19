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
	// I'm using eu-central because DSA is also using eu-central S3 - It minimizes the time Fargate will take
	// Technically for first 50 TB only, but for now max data size is still below
	EUCentralS3StroageStandardPerGB = 0.0245
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
