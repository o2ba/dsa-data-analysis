package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	baseURL     = "https://transparency.dsa.ec.europa.eu/explore-data/download?page="
	scrapeDelay = 500
)

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
	TotalRecords        int     `json:"total_records"`
	TotalStatements     int64   `json:"total_statements"`
	TotalFullCSVSizeTB  float64 `json:"total_full_csv_size_tb"`
	TotalFullZipSizeTB  float64 `json:"total_full_zip_size_tb"`
	TotalLightCSVSizeTB float64 `json:"total_light_csv_size_tb"`
	TotalLightZipSizeTB float64 `json:"total_light_zip_size_tb"`
	DateRange           string  `json:"date_range"`
	ScrapedAt           string  `json:"scraped_at"`
}

type DataExport struct {
	Summary Summary       `json:"summary"`
	Records []DailyRecord `json:"records"`
}

func main() {
	records, err := scrapeData()
	if err != nil {
		log.Fatal(err)
	}

	// Generate summary
	summary := generateSummary(records)

	// Create export structure
	export := DataExport{
		Summary: summary,
		Records: records,
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

	return Summary{
		TotalRecords:        len(records),
		TotalStatements:     totalStatements,
		TotalFullCSVSizeTB:  totalFullCSV / 1024 / 1024,  // MB to TB
		TotalFullZipSizeTB:  totalFullZip / 1024 / 1024,  // MB to TB
		TotalLightCSVSizeTB: totalLightCSV / 1024 / 1024, // MB to TB
		TotalLightZipSizeTB: totalLightZip / 1024 / 1024, // MB to TB
		DateRange:           dateRange,
		ScrapedAt:           time.Now().UTC().Format(time.RFC3339),
	}
}

func formatNumber(n int64) string {
	str := strconv.FormatInt(n, 10)
	if len(str) <= 3 {
		return str
	}

	var result strings.Builder
	for i, digit := range str {
		if i > 0 && (len(str)-i)%3 == 0 {
			result.WriteString(",")
		}
		result.WriteRune(digit)
	}
	return result.String()
}

func scrapeData() ([]DailyRecord, error) {
	var allRecords []DailyRecord
	page := 1

	for {
		url := fmt.Sprintf("%s%d", baseURL, page)
		fmt.Printf("Scraping page %d: %s\n", page, url)

		records, hasMore, err := scrapePage(url)
		if err != nil {
			return nil, fmt.Errorf("error scraping page %d: %v", page, err)
		}

		allRecords = append(allRecords, records...)

		if !hasMore {
			break
		}

		page++
		time.Sleep(time.Duration(scrapeDelay) * time.Millisecond)
	}

	return allRecords, nil
}

func scrapePage(url string) ([]DailyRecord, bool, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()

	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, false, err
	}

	var records []DailyRecord

	doc.Find("tr.dayarchive-row").Each(func(i int, s *goquery.Selection) {
		record := DailyRecord{}

		// Extract date
		record.Date = strings.TrimSpace(s.Find("td").Eq(0).Text())

		// Extract statements of reasons (remove non-breaking spaces)
		statementsText := s.Find("td").Eq(1).Text()
		statementsText = strings.ReplaceAll(statementsText, "\u00a0", "")
		statementsText = strings.ReplaceAll(statementsText, " ", "")
		if statements, err := strconv.ParseInt(statementsText, 10, 64); err == nil {
			record.StatementsOfReasons = statements
		}

		// Extract Full links (3rd column - index 2)
		fullLinksCell := s.Find("td").Eq(2)
		record.FullZipURL, record.FullSha1URL = extractLinks(fullLinksCell)

		// Extract Full sizes (4th column - index 3)
		fullSizeText := s.Find("td").Eq(3).Text()
		record.FullCSVSizeMB, record.FullZipSizeMB = extractSizes(fullSizeText)

		// Extract Light links (5th column - index 4)
		lightLinksCell := s.Find("td").Eq(4)
		record.LightZipURL, record.LightSha1URL = extractLinks(lightLinksCell)

		// Extract Light sizes (6th column - index 5)
		lightSizeText := s.Find("td").Eq(5).Text()
		record.LightCSVSizeMB, record.LightZipSizeMB = extractSizes(lightSizeText)

		records = append(records, record)
	})

	// Check if there are more pages by looking for pagination or next button
	hasMore := doc.Find("a[aria-label='Next page']").Length() > 0 ||
		doc.Find(".ecl-pagination__item--next").Length() > 0

	return records, hasMore, nil
}

func extractLinks(cell *goquery.Selection) (zipURL, sha1URL string) {
	// Find all links in the cell
	cell.Find("a").Each(func(i int, link *goquery.Selection) {
		href, exists := link.Attr("href")
		if !exists {
			return
		}

		linkText := strings.TrimSpace(link.Find(".ecl-link__label").Text())

		switch linkText {
		case "zip":
			zipURL = href
		case "sha1":
			sha1URL = href
		}
	})

	return zipURL, sha1URL
}

func extractSizes(sizeText string) (csvSizeMB, zipSizeMB float64) {
	// Remove non-breaking spaces and normalize
	sizeText = strings.ReplaceAll(sizeText, "\u00a0", " ")
	sizeText = strings.TrimSpace(sizeText)

	// Regex patterns for extracting sizes
	csvPattern := regexp.MustCompile(`csv:\s*([\d.]+)\s*(GB|MB)`)
	zipPattern := regexp.MustCompile(`zip:\s*([\d.]+)\s*(MB|GB)`)

	// Extract CSV size
	if csvMatch := csvPattern.FindStringSubmatch(sizeText); len(csvMatch) >= 3 {
		if size, err := strconv.ParseFloat(csvMatch[1], 64); err == nil {
			if csvMatch[2] == "GB" {
				csvSizeMB = size * 1024 // Convert GB to MB
			} else {
				csvSizeMB = size
			}
		}
	}

	// Extract ZIP size
	if zipMatch := zipPattern.FindStringSubmatch(sizeText); len(zipMatch) >= 3 {
		if size, err := strconv.ParseFloat(zipMatch[1], 64); err == nil {
			if zipMatch[2] == "GB" {
				zipSizeMB = size * 1024 // Convert GB to MB
			} else {
				zipSizeMB = size
			}
		}
	}

	return csvSizeMB, zipSizeMB
}
