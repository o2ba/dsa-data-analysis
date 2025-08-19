package main

import (
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

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
