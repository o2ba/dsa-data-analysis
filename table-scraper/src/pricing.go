package main

func getS3StorageCosts(
	fullCSVSizeTB float64,
	fullZipSizeTB float64,
	lightCSVSizeTB float64,
	lightZipSizeTB float64,
) S3StorageCosts {
	return S3StorageCosts{
		StorageFullCSV:  fullCSVSizeTB * 1000 * EUCentralS3StroageStandardPerGB,
		StorageFullZip:  fullZipSizeTB * 1000 * EUCentralS3StroageStandardPerGB,
		StorageLightCSV: lightCSVSizeTB * 1000 * EUCentralS3StroageStandardPerGB,
		StorageLightZip: lightZipSizeTB * 1000 * EUCentralS3StroageStandardPerGB,
	}
}
