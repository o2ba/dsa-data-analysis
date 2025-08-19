package main

func getS3StorageCosts(
	fullCSVSizeTB float64,
	fullZipSizeTB float64,
	lightCSVSizeTB float64,
	lightZipSizeTB float64,
	pricePerGB float64,
	parquetConservativeFactor float64,
	parquetAggressiveFactor float64,
) S3StorageCosts {
	const tbToGB = 1000.0

	return S3StorageCosts{
		StorageFullCSV:             fullCSVSizeTB * tbToGB * pricePerGB,
		StorageFullZip:             fullZipSizeTB * tbToGB * pricePerGB,
		StorageLightCSV:            lightCSVSizeTB * tbToGB * pricePerGB,
		StorageLightZip:            lightZipSizeTB * tbToGB * pricePerGB,
		StorageParquetConservative: (fullCSVSizeTB * tbToGB * pricePerGB) * parquetConservativeFactor,
		StorageParquetAggressive:   (fullCSVSizeTB * tbToGB * pricePerGB) * parquetAggressiveFactor,
	}
}
