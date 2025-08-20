# Data Lander Pipeline

A containerized ETL pipeline that downloads nested DSA zip files, extracts CSV data, converts to Parquet format, and uploads to S3 with date-based organization. This is the first step of the prep pipeline - It is followed by an AWS Glue task which will seperate the files by partition key

## 🎯 What It Does

1. **Downloads** zip files from provided URLs
2. **Recursively extracts** nested zip archives
3. **Converts** CSV files to Parquet format (parallel processing)
4. **Organizes** data in S3 by date: `s3://bucket/raw/YYYY/MM/DD/`
5. **Extracts dates** automatically from filenames like `sor-global-2023-09-25-light.zip`

## 🏗️ Architecture

- **Runtime**: Python 3.13 in Docker container
- **Compute**: AWS ECS Fargate (2 vCPUs, 4GB RAM)
- **Storage**: Amazon S3
- **Registry**: Amazon ECR
- **CI/CD**: GitHub Actions
- **Logs**: CloudWatch Logs

## Environment Variables

```bash
DOWNLOAD_URLS='["https://example.com/data-2023-09-25.zip"]'
```