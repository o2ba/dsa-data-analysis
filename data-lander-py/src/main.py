from urllib.parse import urlparse
import boto3
import json
import os
import requests
import zipfile
import io
import pandas as pd
import time
import re
import logging
from dotenv import load_dotenv
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

S3_BUCKET_NAME = "dsa-data-analysis-4f9a2b"


def extract_date_from_url(url):
    """
    Extract date from URL like 'sor-global-2023-09-25-light.zip'
    Returns tuple (year, month, day) or None if not found.
    """
    date_pattern = r'(\d{4})-(\d{2})-(\d{2})'
    match = re.search(date_pattern, url)
    
    if match:
        year, month, day = match.groups()
        logger.debug(f"Extracted date from URL: {year}-{month}-{day}")
        return year, month, day
    
    # Fallback: use today's date if no date found in URL
    from datetime import datetime
    today = datetime.now()
    logger.warning(f"No date found in URL {url}, using today's date")
    return str(today.year), f"{today.month:02d}", f"{today.day:02d}"


def process_csv_to_parquet(csv_bytes, filename, bucket, prefix):
    """
    Convert CSV bytes to Parquet and return (s3_key, parquet_bytes).
    This runs in a separate process.
    """
    # Note: logging in multiprocessing can be tricky, so we'll return status messages
    try:
        parquet_buffer = io.BytesIO()
        df = pd.read_csv(io.BytesIO(csv_bytes))
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")

        base_name = os.path.basename(filename)
        parquet_name = os.path.splitext(base_name)[0] + ".parquet"
        s3_key = f"{prefix}{parquet_name}"

        return s3_key, parquet_buffer.getvalue(), None  # success
    except Exception as e:
        return None, None, str(e)  # error


def extract_and_collect_csvs(zip_bytes):
    """
    Recursively extract zip files and collect CSVs as (bytes, filename).
    Returns a list of (csv_bytes, filename).
    """
    csv_files = []
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        for name in z.namelist():
            if name.endswith("/"):
                continue  # skip directories

            with z.open(name) as f:
                if name.lower().endswith(".zip"):
                    logger.info(f"Found nested zip: {name}, extracting...")
                    nested_bytes = f.read()
                    csv_files.extend(extract_and_collect_csvs(nested_bytes))
                elif name.lower().endswith(".csv"):
                    logger.info(f"Found CSV: {name}")
                    csv_files.append((f.read(), name))
    return csv_files


def main():
    start_time = time.perf_counter()
    logger.info("Starting data processing pipeline")

    try:
        raw_urls = os.getenv("DOWNLOAD_URLS")
        if not raw_urls:
            logger.error("No DOWNLOAD_URLS environment variable set")
            return

        try:
            download_urls = json.loads(raw_urls)
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON from DOWNLOAD_URLS: {e}")
            return

        if len(download_urls) == 0:
            logger.warning("No download URLs provided")
            return

        logger.info(f"Processing {len(download_urls)} URLs")
        s3 = boto3.client("s3")

        for i, url in enumerate(download_urls, 1):
            logger.info(f"[{i}/{len(download_urls)}] Downloading: {url}")
            
            # Extract date from URL to create S3 prefix
            year, month, day = extract_date_from_url(url)
            s3_prefix = f"raw/{year}/{month}/{day}/"
            logger.info(f"Using S3 prefix: {s3_prefix}")
            
            response = requests.get(url, stream=True)
            response.raise_for_status()

            # Collect all CSVs from nested zips
            csv_files = extract_and_collect_csvs(response.content)
            logger.info(f"Found {len(csv_files)} CSV files in {url}")

            if not csv_files:
                logger.warning(f"No CSV files found in {url}")
                continue

            def process_and_upload(csv_bytes, filename):
                s3_key, parquet_bytes, error = process_csv_to_parquet(csv_bytes, filename, S3_BUCKET_NAME, s3_prefix)
                if error:
                    logger.error(f"Failed to process file {filename}: {error}")
                    return
                
                try:
                    # Upload result to S3
                    s3.upload_fileobj(io.BytesIO(parquet_bytes), S3_BUCKET_NAME, s3_key)
                    logger.info(f"Uploaded {s3_key}")
                except Exception as e:
                    logger.error(f"Failed to upload {s3_key}: {e}")

            # Use ThreadPoolExecutor with max 2 workers
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [
                    executor.submit(process_and_upload, csv_bytes, filename)
                    for csv_bytes, filename in csv_files
                ]
                for future in as_completed(futures):
                    future.result()  # Wait for each task to complete

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise

    end_time = time.perf_counter()
    elapsed = end_time - start_time
    logger.info(f"Pipeline completed in {elapsed:.2f} seconds")


if __name__ == "__main__":
    load_dotenv()
    main()