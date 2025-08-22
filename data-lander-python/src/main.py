import asyncio
import os
import tempfile
from pathlib import Path

import boto3
from dotenv import load_dotenv

from . import downloader, unzipper, converter_uploader, utils, merge_parquets

async def main():
    load_dotenv()
    
    print("Starting data-lander application...")
    
    url = os.environ["URL"]
    s3_bucket = os.environ["S3_BUCKET_NAME"]
    s3_client = utils.get_s3_config()
    
    # Download zip to temp file
    temp_file_path = await downloader.download_zip_to_temp(url)
    print(f"Downloaded {temp_file_path.stat().st_size / (1024*1024):.2f} MB")

    with tempfile.TemporaryDirectory() as extract_dir:
        # Pass the path, not a file object
        extracted_files = unzipper.streamed_unzip(temp_file_path, Path(extract_dir))
        
        # Process each CSV
        for file_path in extracted_files:
            if file_path.suffix == '.csv':
                await converter_uploader.convert_filter_and_upload_direct(
                    file_path, s3_client, s3_bucket, utils.get_s3_prefix(url)
                )
    
    print(f"✅ Processed {len(extracted_files)} files")
    
    # Keep alive if needed
    if os.getenv("KEEP_ALIVE") == "true":
        while True:
            await asyncio.sleep(3600)

    print(f"✅ Processed {len(extracted_files)} files")
    
    # NEW: Merge phase
    print("Starting parquet merge phase...")
    await merge_parquets.merge_platform_parquets(s3_client, s3_bucket, utils.get_s3_prefix(url))
    print("✅ Merge completed")

if __name__ == "__main__":
    asyncio.run(main())