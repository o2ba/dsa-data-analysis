import boto3
from collections import defaultdict
from loguru import logger
import os
import tempfile
import re
import polars as pl

async def merge_platform_parquets(
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
) -> None:
    """Merge all parquet files per platform into single files."""
    
    # List all objects under the prefix
    paginator = s3_client.get_paginator('list_objects_v2')
    
    # Group files by platform folder
    platform_files = defaultdict(list)
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.parquet'):
                # Extract platform from path: prefix/platform/file.parquet
                parts = key.replace(prefix, '').split('/')
                if len(parts) >= 2:
                    platform = parts[0]
                    platform_files[platform].append(key)
    
    for platform, file_keys in platform_files.items():
        if len(file_keys) <= 1:
            continue
            
        logger.info(f"Merging {len(file_keys)} files for platform: {platform}")
        
        # Create a temporary directory to hold all files
        with tempfile.TemporaryDirectory() as temp_dir:
            # Download all files to temp directory
            temp_paths = []
            for i, key in enumerate(file_keys):
                temp_path = os.path.join(temp_dir, f"file_{i}.parquet")
                s3_client.download_file(bucket, key, temp_path)
                temp_paths.append(temp_path)
            
            # Use Polars streaming engine
            merged_key = f"{prefix}{platform}/{re.sub(r'/', '-', prefix)}-{platform}-merged.parquet"
            with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_merged:
                # This processes in streaming mode - low memory!
                (
                    pl.scan_parquet(temp_paths)  # Can pass list of paths
                    .sink_parquet(temp_merged.name, maintain_order=False)
                )
                
                s3_client.upload_file(temp_merged.name, bucket, merged_key)
        
        # Delete original S3 files
        for key in file_keys:
            s3_client.delete_object(Bucket=bucket, Key=key)
        
        logger.info(f"Merged {platform}: {len(file_keys)} -> 1 file")