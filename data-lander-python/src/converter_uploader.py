import tempfile
from pathlib import Path
from typing import Dict, List
from collections import defaultdict

import polars as pl
import boto3
from loguru import logger

async def convert_filter_and_upload_direct(
    csv_path: Path,
    s3_client: boto3.client,
    bucket: str,
    prefix: str,
) -> Dict[str, int]:
    """Process CSV, split by platform, and upload separate Parquet files."""
    
    allowed_platforms = [
        # "Facebook", "Discord Netherlands B.V.", 
        "Google Maps",
        # "Instagram",
        # "Kleinanzeigen", "Leboncoin", "LinkedIn", "Reddit", "Telegram",
        # "TikTok", "X"
    ]
    
    # Read CSV with robust settings to handle messy data
    df = pl.scan_csv(
        csv_path,
        infer_schema_length=0,  # Don't infer schema, treat everything as strings
        ignore_errors=True      # Skip problematic rows instead of failing
    ).filter(
        pl.col("platform_name").is_in(allowed_platforms)
    ).collect()
    
    if df.height == 0:
        logger.warning(f"No rows after filtering for {csv_path}")
        return {}
    
    # Group by platform
    platform_counts = {}
    
    for platform in allowed_platforms:
        platform_df = df.filter(pl.col("platform_name") == platform)
        
        if platform_df.height == 0:
            continue
            
        # Sanitize platform name for file/folder names
        safe_platform = platform.replace(" ", "_").replace(".", "")
        
        # Create subfolder structure: prefix/platform/filename.parquet
        s3_key = f"{prefix}{safe_platform}/{csv_path.stem}.parquet"
        
        # Write to temporary Parquet file
        with tempfile.NamedTemporaryFile(suffix='.parquet') as temp_parquet:
            platform_df.write_parquet(temp_parquet.name)
            s3_client.upload_file(temp_parquet.name, bucket, s3_key)
        
        platform_counts[platform] = platform_df.height
        logger.info(f"Uploaded {safe_platform}/{csv_path.stem} ({platform_df.height} rows)")
    
    total_rows = sum(platform_counts.values())
    logger.info(f"Split {csv_path.stem} into {len(platform_counts)} platform files ({total_rows} total rows)")
    
    return platform_counts