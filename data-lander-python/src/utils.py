import os
import re
import tempfile
from pathlib import Path
from typing import Tuple

import boto3

def get_s3_prefix(url: str) -> str:
    """Extract S3 prefix from URL based on date and variant."""
    is_light = is_light_variant(url)
    year, month, day = get_date_from_url(url)
    
    variant = "global-light" if is_light else "global-full"
    return f"{variant}/{year:04d}/{month:02d}/{day:02d}/"

def get_s3_config():
    """Get S3 client with region configuration."""
    s3_region = os.environ["S3_REGION"]
    print(f"Using AWS region: {s3_region}")
    
    return boto3.client('s3', region_name=s3_region)

def get_file_size(file: tempfile.NamedTemporaryFile) -> float:
    """Get file size in MB."""
    return Path(file.name).stat().st_size / (1024 * 1024)

def get_date_from_url(url: str) -> Tuple[int, int, int]:
    """Extract date from URL pattern."""
    match = re.search(r"global-(\d{4})-(\d{2})-(\d{2})", url)
    if not match:
        raise ValueError("No date found in URL")
    
    return int(match.group(1)), int(match.group(2)), int(match.group(3))

def is_light_variant(url: str) -> bool:
    """Check if URL contains light variant."""
    if not re.search(r"global-\d{4}-\d{2}-\d{2}-(light|full)\.zip", url):
        raise ValueError("Invalid URL format")
    
    return "-light" in url