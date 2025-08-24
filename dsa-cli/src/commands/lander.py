import os
from typing import List
import boto3
from s3.date_util import get_existing_dates_from_s3
from utils.dsa_url_generator import generate_urls
from utils.date_parser import parse_date_or_range
import typer
from rich.console import Console
from rich.text import Text

console = Console()

def filter_existing_urls(urls: List[str], dates: List[str]) -> List[str]:
    """Filter out URLs that already exist in S3."""
    existing_dates = get_existing_dates_from_s3(
        boto3.client('s3'), 
        os.getenv('S3_BUCKET_NAME')
    )
    
    urls_to_process = []
    for i, url in enumerate(urls):
        if dates[i] in existing_dates:
            typer.secho(f"Data for {url} already exists in target S3. Skipping.", fg=typer.colors.YELLOW)
        else:
            urls_to_process.append(url)
    
    return urls_to_process

def lander(
    date: str = typer.Option(..., "--date", "-d", help="Date or date range to land data in YYYY-MM-DD format. Expects a single date (e.g., '2023-10-01') or a range (e.g., '2023-10-01:2023-10-05')"),
    force: bool = typer.Option(False, "--force", "-f", help="Force backfill even if data already exists"),
    max_retries: int = typer.Option(3, "--max-retries", "-r", help="Maximum number of retries for task start failures"),
    max_concurrent: int = typer.Option(15, "--max-concurrent", "-c", help="Maximum concurrent tasks to run"),
):
    dates: List[str] = parse_date_or_range(date)
    
    if len(dates) == 1:
        typer.secho(f"Attempting to land data for {dates[0]}", fg=typer.colors.GREEN, bold=True)
    else:
        typer.secho(f"Attempting to land data for {len(dates)} days from {dates[0]} to {dates[-1]}", fg=typer.colors.GREEN)

    urls: List[str] = generate_urls(dates)
    
    if not force:
        urls = filter_existing_urls(urls, dates)
    
    # Continue with processing urls...