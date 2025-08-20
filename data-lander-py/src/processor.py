import boto3
import zipfile
import pyarrow.csv as pv
import pyarrow.parquet as pq
import tempfile
import os

s3 = boto3.client("s3")

def get_next_date(last_date: str, interval: int = 6) -> str:
    """
    Returns the next date after adding `interval` months to `last_date`.

    Args:
        last_date (str): Date in 'YYYY-MM-DD' format.
        interval (int): Number of months to add (default: 6).

    Returns:
        str: New date in 'YYYY-MM-DD' format.
    """
    date_obj = datetime.strptime(last_date, "%Y-%m-%d")
    next_date = date_obj + relativedelta(months=interval)
    return next_date.strftime("%Y-%m-%d")

def download_zip(zip_url):
    # Download the zip file from the provided URL
    response = requests.get(zip_url)
    zip_file = zipfile.ZipFile(io.BytesIO(response.content))
    return zip_file

# Unzips a .parquet.zip to get all the parquet files
def process_zip(zip_url):
    ...