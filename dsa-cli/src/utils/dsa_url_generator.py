from typing import List

def generate_urls(dates: List[str]) -> List[str]:
    """Generate URLs for the given dates."""
    base_url = "https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/sor-global-{}-full.zip"
    return [base_url.format(date) for date in dates]