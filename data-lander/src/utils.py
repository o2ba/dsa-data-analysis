from datetime import datetime, timedelta
from typing import Generator
from dateutil.relativedelta import relativedelta

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

def generate_urls(
    start_date: str,
    stop_date: str,
    interval_days: int,
    prefix: str,
    suffix: str,
) -> Generator[str, None, None]:
    """
    Generate URLs for a given date range at fixed intervals.

    Args:
        start_date (str): The start date in "YYYY-MM-DD" format.
        stop_date (str): The stop date in "YYYY-MM-DD" format.
        interval_days (int): The number of days between each generated URL.
        prefix (str): The URL prefix (before the date).
        suffix (str): The URL suffix (after the date).

    Yields:
        str: A URL string for each date in the range.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    stop = datetime.strptime(stop_date, "%Y-%m-%d").date()

    current = start
    while current <= stop:
        yield f"{prefix}{current.strftime('%Y-%m-%d')}{suffix}"
        current += timedelta(days=interval_days)