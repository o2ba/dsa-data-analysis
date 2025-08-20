from dotenv import load_dotenv
from src.utils import generate_urls
import datetime


def main():
    urls: list[str] = list(
        generate_urls(START_DATE, STOP_DATE, INTERVAL_DAYS, URL_PREFIX, URL_SUFFIX)
    )

    # TODO Introscpt S3 and see which files are already there, minus them from urls
    # s3_urls = ...
    print(f"Downloading {len(urls)} files...")


if __name__ == "__main__":
    main()