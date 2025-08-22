"""
    This script orchestrates the backfill process for the data-lander.

    It identifies missing data files in an S3 raw bucket by comparing against a manifest,
    then triggers AWS ECS Fargate tasks to download the missing files.
    Each Fargate task is launched with an override for the 'DOWNLOAD_URL' environment variable,
"""
import boto3
import json


