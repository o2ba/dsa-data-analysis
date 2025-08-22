"""
This script orchestrates the backfill process for the data-lander.

It identifies missing data files in an S3 raw bucket by comparing against a manifest,
then triggers AWS ECS Fargate tasks to download the missing files.
Each Fargate task is launched with an override for the 'DOWNLOAD_URL' environment variable.
"""
import boto3
import json
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from botocore.exceptions import ClientError
import argparse
import time

class DataDumpBackfiller:
    def __init__(self, 
                 cluster_name: str,
                 task_definition: str,
                 subnets: List[str],
                 security_groups: List[str],
                 s3_bucket: str,
                 s3_prefix: str = "global-full",
                 region: str = 'us-east-1',
                 assign_public_ip: bool = True):
        self.cluster_name = cluster_name
        self.task_definition = task_definition
        self.subnets = subnets
        self.security_groups = security_groups
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.region = region
        self.assign_public_ip = assign_public_ip
        self.ecs_client = boto3.client('ecs', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
    
    def load_manifest(self, manifest_path: str) -> List[Dict]:
        """Load manifest from file or URL"""
        if manifest_path.startswith('http'):
            response = requests.get(manifest_path)
            response.raise_for_status()
            return response.json()
        else:
            with open(manifest_path, 'r') as f:
                return json.load(f)
    
    def get_existing_dates_from_manifest(self, manifest_data: List[Dict]) -> Set[str]:
        """Extract existing dates from manifest"""
        return {entry['date'] for entry in manifest_data}
    
    def get_existing_dates_from_s3(self) -> Set[str]:
        """Get existing dates from S3 bucket structure (global-full/yyyy/mm/dd)"""
        existing_dates = set()
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.s3_bucket,
                Prefix=f"{self.s3_prefix}/",
                Delimiter='/'
            )
            
            for page in pages:
                # Get year prefixes
                for prefix in page.get('CommonPrefixes', []):
                    year_prefix = prefix['Prefix']
                    
                    # List months for this year
                    month_pages = paginator.paginate(
                        Bucket=self.s3_bucket,
                        Prefix=year_prefix,
                        Delimiter='/'
                    )
                    
                    for month_page in month_pages:
                        for month_prefix in month_page.get('CommonPrefixes', []):
                            # List days for this month
                            day_pages = paginator.paginate(
                                Bucket=self.s3_bucket,
                                Prefix=month_prefix['Prefix'],
                                Delimiter='/'
                            )
                            
                            for day_page in day_pages:
                                for day_prefix in day_page.get('CommonPrefixes', []):
                                    # Extract date from path: global-full/2025/08/18/
                                    path_parts = day_prefix['Prefix'].strip('/').split('/')
                                    if len(path_parts) >= 4:
                                        year, month, day = path_parts[1:4]
                                        date_str = f"{year}-{month}-{day}"
                                        existing_dates.add(date_str)
        
        except ClientError as e:
            print(f"Error listing S3 objects: {e}")
        
        return existing_dates
    
    def generate_date_range(self, start_date: str, end_date: str, skip: int = 1) -> List[str]:
        """Generate date range with skip parameter"""
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        dates = []
        current = start
        while current <= end:
            dates.append(current.strftime('%Y-%m-%d'))
            current += timedelta(days=skip)
        
        return dates
    
    def find_missing_dates(self, 
                          manifest_data: List[Dict], 
                          start_date: str, 
                          end_date: str, 
                          skip: int = 1,
                          check_s3: bool = True) -> List[Dict]:
        """Find missing dates and their corresponding URLs"""
        # Get available dates from manifest
        manifest_dates = self.get_existing_dates_from_manifest(manifest_data)
        manifest_dict = {entry['date']: entry for entry in manifest_data}
        
        # Get existing dates from S3 if requested
        existing_s3_dates = set()
        if check_s3:
            existing_s3_dates = self.get_existing_dates_from_s3()
            print(f"Found {len(existing_s3_dates)} existing dates in S3")
        
        # Generate expected date range
        expected_dates = self.generate_date_range(start_date, end_date, skip)
        
        missing_entries = []
        for date in expected_dates:
            # Check if date exists in manifest
            if date not in manifest_dates:
                print(f"Date {date} not found in manifest")
                continue
            
            # Check if date already exists in S3
            if check_s3 and date in existing_s3_dates:
                print(f"Date {date} already exists in S3, skipping")
                continue
            
            missing_entries.append(manifest_dict[date])
        
        return missing_entries
    
    def start_fargate_task(self, download_url: str, date: str) -> Optional[str]:
        """Start a Fargate task with the download URL as environment override"""
        try:
            response = self.ecs_client.run_task(
                cluster=self.cluster_name,
                taskDefinition=self.task_definition,
                launchType='FARGATE',
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': self.subnets,
                        'securityGroups': self.security_groups,
                        'assignPublicIp': 'ENABLED' if self.assign_public_ip else 'DISABLED'
                    }
                },
                overrides={
                    'containerOverrides': [
                        {
                            'name': 'data-lander',  # Replace with your container name
                            'environment': [
                                {
                                    'name': 'DOWNLOAD_URL',
                                    'value': download_url
                                },
                                {
                                    'name': 'TARGET_DATE',
                                    'value': date
                                }
                            ]
                        }
                    ]
                },
                count=1,
                tags=[
                    {
                        'key': 'Purpose',
                        'value': 'DataBackfill'
                    },
                    {
                        'key': 'Date',
                        'value': date
                    }
                ]
            )
            
            if response['tasks']:
                task_arn = response['tasks'][0]['taskArn']
                print(f"Started task for {date}: {task_arn}")
                return task_arn
            else:
                print(f"Failed to start task for {date}")
                return None
                
        except ClientError as e:
            print(f"Error starting task for {date}: {e}")
            return None
    
    def backfill_missing_data(self, 
                             manifest_path: str,
                             start_date: str,
                             end_date: str,
                             skip: int = 1,
                             max_concurrent: int = 5,
                             use_light: bool = False,
                             check_s3: bool = True,
                             dry_run: bool = False) -> List[str]:
        """Main backfill orchestration method"""
        print(f"Loading manifest from {manifest_path}")
        manifest_data = self.load_manifest(manifest_path)
        
        print(f"Finding missing data between {start_date} and {end_date} (skip={skip})")
        missing_entries = self.find_missing_dates(
            manifest_data, start_date, end_date, skip, check_s3
        )
        
        print(f"Found {len(missing_entries)} missing entries")
        
        if dry_run:
            print("DRY RUN - Would start tasks for:")
            for entry in missing_entries:
                url_key = 'light_zip_url' if use_light else 'full_zip_url'
                print(f"  {entry['date']}: {entry[url_key]}")
            return []
        
        started_tasks = []
        running_tasks = []
        
        for entry in missing_entries:
            # Wait if we've hit the concurrent limit
            while len(running_tasks) >= max_concurrent:
                print(f"Waiting for tasks to complete (running: {len(running_tasks)})")
                time.sleep(30)
                running_tasks = self._get_running_tasks(running_tasks)
            
            # Start new task
            url_key = 'light_zip_url' if use_light else 'full_zip_url'
            download_url = entry[url_key]
            
            task_arn = self.start_fargate_task(download_url, entry['date'])
            if task_arn:
                started_tasks.append(task_arn)
                running_tasks.append(task_arn)
        
        print(f"Started {len(started_tasks)} backfill tasks")
        return started_tasks
    
    def _get_running_tasks(self, task_arns: List[str]) -> List[str]:
        """Filter task ARNs to only return those still running"""
        if not task_arns:
            return []
        
        try:
            response = self.ecs_client.describe_tasks(
                cluster=self.cluster_name,
                tasks=task_arns
            )
            
            running = []
            for task in response['tasks']:
                if task['lastStatus'] in ['PENDING', 'RUNNING']:
                    running.append(task['taskArn'])
            
            return running
        except ClientError as e:
            print(f"Error checking task status: {e}")
            return task_arns  # Assume still running on error

def main():
    parser = argparse.ArgumentParser(description='Backfill missing data dumps')
    parser.add_argument('--manifest', required=True, help='Path or URL to manifest.json')
    parser.add_argument('--start-date', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--skip', type=int, default=1, help='Skip every N days (default: 1)')
    parser.add_argument('--max-concurrent', type=int, default=5, help='Max concurrent tasks')
    parser.add_argument('--cluster', required=True, help='ECS cluster name')
    parser.add_argument('--task-definition', required=True, help='ECS task definition')
    parser.add_argument('--subnets', required=True, nargs='+', help='Subnet IDs')
    parser.add_argument('--security-groups', required=True, nargs='+', help='Security group IDs')
    parser.add_argument('--s3-bucket', required=True, help='S3 bucket name', default=os.getenv('S3_BUCKET_NAME'))
    parser.add_argument('--s3-prefix', default='global-full', help='S3 prefix (default: global-full)')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    parser.add_argument('--use-light', action='store_true', help='Use light ZIP URLs instead of full')
    parser.add_argument('--no-s3-check', action='store_true', help='Skip checking existing files in S3')
    parser.add_argument('--dry-run', action='store_true', help='Show what would be done without executing')
    
    args = parser.parse_args()
    
    backfiller = DataDumpBackfiller(
        cluster_name=args.cluster,
        task_definition=args.task_definition,
        subnets=args.subnets,
        security_groups=args.security_groups,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        region=args.region
    )
    
    started_tasks = backfiller.backfill_missing_data(
        manifest_path=args.manifest,
        start_date=args.start_date,
        end_date=args.end_date,
        skip=args.skip,
        max_concurrent=args.max_concurrent,
        use_light=args.use_light,
        check_s3=not args.no_s3_check,
        dry_run=args.dry_run
    )
    
    if not args.dry_run:
        print(f"Backfill complete. Started {len(started_tasks)} tasks.")

if __name__ == '__main__':
    main()