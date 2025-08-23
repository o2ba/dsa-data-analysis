import os
import json
import boto3
import time
from dotenv import load_dotenv
from botocore.exceptions import ClientError

def check_date_exists_in_s3(s3_client, bucket, date):
    """Check if data for a specific date already exists in S3"""
    # Parse date: 2025-08-18 -> global-full/2025/08/18/
    year, month, day = date.split('-')
    prefix = f"global-full/{year}/{month}/{day}/"
    
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1
        )
        return response.get('KeyCount', 0) > 0
    except ClientError:
        return False

def get_existing_dates_from_s3(s3_client, bucket):
    """Get all existing dates from S3 bucket structure"""
    existing_dates = set()
    
    try:
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=bucket,
            Prefix="global-full/",
            Delimiter='/'
        )
        
        for page in pages:
            # Get year prefixes
            for prefix in page.get('CommonPrefixes', []):
                year_prefix = prefix['Prefix']
                
                # List months for this year
                month_pages = paginator.paginate(
                    Bucket=bucket,
                    Prefix=year_prefix,
                    Delimiter='/'
                )
                
                for month_page in month_pages:
                    for month_prefix in month_page.get('CommonPrefixes', []):
                        # List days for this month
                        day_pages = paginator.paginate(
                            Bucket=bucket,
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

def main():
    load_dotenv()
    
    # Load manifest
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)
    
    # Get first 50 entries
    first_50 = manifest[:50]
    
    # AWS clients
    ecs = boto3.client('ecs', region_name=os.environ['AWS_REGION'])
    s3 = boto3.client('s3', region_name=os.environ['S3_REGION'])
    bucket = os.environ['S3_BUCKET_NAME']
    
    print("🔍 Checking existing dates in S3...")
    existing_dates = get_existing_dates_from_s3(s3, bucket)
    print(f"Found {len(existing_dates)} existing dates in S3")
    
    successful = 0
    failed = 0
    skipped = 0
    
    for i, entry in enumerate(first_50, 1):
        url = entry['full_zip_url']
        date = entry['date']
        
        # Check if this date already exists
        if date in existing_dates:
            print(f"[{i}/{len(first_50)}] ⏭️  Skipping {date} - already exists in S3")
            skipped += 1
            continue
        
        print(f"[{i}/{len(first_50)}] 🚀 Starting task for {date}")
        
        try:
            # Start Fargate task with URL override
            response = ecs.run_task(
                cluster=os.environ['ECS_CLUSTER_NAME'],
                taskDefinition=os.environ['ECS_TASK_DEFINITION'],
                startedBy=f"etl-{date}",
                capacityProviderStrategy=[
                    {
                        'capacityProvider': 'FARGATE_SPOT',
                        'weight': 1
                    }
                ],
                networkConfiguration={
                    'awsvpcConfiguration': {
                        'subnets': os.environ['ECS_SUBNETS'].split(','),
                        'securityGroups': os.environ['ECS_SECURITY_GROUPS'].split(','),
                        'assignPublicIp': 'ENABLED'
                    }
                },
                overrides={
                    'containerOverrides': [
                        {
                            'name': os.environ['ECS_CONTAINER_NAME'],
                            'environment': [
                                {
                                    'name': 'URL',
                                    'value': url
                                }
                            ]
                        }
                    ]
                }
            )
            
            if response['tasks']:
                task_arn = response['tasks'][0]['taskArn']
                print(f"✅ Started: {task_arn.split('/')[-1]}")
                successful += 1
            else:
                print(f"❌ Failed to start task for {date}")
                failed += 1
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            print(f"❌ AWS Error for {date}: {error_code} - {e.response['Error']['Message']}")
            failed += 1
            
            # If we hit throttling, wait longer
            if error_code in ['Throttling', 'ThrottlingException']:
                print("⏳ Throttling detected, waiting 10 seconds...")
                time.sleep(10)
            
        except Exception as e:
            print(f"❌ Unexpected error for {date}: {e}")
            failed += 1
        
        # Add delay between launches to avoid throttling
        if i < len(first_50):  # Don't sleep after the last one
            time.sleep(2)
    
    print(f"\n🎯 Summary: {successful} started, {skipped} skipped, {failed} failed")

if __name__ == '__main__':
    main()