import os
import json
from typing import List
import boto3
import time
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from s3.date_util import get_existing_dates_from_s3
from utils.date_parser import parse_date_or_range
import typer

app = typer.Typer()

@app.command()
def land_data(
    date: str = typer.Option(..., "--date", "-d", help="Date or date range to land data in YYYY-MM-DD format. Expects a single date (e.g., '2023-10-01') or a range (e.g., '2023-10-01:2023-10-05')"),
    force: bool = typer.Option(False, "--force", "-f", help="Force backfill even if data already exists"),
    max_retries: int = typer.Option(3, "--max-retries", "-r", help="Maximum number of retries for task start failures"),
    max_concurrent: int = typer.Option(15, "--max-concurrent", "-c", help="Maximum concurrent tasks to run"),
):
    dates: List[str] = parse_date_or_range(date)
    
    if len(dates) == 1:
        typer.echo(f"Attempting to land data for {dates[0]}")
    else:
        typer.echo(f"Attempting to land data for {len(dates)} days from {dates[0]} to {dates[-1]}")


    print("Hallo Welt! Land data utility started for date(s):", date)
    
    ...


@app.command()
def merge_to_db(
    date: str = typer.Argument(..., help="Date to merge into database in YYYY-MM-DD format"),
):
    print("Hallo Welt! Merge utility started for date:", date)


def get_running_tasks(ecs_client, cluster_name):
    """Get count of currently running tasks in the cluster"""
    try:
        response = ecs_client.list_tasks(
            cluster=cluster_name,
            desiredStatus='RUNNING'
        )
        return len(response['taskArns'])
    except ClientError:
        return 0

def wait_for_capacity(ecs_client, cluster_name, max_concurrent=15):
    """Wait until we have capacity to launch more tasks"""
    while True:
        running_count = get_running_tasks(ecs_client, cluster_name)
        if running_count < max_concurrent:
            print(f"📊 Current running tasks: {running_count}/{max_concurrent}")
            return
        
        print(f"⏳ Waiting for capacity... ({running_count}/{max_concurrent} tasks running)")
        time.sleep(30)

def start_task_with_retry(ecs_client, task_config, date, max_retries=3):
    """Start a task with exponential backoff retry logic"""
    for attempt in range(max_retries):
        try:
            response = ecs_client.run_task(**task_config)
            
            if response['tasks']:
                task_arn = response['tasks'][0]['taskArn']
                return True, task_arn.split('/')[-1]
            else:
                return False, f"No tasks returned (attempt {attempt + 1})"
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_msg = e.response['Error']['Message']
            
            # Don't retry on certain errors
            if error_code in ['InvalidParameterException', 'AccessDeniedException']:
                return False, f"{error_code}: {error_msg}"
            
            # Retry on throttling/capacity issues
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5  # 5, 10, 20 seconds
                print(f"⚠️  Attempt {attempt + 1} failed for {date}: {error_code}")
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                return False, f"{error_code}: {error_msg}"
        
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) * 5
                print(f"⚠️  Unexpected error for {date} (attempt {attempt + 1}): {e}")
                print(f"⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                return False, str(e)
    
    return False, "Max retries exceeded"

def main_alt():
    load_dotenv()
    
    # Configuration
    max_concurrent = int(os.getenv('MAX_CONCURRENT_TASKS', '15'))
    max_retries = int(os.getenv('MAX_RETRIES', '3'))
    final_retry_attempts = int(os.getenv('FINAL_RETRY_ATTEMPTS', '2'))
    
    # Load manifest - process ALL entries, not just first 50
    with open('manifest.json', 'r') as f:
        manifest = json.load(f)
    
    # AWS clients
    ecs = boto3.client('ecs', region_name=os.environ['AWS_REGION'])
    s3 = boto3.client('s3', region_name=os.environ['S3_REGION'])
    bucket = os.environ['S3_BUCKET_NAME']
    cluster = os.environ['ECS_CLUSTER_NAME']
    
    print("🔍 Checking existing dates in S3...")
    existing_dates = get_existing_dates_from_s3(s3, bucket)
    print(f"Found {len(existing_dates)} existing dates in S3")
    
    # Filter to only missing dates
    missing_entries = [entry for entry in manifest if entry['date'] not in existing_dates]
    print(f"📋 Found {len(missing_entries)} dates to process")
    print(f"⚙️  Config: max_concurrent={max_concurrent}, max_retries={max_retries}")
    
    successful = 0
    failed = 0
    failed_dates = []  # Track failed dates for retry
    
    for i, entry in enumerate(missing_entries, 1):
        url = entry['full_zip_url']
        date = entry['date']
        
        # Wait for capacity before launching
        wait_for_capacity(ecs, cluster, max_concurrent)
        
        print(f"[{i}/{len(missing_entries)}] 🚀 Starting task for {date}")
        
        # Prepare task configuration
        task_config = {
            'cluster': cluster,
            'taskDefinition': os.environ['ECS_TASK_DEFINITION'],
            'startedBy': f"backfill-{date}",
            'capacityProviderStrategy': [
                {
                    'capacityProvider': 'FARGATE_SPOT',
                    'weight': 1
                }
            ],
            'networkConfiguration': {
                'awsvpcConfiguration': {
                    'subnets': os.environ['ECS_SUBNETS'].split(','),
                    'securityGroups': os.environ['ECS_SECURITY_GROUPS'].split(','),
                    'assignPublicIp': 'ENABLED'
                }
            },
            'overrides': {
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
            },
            'tags': [
                {
                    'key': 'Purpose',
                    'value': 'Backfill'
                },
                {
                    'key': 'Date',
                    'value': date
                }
            ]
        }
        
        # Try to start the task with retries
        success, result = start_task_with_retry(ecs, task_config, date, max_retries)
        
        if success:
            print(f"✅ Started: {result}")
            successful += 1
        else:
            print(f"❌ Failed to start task for {date}: {result}")
            failed += 1
            failed_dates.append({'date': date, 'url': url, 'error': result})
        
        # Small delay between launches
        time.sleep(3)
    
    # Retry failed dates once more
    if failed_dates:
        print(f"\n🔄 Final retry round for {len(failed_dates)} failed dates...")
        retry_successful = 0
        
        for retry_entry in failed_dates:
            date = retry_entry['date']
            url = retry_entry['url']
            
            print(f"🔄 Final retry for {date}...")
            wait_for_capacity(ecs, cluster, max_concurrent)
            
            task_config['startedBy'] = f"final-retry-{date}"
            task_config['overrides']['containerOverrides'][0]['environment'][0]['value'] = url
            
            success, result = start_task_with_retry(ecs, task_config, date, final_retry_attempts)
            
            if success:
                print(f"✅ Final retry succeeded: {result}")
                retry_successful += 1
                successful += 1
                failed -= 1
            else:
                print(f"❌ Final retry failed for {date}: {result}")
            
            time.sleep(3)
        
        print(f"🔄 Final retry summary: {retry_successful}/{len(failed_dates)} succeeded")
    
    print(f"\n🎯 Final Summary: {successful} started, {failed} failed")
    print(f"📊 Total processed: {len(missing_entries)} dates")

if __name__ == '__main__':
    app()