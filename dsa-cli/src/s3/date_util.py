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