from datetime import datetime, timedelta
from typing import List
import typer

def parse_date_or_range(date_input: str) -> List[str]:
    """Parse a single date or date range into a list of dates."""
    DATE_FORMAT = "%Y-%m-%d"
    
    def parse_single_date(date_str: str) -> datetime:
        try:
            return datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            typer.echo(f"Error: Invalid date '{date_str}'. Expected YYYY-MM-DD format.", err=True)
            raise typer.Exit(code=1)
    
    if ':' in date_input:
        start_str, end_str = date_input.split(':', 1)  # Limit split to 1
        start_date = parse_single_date(start_str)
        end_date = parse_single_date(end_str)
        
        if start_date > end_date:
            typer.echo(f"Error: Start date cannot be after end date.", err=True)
            raise typer.Exit(code=1)
        
        delta = (end_date - start_date).days
        return [(start_date + timedelta(days=i)).strftime(DATE_FORMAT) 
                for i in range(delta + 1)]
    else:
        return [parse_single_date(date_input).strftime(DATE_FORMAT)]