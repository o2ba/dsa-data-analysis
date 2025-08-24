from datetime import datetime, timedelta
from typing import List, Optional
import typer

def parse_date_or_range(
    date_input: str, 
    min_date: Optional[str] = "2023-09-25"
) -> List[str]:
    """Parse a single date or date range into a list of dates."""
    DATE_FORMAT = "%Y-%m-%d"
    today = datetime.now().date()
    
    # Parse minimum date
    try:
        min_date_obj = datetime.strptime(min_date, DATE_FORMAT).date() if min_date else None
    except ValueError:
        typer.echo(f"Error: Invalid minimum date format '{min_date}'. Expected YYYY-MM-DD.", err=True)
        raise typer.Exit(code=1)
    
    def parse_single_date(date_str: str) -> datetime:
        try:
            parsed = datetime.strptime(date_str, DATE_FORMAT)
        except ValueError:
            typer.echo(f"Error: Invalid date '{date_str}'. Expected YYYY-MM-DD format.", err=True)
            raise typer.Exit(code=1)
        
        date_obj = parsed.date()
        
        if date_obj > today:
            typer.echo(f"Error: Date '{date_str}' cannot be in the future.", err=True)
            raise typer.Exit(code=1)
        
        if min_date_obj and date_obj < min_date_obj:
            typer.echo(f"Error: Date '{date_str}' cannot be before {min_date}.", err=True)
            raise typer.Exit(code=1)
            
        return parsed
    
    if ':' in date_input:
        start_str, end_str = date_input.split(':', 1)
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