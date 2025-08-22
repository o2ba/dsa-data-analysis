import zipfile
import shutil
import tempfile
from pathlib import Path
from typing import List
from loguru import logger

def streamed_unzip(zip_path: Path, extract_to: Path) -> List[Path]:
    """Extract ZIP file contents, handling nested ZIPs recursively."""
    extracted_files = []
    
    with zipfile.ZipFile(zip_path, 'r') as archive:
        for member in archive.infolist():
            entry_name = member.filename
            outpath = extract_to / entry_name
            
            if entry_name.endswith('/'):
                # It's a directory
                outpath.mkdir(parents=True, exist_ok=True)
                continue
            
            # Create parent directories if needed
            outpath.parent.mkdir(parents=True, exist_ok=True)
            
            # Extract the file
            with archive.open(member) as source, open(outpath, 'wb') as target:
                shutil.copyfileobj(source, target)
            
            logger.info(f"  Extracted: {entry_name}")
            extracted_files.append(outpath)
            
            # Handle nested ZIPs
            if entry_name.lower().endswith('.zip'):
                logger.info(f"Found nested zip: {entry_name}, attempting to extract...")
                
                # Create temp file for the inner ZIP
                with tempfile.NamedTemporaryFile(suffix='.zip') as temp_inner_zip:
                    shutil.copy2(outpath, temp_inner_zip.name)
                    
                    # Extract to directory named after the ZIP (without extension)
                    inner_extract_dir = outpath.with_suffix('')
                    inner_extract_dir.mkdir(exist_ok=True)
                    
                    # Recursive call
                    inner_extracted = streamed_unzip(temp_inner_zip, inner_extract_dir)
                    extracted_files.extend(inner_extracted)
    
    return extracted_files