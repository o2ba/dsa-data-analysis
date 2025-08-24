import asyncio
import tempfile
from pathlib import Path

import aiohttp
import aiofiles
from loguru import logger

async def download_zip_to_temp(url: str) -> Path:
    """Download ZIP file to temporary file, streaming the response."""
    logger.info(f"Starting ZIP download from {url}")
    
    # Create temp file but keep it open
    temp_file = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    temp_path = Path(temp_file.name)
    
    timeout = aiohttp.ClientTimeout(total=300)
    
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                
                # Write directly to the temp file
                async for chunk in response.content.iter_chunked(8192):
                    temp_file.write(chunk)
        
        temp_file.close()  # Close after writing
        
        size_bytes = temp_path.stat().st_size
        logger.info(f"Download completed: {temp_path}, {size_bytes / (1024*1024):.2f} MB")
        
        return temp_path
        
    except Exception as e:
        temp_file.close()
        temp_path.unlink(missing_ok=True)  # Clean up on error
        raise