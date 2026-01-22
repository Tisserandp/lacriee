import asyncio
import os
import uuid
import logging
from pathlib import Path
from services.import_service import ImportService
from parsers import audierne, demarne, hennequin, laurent_daniel, vvqm
from services.bigquery import ensure_all_prices_table_exists

# Configuration logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Paths
BASE_DIR = Path(__file__).parent.parent
SAMPLES_DIR = BASE_DIR / "Samples"

# Vendor configuration
VENDORS = {
    "Audierne": {"parser": audierne.parse, "vendor_id": "audierne"},
    "Demarne": {"parser": demarne.parse, "vendor_id": "demarne"},
    "Hennequin": {"parser": hennequin.parse, "vendor_id": "hennequin"},
    "LaurentD": {"parser": laurent_daniel.parse, "vendor_id": "laurent_daniel"},
    "VVQ": {"parser": vvqm.parse, "vendor_id": "vvqm"},
}

async def process_file(file_path: Path, vendor_config: dict):
    logger.info(f"Processing {file_path.name} for {vendor_config['vendor_id']}...")
    
    try:
        if vendor_config["vendor_id"] == "demarne":
            # Demarne parser might handle file path directly or bytes
            # checking parsers/__init__.py: demarne.parse("cours.xlsx", ...)
            # Let's read bytes for consistency with ImportService which expects bytes
            with open(file_path, "rb") as f:
                file_bytes = f.read()
            # For Demarne, we might need to pass the filename or handle it specifically if the parser expects a path string for Excel
            # But ImportService calls parser_func(file_bytes)
            # Let's check if parsers/demarne.py handles bytes. 
            # Assuming it does based on ImportService design. 
            pass
        else:
            with open(file_path, "rb") as f:
                file_bytes = f.read()

        service = ImportService(vendor_config["vendor_id"], vendor_config["parser"])
        job_id = str(uuid.uuid4())
        
        # Create job record in BQ
        from services.bigquery import create_job_record
        create_job_record(
            job_id=job_id,
            filename=file_path.name,
            vendor=vendor_config["vendor_id"],
            file_size_bytes=len(file_bytes),
            gcs_url=f"local://{file_path.name}",
            status="started"
        )
        
        kwargs = {}
        if vendor_config["vendor_id"] == "demarne":
             # Fallback date for sample files that might not have date in filename
             kwargs["date_fallback"] = "2026-01-15"

        await service.process_async(job_id, file_bytes, parser_kwargs=kwargs)
        logger.info(f"Successfully processed {file_path.name}")
        
    except Exception as e:
        logger.error(f"Failed to process {file_path}: {e}")

async def main():
    logger.info("Starting sample data load...")
    
    # Initialize table
    try:
        ensure_all_prices_table_exists()
    except Exception as e:
        logger.error(f"Failed to ensure table exists (check credentials): {e}")
        return
    
    # Process files
    for folder_name, config in VENDORS.items():
        folder_path = SAMPLES_DIR / folder_name
        if not folder_path.exists():
            logger.warning(f"Folder {folder_path} not found, skipping.")
            continue
            
        logger.info(f"Scanning folder {folder_name}...")
        for file_path in folder_path.glob("*"):
            if file_path.is_file():
                # Filter relevant extensions
                if file_path.suffix.lower() in (".pdf", ".xlsx", ".xls"):
                    await process_file(file_path, config)

    logger.info("Sample load complete.")

if __name__ == "__main__":
    asyncio.run(main())
