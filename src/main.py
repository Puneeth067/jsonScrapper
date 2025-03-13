import os
import logging
import json
from scraper import fetch_data, read_data_file
from processor import normalize_data, save_data
from config import API_SOURCES

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def process_source(source_id):
    """Process a single data source by ID."""
    if source_id not in API_SOURCES:
        logging.error(f"Source ID '{source_id}' not found in configuration.")
        return False
    
    source_config = API_SOURCES[source_id]
    source_type = source_config["type"]
    
    # Fetch and download the raw data
    file_path = fetch_data(source_id)
    if not file_path:
        logging.error(f"Failed to fetch data for source: {source_id}")
        return False
    
    # Read the downloaded file
    raw_data = read_data_file(file_path)
    if raw_data is None:
        logging.error(f"Failed to read data from file: {file_path}")
        return False
    
    # Normalize and process the data
    processed_df = normalize_data(raw_data, source_type)
    if processed_df is None:
        logging.error(f"Failed to normalize data for source: {source_id}")
        return False
    
    # Save the processed data
    save_data(processed_df, source_id)
    logging.info(f"Successfully processed data for source: {source_id}")
    return True

def lambdaHandler(event, context):
    scraper_info = event.get("scraper_input", {})
    scraper_name = scraper_info.get("scraper_name", "unknown_scraper")
    run_id = scraper_info.get("run_scraper_id", "000")
    
    # Get sources to process - if specified in event, use those, otherwise process all
    sources_to_process = scraper_info.get("sources", list(API_SOURCES.keys()))
    
    logging.info(f"Running Scraper: {scraper_name} | Run ID: {run_id}")
    logging.info(f"Processing sources: {sources_to_process}")
    
    success_count = 0
    fail_count = 0
    
    for source_id in sources_to_process:
        if process_source(source_id):
            success_count += 1
        else:
            fail_count += 1
    
    if fail_count == 0:
        return {
            "statusCode": 200,
            "body": f"Data processing completed successfully for {success_count} sources."
        }
    else:
        return {
            "statusCode": 207,  # Partial success
            "body": f"Data processing completed with {success_count} successes and {fail_count} failures."
        }

if __name__ == "__main__":
    inputDA = {
        "scraper_input": {
            "scraper_name": "data_scraper",
            "run_scraper_id": "101",
            "sources": ["employees_json","employees_csv"]  # Specify sources or leave empty to process all
        }
    }
    result = lambdaHandler(inputDA, "")
    print(json.dumps(result, indent=2))