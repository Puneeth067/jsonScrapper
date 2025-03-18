import os
import logging
import json
from ingestion.scraper import fetch_data, read_data_file
from ingestion.processor import normalize_data, save_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "../config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()
API_SOURCES = CONFIG["API_SOURCES"]

def get_source_by_id(source_id):
    """Get source key based on ID."""
    for source_key, source_data in API_SOURCES.items():
        if source_data["id"] == source_id:
            return source_key
    return None

def process_source(source_key):
    """Process a single data source by key."""
    if source_key not in API_SOURCES:
        logging.error(f"Source key '{source_key}' not found in configuration.")
        return False
    
    source_config = API_SOURCES[source_key]
    source_type = source_config["type"]
    
    # Fetch and download the raw data
    file_path = fetch_data(source_key)
    if not file_path:
        logging.error(f"Failed to fetch data for source: {source_key}")
        return False
    
    # Read the downloaded file
    raw_data = read_data_file(file_path)
    if raw_data is None:
        logging.error(f"Failed to read data from file: {file_path}")
        return False
    
    # Normalize and process the data
    processed_df = normalize_data(raw_data, source_type)
    if processed_df is None:
        logging.error(f"Failed to normalize data for source: {source_key}")
        return False
    
    # Save the processed data
    save_data(processed_df, source_key)
    logging.info(f"Successfully processed data for source: {source_key}")
    return True

def lambdaHandler(event, context):
    scraper_info = event.get("scraper_input", {})
    scraper_name = scraper_info.get("scraper_name", "unknown_scraper")
    run_id = scraper_info.get("run_scraper_id", "000")
    
    logging.info(f"Running Scraper: {scraper_name} | Run ID: {run_id}")
    
    try:
        source_id = int(run_id)
        source_key = get_source_by_id(source_id)
        
        if source_key:
            logging.info(f"Processing source with ID {source_id} (key: {source_key})")
            if process_source(source_key):
                return {
                    "statusCode": 200,
                    "body": f"Data processing completed successfully for source ID {source_id}."
                }
            else:
                return {
                    "statusCode": 500,
                    "body": f"Data processing failed for source ID {source_id}."
                }
        else:
            # If source ID not found, process all sources
            logging.info(f"Source ID {source_id} not found. Processing all sources.")
            return process_all_sources()
        
    except ValueError:
        # If run_id is not a valid integer, process all sources
        logging.info(f"Invalid run ID format: {run_id}. Processing all sources.")
        return process_all_sources()

def process_all_sources():
    """Process all available sources."""
    logging.info(f"Processing all sources: {list(API_SOURCES.keys())}")
    
    success_count = 0
    fail_count = 0
    
    for source_key in API_SOURCES.keys():
        if process_source(source_key):
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
            "statusCode": 207,  
            "body": f"Data processing completed with {success_count} successes and {fail_count} failures."
        }

if __name__ == "__main__":
    inputDA = {
        "scraper_input": {
            "scraper_name": "data_scraper",
            "run_scraper_id": "101"  # Source ID
        }
    }
    result = lambdaHandler(inputDA, "")
    print(json.dumps(result, indent=2))