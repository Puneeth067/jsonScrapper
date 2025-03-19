import os
import logging
import json
import sys

# Add the parent directory to sys.path to import from processing
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processor import read_raw_data, normalize_data, save_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def load_config():
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()
RAW_DATA_SOURCES = CONFIG["RAW_DATA_SOURCES"]

def get_source_by_id(source_id):
    """Get source key based on ID."""
    for source_key, source_data in RAW_DATA_SOURCES.items():
        if source_data["id"] == source_id:
            return source_key
    return None

def process_source(source_key):
    """Process a single data source by key."""
    if source_key not in RAW_DATA_SOURCES:
        logging.error(f"Source key '{source_key}' not found in configuration.")
        return False
    
    source_config = RAW_DATA_SOURCES[source_key]
    source_type = source_config["type"]
    
    # Read the raw data file
    raw_data = read_raw_data(source_key)
    if raw_data is None:
        logging.error(f"Failed to read raw data for source: {source_key}")
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
    processor_info = event.get("processor_input", {})
    processor_name = processor_info.get("processor_name", "unknown_processor")
    run_id = processor_info.get("run_processor_id", "000")
    
    logging.info(f"Running Processor: {processor_name} | Run ID: {run_id}")
    
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
    logging.info(f"Processing all sources: {list(RAW_DATA_SOURCES.keys())}")
    
    success_count = 0
    fail_count = 0
    
    for source_key in RAW_DATA_SOURCES.keys():
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
        "processor_input": {
            "processor_name": "data_processor",
            "run_processor_id": "102"
        }
    }
    result = lambdaHandler(inputDA, "")
    print(json.dumps(result, indent=2))