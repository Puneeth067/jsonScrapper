import requests
import json
import logging
import os
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configuration from JSON file
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()
API_SOURCES = CONFIG["API_SOURCES"]
RETRY_COUNT = CONFIG["RETRY_COUNT"]
TIMEOUT = CONFIG["TIMEOUT"]
RAW_DATA_DIR = CONFIG["RAW_DATA_DIR"]

def fetch_data(source_id):
    """
    Fetch data from specified source and save the raw file to raw_data directory.
    """
    if source_id not in API_SOURCES:
        logging.error(f"Source ID '{source_id}' not found in configuration.")
        return None
    
    source_config = API_SOURCES[source_id]
    url = source_config["url"]
    file_type = source_config["type"]
    
    attempt = 0
    while attempt < RETRY_COUNT:
        try:
            logging.info(f"Fetching data from {url}")
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            
            # Create raw_data directory if it doesn't exist
            os.makedirs(RAW_DATA_DIR, exist_ok=True)
            
            # Generate a filename based on source ID and file type
            file_path = os.path.join(RAW_DATA_DIR, f"{source_id}.{file_type}")
            
            # Save the raw response content
            with open(file_path, 'wb') as f:
                f.write(response.content)
            
            logging.info(f"Successfully saved raw data to {file_path}")
            return file_path
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")
            
        attempt += 1
        logging.info(f"Retrying... ({attempt}/{RETRY_COUNT})")
    
    logging.error("Max retries reached. Failed to fetch data.")
    return None