import requests
import json
import logging
import os
import pandas as pd
from config import API_SOURCES, RETRY_COUNT, TIMEOUT, INGESTION_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_data(source_id):
    """
    Fetch data from specified source and save the raw file to ingestion directory.
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
            
            # Create ingestion directory if it doesn't exist
            os.makedirs(INGESTION_DIR, exist_ok=True)
            
            # Generate a filename based on source ID and file type
            file_path = os.path.join(INGESTION_DIR, f"{source_id}.{file_type}")
            
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

def read_data_file(file_path):
    """
    Read the downloaded file based on its type and return the data.
    
    Args:
        file_path (str): Path to the downloaded file
        
    Returns:
        dict/DataFrame: The data read from the file or None if failed
    """
    if not file_path or not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        return None
    
    file_extension = os.path.splitext(file_path)[1].lower()
    
    try:
        if file_extension == '.json':
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # If it's a list, wrap it in a dict with 'employees' key
                if isinstance(data, list):
                    return {"employees": data}
                return data
                
        elif file_extension == ".csv":
            try:
                # Open the file manually to check the first few lines before passing it to Pandas
                with open(file_path, "r", encoding="utf-8") as f:
                    lines = [line.strip() for line in f.readlines()]

                # If the file is empty or has only a single row (header with no data), return None
                if not lines or len(lines) < 2:
                    logging.error(f"CSV file {file_path} is empty or contains only a header.")
                    return None

                # Load CSV into a DataFrame
                df = pd.read_csv(file_path, dtype=str, on_bad_lines="skip")

                # Ensure valid CSV: must have at least 2 columns and 2 rows (header + 1 row of data)
                if df.empty or df.shape[1] < 2 or len(df) < 2:
                    logging.error(f"CSV file {file_path} is invalid or contains only one row of data.")
                    return None

                return df

            except (pd.errors.ParserError, UnicodeDecodeError, pd.errors.EmptyDataError, OSError) as e:
                logging.error(f"Failed to read CSV file {file_path}: {str(e)}")
                return None

        elif file_extension in ['.xlsx', '.xls']:
            return pd.read_excel(file_path)
            
        else:
            logging.error(f"Unsupported file format: {file_extension}")
            return None
            
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def fetch_employee_data(source_id="employees_json"):
    """
    Legacy function to maintain backward compatibility.
    Now just calls the new functions.
    """
    file_path = fetch_data(source_id)
    if file_path:
        return read_data_file(file_path)
    return None