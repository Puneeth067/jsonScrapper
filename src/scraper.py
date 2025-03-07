import requests
import json
import logging
from config import API_URL, RETRY_COUNT, TIMEOUT

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def fetch_employee_data():
    attempt = 0

    while attempt < RETRY_COUNT:
        try:
            response = requests.get(API_URL, timeout=TIMEOUT)
            response.raise_for_status()  # Raises an error for 4xx/5xx responses

            # Check if response is JSON or a downloadable file
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' in content_type:
                logging.info("Successfully fetched employee data.")
                json_data = response.json()
                
                # Check if it's already a list, wrap it in a dict with 'employees' key
                if isinstance(json_data, list):
                    return {"employees": json_data}
                return json_data

            # If it's a file, try to parse it as JSON
            logging.warning("API returned a file, attempting to parse as JSON.")
            json_data = json.loads(response.text)
            
            # Check if it's already a list, wrap it in a dict with 'employees' key
            if isinstance(json_data, list):
                return {"employees": json_data}
            return json_data

        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed: {e}")

        attempt += 1
        logging.info(f"Retrying... ({attempt}/{RETRY_COUNT})")

    logging.error("Max retries reached. Failed to fetch data.")
    return None