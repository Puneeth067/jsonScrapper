import os
import logging
from processor import fetch_employee_data, normalize_data, save_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def lambdaHandler(event, context):
    """AWS Lambda Handler Function"""
    scraper_info = event.get("scraper_input", {})
    scraper_name = scraper_info.get("scraper_name", "unknown_scraper")
    run_id = scraper_info.get("run_scraper_id", "000")

    logging.info(f"Running Scraper: {scraper_name} | Run ID: {run_id}")

    # Fetch and process data
    raw_data = fetch_employee_data()
    if raw_data:
        processed_df = normalize_data(raw_data)
        if processed_df is not None:
            save_data(processed_df)
            return {"statusCode": 200, "body": "Data processing completed successfully."}
    
    return {"statusCode": 500, "body": "Data processing failed."}

if __name__ == "__main__":
    inputDA = {
        "scraper_input": {
            "scraper_name": "employee_scraper",
            "run_scraper_id": "101"
        }
    }
    lambdaHandler(inputDA, "")
