# Description: Configuration file for the project
API_SOURCES = {
    "employees_json": {
        "id": 1,
        "url": "https://api.slingacademy.com/v1/sample-data/files/employees.json",
        "type": "json",
    },
    "employees_csv": {
        "id": 2,
        "url": "https://api.slingacademy.com/v1/sample-data/files/employees.csv",
        "type": "csv",
    },
    # "employees_xlsx": {
    #     "id": 3,
    #     "url": "https://api.slingacademy.com/v1/sample-data/files/employees.xlsx",
    #     "type": "xlsx",
    # },
    # Can Add more sources as needed
}
RETRY_COUNT = 3  
TIMEOUT = 5  
INGESTION_DIR = "../ingestion/"