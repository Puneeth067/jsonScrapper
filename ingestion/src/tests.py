import os
import unittest
import json
import pandas as pd
import tempfile
import shutil
from unittest.mock import patch, call, MagicMock

import sys
import logging

# Add the parent directory to the path to import local modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from ingestion.scraper import fetch_data, read_data_file, fetch_employee_data
from ingestion.processor import normalize_data, save_data
from ingestion.src.main import process_source, lambdaHandler

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

class TestDataPipeline(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment before each test."""
        # Create a temporary directory for test data
        self.temp_dir = tempfile.mkdtemp()
        
        # Create mock configuration
        self.mock_config = {
            "API_SOURCES": {
                "employees_json": {
                    "id": 1,
                    "url": "https://api.slingacademy.com/v1/sample-data/files/employees.json",
                    "type": "json"
                },
                "employees_csv": {
                    "id": 2,
                    "url": "https://api.slingacademy.com/v1/sample-data/files/employees.csv",
                    "type": "csv"
                }
            },
            "RETRY_COUNT": 3,
            "TIMEOUT": 5,
            "INGESTION_DIR": self.temp_dir + '/'
        }
        
        # Patch the configuration loading functions
        self.patcher_processor = patch('ingestion.processor.load_config')
        self.mock_processor_config = self.patcher_processor.start()
        self.mock_processor_config.return_value = self.mock_config
        
        self.patcher_scraper = patch('ingestion.scraper.load_config')
        self.mock_scraper_config = self.patcher_scraper.start()
        self.mock_scraper_config.return_value = self.mock_config
        
        self.patcher_main = patch('ingestion.src.main.load_config')
        self.mock_main_config = self.patcher_main.start()
        self.mock_main_config.return_value = self.mock_config
        
        # Create sample test data
        self.create_test_data()

    def tearDown(self):
        """Clean up after each test."""
        # Stop all patchers
        self.patcher_processor.stop()
        self.patcher_scraper.stop()
        self.patcher_main.stop()
        
        # Remove temporary directory
        shutil.rmtree(self.temp_dir)
    
    def create_test_data(self):
        """Create sample data for testing."""
        # Sample JSON data
        self.json_data = {
            "employees": [
                {
                    "id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john.doe@example.com",
                    "phone": "123-456-7890",
                    "gender": "Male",
                    "age": 32,
                    "job_title": "Data Engineer",
                    "years_of_experience": 5,
                    "salary": 90000,
                    "department": "Engineering"
                },
                {
                    "id": 2,
                    "first_name": "Jane",
                    "last_name": "Smith",
                    "email": "jane.smith@example.com",
                    "phone": "987-654-3210",
                    "gender": "Female",
                    "age": 28,
                    "job_title": "Data Scientist",
                    "years_of_experience": 3,
                    "salary": 85000,
                    "department": "Data Science"
                }
            ]
        }
        
        # Sample CSV data
        self.csv_data = pd.DataFrame([
            {
                "employee_id": 1,
                "employee_first_name": "John",
                "employee_last_name": "Doe",
                "employee_email": "john.doe@example.com",
                "employee_phone": "123-456-7890",
                "employee_gender": "Male",
                "employee_age": 32,
                "employee_job_title": "Data Engineer",
                "employee_experience": 5,
                "employee_salary": 90000,
                "employee_department": "Engineering"
            },
            {
                "employee_id": 2,
                "employee_first_name": "Jane",
                "employee_last_name": "Smith",
                "employee_email": "jane.smith@example.com",
                "employee_phone": "987-654-3210",
                "employee_gender": "Female",
                "employee_age": 28,
                "employee_job_title": "Data Scientist",
                "employee_experience": 3,
                "employee_salary": 85000,
                "employee_department": "Data Science"
            }
        ])
        
        # Sample Excel data (same structure as CSV but in Excel format)
        self.excel_data = self.csv_data.copy()
        
        # Save test data to files
        self.json_file_path = os.path.join(self.temp_dir, "test_employees.json")
        self.csv_file_path = os.path.join(self.temp_dir, "test_employees.csv")
        self.excel_file_path = os.path.join(self.temp_dir, "test_employees.xlsx")
        
        with open(self.json_file_path, 'w') as f:
            json.dump(self.json_data, f)
        
        self.csv_data.to_csv(self.csv_file_path, index=False)
        self.excel_data.to_excel(self.excel_file_path, index=False)
    
    @patch('requests.get')
    def test_fetch_data(self, mock_get):
        """Test Case 1: Verify file download for different formats."""
        # Setup mock response for different file types
        for source_id, extension in [
            ("employees_json", "json"),
            ("employees_csv", "csv")
        ]:
            # Setup mock
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_response.raise_for_status.return_value = None
            
            # Set content based on file type
            if extension == "json":
                with open(self.json_file_path, 'rb') as f:
                    mock_response.content = f.read()
            elif extension == "csv":
                with open(self.csv_file_path, 'rb') as f:
                    mock_response.content = f.read()
            
            mock_get.return_value = mock_response
            
            # Execute
            file_path = fetch_data(source_id)
            
            # Verify
            self.assertIsNotNone(file_path)
            self.assertTrue(os.path.exists(file_path))
            self.assertTrue(file_path.endswith(extension))
            
            # Clean up the downloaded file
            if os.path.exists(file_path):
                os.remove(file_path)
    
    def test_read_data_file(self):
        """Test Case 2: Verify file extraction for different formats."""
        # Test JSON file extraction
        json_data = read_data_file(self.json_file_path)
        self.assertIsNotNone(json_data)
        self.assertIn("employees", json_data)
        self.assertEqual(len(json_data["employees"]), 2)
        
        # Test CSV file extraction
        csv_data = read_data_file(self.csv_file_path)
        self.assertIsNotNone(csv_data)
        self.assertIsInstance(csv_data, pd.DataFrame)
        self.assertEqual(len(csv_data), 2)
        
        # Test Excel file extraction
        excel_data = read_data_file(self.excel_file_path)
        self.assertIsNotNone(excel_data)
        self.assertIsInstance(excel_data, pd.DataFrame)
        self.assertEqual(len(excel_data), 2)
        
        # Test nonexistent file
        no_data = read_data_file("nonexistent_file.txt")
        self.assertIsNone(no_data)
    
    def test_validate_file_type_and_format(self):
        """Test Case 3: Validate file type and format."""
        # Create invalid files
        invalid_json_path = os.path.join(self.temp_dir, "invalid.json")
        invalid_csv_path = os.path.join(self.temp_dir, "invalid.csv")
        
        # Write invalid JSON
        with open(invalid_json_path, 'w') as f:
            f.write("{invalid json")
        
        # Write invalid CSV (just a header, no data)
        with open(invalid_csv_path, 'w') as f:
            f.write("header1,header2\n")
        
        # Test invalid JSON
        invalid_json_data = read_data_file(invalid_json_path)
        self.assertIsNone(invalid_json_data)
        
        # Test invalid CSV
        with patch('logging.error') as mock_log:
            invalid_csv_data = read_data_file(invalid_csv_path)
            # Might return an empty DataFrame, but should log an error
            mock_log.assert_called()
        
        # Test unsupported file type
        with open(os.path.join(self.temp_dir, "test.txt"), 'w') as f:
            f.write("This is a text file")
        
        with patch('logging.error') as mock_log:
            unsupported_data = read_data_file(os.path.join(self.temp_dir, "test.txt"))
            self.assertIsNone(unsupported_data)
            mock_log.assert_called_with("Unsupported file format: .txt")
    
    def test_validate_data_structure(self):
        """Test Case 4: Validate data structure."""
        # JSON data normalization
        normalized_json = normalize_data(self.json_data, 'json')
        self.assertIsNotNone(normalized_json)
        
        # Verify expected columns
        expected_columns = ["id", "Full Name", "email", "phone", "gender", "age", 
                           "job_title", "years_of_experience", "salary", "department", "designation"]
        
        for col in expected_columns:
            self.assertIn(col, normalized_json.columns)
        
        # Verify data values
        self.assertEqual(normalized_json.iloc[0]["Full Name"], "John Doe")
        self.assertEqual(normalized_json.iloc[1]["Full Name"], "Jane Smith")
        
        # Verify designation assignment
        self.assertEqual(normalized_json.iloc[0]["designation"], "Data Engineer")
        self.assertEqual(normalized_json.iloc[1]["designation"], "Data Engineer")
        
        # CSV data normalization
        normalized_csv = normalize_data(self.csv_data, 'csv')
        self.assertIsNotNone(normalized_csv)
        
        # Verify column standardization in CSV
        for col in expected_columns:
            self.assertIn(col, normalized_csv.columns)
        
        # Excel data normalization
        normalized_excel = normalize_data(self.excel_data, 'xlsx')
        self.assertIsNotNone(normalized_excel)
        
        # Verify column standardization in Excel
        for col in expected_columns:
            self.assertIn(col, normalized_excel.columns)
    
    def test_handle_missing_invalid_data(self):
        """Test Case 5: Handle missing or invalid data."""
        # Create data with missing values
        missing_data = {
            "employees": [
                {
                    "id": None,
                    "first_name": "John",
                    "last_name": None,
                    "email": None,
                    "phone": "x123-4321",  # Invalid phone format
                    "gender": "Male",
                    "age": None,
                    "years_of_experience": "unknown",  # Changed from 'invalid' to 'unknown'
                },
                {
                    # Missing id completely
                    "first_name": "Jane",
                    "gender": "Female",
                    # Missing other fields
                }
            ]
        }
        
        # Process the data with missing values
        normalized_missing = normalize_data(missing_data, 'json')
        self.assertIsNotNone(normalized_missing)
        
        # Verify proper handling of missing data
        self.assertEqual(len(normalized_missing), 2)
        
        # Check that IDs were assigned
        self.assertTrue(all(normalized_missing["id"].notna()))
        self.assertTrue(all(normalized_missing["id"] > 0))
        
        # Check that phone was marked as invalid
        self.assertEqual(normalized_missing.iloc[0]["phone"], "Invalid Number")
        
        # Check that designation was assigned properly for invalid experience
        self.assertEqual(normalized_missing.iloc[0]["designation"], "Unknown")
        
        # Check that missing fields were filled with defaults
        self.assertEqual(normalized_missing.iloc[1]["salary"], 0)
        self.assertEqual(normalized_missing.iloc[1]["years_of_experience"], 0)
        self.assertEqual(normalized_missing.iloc[1]["department"], "")
    
    def test_process_source(self):
        """Test the entire process_source function with mocks."""
        # Setup patchers for each step
        with patch('ingestion.src.main.fetch_data') as mock_fetch, \
             patch('ingestion.src.main.read_data_file') as mock_read, \
             patch('ingestion.src.main.normalize_data') as mock_normalize, \
             patch('ingestion.src.main.save_data') as mock_save:
            
            # Setup mocks
            mock_fetch.return_value = "mock_file_path.json"
            mock_read.return_value = self.json_data
            mock_normalize.return_value = pd.DataFrame({"id": [1, 2], "Full Name": ["John Doe", "Jane Smith"]})
            mock_save.return_value = None
            
            # Execute
            result = process_source("employees_json")
            
            # Verify
            self.assertTrue(result)
            mock_fetch.assert_called_once_with("employees_json")
            mock_read.assert_called_once_with("mock_file_path.json")
            mock_normalize.assert_called_once()
            mock_save.assert_called_once()
            
            # Test with nonexistent source
            result = process_source("nonexistent_source")
            self.assertFalse(result)
            
            # Test with fetch failure
            mock_fetch.return_value = None
            result = process_source("employees_json")
            self.assertFalse(result)
            
            # Test with read failure
            mock_fetch.return_value = "mock_file_path.json"
            mock_read.return_value = None
            result = process_source("employees_json")
            self.assertFalse(result)
            
            # Test with normalize failure
            mock_read.return_value = self.json_data
            mock_normalize.return_value = None
            result = process_source("employees_json")
            self.assertFalse(result)
    
    @patch('ingestion.src.main.process_source')
    def test_lambda_handler(self, mock_process):
        """Test the lambdaHandler function."""
        # Setup mock
        mock_process.return_value = True
        
        # Test with specific sources
        event = {
            "scraper_input": {
                "scraper_name": "test_scraper",
                "run_scraper_id": "123",
                "sources": ["employees_json", "employees_csv"]
            }
        }
        
        result = lambdaHandler(event, None)
        self.assertEqual(result["statusCode"], 200)
        self.assertEqual(mock_process.call_count, 2)
        
        # Reset mock
        mock_process.reset_mock()
        
        # Test with numeric run_id as source ID
        event = {
            "scraper_input": {
                "scraper_name": "test_scraper",
                "run_scraper_id": "1"  # Source ID for employees_json
            }
        }
        
        result = lambdaHandler(event, None)
        self.assertEqual(result["statusCode"], 200)
        expected_sources = ["employees_json", "employees_csv"]  
        mock_process.assert_has_calls([call(source) for source in expected_sources], any_order=True)
        self.assertEqual(mock_process.call_count, len(expected_sources))  
        
        # Reset mock
        mock_process.reset_mock()
        
        # Test with partial failures
        mock_process.side_effect = [True, False]
        
        event = {
            "scraper_input": {
                "scraper_name": "test_scraper",
                "run_scraper_id": "123",
                "sources": ["employees_json", "employees_csv"]
            }
        }
        
        result = lambdaHandler(event, None)
        self.assertEqual(result["statusCode"], 207)
        self.assertEqual(mock_process.call_count, 2)
    
    def test_save_data(self):
        """Test saving data to CSV and Parquet."""
        # Create test DataFrame
        df = pd.DataFrame({
            "id": [1, 2],
            "Full Name": ["John Doe", "Jane Smith"],
            "email": ["john@example.com", "jane@example.com"]
        })
        
        # Make sure the directory exists
        os.makedirs(self.temp_dir, exist_ok=True)
        
        with patch('ingestion.processor.INGESTION_DIR', self.temp_dir + '/'):
            save_data(df, "test_source")
        
        # Verify files exist
        csv_path = os.path.join(self.temp_dir, "test_source_processed.csv")
        parquet_path = os.path.join(self.temp_dir, "test_source_processed.parquet")
        
        self.assertTrue(os.path.exists(csv_path), f"CSV file not found at {csv_path}")
        self.assertTrue(os.path.exists(parquet_path), f"Parquet file not found at {parquet_path}")
        
        # Verify data integrity
        saved_csv = pd.read_csv(csv_path)
        self.assertEqual(len(saved_csv), 2)
        self.assertEqual(saved_csv.iloc[0]["Full Name"], "John Doe")
        
        saved_parquet = pd.read_parquet(parquet_path)
        self.assertEqual(len(saved_parquet), 2)
        self.assertEqual(saved_parquet.iloc[1]["email"], "jane@example.com")
    
    @patch('requests.get')
    def test_fetch_employee_data_legacy(self, mock_get):
        """Test the legacy fetch_employee_data function."""
        # Setup mock
        mock_response = MagicMock()
        mock_response.status_code = 200
        with open(self.json_file_path, 'rb') as f:
            mock_response.content = f.read()

        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        # Execute
        data = fetch_employee_data()
        
        # Verify
        self.assertIsNotNone(data)
        self.assertIn("employees", data)
        self.assertEqual(len(data["employees"]), 2)

if __name__ == "__main__":
    unittest.main()