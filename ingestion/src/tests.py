import unittest
import os
import sys
import json
import shutil
from unittest.mock import patch, MagicMock
import requests

# Add the parent directory to sys.path to import from ingestion
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from scraper import fetch_data, load_config
import src.main as ingestion_main

class TestIngestion(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment before each test"""
        self.config = load_config()
        self.test_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "raw_data")
        os.makedirs(self.test_data_dir, exist_ok=True)
        
    def tearDown(self):
        """Clean up after each test"""
        # Remove any test files created during tests
        for file in os.listdir(self.test_data_dir):
            file_path = os.path.join(self.test_data_dir, file)
            if file != ".gitkeep" and os.path.isfile(file_path):
                os.remove(file_path)
    
    @patch('requests.get')
    def test_json_download(self, mock_get):
        """Test Case 1: Verify JSON file download"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"employees": [{"id": 1, "name": "John Doe", "email": "john@example.com"}]}'
        mock_get.return_value = mock_response
        
        # Test function
        source_id = "employees_json"
        file_path = fetch_data(source_id)
        
        # Assert file was created and content is correct
        self.assertIsNotNone(file_path)
        self.assertTrue(os.path.exists(file_path))
        
        # Verify file content
        with open(file_path, 'rb') as f:
            data = f.read()
            self.assertEqual(data, mock_response.content)
    
    @patch('requests.get')
    def test_csv_download(self, mock_get):
        """Test Case 1: Verify CSV file download"""
        # Setup mock response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'id,name,email\n1,John Doe,john@example.com'
        mock_get.return_value = mock_response
        
        # Test function
        source_id = "employees_csv"
        file_path = fetch_data(source_id)
        
        # Assert file was created and content is correct
        self.assertIsNotNone(file_path)
        self.assertTrue(os.path.exists(file_path))
        
        # Verify file content
        with open(file_path, 'rb') as f:
            data = f.read()
            self.assertEqual(data, mock_response.content)
    
    @patch('requests.get')
    def test_download_retry_mechanism(self, mock_get):
        """Test Case 1: Verify retry mechanism works"""
        # Create mock responses - first two fail, third succeeds
        mock_fail1 = MagicMock()
        mock_fail1.raise_for_status.side_effect = requests.exceptions.ConnectionError("Connection error")
        
        mock_fail2 = MagicMock()
        mock_fail2.raise_for_status.side_effect = requests.exceptions.Timeout("Timeout error")
        
        mock_success = MagicMock()
        mock_success.status_code = 200
        mock_success.content = b'{"employees": [{"id": 1}]}'
        
        # Set the side effect sequence
        mock_get.side_effect = [mock_fail1, mock_fail2, mock_success]
        
        # Test function
        source_id = "employees_json"
        file_path = fetch_data(source_id)
        
        # Assert file was created after retries
        self.assertIsNotNone(file_path)
        self.assertTrue(os.path.exists(file_path))
        
        # Verify mock was called 3 times (original + 2 retries)
        self.assertEqual(mock_get.call_count, 3)
    
    @patch('ingestion.scraper.fetch_data')
    def test_json_extraction(self, mock_fetch):
        """Test Case 2: Verify JSON file extraction"""
        # Create a test JSON file
        test_file = os.path.join(self.test_data_dir, "employees_json.json")
        test_data = {"employees": [{"id": 1, "name": "John Doe", "email": "john@example.com"}]}
        with open(test_file, 'w') as f:
            json.dump(test_data, f)
        
        # Mock the fetch_data function to return our test file
        mock_fetch.return_value = test_file
        
        # Run the processor using lambdaHandler
        test_event = {
            "scraper_input": {
                "scraper_name": "test_scraper",
                "run_scraper_id": "101"  # ID for employees_json
            }
        }
        
        result = ingestion_main.lambdaHandler(test_event, {})
        
        # Assert the processing was successful
        self.assertEqual(result["statusCode"], 200)
        self.assertIn("successfully", result["body"])
    
    @patch('ingestion.scraper.fetch_data')
    def test_csv_extraction(self, mock_fetch):
        """Test Case 2: Verify CSV file extraction"""
        # Create a test CSV file
        test_file = os.path.join(self.test_data_dir, "employees_csv.csv")
        with open(test_file, 'w') as f:
            f.write("id,name,email\n1,John Doe,john@example.com")
        
        # Mock the fetch_data function to return our test file
        mock_fetch.return_value = test_file
        
        # Run the processor using lambdaHandler
        test_event = {
            "scraper_input": {
                "scraper_name": "test_scraper",
                "run_scraper_id": "102"  # ID for employees_csv
            }
        }
        
        result = ingestion_main.lambdaHandler(test_event, {})
        
        # Assert the processing was successful
        self.assertEqual(result["statusCode"], 200)
        self.assertIn("successfully", result["body"])
    
    def test_invalid_source_id(self):
        """Test handling of invalid source ID"""
        # Test with a non-existent source ID
        source_id = "non_existent_source"
        file_path = fetch_data(source_id)
        
        # Assert function returns None for invalid source
        self.assertIsNone(file_path)
    
    def test_process_all_sources(self):
        """Test processing all sources at once"""
        # Create test files for all sources
        for source_key, source_config in self.config["API_SOURCES"].items():
            file_type = source_config["type"]
            test_file = os.path.join(self.test_data_dir, f"{source_key}.{file_type}")
            
            if file_type == "json":
                with open(test_file, 'w') as f:
                    json.dump({"employees": [{"id": 1, "name": "Test User"}]}, f)
            elif file_type == "csv":
                with open(test_file, 'w') as f:
                    f.write("id,name,email\n1,Test User,test@example.com")
        
        # Test the process_all_sources function
        with patch('ingestion.scraper.fetch_data', return_value=True):
            result = ingestion_main.process_all_sources()
            
            # Assert all sources were processed successfully
            self.assertEqual(result["statusCode"], 200)
            self.assertIn("successfully", result["body"])

if __name__ == '__main__':
    unittest.main()