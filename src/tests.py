import unittest
import requests
import pandas as pd
import json
import os
from unittest.mock import patch, MagicMock
from scraper import fetch_data, read_data_file, fetch_employee_data
from processor import normalize_data, clean_html
from config import API_SOURCES, INGESTION_DIR

class TestMultiSourceScraper(unittest.TestCase):

    def setUp(self):
        # Ensure ingestion directory exists for tests
        os.makedirs(INGESTION_DIR, exist_ok=True)
    
    def test_api_response(self):
        """Test Case 1: Verify JSON File Download - Test API returns valid response."""
        # Test the first source URL
        source_id = next(iter(API_SOURCES))
        source_url = API_SOURCES[source_id]["url"]
        
        response = requests.get(source_url)
        self.assertEqual(response.status_code, 200, "API request failed!")
        
        # Check content type based on source type
        source_type = API_SOURCES[source_id]["type"]
        if source_type == "json":
            self.assertIn('application/json', response.headers.get('Content-Type', ''), 
                         "Response is not in JSON format!")
        elif source_type == "csv":
            self.assertIn('text/csv', response.headers.get('Content-Type', ''), 
                         "Response is not in CSV format!")
        elif source_type == "excel":
            self.assertIn('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                         response.headers.get('Content-Type', ''), 
                         "Response is not in Excel format!")

    @patch('scraper.requests.get')
    def test_fetch_data(self, mock_get):
        """Test Case 2: Test fetch_data function with mock response."""
        # Mock a successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'{"employees":[{"id":1,"first_name":"Test","last_name":"User"}]}'
        mock_get.return_value = mock_response
        
        # Get first source ID
        source_id = next(iter(API_SOURCES))
        
        # Test the function
        result = fetch_data(source_id)
        
        # Verify the function called requests.get with the right URL
        mock_get.assert_called_once_with(API_SOURCES[source_id]["url"], timeout=API_SOURCES["TIMEOUT"] if "TIMEOUT" in API_SOURCES else 5)
        
        # Verify a file path was returned
        self.assertIsNotNone(result, "fetch_data should return a file path")
        self.assertTrue(os.path.exists(result), "File should exist at the returned path")
        
        # Clean up test file
        if os.path.exists(result):
            os.remove(result)

    def test_read_data_file_json(self):
        """Test Case 3: Test read_data_file function with JSON file."""
        # Create a test JSON file
        test_file_path = os.path.join(INGESTION_DIR, "test_employees.json")
        test_data = {"employees": [{"id": 1, "first_name": "Test", "last_name": "User"}]}
        
        with open(test_file_path, 'w') as f:
            json.dump(test_data, f)
        
        # Test the function
        result = read_data_file(test_file_path)
        
        # Verify the result
        self.assertIsNotNone(result, "read_data_file should return data for valid JSON")
        self.assertIsInstance(result, dict, "Result should be a dictionary for JSON files")
        self.assertIn("employees", result, "Result should contain 'employees' key")
        
        # Clean up test file
        os.remove(test_file_path)

    def test_read_data_file_csv(self):
        """Test Case 4: Test read_data_file function with CSV file."""
        # Create a test CSV file
        test_file_path = os.path.join(INGESTION_DIR, "test_employees.csv")
        test_df = pd.DataFrame({
            "id": [1, 2],
            "first_name": ["Test", "Another"],
            "last_name": ["User", "Person"]
        })
        test_df.to_csv(test_file_path, index=False)
        
        # Test the function
        result = read_data_file(test_file_path)
        
        # Verify the result
        self.assertIsNotNone(result, "read_data_file should return data for valid CSV")
        self.assertIsInstance(result, pd.DataFrame, "Result should be a DataFrame for CSV files")
        self.assertEqual(len(result), 2, "DataFrame should have 2 rows")
        
        # Clean up test file
        os.remove(test_file_path)

    @patch('scraper.fetch_data')
    @patch('scraper.read_data_file')
    def test_fetch_employee_data(self, mock_read_data_file, mock_fetch_data):
        """Test Case 5: Test legacy fetch_employee_data function."""
        # Mock the new functions that fetch_employee_data calls
        mock_fetch_data.return_value = "mock_file_path"
        mock_read_data_file.return_value = {"employees": [{"id": 1, "name": "Test User"}]}
        
        # Test the function
        result = fetch_employee_data()
        
        # Verify the function called the new functions correctly
        mock_fetch_data.assert_called_once_with("employees_json")
        mock_read_data_file.assert_called_once_with("mock_file_path")
        
        # Verify the result
        self.assertIsNotNone(result, "fetch_employee_data should return data")
        self.assertIsInstance(result, dict, "Result should be a dictionary")
        self.assertIn("employees", result, "Result should contain 'employees' key")

    def test_normalize_data_json(self):
        """Test Case 6: Test normalize_data function with JSON data."""
        # Sample data for testing
        sample_data = {
            "employees": [
                {
                    "id": 1, 
                    "first_name": "Jose", 
                    "last_name": "Lopez", 
                    "email": "joselopez0944@slingacademy.com", 
                    "phone": "+1-971-533-4552x1542", 
                    "gender": "male", 
                    "age": 25, 
                    "job_title": "Project Manager", 
                    "years_of_experience": 1, 
                    "salary": 8500, 
                    "department": "Product"
                }
            ]
        }
        
        # Process the data
        df = normalize_data(sample_data, source_type='json')
        
        # Validate the result
        self.assertIsInstance(df, pd.DataFrame, "Result is not a pandas DataFrame!")
        self.assertIn("Full Name", df.columns, "Missing 'Full Name' column!")
        self.assertIn("designation", df.columns, "Missing 'designation' column!")
        self.assertEqual(df.iloc[0]["Full Name"], "Jose Lopez", "Full name not correctly concatenated")
        self.assertEqual(df.iloc[0]["designation"], "System Engineer", "Designation not correctly assigned")

    def test_normalize_data_csv(self):
        """Test Case 7: Test normalize_data function with CSV data."""
        # Sample DataFrame for testing (as if read from CSV)
        sample_df = pd.DataFrame({
            "id": [1],
            "name": ["John Smith"],
            "email": ["johnsmith@example.com"],
            "phone": ["555-123-4567"],
            "gender": ["male"],
            "age": [30],
            "job_title": ["Data Analyst"],
            "years_of_experience": [4],
            "salary": [75000],
            "department": ["Analytics"]
        })
        
        # Process the data
        df = normalize_data(sample_df, source_type='csv')
        
        # Validate the result
        self.assertIsInstance(df, pd.DataFrame, "Result is not a pandas DataFrame!")
        self.assertIn("Full Name", df.columns, "Missing 'Full Name' column!")
        self.assertIn("designation", df.columns, "Missing 'designation' column!")
        self.assertEqual(df.iloc[0]["Full Name"], "John Smith", "Full name not correctly assigned")
        self.assertEqual(df.iloc[0]["designation"], "Data Engineer", "Designation not correctly assigned")

    def test_normalize_data_missing_columns(self):
        """Test Case 8: Test normalize_data function with missing columns."""
        # Sample data with missing columns
        sample_data = {
            "employees": [
                {
                    "id": 1,
                    "first_name": "Test",
                    "last_name": "User",
                    "years_of_experience": 12
                    # Many columns missing
                }
            ]
        }
        
        # Process the data
        df = normalize_data(sample_data, source_type='json')
        
        # Validate the result
        self.assertIsInstance(df, pd.DataFrame, "Result is not a pandas DataFrame!")
        
        # Check that all required columns exist
        required_columns = ["id", "Full Name", "email", "phone", "gender", "age", 
                           "job_title", "years_of_experience", "salary", "department", "designation"]
        for column in required_columns:
            self.assertIn(column, df.columns, f"Missing required column: {column}")
        
        # Check the values
        self.assertEqual(df.iloc[0]["Full Name"], "Test User", "Full name not correctly concatenated")
        self.assertEqual(df.iloc[0]["designation"], "Lead", "Designation not correctly assigned for 12 years experience")
        self.assertEqual(df.iloc[0]["email"], "", "Default value for missing email should be empty string")
        self.assertEqual(df.iloc[0]["salary"], 0, "Default value for missing salary should be 0")

    def test_clean_html(self):
        """Test Case 9: Test the clean_html function."""
        # Test with HTML
        html_text = "<p>This is a <b>test</b></p>"
        cleaned = clean_html(html_text)
        self.assertEqual(cleaned, "This is a test", "HTML not properly cleaned")
        
        # Test with plain text
        plain_text = "This is plain text"
        cleaned = clean_html(plain_text)
        self.assertEqual(cleaned, plain_text, "Plain text should remain unchanged")
        
        # Test with None value
        cleaned = clean_html(None)
        self.assertEqual(cleaned, None, "None value should be handled properly")

    @patch('scraper.requests.get')
    def test_handle_missing_or_invalid_data(self, mock_get):
        """Test Case 10: Handle Missing or Invalid Data."""
        # Mock a failed response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_get.side_effect = requests.exceptions.RequestException("API is down")
        
        # Test with a source that doesn't exist
        result = fetch_data("nonexistent_source")
        self.assertIsNone(result, "Function should return None for nonexistent source")
        
        # Test normalize_data with empty data
        empty_data = {}
        result = normalize_data(empty_data, source_type='json')
        self.assertIsNone(result, "Should handle empty data gracefully")
        
        # Test normalize_data with malformed data
        malformed_data = {"not_employees": []}
        result = normalize_data(malformed_data, source_type='json')
        self.assertIsNone(result, "Should handle malformed data gracefully")
        
        # Test normalize_data with invalid source type
        invalid_data = {"employees": [{"id": 1, "name": "Test"}]}
        result = normalize_data(invalid_data, source_type='unknown_type')
        self.assertIsNone(result, "Should handle invalid source type gracefully")
        
        # Test with none data
        result = read_data_file(None)
        self.assertIsNone(result, "Should handle None file path gracefully")

if __name__ == "__main__":
    unittest.main()