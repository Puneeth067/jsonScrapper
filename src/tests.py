import unittest
import requests
import pandas as pd
import json
import os
from unittest.mock import patch, MagicMock
from scraper import fetch_employee_data
from processor import normalize_data, clean_html
from config import API_URL

class TestEmployeeScraper(unittest.TestCase):

    def test_api_response(self):
        """Test Case 1: Verify JSON File Download - Test API returns valid response."""
        response = requests.get("https://api.slingacademy.com/v1/sample-data/files/employees.json")
        self.assertEqual(response.status_code, 200, "API request failed!")
        self.assertIn('application/json', response.headers.get('Content-Type', ''), 
                     "Response is not in JSON format!")

    def test_json_structure(self):
        """Test Case 2: Verify JSON File Extraction - Test API returns valid JSON format."""
        data = fetch_employee_data()
        self.assertIsInstance(data, dict, "Data is not a JSON dictionary!")
        self.assertIn("employees", data, "Missing 'employees' key in JSON!")
        self.assertIsInstance(data["employees"], list, "Employees data is not a list!")
        self.assertTrue(len(data["employees"]) > 0, "Employees list is empty!")

    def test_file_type_and_format(self):
        """Test Case 3: Validate File Type and Format."""
        response = requests.get("https://api.slingacademy.com/v1/sample-data/files/employees.json")
        content_type = response.headers.get('Content-Type', '')
        
        # Check if it's JSON format
        self.assertIn('application/json', content_type, "Response is not in JSON format!")
        
        # Try to parse the JSON data
        try:
            json_data = response.json()
            self.assertTrue(True, "JSON parsed successfully")
        except json.JSONDecodeError:
            self.fail("Response is not a valid JSON")

    def test_data_structure(self):
        """Test Case 4: Validate Data Structure."""
        data = fetch_employee_data()
        self.assertIn("employees", data, "Missing 'employees' key in JSON!")
        
        # Check if at least one employee exists
        self.assertTrue(len(data["employees"]) > 0, "No employees found in the data!")
        
        # Check required fields in the first employee
        employee = data["employees"][0]
        required_fields = ["id", "first_name", "last_name", "email", "phone", 
                          "gender", "age", "job_title", "years_of_experience", 
                          "salary", "department"]
        
        for field in required_fields:
            self.assertIn(field, employee, f"Missing required field: {field}")
    
    def test_normalize_data(self):
        """Test the data normalization process."""
        # Sample employee data for testing
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
                },
                {
                    "id": 3, 
                    "first_name": "Shawn", 
                    "last_name": "Foster", 
                    "email": "shawnfoster2695@slingacademy.com", 
                    "phone": "001-966-861-0065", 
                    "gender": "male", 
                    "age": 37, 
                    "job_title": "Project Manager", 
                    "years_of_experience": 14, 
                    "salary": 17000, 
                    "department": "Product"
                }
            ]
        }
        
        # Process the data
        df = normalize_data(sample_data)
        
        # Validate the result
        self.assertIsInstance(df, pd.DataFrame, "Result is not a pandas DataFrame!")
        
        # Check that required columns exist and have the correct data types
        self.assertIn("Full Name", df.columns, "Missing 'Full Name' column!")
        self.assertIn("designation", df.columns, "Missing 'designation' column!")
        
        # Check data types
        self.assertEqual(df["Full Name"].dtype.kind, 'O', "Full Name should be string type")
        self.assertEqual(df["email"].dtype.kind, 'O', "Email should be string type")
        self.assertEqual(df["gender"].dtype.kind, 'O', "Gender should be string type")
        self.assertEqual(df["job_title"].dtype.kind, 'O', "Job title should be string type")
        self.assertEqual(df["department"].dtype.kind, 'O', "Department should be string type")
        self.assertEqual(df["age"].dtype.kind, 'i', "Age should be integer type")
        self.assertEqual(df["years_of_experience"].dtype.kind, 'i', "Years of experience should be integer type")
        self.assertEqual(df["salary"].dtype.kind, 'i', "Salary should be integer type")
        
        # Check designation assignment logic
        for _, row in df.iterrows():
            exp = row["years_of_experience"]
            if exp < 3:
                self.assertEqual(row["designation"], "System Engineer", 
                               f"Wrong designation for experience {exp}")
            elif 3 <= exp <= 5:
                self.assertEqual(row["designation"], "Data Engineer", 
                               f"Wrong designation for experience {exp}")
            elif 5 < exp <= 10:
                self.assertEqual(row["designation"], "Senior Data Engineer", 
                               f"Wrong designation for experience {exp}")
            else:
                self.assertEqual(row["designation"], "Lead", 
                               f"Wrong designation for experience {exp}")
        
        # Check full name concatenation
        self.assertEqual(df.iloc[0]["Full Name"], "Jose Lopez", "Full name not correctly concatenated")
        
        # Check phone number processing for "x" in the number
        self.assertEqual(df.iloc[0]["phone"], "Invalid Number", 
                       "Phone number with 'x' should be marked as 'Invalid Number'")

    @patch('scraper.requests.get')
    def test_handle_missing_or_invalid_data(self, mock_get):
        """Test Case 5: Handle Missing or Invalid Data."""
        # Mock a failed response
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response
        mock_get.side_effect = requests.exceptions.RequestException("API is down")
        
        # Test the function with mocked failed request
        result = fetch_employee_data()
        self.assertIsNone(result, "Function should return None for failed request")
        
        # Test with empty data
        empty_data = {}
        result = normalize_data(empty_data)
        self.assertIsNone(result, "Should handle empty data gracefully")
        
        # Test with malformed data
        malformed_data = {"not_employees": []}
        result = normalize_data(malformed_data)
        self.assertIsNone(result, "Should handle malformed data gracefully")
        
        # Test with more complete but still missing some fields
        # Include all fields that are directly accessed in normalize_data
        incomplete_data = {
            "employees": [
                {
                    "id": 1,
                    "first_name": "Test",
                    "last_name": "User",
                    "phone": "123-456-7890",
                    "years_of_experience": 2
                    # Other fields will be set to None by normalize_data
                }
            ]
        }
        result = normalize_data(incomplete_data)
        self.assertIsNotNone(result, "Should handle incomplete data gracefully")
        self.assertIn("Full Name", result.columns, "Should create missing columns")
        self.assertEqual(result.iloc[0]["designation"], "System Engineer", "Should assign correct designation based on years_of_experience")

    def test_clean_html(self):
        """Test the clean_html function."""
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


if __name__ == "__main__":
    unittest.main()