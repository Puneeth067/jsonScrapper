import unittest
import os
import sys
import json
import pandas as pd
import shutil
from unittest.mock import patch, MagicMock

# Add the parent directory to sys.path to import from processing
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from processor import read_raw_data, normalize_data, save_data, load_config
import src.main as processing_main

class TestProcessing(unittest.TestCase):
    
    def setUp(self):
        """Set up test environment before each test"""
        self.config = load_config()
        
        # Set up paths for raw and processed data
        self.raw_data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 
                                         "ingestion", "raw_data")
        self.processed_data_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                              "processed_data")
        
        # Create directories if they don't exist
        os.makedirs(self.raw_data_dir, exist_ok=True)
        os.makedirs(self.processed_data_dir, exist_ok=True)
        
        # Create test files for different types
        self.create_test_files()
        
    def tearDown(self):
        """Clean up after each test"""
        # Remove test files
        for file in os.listdir(self.raw_data_dir):
            file_path = os.path.join(self.raw_data_dir, file)
            if file != ".gitkeep" and os.path.isfile(file_path):
                os.remove(file_path)
                
        for file in os.listdir(self.processed_data_dir):
            file_path = os.path.join(self.processed_data_dir, file)
            if file != ".gitkeep" and os.path.isfile(file_path):
                os.remove(file_path)
    
    def create_test_files(self):
        """Create test files for different source types"""
        # JSON test file
        json_file = os.path.join(self.raw_data_dir, "employees_json.json")
        json_data = {
            "employees": [
                {
                    "id": 1,
                    "first_name": "John",
                    "last_name": "Doe",
                    "email": "john@example.com",
                    "phone": "555-1234",
                    "gender": "Male",
                    "age": 30,
                    "job_title": "Software Engineer",
                    "years_of_experience": 5,
                    "salary": 85000,
                    "department": "Engineering"
                },
                {
                    "id": 2,
                    "first_name": "Jane",
                    "last_name": "Smith",
                    "email": "jane@example.com",
                    "phone": "555-5678",
                    "gender": "Female",
                    "age": 28,
                    "job_title": "Data Scientist",
                    "years_of_experience": 3,
                    "salary": 80000,
                    "department": "Data"
                }
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)
        
        # CSV test file
        csv_file = os.path.join(self.raw_data_dir, "employees_csv.csv")
        csv_data = pd.DataFrame({
            "id": [1, 2],
            "first_name": ["John", "Jane"],
            "last_name": ["Doe", "Smith"],
            "email": ["john@example.com", "jane@example.com"],
            "phone": ["555-1234", "555-5678"],
            "gender": ["Male", "Female"],
            "age": [30, 28],
            "job_title": ["Software Engineer", "Data Scientist"],
            "years_of_experience": [5, 3],
            "salary": [85000, 80000],
            "department": ["Engineering", "Data"]
        })
        csv_data.to_csv(csv_file, index=False)
    
    def test_validate_file_type_json(self):
        """Test Case 3: Validate JSON file type and format"""
        # Test reading a valid JSON file
        data = read_raw_data("employees_json")
        
        # Assert data was read correctly
        self.assertIsNotNone(data)
        self.assertIn("employees", data)
        self.assertIsInstance(data["employees"], list)
        self.assertEqual(len(data["employees"]), 2)
    
    def test_validate_file_type_csv(self):
        """Test Case 3: Validate CSV file type and format"""
        # Test reading a valid CSV file
        data = read_raw_data("employees_csv")
        
        # Assert data was read correctly
        self.assertIsNotNone(data)
        self.assertIsInstance(data, pd.DataFrame)
        self.assertEqual(len(data), 2)
        self.assertIn("first_name", data.columns)
    
    def test_invalid_file_type(self):
        """Test Case 3: Validate handling of unsupported file types"""
        # Create an unsupported file type in the config for testing
        temp_config = self.config.copy()
        temp_config["RAW_DATA_SOURCES"]["employees_xml"] = {
            "id": 103,
            "path": "./ingestion/raw_data/employees_xml.xml",
            "type": "xml"
        }
        
        with patch('processing.processor.CONFIG', temp_config):
            # Test reading an unsupported file type
            data = read_raw_data("employees_xml")
            
            # Assert function returns None for unsupported format
            self.assertIsNone(data)
    
    def test_data_structure_json(self):
        """Test Case 4: Validate data structure for JSON"""
        # Read the JSON test file
        data = read_raw_data("employees_json")
        
        # Process the data
        processed_df = normalize_data(data, "json")
        
        # Assert the data structure is correct
        self.assertIsNotNone(processed_df)
        self.assertIsInstance(processed_df, pd.DataFrame)
        
        # Check for required columns
        required_cols = ["id", "Full Name", "email", "phone", "gender", "age", 
                         "job_title", "years_of_experience", "salary", "department", "designation"]
        for col in required_cols:
            self.assertIn(col, processed_df.columns)
        
        # Check specific data transformations
        self.assertEqual(processed_df["Full Name"].iloc[0], "John Doe")
        self.assertEqual(processed_df["designation"].iloc[0], "Data Engineer")  # Based on 5 years experience
    
    def test_data_structure_csv(self):
        """Test Case 4: Validate data structure for CSV"""
        # Read the CSV test file
        data = read_raw_data("employees_csv")
        
        # Process the data
        processed_df = normalize_data(data, "csv")
        
        # Assert the data structure is correct
        self.assertIsNotNone(processed_df)
        self.assertIsInstance(processed_df, pd.DataFrame)
        
        # Check for required columns
        required_cols = ["id", "Full Name", "email", "phone", "gender", "age", 
                         "job_title", "years_of_experience", "salary", "department", "designation"]
        for col in required_cols:
            self.assertIn(col, processed_df.columns)
        
        # Check specific data transformations
        self.assertEqual(processed_df["Full Name"].iloc[0], "John Doe")
        self.assertEqual(processed_df["designation"].iloc[0], "Data Engineer")  # Based on 5 years experience
    
    def test_missing_data(self):
        """Test Case 5: Handle missing or invalid data"""
        # Create a JSON file with missing data
        json_file = os.path.join(self.raw_data_dir, "employees_json.json")
        json_data = {
            "employees": [
                {
                    "id": 1,
                    "first_name": "John",
                    # missing last_name
                    # missing email
                    "phone": None,  # NULL phone
                    # missing gender
                    "age": "invalid",  # invalid age
                    # missing job_title
                    "years_of_experience": "",  # empty experience
                    # missing salary
                    # missing department
                },
                {
                    # missing id
                    # missing first_name
                    "last_name": "Smith",
                    "email": "jane@example.com",
                    # missing phone
                    "gender": "Female",
                    "age": 28,
                    "job_title": "Data Scientist",
                    "years_of_experience": 3,
                    "salary": 80000,
                    "department": "Data"
                }
            ]
        }
        with open(json_file, 'w') as f:
            json.dump(json_data, f)
        
        # Read and process the data
        data = read_raw_data("employees_json")
        processed_df = normalize_data(data, "json")
        
        # Assert the processor handled missing data correctly
        self.assertIsNotNone(processed_df)
        
        # Check first row (with missing data)
        self.assertEqual(processed_df["Full Name"].iloc[0], "John ")  # Missing last name
        self.assertEqual(processed_df["email"].iloc[0], "")  # Default for missing email
        self.assertEqual(processed_df["phone"].iloc[0], "Invalid Number")  # NULL phone
        self.assertEqual(processed_df["gender"].iloc[0], "")  # Default for missing gender
        self.assertEqual(processed_df["age"].iloc[0], 0)  # Invalid age converted to 0
        self.assertEqual(processed_df["job_title"].iloc[0], "")  # Default for missing job title
        self.assertEqual(processed_df["years_of_experience"].iloc[0], 0)  # Empty exp converted to 0
        self.assertEqual(processed_df["salary"].iloc[0], 0)  # Default for missing salary
        self.assertEqual(processed_df["department"].iloc[0], "")  # Default for missing department
        self.assertEqual(processed_df["designation"].iloc[0], "Unknown")  # Based on 0 years experience
    
    def test_invalid_data_csv(self):
        """Test Case 5: Handle invalid CSV data"""
        # Create an invalid CSV file (header only, no data)
        csv_file = os.path.join(self.raw_data_dir, "employees_csv.csv")
        with open(csv_file, 'w') as f:
            f.write("id,first_name,last_name,email")  # Header only
        
        # Test reading an invalid CSV file
        data = read_raw_data("employees_csv")
        
        # Assert function returns None for invalid CSV
        self.assertIsNone(data)
    
    def test_empty_json(self):
        """Test Case 5: Handle empty JSON data"""
        # Create an empty JSON file
        json_file = os.path.join(self.raw_data_dir, "employees_json.json")
        with open(json_file, 'w') as f:
            f.write("{}")  # Empty JSON
        
        # Test normalizing empty JSON
        data = read_raw_data("employees_json")
        processed_df = normalize_data(data, "json")
        
        # Assert function returns None for empty data
        self.assertIsNone(processed_df)
    
    def test_corrupted_json(self):
        """Test Case 5: Handle corrupted JSON data"""
        # Create a corrupted JSON file
        json_file = os.path.join(self.raw_data_dir, "employees_json.json")
        with open(json_file, 'w') as f:
            f.write("{corrupted json content")  # Corrupted JSON
        
        # Test reading corrupted JSON
        data = read_raw_data("employees_json")
        
        # Assert function returns None for corrupted JSON
        self.assertIsNone(data)
    
    @patch('processing.processor.read_raw_data')
    def test_process_source_handler(self, mock_read):
        """Test the process_source handler in the main module"""
        # Create a mock DataFrame that would be returned by normalize_data
        mock_df = pd.DataFrame({
            "id": [1, 2],
            "Full Name": ["John Doe", "Jane Smith"],
            "email": ["john@example.com", "jane@example.com"],
            "phone": ["555-1234", "555-5678"],
            "gender": ["Male", "Female"],
            "age": [30, 28],
            "job_title": ["Software Engineer", "Data Scientist"],
            "years_of_experience": [5, 3],
            "salary": [85000, 80000],
            "department": ["Engineering", "Data"],
            "designation": ["Data Engineer", "Data Engineer"]
        })
        
        # Mock the read_raw_data function
        mock_read.return_value = {"employees": [{"id": 1, "name": "John"}]}
        
        # Patch normalize_data and save_data to avoid actual processing
        with patch('processing.processor.normalize_data', return_value=mock_df), \
             patch('processing.processor.save_data'):
            
            # Test the process_source function
            result = processing_main.process_source("employees_json")
            
            # Assert function returned True (success)
            self.assertTrue(result)

if __name__ == '__main__':
    unittest.main()