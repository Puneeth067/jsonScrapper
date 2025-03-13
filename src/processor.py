import pandas as pd
import logging
import os
from bs4 import BeautifulSoup
import pyarrow
from config import INGESTION_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clean_html(text):
    """Remove HTML tags and return plain text."""
    return BeautifulSoup(text, "lxml").text.strip() if pd.notna(text) and "<" in str(text) and ">" in str(text) else text

def normalize_data(data, source_type='json'):
    """
    Normalize and clean data from different source types.
    
    Args:
        data: The data to normalize (DataFrame or dict)
        source_type: The type of the source data ('json', 'csv', 'excel')
        
    Returns:
        DataFrame: Normalized data
    """
    # Convert data to DataFrame based on source type
    if source_type == 'json':
        if not data or "employees" not in data:
            logging.error("Invalid data format received for JSON.")
            return None
        df = pd.DataFrame(data["employees"])
    elif source_type in ['csv', 'excel']:
        # For CSV and Excel, data should already be a DataFrame
        if not isinstance(data, pd.DataFrame):
            logging.error(f"Expected DataFrame for {source_type} but got {type(data)}")
            return None
        df = data.copy()
        
        # Check if this is a flat CSV/Excel without nested structure
        # Look for common patterns in column names to determine format
        columns = df.columns.str.lower()
        
        # Some CSV/Excel files might have 'employee' as a prefix in column names
        # Strip it if present consistently
        if all('employee.' in col or col.startswith('employee_') for col in columns):
            # Rename columns to remove 'employee.' or 'employee_' prefix
            df.columns = [col.replace('employee.', '').replace('employee_', '') for col in df.columns]
    else:
        # For any other type, try to use the data if it's already a DataFrame
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            logging.error(f"Unsupported data type for normalization: {type(data)}")
            return None
    
    # Log the columns for debugging
    logging.info(f"Columns in dataframe: {df.columns.tolist()}")
    
    # Standardize column names (convert to lowercase for case-insensitive matching)
    df.columns = [col.lower() for col in df.columns]
    
    # Handle name variations
    # Check if we have first_name and last_name columns for creating Full Name
    if 'first_name' in df.columns and 'last_name' in df.columns:
        df["full name"] = df["first_name"] + " " + df["last_name"]
        # Drop the original name columns after creating Full Name
        df = df.drop(columns=["first_name", "last_name"], errors="ignore")
    elif 'firstname' in df.columns and 'lastname' in df.columns:
        df["full name"] = df["firstname"] + " " + df["lastname"]
        df = df.drop(columns=["firstname", "lastname"], errors="ignore")
    elif 'name' in df.columns:
        # If there's just a name column, use that as Full Name
        df["full name"] = df["name"]
        df = df.drop(columns=["name"], errors="ignore")
    elif 'full name' not in df.columns:
        # If neither pattern exists, create an empty Full Name column
        df["full name"] = ""
    
    # Process phone numbers (if column exists) with multiple possible column names
    phone_cols = ['phone', 'phone_number', 'phonenumber', 'contact', 'telephone']
    found_phone_col = next((col for col in phone_cols if col in df.columns), None)
    
    if found_phone_col:
        def process_phone(phone):
            if pd.notna(phone):
                if "x" in str(phone):
                    return "Invalid Number"
                digits = ''.join(filter(str.isdigit, str(phone)))
                return int(digits) if digits else "Invalid Number"
            return "Invalid Number"
        
        df["phone"] = df[found_phone_col].apply(process_phone)
        # Drop the original phone column if it's not already named 'phone'
        if found_phone_col != 'phone':
            df = df.drop(columns=[found_phone_col], errors="ignore")
    else:
        df["phone"] = "Not Available"
    
    # Assign designation based on experience (with multiple possible column names)
    exp_cols = ['years_of_experience', 'experience', 'experience_years', 'years_experience', 'yoe']
    found_exp_col = next((col for col in exp_cols if col in df.columns), None)
    
    if found_exp_col:
        def assign_designation(exp):
            try:
                exp = float(exp)
                if exp < 3:
                    return "System Engineer"
                elif 3 <= exp <= 5:
                    return "Data Engineer"
                elif 5 < exp <= 10:
                    return "Senior Data Engineer"
                else:
                    return "Lead"
            except (ValueError, TypeError):
                return "Unknown"
        
        df["designation"] = df[found_exp_col].apply(assign_designation)
        # Make sure we keep the experience column with a standardized name
        if found_exp_col != 'years_of_experience':
            df["years_of_experience"] = df[found_exp_col]
            df = df.drop(columns=[found_exp_col], errors="ignore")
    else:
        df["designation"] = "Unknown"
        df["years_of_experience"] = 0
    
    # Handle salary column variations
    salary_cols = ['salary', 'annual_salary', 'pay', 'compensation']
    found_salary_col = next((col for col in salary_cols if col in df.columns), None)
    if found_salary_col and found_salary_col != 'salary':
        df["salary"] = df[found_salary_col]
        df = df.drop(columns=[found_salary_col], errors="ignore")
    elif 'salary' not in df.columns:
        df["salary"] = 0
        
    # Handle email column variations
    email_cols = ['email', 'email_address', 'emailaddress', 'mail']
    found_email_col = next((col for col in email_cols if col in df.columns), None)
    if found_email_col and found_email_col != 'email':
        df["email"] = df[found_email_col]
        df = df.drop(columns=[found_email_col], errors="ignore")
    elif 'email' not in df.columns:
        df["email"] = ""
        
    # Handle gender column variations
    gender_cols = ['gender', 'sex']
    found_gender_col = next((col for col in gender_cols if col in df.columns), None)
    if found_gender_col and found_gender_col != 'gender':
        df["gender"] = df[found_gender_col]
        df = df.drop(columns=[found_gender_col], errors="ignore")
    elif 'gender' not in df.columns:
        df["gender"] = ""
        
    # Handle job title column variations
    job_cols = ['job_title', 'jobtitle', 'title', 'position', 'role']
    found_job_col = next((col for col in job_cols if col in df.columns), None)
    if found_job_col and found_job_col != 'job_title':
        df["job_title"] = df[found_job_col]
        df = df.drop(columns=[found_job_col], errors="ignore")
    elif 'job_title' not in df.columns:
        df["job_title"] = ""
        
    # Handle department column variations
    dept_cols = ['department', 'dept', 'team']
    found_dept_col = next((col for col in dept_cols if col in df.columns), None)
    if found_dept_col and found_dept_col != 'department':
        df["department"] = df[found_dept_col]
        df = df.drop(columns=[found_dept_col], errors="ignore")
    elif 'department' not in df.columns:
        df["department"] = ""
        
    # Handle age column variations
    age_cols = ['age', 'years_old']
    found_age_col = next((col for col in age_cols if col in df.columns), None)
    if found_age_col and found_age_col != 'age':
        df["age"] = df[found_age_col]
        df = df.drop(columns=[found_age_col], errors="ignore")
    elif 'age' not in df.columns:
        df["age"] = 0
    
    # Define expected columns and ensure they exist with standardized names
    expected_columns = ["id", "full name", "email", "phone", "gender", "age", 
                        "job_title", "years_of_experience", "salary", "department", "designation"]
    
    # Add missing columns with default values
    for col in expected_columns:
        if col not in df.columns:
            if col in ["age", "years_of_experience", "salary"]:
                df[col] = 0
            else:
                df[col] = ""
    
    # Handle potential NaN values for Integer columns
    for col in ["age", "years_of_experience", "salary"]:
        if col in df.columns:
            df[col] = df[col].fillna(0).astype(int)
    
    # Convert string columns
    string_columns = ["full name", "email", "gender", "job_title", "department", "designation"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    
    # Convert phone to string
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str)
    
    # Ensure id column exists and is proper
    if "id" not in df.columns:
        df["id"] = range(1, len(df) + 1)
    else:
        # Make sure id is numeric
        try:
            df["id"] = pd.to_numeric(df["id"], errors='coerce').fillna(0).astype(int)
        except:
            # If conversion fails, create new IDs
            df["id"] = range(1, len(df) + 1)
    
    # Rename 'full name' to 'Full Name' for final output consistency
    df = df.rename(columns={'full name': 'Full Name'})
    
    # Reorder columns in the exact specified order (keeping any additional columns at the end)
    final_expected_columns = ["id", "Full Name", "email", "phone", "gender", "age", 
                         "job_title", "years_of_experience", "salary", "department", "designation"]
    
    available_columns = [col for col in final_expected_columns if col in df.columns]
    other_columns = [col for col in df.columns if col not in final_expected_columns]
    final_column_order = available_columns + other_columns
    
    # Return DataFrame with columns in the specified order
    return df[final_column_order]

def save_data(df, source_id):
    """
    Save data to CSV and Parquet.
    
    Args:
        df: DataFrame to save
        source_id: Source identifier for filename
    """
    # Ensure ingestion directory exists
    os.makedirs(INGESTION_DIR, exist_ok=True)
    
    csv_path = f"{INGESTION_DIR}{source_id}_processed.csv"
    parquet_path = f"{INGESTION_DIR}{source_id}_processed.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    logging.info(f"Data saved: {csv_path}, {parquet_path}")