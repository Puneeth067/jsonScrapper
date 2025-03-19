import pandas as pd
import logging
import os
import json
from bs4 import BeautifulSoup
import pyarrow

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Load configuration from JSON file
def load_config():
    config_path = os.path.join(os.path.dirname(__file__), "config.json")
    with open(config_path, 'r') as f:
        return json.load(f)

CONFIG = load_config()
PROCESSED_DATA_DIR = CONFIG["PROCESSED_DATA_DIR"]

def clean_html(text):
    """Remove HTML tags and return plain text."""
    return BeautifulSoup(text, "lxml").text.strip() if pd.notna(text) and "<" in str(text) and ">" in str(text) else text

def read_raw_data(source_id):
    """
    Read the raw data file based on its type and return the data.
    """
    if source_id not in CONFIG["RAW_DATA_SOURCES"]:
        logging.error(f"Source ID '{source_id}' not found in configuration.")
        return None
    
    source_config = CONFIG["RAW_DATA_SOURCES"][source_id]
    file_path = source_config["path"]
    file_type = source_config["type"]
    
    if not os.path.exists(file_path):
        logging.error(f"File does not exist: {file_path}")
        logging.info(f"Current working directory: {os.getcwd()}")
        return None
    
    try:
        if file_type == 'json':
            with open(file_path, 'r') as f:
                data = json.load(f)
                
                # If it's a list, wrap it in a dict with 'employees' key
                if isinstance(data, list):
                    return {"employees": data}
                return data
                
        elif file_type == "csv":
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

        elif file_type in ['xlsx', 'xls']:
            return pd.read_excel(file_path)
            
        else:
            logging.error(f"Unsupported file format: {file_type}")
            return None
            
    except Exception as e:
        logging.error(f"Error reading file {file_path}: {e}")
        return None

def normalize_data(data, source_type='json'):
    """
    Normalize and clean data from different source types.
    """
    # Convert data to DataFrame based on source type
    if source_type == 'json':
        if not data or "employees" not in data:
            logging.error("Invalid data format received for JSON.")
            return None
        df = pd.DataFrame(data["employees"])
    elif source_type in ['csv', 'xlsx']:
        # For CSV and Excel, data should already be a DataFrame
        if not isinstance(data, pd.DataFrame):
            logging.error(f"Expected DataFrame for {source_type} but got {type(data)}")
            return None
        df = data.copy()
        
        # Log the original columns to understand what we're working with
        logging.info(f"Original columns in {source_type} dataframe: {df.columns.tolist()}")
    else:
        # For any other type, try to use the data if it's already a DataFrame
        if isinstance(data, pd.DataFrame):
            df = data.copy()
        else:
            logging.error(f"Unsupported data type for normalization: {type(data)}")
            return None
    
    # Standardize column names (convert to lowercase for case-insensitive matching)
    df.columns = [col.lower() for col in df.columns]
    
    # Handle name variations
    # Check if we have first_name and last_name columns for creating Full Name
    if 'first_name' in df.columns and 'last_name' in df.columns:
        df["full name"] = df["first_name"] + " " + df["last_name"]
    elif 'firstname' in df.columns and 'lastname' in df.columns:
        df["full name"] = df["firstname"] + " " + df["lastname"]
    elif 'name' in df.columns:
        # If there's just a name column, use that as Full Name
        df["full name"] = df["name"]
    # For CSV/Excel specific patterns - look for column patterns
    elif 'employee_first_name' in df.columns and 'employee_last_name' in df.columns:
        df["full name"] = df["employee_first_name"] + " " + df["employee_last_name"]
    elif 'first name' in df.columns and 'last name' in df.columns:
        df["full name"] = df["first name"] + " " + df["last name"]
    elif 'full name' not in df.columns:
        # If neither pattern exists, create an empty Full Name column
        df["full name"] = ""

    # Clean up original name columns after creating Full Name if we combined them
    if 'full name' in df.columns and not df.empty and df['full name'].iloc[0] != "":
        for col in ['first_name', 'last_name', 'first name', 'last name', 'name', 
                   'employee_first_name', 'employee_last_name', 'first', 'last']:
            if col in df.columns:
                df = df.drop(columns=[col], errors="ignore")
    
    # Process phone numbers (if column exists) with multiple possible column names
    phone_cols = ['phone', 'phone_number', 'phonenumber', 'contact', 'telephone', 
                  'employee_phone', 'employee_contact']
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
    exp_cols = ['years_of_experience','years of experience', 'experience', 'experience_years', 'years_experience', 'yoe',
               'years', 'employee_experience', 'experience_yrs']
    found_exp_col = next((col for col in exp_cols if col in df.columns), None)
    
    if found_exp_col:
        def assign_designation(exp):
            try:
                exp = float(exp)
                if pd.isna(exp) or exp == 0:
                    return "Unknown"
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

        
        df[found_exp_col] = pd.to_numeric(df[found_exp_col], errors='coerce').fillna(0).astype(int)
        df["designation"] = df[found_exp_col].apply(assign_designation)

        # Make sure we keep the experience column with a standardized name
        if found_exp_col != 'years_of_experience':
            df["years_of_experience"] = df[found_exp_col]
            df = df.drop(columns=[found_exp_col], errors="ignore")
    else:
        df["designation"] = "Unknown"
        df["years_of_experience"] = 0
    
    # Handle salary column variations
    salary_cols = ['salary', 'annual_salary', 'pay', 'compensation', 'employee_salary']
    found_salary_col = next((col for col in salary_cols if col in df.columns), None)
    if found_salary_col and found_salary_col != 'salary':
        df["salary"] = df[found_salary_col]
        df = df.drop(columns=[found_salary_col], errors="ignore")
    elif 'salary' not in df.columns:
        df["salary"] = 0
        
    # Handle email column variations
    email_cols = ['email', 'email_address', 'emailaddress', 'mail', 'employee_email']
    found_email_col = next((col for col in email_cols if col in df.columns), None)
    if found_email_col and found_email_col != 'email':
        df["email"] = df[found_email_col]
        df = df.drop(columns=[found_email_col], errors="ignore")
    elif 'email' not in df.columns:
        df["email"] = ""
        
    # Handle gender column variations
    gender_cols = ['gender', 'sex', 'employee_gender']
    found_gender_col = next((col for col in gender_cols if col in df.columns), None)
    if found_gender_col and found_gender_col != 'gender':
        df["gender"] = df[found_gender_col]
        df = df.drop(columns=[found_gender_col], errors="ignore")
    elif 'gender' not in df.columns:
        df["gender"] = ""
        
    # Handle job title column variations
    job_cols = ['job_title', 'jobtitle', 'job title', 'position', 'role', 'employee_jobtitle', 
               'employee_title', 'employee_position', 'job']
    found_job_col = next((col for col in job_cols if col in df.columns), None)
    if found_job_col and found_job_col != 'job_title':
        df["job_title"] = df[found_job_col]
        df = df.drop(columns=[found_job_col], errors="ignore")
    elif 'job_title' not in df.columns:
        df["job_title"] = ""
        
    # Handle department column variations
    dept_cols = ['department', 'dept', 'team', 'employee_department', 'employee_dept']
    found_dept_col = next((col for col in dept_cols if col in df.columns), None)
    if found_dept_col and found_dept_col != 'department':
        df["department"] = df[found_dept_col]
        df = df.drop(columns=[found_dept_col], errors="ignore")
    elif 'department' not in df.columns:
        df["department"] = ""
        
    # Handle age column variations
    age_cols = ['age', 'years_old', 'employee_age']
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
    
    # Ensure id column exists and is proper
    if "id" not in df.columns:
        df["id"] = range(1, len(df) + 1)
    else:
        # Make sure id is numeric
        try:
            df["id"] = pd.to_numeric(df["id"], errors='coerce')
            # Check if all values are NaN or 0
            if df["id"].isna().all() or (df["id"] == 0).all():
                # Create new IDs
                df["id"] = range(1, len(df) + 1)
            else:
                # Fill any NaN values with new sequential IDs starting from max+1
                max_id = df["id"].max()
                missing_mask = df["id"].isna()
                missing_count = missing_mask.sum()
                if missing_count > 0:
                    df.loc[missing_mask, "id"] = range(int(max_id)+1, int(max_id)+missing_count+1)
                df["id"] = df["id"].fillna(0).astype(int)
        except:
            # If conversion fails, create new IDs
            df["id"] = range(1, len(df) + 1)
    
    # Handle potential NaN values for Integer columns
    for col in ["age", "years_of_experience", "salary"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    
    # Convert string columns
    string_columns = ["full name", "email", "gender", "job_title", "department", "designation"]
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna("").astype(str)
    
    # Convert phone to string
    if "phone" in df.columns:
        df["phone"] = df["phone"].astype(str)
    
    # Rename 'full name' to 'Full Name' for final output consistency
    df = df.rename(columns={'full name': 'Full Name'})
    
    # Print the final columns for debugging
    logging.info(f"Final columns in dataframe: {df.columns.tolist()}")
    
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
    """
    # Ensure processed data directory exists
    os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)
    
    csv_path = os.path.join(PROCESSED_DATA_DIR, f"{source_id}_processed.csv")
    parquet_path = os.path.join(PROCESSED_DATA_DIR, f"{source_id}_processed.parquet")

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    logging.info(f"Processed data saved: {csv_path}, {parquet_path}")