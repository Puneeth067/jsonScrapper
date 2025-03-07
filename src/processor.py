import pandas as pd
import logging
from bs4 import BeautifulSoup
from scraper import fetch_employee_data
from config import INGESTION_DIR
import pyarrow

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def clean_html(text):
    """Remove HTML tags and return plain text."""
    return BeautifulSoup(text, "lxml").text.strip() if pd.notna(text) and "<" in str(text) and ">" in str(text) else text

def normalize_data(data):
    """Normalize and clean employee data."""
    if not data or "employees" not in data:
        logging.error("Invalid data format received.")
        return None

    df = pd.DataFrame(data["employees"])

    # Create "Full Name"
    df["Full Name"] = df["first_name"] + " " + df["last_name"]
    
    # Process phone numbers
    def process_phone(phone):
        if pd.notna(phone):
            if "x" in str(phone):
                return "Invalid Number"
            digits = ''.join(filter(str.isdigit, str(phone)))
            return int(digits) if digits else "Invalid Number"
        return "Invalid Number"
    
    df["phone"] = df["phone"].apply(process_phone)
    
    # Assign designation based on experience
    def assign_designation(exp):
        if exp < 3:
            return "System Engineer"
        elif 3 <= exp <= 5:
            return "Data Engineer"
        elif 5 < exp <= 10:
            return "Senior Data Engineer"
        else:
            return "Lead"
    
    df["designation"] = df["years_of_experience"].apply(assign_designation)
    
    # Convert data types and ensure required columns exist
    expected_columns = ["Full Name", "email", "phone", "gender", "age", "job_title", "years_of_experience", "salary", "department"]
    for col in expected_columns:
        if col not in df.columns:
            df[col] = None  # Ensure all required columns exist
    
    # Handle potential NaN values for Integer columns
    for col in ["age", "years_of_experience", "salary"]:
        df[col] = df[col].fillna(0).astype(int)
    
    # Convert string columns
    string_columns = ["Full Name", "email", "gender", "job_title", "department", "designation"]
    for col in string_columns:
        df[col] = df[col].fillna("").astype(str)
    
    # Convert phone to int where possible, otherwise keep as string
    # Since "Invalid Number" can't be converted to int, we'll keep phone as string
    df["phone"] = df["phone"].astype(str)
    df = df.drop(columns=["first_name", "last_name"], errors="ignore")

    # Reorder columns in the exact specified order
    ordered_columns = [
        "id",
        "Full Name",
        "email",
        "phone",
        "gender",
        "age",
        "job_title",
        "years_of_experience",
        "salary",
        "department",
        "designation"
    ]
    
    # Keep designation and any other columns at the end
    other_columns = [col for col in df.columns if col not in ordered_columns]
    final_column_order = ordered_columns + other_columns
    
    # Return DataFrame with columns in the specified order
    return df[final_column_order]

def save_data(df):
    """Save data to CSV and Parquet."""
    csv_path = f"{INGESTION_DIR}employee_data.csv"
    parquet_path = f"{INGESTION_DIR}employee_data.parquet"

    df.to_csv(csv_path, index=False)
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    logging.info(f"Data saved: {csv_path}, {parquet_path}")