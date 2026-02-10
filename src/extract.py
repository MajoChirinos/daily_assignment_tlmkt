import pandas as pd
import numpy as np
import pandas_gbq
from oauth2client.service_account import ServiceAccountCredentials
import gspread
import os
from typing import List
import json

def read_google_sheet(sheet, work_sheet):
    

    json_keyfile_path = os.getenv('SHEET_CREDENTIALS')
    
    if json_keyfile_path is None:
        raise ValueError("SHEET_CREDENTIALS environment variable not set. Please configure it in Cloud Run.")
    
    # Define the scope
    scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
    # Add credentials to the account
    creds = ServiceAccountCredentials.from_json_keyfile_dict(json.loads(json_keyfile_path), scope)

    # Authorize the clientsheet
    client = gspread.authorize(creds)

    # Get the instance of the Spreadsheet
    df_sheet = client.open(sheet)
    # Get the first sheet of the Spreadsheet
    df_sheet_instance = df_sheet.get_worksheet(work_sheet)
    # Get all the records of the data
    df_data = df_sheet_instance.get_all_records()
    # Convert the json to dataframe
    df_data = pd.DataFrame.from_dict(df_data)

    return df_data

def get_data(campaigns_to_assign: List[str], currencies_to_filter: List[str], credentials=None) -> pd.DataFrame:
    """
    Fetches data from multiple BigQuery tables based on a list of table names, filters out specific currencies,
    removes rows with missing phone numbers, processes campaign names, and concatenates the results.

    Args:
        campaigns_to_assign (List[str]): List of table names to query.
        currencies_to_filter (List[str]): List of currencies to filter out.
        credentials: Google Cloud credentials object.

    Returns:
        pd.DataFrame: A processed and concatenated DataFrame with the query results from all tables.
    """
    all_data = []  # List to store DataFrames from each table

    for table_name in campaigns_to_assign:
        try:
            # Define the query for each table
            query = f"SELECT DISTINCT * FROM `mi-casino.dm_telemarketing.{table_name}`;"
            
            # Execute the query and fetch the data
            print(f"* {table_name}")
            df = pandas_gbq.read_gbq(query, project_id='mi-casino', use_bqstorage_api=True, credentials=credentials)
            
            # Check if the table is empty
            if df.empty:
                print(f"⚠️  Table {table_name} is empty, skipping to next campaign")
                continue
            
            # Apply filters
            df = df[~df['register_currency'].isin(['CAD', 'ARS', 'BRL'])]
            df = df[df.level.isin([1, 2, 3])]
            df.dropna(subset=['phone'], inplace=True)
            
            # Add the filtered DataFrame to the list
            all_data.append(df)
            
        except Exception as e:
            # Handle table not found or other errors
            if "Not found" in str(e) or "404" in str(e):
                print(f"⚠️  Table {table_name} does not exist, skipping to next campaign")
            else:
                print(f"⚠️  Error reading table {table_name}: {str(e)}, skipping to next campaign")
            continue

    # Concatenate all DataFrames into one
    if not all_data:
        # If no data was collected, return an empty DataFrame with expected columns
        print("⚠️  No data available from any campaign table")
        return pd.DataFrame()
    
    available_users = pd.concat(all_data, ignore_index=True)

    # Filter out users from currencies_to_filter
    available_users = available_users[~available_users['register_currency'].isin(currencies_to_filter)]

    # Create assignment date column
    available_users['assignment_date'] = pd.to_datetime('today').strftime('%Y-%m-%d')

    # Ensure campaign_details exists (for tables without this column, add it as None)
    if 'campaign_details' not in available_users.columns:
        available_users['campaign_details'] = None

    # Select relevant columns
    col = ['assignment_date', 'campaign_name', 'campaign_details', 'user_id', 'username', 'firstLast_name', 'phone', 'level',
           'register_currency', 'last_activity']
    available_users = available_users[col]

    # Replace campaign names
    replace = {
        'Non Depositors Telemarketing': 'non_depositors',
        'Reactivation': 'reactivation',
        'Days since FTD Telemarketing': 'second_deposit',
        'Days sice STD Telemarketing': 'third_deposit',
        'TeleMarketing Rejected': 'rejected',
    }

    # Use np.where to replace values and assign 'reactivation' if NaN
    available_users['campaign_name'] = np.where(
        available_users['campaign_name'].isna(),
        'reactivation',
        available_users['campaign_name'].replace(replace)
    )

    # Remove duplicates in available_users based only on user_id
    available_users = available_users.drop_duplicates(subset=['user_id'], keep='first')

    return available_users


def get_data_hist(table_name: str, start_date: str, credentials=None) -> pd.DataFrame:
    """
    Fetches historical data from a specified BigQuery table, filters out specific currencies,
    removes rows with missing phone numbers, and applies a date filter.

    Args:
        table_name (str): The name of the table to query.
        start_date (str): The starting date (inclusive) to filter the assignment_date column (format: 'YYYY-MM-DD').
        credentials: Google Cloud credentials object.

    Returns:
        pd.DataFrame: A filtered DataFrame with the query results.
    """
    # Define the query with a WHERE clause for the assignment_date
    query = f"""
        SELECT * 
        FROM `mi-casino.dm_telemarketing.{table_name}`
        WHERE assignment_date >= '{start_date}';
    """
    
    # Execute the query and fetch the data
    df = pandas_gbq.read_gbq(query, project_id='mi-casino', use_bqstorage_api=True, credentials=credentials)
    
    df['assignment_date'] = pd.to_datetime(df['assignment_date'])

    return df

