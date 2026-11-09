import pandas as pd
import numpy as np
import pandas_gbq
from typing import List


def _priority_sort_key(priority_value):
    """Build a sortable key where lower tuple means higher priority."""
    if pd.isna(priority_value) or not priority_value:
        return (99, 99)

    priority_levels = {
        'ULTRA': 1,
        'ALTA': 2,
        'MEDIA': 3,
        'BAJA': 4,
    }

    try:
        parts = str(priority_value).split('-')
        if len(parts) == 2:
            level = parts[0].strip().upper()
            number = int(parts[1].strip())
            level_rank = priority_levels.get(level)
            if level_rank is not None:
                return (level_rank, number)
    except (ValueError, AttributeError):
        pass

    return (99, 99)

def read_bigquery_table(table_name: str, credentials=None) -> pd.DataFrame:
    """Read a configuration or operator table from the telemarketing dataset."""
    query = f"SELECT * FROM `mi-casino.dm_telemarketing.{table_name}`;"
    return pandas_gbq.read_gbq(
        query, project_id='mi-casino', use_bqstorage_api=True, credentials=credentials
    )

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
            raw_count = len(df)
            
            # Check if the table is empty
            if df.empty:
                print(f"⚠️  Table {table_name} is empty, skipping to next campaign")
                continue
            
            # Apply filters
            df = df[~df['register_currency'].isin(['CAD', 'ARS', 'BRL'])]
            df = df[df.level.isin([1, 2, 3])]
            df.dropna(subset=['phone', 'firstLast_name'], inplace=True)
            df = df[df['firstLast_name'].astype(str).str.strip().ne('')]
            df['source_table'] = table_name
            print(f"  - Raw rows: {raw_count} | Rows after base filters: {len(df)}")
            
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
    col = ['assignment_date', 'campaign_name', 'priority', 'campaign_details', 'user_id', 'username', 'firstLast_name', 'phone', 'level',
            'register_currency', 'last_activity', 'source_table']
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

    # Keep one row per user_id, preserving the highest-priority campaign row.
    # This prevents assigning the same user in two campaigns on the same day.
    before_dedup = len(available_users)
    available_users['_priority_sort_key'] = available_users['priority'].apply(_priority_sort_key)
    available_users = available_users.sort_values(['user_id', '_priority_sort_key'], kind='stable')
    discarded_duplicates = available_users[available_users.duplicated(subset=['user_id'], keep='first')].copy()
    available_users = available_users.drop_duplicates(subset=['user_id'], keep='first')
    available_users = available_users.drop(columns=['_priority_sort_key'])
    dedup_removed = before_dedup - len(available_users)
    if dedup_removed > 0:
        print(f"\nRemoved {dedup_removed} duplicate rows by user_id using priority order")
        discarded_by_campaign = discarded_duplicates['campaign_name'].value_counts().sort_values(ascending=False)
        print("\nDiscarded duplicate users by campaign:")
        for campaign, count in discarded_by_campaign.items():
            print(f"  - {campaign}: {count} users")

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

