import pandas as pd
import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.auth import default

# Local project modules
from src.extract import read_google_sheet, get_data, get_data_hist
from src.config import Config
from src.transform import (
    normalize_campaign_to_code,
    normalize_campaign_to_display,
    create_campaign_dataframes,
    assign_currencies,
    calculate_remaining_assignments_dict,
    complete_assignments,
    count_users_per_operator
)
from src.load import CreateAndLoad_BQ


def run_daily_assignment(request) -> str:
    try:
        creds, project = default()

        # ========== ASSIGNMENT CONFIGURATION ==========
        # Read configuration file
        try:
            conf = read_google_sheet('Daily_Assignment_Configuration', 0)
        except Exception as error:
            print(f"Error reading configuration from Google Sheets: {error}")
            return f"Error: Failed to read configuration - {error}"

        # Create configuration object
        config = Config(conf)

        # Variable declaration from Config class
        days_ago_to_discard = config.days_ago_to_discard
        users_to_assign_per_operator = config.users_to_assign_per_operator
        currencies_to_filter = config.currencies_to_filter
        priority_currencies = config.priority_currencies
        max_priority_currencies_percent = config.max_priority_currencies_percent
        small_currencies_to_limit = config.small_currencies_to_limit
        max_small_currencies_percent = config.max_small_currencies_percent
        big_currencies_to_limit = config.big_currencies_to_limit
        max_big_currencies_percent = config.max_big_currencies_percent
        relevant_currencies = config.relevant_currencies
        extra_users_campaign = config.extra_users_campaign

        # ========== DATES TO USE ==========
        today = datetime.now() - timedelta(days=0)
        today = datetime(today.year, today.month, today.day)
        today = datetime.strftime(today, '%Y-%m-%d').replace('-', '')

        days_ago_to_discard = datetime.now() - timedelta(days=days_ago_to_discard)
        days_ago_to_discard = datetime(days_ago_to_discard.year, days_ago_to_discard.month, days_ago_to_discard.day)
        days_ago_to_discard = datetime.strftime(days_ago_to_discard, '%Y-%m-%d')

        # ========== DATA PREPARATION ==========
        # LP-TLMKT processing
        try:
            lp = read_google_sheet('LP_TLMKT', 0)
        except Exception as error:
            print(f"Error reading LP_TLMKT from Google Sheets: {error}")
            return f"Error: Failed to read LP_TLMKT - {error}"
        
        lp = lp[lp['Cargo'] == 'Ejecutivo de Televentas']
        lp = lp[lp['Estatus'] == 'Activo']
        lp.rename(columns={'Nombre y Apellido': 'operator',
                        'Usuario DotPanel' : 'user_dotpanel',
                        'Campaña' : 'campaign_name'}, inplace=True)
        
        # Apply campaign normalization using imported function
        lp['campaign_name'] = lp['campaign_name'].apply(normalize_campaign_to_code)
        lp['campaign_name'] = lp['campaign_name'].str.split(',\s*')

        # Historical assignment users
        try:
            daily_assigment_hist = get_data_hist('tlmkt_DailyAssignment', days_ago_to_discard, credentials=creds)
        except Exception as error:
            print(f"Error getting historical data from BigQuery: {error}")
            return f"Error: Failed to get historical data - {error}"
        
        daily_assigment_hist['campaign_name'] = daily_assigment_hist['campaign_name'].apply(normalize_campaign_to_code)
        daily_assigment_hist = daily_assigment_hist[daily_assigment_hist['assignment_date'] < today]

        # Users to discard
        users_to_discard = daily_assigment_hist[['user_id', 'campaign_name']]

        # Segment tables to assign
        try:
            segments_tables = read_google_sheet('Daily_Assignment_Configuration', 1)
        except Exception as error:
            print(f"Error reading segment tables from Google Sheets: {error}")
            return f"Error: Failed to read segment tables - {error}"
        
        campaigns_to_assign = segments_tables['table_name'].tolist()

        # Data extraction to assign
        print("Extracting data to assign...")
        try:
            available_users = get_data(campaigns_to_assign, currencies_to_filter, credentials=creds)
        except Exception as error:
            print(f"Error extracting data from BigQuery: {error}")
            return f"Error: Failed to extract data - {error}"
        print("Data extracted successfully")

        # Remove users contacted 'days_ago_to_discard' ago (users_to_discard)
        print(f"Discarding users contacted since {days_ago_to_discard}")
        available_users = available_users.merge(
            users_to_discard, 
            on=['user_id', 'campaign_name'], 
            how='left', 
            indicator=True
        )

        available_users = available_users[available_users['_merge'] == 'left_only'].drop(columns=['_merge'])

        print(f"Available users for assignment: {available_users.shape[0]}")

        # Create dictionary of DataFrames by campaign 
        campaign_dfs = create_campaign_dataframes(available_users)

        # ========== USER ASSIGNMENT ==========
        # Create assignment dictionary
        print("Creating assignment dictionary...")
        assignment_dict = {}

        # Define percentages according to number of assigned campaigns
        percentages = {
            1: [1.0],  # 100% for single campaign
            2: [0.7, 0.3],  # 70% and 30% for two campaigns
            3: [0.5, 0.3, 0.2]  # 50%, 30% and 20% for three campaigns
        }

        # Iterate over operators in lp DataFrame
        for _, row in lp.iterrows():
            operator = row['operator']
            campaigns = row['campaign_name']  # List of campaigns assigned to operator
            num_campaigns = len(campaigns)  # Number of campaigns assigned to operator

            # Check if there are defined percentages for this number of campaigns
            if num_campaigns in percentages:
                for idx, campaign in enumerate(campaigns):
                    if idx < len(percentages[num_campaigns]):  # Check if there's a defined percentage for this position
                        # Calculate users to assign rounding to nearest integer
                        users_to_assign = round(users_to_assign_per_operator * percentages[num_campaigns][idx])
                        
                        # Initialize campaign in dictionary if it doesn't exist
                        if campaign not in assignment_dict:
                            assignment_dict[campaign] = []
                        
                        # Add operator and assigned users to campaign
                        assignment_dict[campaign].append({
                            'operator': operator,
                            'users_to_assign': users_to_assign
                        })

        print("Assignment Dictionary created successfully.")

        # Priority Currencies Assignment
        print("Assigning Priority Currencies...")
        try:
            priority_curr_assign, priority_curr_rem = assign_currencies(assignment_dict, priority_currencies, campaign_dfs, 
                                                max_percent=max_priority_currencies_percent, 
                                                split_percentage=True)
        except Exception as error:
            print(f"Error assigning priority currencies: {error}")
            return f"Error: Failed to assign priority currencies - {error}"
    
        
        # Small Currencies Assignment
        print("Assigning Small Currencies...")
        try:
            small_curr_assign, small_curr_rem = assign_currencies(assignment_dict, small_currencies_to_limit, campaign_dfs, 
                                                max_percent=max_small_currencies_percent, 
                                                split_percentage=False)
        except Exception as error:
            print(f"Error assigning small currencies: {error}")
            return f"Error: Failed to assign small currencies - {error}"
        
        # Big Currencies Assignment
        print("Assigning Big Currencies...")
        try:
            big_curr_assign, big_curr_rem = assign_currencies(assignment_dict, big_currencies_to_limit, campaign_dfs, 
                                                        max_percent=max_big_currencies_percent,
                                                        split_percentage=True)
        except Exception as error:
            print(f"Error assigning big currencies: {error}")
            return f"Error: Failed to assign big currencies - {error}"

        # Union of assignments and Assignment Dictionary update
        assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign], ignore_index=True)
        remaining_assignments_dict = calculate_remaining_assignments_dict(assigned_users, assignment_dict)

        # Relevant Currencies Assignment
        print("Assigning Relevant Currencies...")
        try:
            relevant_curr_assign, relevant_curr_rem = assign_currencies(remaining_assignments_dict, relevant_currencies, campaign_dfs, 
                                                max_percent=None, 
                                                split_percentage=False)
        except Exception as error:
            print(f"Error assigning relevant currencies: {error}")
            return f"Error: Failed to assign relevant currencies - {error}"

        # Concatenation of assigned users and available users for assignment for different Currency types
        assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign, relevant_curr_assign], ignore_index=True)
        remaining_users = pd.concat([priority_curr_rem, small_curr_rem, big_curr_rem, relevant_curr_rem], ignore_index=True)

        # Update Assignment Dictionary
        print("Updating Assignment Dictionary...")

        remaining_assignments_dict = calculate_remaining_assignments_dict(assigned_users, assignment_dict)

        # Complete Assignment
        print("Completing Assignment with Additional Users...")
        try:
            complete_curr_assign, remaining_users_after_assigment = complete_assignments(
                remaining_users,
                remaining_assignments_dict,
                extra_users_campaign,
                priority_currencies,
                relevant_currencies
            )
        except Exception as error:
            print(f"Error completing assignments: {error}")
            return f"Error: Failed to complete assignments - {error}"

        # Concatenate all assigned users
        assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign, relevant_curr_assign, complete_curr_assign], ignore_index=True)
        print("User assignment process completed successfully.")

        print(count_users_per_operator(assigned_users))
        
        # ========== DATA LOADING ==========
        assigned_users = assigned_users.copy()

        # Convert internal codes to Spanish names for BigQuery
        assigned_users['campaign_name'] = assigned_users['campaign_name'].apply(normalize_campaign_to_display)

        # Ensure campaign_details column exists (NULL for regular campaigns, filled for external campaigns)
        if 'campaign_details' not in assigned_users.columns:
            assigned_users['campaign_details'] = None

        # Apply data type transformations
        assigned_users['assignment_date'] = pd.to_datetime(assigned_users['assignment_date'])
        assigned_users['user_id'] = assigned_users['user_id'].astype('Int64')
        assigned_users['level'] = assigned_users['level'].astype('Int64')
        assigned_users['phone'] = assigned_users['phone'].astype(str)
        assigned_users['last_activity'] = pd.to_datetime(assigned_users['last_activity'])

        # Reorder columns
        column_order = [
            'assignment_date',
            'operator', 
            'campaign_name',
            'campaign_details',
            'user_id',
            'username',
            'firstLast_name',
            'phone',
            'level',
            'register_currency',
            'last_activity'
        ]

        assigned_users = assigned_users[column_order]

        # Save assignment locally
        print("Saving assignment to local file...")
        try:
            assigned_users.to_excel(f'./data/Telemarketing_Assignment_{today}.xlsx', index=False)
            print("Assignment saved to local file.")
        except Exception as error:
            print(f"Error saving to local file: {error}")
            # Continue even if local save fails

        # Save assignment to BigQuery
        # Create BigQuery client
        bq_client = bigquery.Client(credentials=creds, project=project)

        # Create Assignment data dictionary
        dict_tlmkt_assignment = {
            'DailyAssignment': assigned_users[['assignment_date', 'operator', 'campaign_name', 'campaign_details',
            'user_id', 'username', 'firstLast_name', 'phone', 'level', 'register_currency', 'last_activity']]
        }

        # Call to Loading function
        print("Loading data to BigQuery...")
        try:
            CreateAndLoad_BQ(dict_tlmkt_assignment, 
                             bq_client, project_id='mi-casino', 
                             dataset_id='dm_telemarketing', 
                             prefix='tlmkt_', 
                             deleted_if_exist=False, 
                             load_data=True, 
                             delete_today=False)
        except Exception as error:
            print(f"Error loading data to BigQuery: {error}")
            return f"Error: Failed to load data to BigQuery - {error}"

        print("Daily assignment process finalized successfully.")
        return 'Assignment Completed'
    
    except Exception as error:
        print(f"Unexpected error in run_daily_assignment: {error}")
        return f"Error: Unexpected error - {error}"


