import pandas as pd
import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.auth import default

# Local project modules
from src.extract import *
from src.config import *
from src.transform import *
from src.load import *


def run_daily_assignment() -> str:

    # ========== CREDENTIALS CONFIGURATION ==========
    # Clear service account to use CLI credentials
    os.environ.pop('GOOGLE_APPLICATION_CREDENTIALS', None)

    # Get credentials and project
    creds, project = default()

    print("CLI credentials loaded successfully")

    # ========== ASSIGNMENT CONFIGURATION ==========
    # Read configuration file
    conf = read_google_sheet('Daily_Assignment_Configuration', 0)

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
    lp = read_google_sheet('LP_TLMKT', 0)
    lp = lp[lp['Cargo'] == 'Ejecutivo de Televentas']
    lp = lp[lp['Estatus'] == 'Activo']
    lp.rename(columns={'Nombre y Apellido': 'operator',
                    'Usuario DotPanel' : 'user_dotpanel',
                    'Campaña' : 'campaign_name'}, inplace=True)
    
    # Apply campaign normalization using imported function
    lp['campaign_name'] = lp['campaign_name'].apply(normalize_campaign_to_code)
    lp['campaign_name'] = lp['campaign_name'].str.split(',\s*')

    # Historical assignment users
    daily_assigment_hist = get_data_hist('tlmkt_DailyAssignment', days_ago_to_discard, credentials=creds)
    daily_assigment_hist['campaign_name'] = daily_assigment_hist['campaign_name'].apply(normalize_campaign_to_code)
    daily_assigment_hist = daily_assigment_hist[daily_assigment_hist['assignment_date'] < today]

    # Users to discard
    users_to_discard = daily_assigment_hist[['user_id', 'campaign_name']]

    # Segment tables to assign
    segments_tables = read_google_sheet('Daily_Assignment_Configuration', 1)
    campaigns_to_assign = segments_tables['table_name'].tolist()

    # Data extraction to assign
    print("Extracting data to assign...")
    available_users = get_data(campaigns_to_assign, currencies_to_filter, credentials=creds)
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

    print("Available users for assignment:")
    print(available_users.shape[0])

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
    priority_curr_assign, priority_curr_rem  = assign_currencies(assignment_dict, priority_currencies, campaign_dfs, 
                                         max_percent=max_priority_currencies_percent, 
                                         split_percentage=True)
    
    
    # Small Currencies Assignment
    print("Assigning Small Currencies...")
    small_curr_assign, small_curr_rem = assign_currencies(assignment_dict, small_currencies_to_limit, campaign_dfs, 
                                         max_percent=max_small_currencies_percent, 
                                         split_percentage=False)
    
    # Big Currencies Assignment
    print("Assigning Big Currencies...")
    big_curr_assign, big_curr_rem = assign_currencies(assignment_dict, big_currencies_to_limit, campaign_dfs, 
                                                  max_percent=max_big_currencies_percent,
                                                  split_percentage=True)

    # Union of assignments and Assignment Dictionary update
    assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign], ignore_index=True)
    remaining_assignments_dict = calculate_remaining_assignments_dict(assigned_users, assignment_dict)

    # Relevant Currencies Assignment
    print("Assigning Relevant Currencies...")
    relevant_curr_assign, relevant_curr_rem  = assign_currencies(remaining_assignments_dict, relevant_currencies, campaign_dfs, 
                                         max_percent=None, 
                                         split_percentage=False)

    # Concatenation of assigned users and available users for assignment for different Currency types
    assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign, relevant_curr_assign], ignore_index=True)
    remaining_users = pd.concat([priority_curr_rem, small_curr_rem, big_curr_rem, relevant_curr_rem], ignore_index=True)

    # Update Assignment Dictionary
    print("Updating Assignment Dictionary...")

    remaining_assignments_dict = calculate_remaining_assignments_dict(assigned_users, assignment_dict)

    # Complete Assignment
    print("Completing Assignment with Additional Users...")
    complete_curr_assign, remaining_users_after_assigment = complete_assignments(
    remaining_users,
    remaining_assignments_dict,
    extra_users_campaign,
    priority_currencies,
    relevant_currencies
    )

    # Concatenate all assigned users
    assigned_users = pd.concat([priority_curr_assign, small_curr_assign, big_curr_assign, relevant_curr_assign, complete_curr_assign], ignore_index=True)
    print("Assignment completed.")

    print(count_users_per_operator(assigned_users))
    # ========== DATA LOADING ==========
    assigned_users = assigned_users.copy()

    # Convert internal codes to Spanish names for BigQuery
    assigned_users['campaign_name'] = assigned_users['campaign_name'].apply(normalize_campaign_to_display)

    # Apply data type transformations
    assigned_users['assignment_date'] = pd.to_datetime(assigned_users['assignment_date'])
    assigned_users['user_id'] = assigned_users['user_id'].astype('Int64')
    assigned_users['level'] = assigned_users['level'].astype('Int64')
    assigned_users['last_activity'] = pd.to_datetime(assigned_users['last_activity'])

    # Reorder columns
    column_order = [
        'assignment_date',
        'operator', 
        'campaign_name',
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
    assigned_users.to_excel(f'./data/Telemarketing_Assignment_{today}.xlsx', index=False)
    print("Assignment saved to local file.")

    # Save assignment to BigQuery
    # Create BigQuery client
    bq_client = bigquery.Client(credentials=creds, project=project)

    # Create Assignment data dictionary
    dict_tlmkt_assignment = {
        'DailyAssignment': assigned_users[['assignment_date', 'operator', 'campaign_name', 'user_id', 'username',
        'firstLast_name', 'phone', 'level', 'register_currency', 'last_activity']]
    }

    # Call to Loading function
    print("Loading data to BigQuery...")
    #CreateAndLoad_BQ(dict_tlmkt_assignment, bq_client, project_id='mi-casino', dataset_id='dm_telemarketing', prefix='tlmkt_', deleted_if_exist=False, load_data=True)
    print("Data loaded to BigQuery successfully.")

    return 'Assignment Completed'

run_daily_assignment()
