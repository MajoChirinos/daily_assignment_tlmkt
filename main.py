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
    create_priority_sort_key,
    assign_users_by_country,
    count_users_per_operator,
    create_assignment_metrics
)
from src.load import CreateAndLoad_BQ


def run_daily_assignment(request) -> str:
    try:
        creds, project = default()

        # Normalize LP country/country-code values to register_currency codes used by users data.
        country_to_currency = {
            'VE': 'VES', 'VES': 'VES', 'VENEZUELA': 'VES',
            'CL': 'CLP', 'CLP': 'CLP', 'CHILE': 'CLP',
            'PE': 'PEN', 'PEN': 'PEN', 'PERU': 'PEN', 'PERU\u0301': 'PEN',
            'EC': 'USD', 'ECUADOR': 'USD',
            'US': 'USD', 'USA': 'USD', 'USD': 'USD',
            'GT': 'GTQ', 'GTQ': 'GTQ', 'GUATEMALA': 'GTQ',
            'HN': 'HNL', 'HNL': 'HNL', 'HONDURAS': 'HNL',
            'MX': 'MXN', 'MXN': 'MXN', 'MEXICO': 'MXN', 'ME\u0301XICO': 'MXN',
            'CR': 'CRC', 'CRC': 'CRC', 'COSTARICA': 'CRC', 'COSTA RICA': 'CRC'
        }

        def normalize_country_to_currency(value):
            text = str(value).strip().upper()
            return country_to_currency.get(text, text)

        # ========== ASSIGNMENT CONFIGURATION ==========
        # Read configuration file
        try:
            conf = read_google_sheet('Daily_Assignment_Configuration', 2)
        except Exception as error:
            print(f"Error reading configuration from Google Sheets: {error}")
            return f"Error: Failed to read configuration - {error}"

        # Create configuration object
        config = Config(conf)

        # Variable declaration from Config class
        days_ago_to_discard = config.days_ago_to_discard
        exclude_email_mkt_users = config.exclude_email_mkt_users
        if isinstance(exclude_email_mkt_users, str):
            exclude_email_mkt_users = exclude_email_mkt_users.strip().lower() in (
                'true', '1', 'yes', 'y', 'si', 's'
            )
        users_to_assign_per_operator = config.users_to_assign_per_operator
        currencies_to_filter = config.currencies_to_filter
        campaigns_to_filter = config.campaigns_to_filter
        extra_users_country = getattr(config, 'extra_users_country', [])

        #print(f"exclude_email_mkt_users: {exclude_email_mkt_users}")

        # ========== DATES TO USE ==========
        today_dt = datetime.now()
        today_midnight = datetime(today_dt.year, today_dt.month, today_dt.day)
        today = datetime.strftime(today_midnight, '%Y-%m-%d').replace('-', '')
        yesterday_str = datetime.strftime(today_midnight - timedelta(days=1), '%Y-%m-%d')

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
                        'Usuario DotPanel' : 'user_dotpanel'}, inplace=True)

        if 'País' in lp.columns:
            lp.rename(columns={'País': 'country'}, inplace=True)
        elif 'Pais' in lp.columns:
            lp.rename(columns={'Pais': 'country'}, inplace=True)
        else:
            return "Error: LP_TLMKT is missing country column (País/Pais)"

        lp['country'] = lp['country'].astype(str).str.upper().str.split(',\s*')
        original_lp_countries = (
            lp[['country']]
            .explode('country')['country']
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
            .tolist()
        )
        lp['country'] = lp['country'].apply(
            lambda countries: [
                normalize_country_to_currency(country)
                for country in countries
                if str(country).strip()
            ]
        )

        # Normalize fallback countries from config to same currency code standard.
        extra_users_country = [
            normalize_country_to_currency(country)
            for country in extra_users_country
            if str(country).strip()
        ]

        # Historical assignment users from telemarketing
        try:
            daily_assigment_hist = get_data_hist('tlmkt_DailyAssignment', days_ago_to_discard, credentials=creds)
        except Exception as error:
            print(f"Error getting historical telemarketing data from BigQuery: {error}")
            return f"Error: Failed to get historical telemarketing data - {error}"
        
        print(f"Telemarketing historical users loaded: {daily_assigment_hist.shape[0]}")
        daily_assigment_hist['campaign_name'] = daily_assigment_hist['campaign_name'].apply(normalize_campaign_to_code)
        # Exclude today's assignments from discard; keep yesterday and older.
        daily_assigment_hist = daily_assigment_hist[daily_assigment_hist['assignment_date'] < today_midnight]

        # Users to discard (telemarketing + optional email marketing)
        tlmkt_users_to_discard = daily_assigment_hist[['user_id', 'campaign_name']]
        users_to_discard = tlmkt_users_to_discard.copy()

        if exclude_email_mkt_users:
            try:
                email_mkt_hist = get_data_hist('email_mkt_DailyAssignment', days_ago_to_discard, credentials=creds)
                print(f"Email marketing historical users loaded: {email_mkt_hist.shape[0]}")
                if not email_mkt_hist.empty:
                    email_mkt_hist['campaign_name'] = email_mkt_hist['campaign_name'].apply(normalize_campaign_to_code)
                    # Exclude today's assignments from discard; keep yesterday and older.
                    email_mkt_hist = email_mkt_hist[email_mkt_hist['assignment_date'] < today_midnight]
                    email_users_to_discard = email_mkt_hist[['user_id', 'campaign_name']]
                    users_to_discard = pd.concat([users_to_discard, email_users_to_discard], ignore_index=True)
            except Exception as error:
                print(f"Warning: Could not load email marketing history, continuing with TLMKT only: {error}")
        else:
            print("Skipping email marketing historical load (exclude_email_mkt_users=False)")

        print(f"Total users to discard: {users_to_discard.shape[0]}")

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



        if campaigns_to_filter:
            print(f"Filtering campaigns from config: {campaigns_to_filter}")
            available_users = available_users[~available_users['campaign_name'].isin(campaigns_to_filter)]
            print(f"Users after campaign filter: {available_users.shape[0]}")

        # Remove users contacted 'days_ago_to_discard' ago (users_to_discard)
        print(f"Discarding users contacted from {days_ago_to_discard} to {yesterday_str}")
        available_users = available_users.merge(
            users_to_discard, 
            on=['user_id', 'campaign_name'], 
            how='left', 
            indicator=True
        )

        available_users = available_users[available_users['_merge'] == 'left_only'].drop(columns=['_merge'])

        print(f"Available users for assignment: {available_users.shape[0]}")

        # Summary of available users by currency (after discarding contacted users)
        print("\nAvailable users by currency (after discarding contacted users):")
        currency_summary = available_users['register_currency'].value_counts()
        for currency, count in currency_summary.items():
            print(f"  \u2022 {currency}: {count} users")
        
        # Show users by campaign after filtering
        if not available_users.empty:
            print("\nUsers available per campaign after filtering contacted users:")
            users_per_campaign = available_users.groupby('campaign_name').size().sort_values(ascending=False)
            for campaign, count in users_per_campaign.items():
                print(f"  • {campaign} Campaign: {count} users")
                # Show currency distribution per campaign
                campaign_currencies = available_users[available_users['campaign_name'] == campaign]['register_currency'].value_counts()
                currency_info = ", ".join([f"{curr}: {cnt}" for curr, cnt in campaign_currencies.items()])
                print(f"    Currency Distribution: {currency_info}")

            # Operators assignment visibility by country
            print("\nOperators assigned by country:")
            lp_by_country = lp[['operator', 'country']].explode('country').copy()
            lp_by_country['country'] = lp_by_country['country'].astype(str).str.strip().str.upper()
            lp_by_country = lp_by_country[lp_by_country['country'] != '']

            for country, group in lp_by_country.groupby('country'):
                operators = sorted(group['operator'].dropna().astype(str).unique().tolist())
                print(f"  • {country}: {len(operators)} operators")
                print(f"    Operators: {', '.join(operators)}")

            # Priority visibility before assignment
            print("\nAvailable users by campaign and priority:")
            available_priority = (
                available_users
                .groupby(['campaign_name', 'priority'], dropna=False)
                .size()
                .reset_index(name='users')
            )
            available_priority['_priority_key'] = available_priority['priority'].apply(create_priority_sort_key)
            available_priority['_priority_key'] = available_priority['_priority_key'].apply(
                lambda key: key if key is not None else (99, 99)
            )
            available_priority = available_priority.sort_values(['_priority_key', 'campaign_name'])
            for _, row in available_priority.iterrows():
                print(f"  • {row['campaign_name']} | {row['priority']}: {row['users']}")
        else:
            print("⚠️ No users available for any campaign after filtering!")

        # Create dictionary of DataFrames by campaign 
        campaign_dfs = create_campaign_dataframes(available_users)

        # ========== USER ASSIGNMENT ==========
        # Create assignment dictionary
        print("\nCreating assignment dictionary...")
        assignment_dict = {}

        # Define percentages according to number of assigned countries per operator
        percentages = {
            1: [1.0],
            2: [0.7, 0.3],
            3: [0.5, 0.3, 0.2]
        }

        # Iterate over operators in LP and build quotas by country
        for _, row in lp.iterrows():
            operator = row['operator']
            countries = row['country']

            if not isinstance(countries, list):
                countries = [countries]

            countries = [str(country).strip().upper() for country in countries if str(country).strip()]
            num_countries = len(countries)

            if num_countries == 0:
                continue

            if num_countries in percentages:
                country_percentages = percentages[num_countries]
            else:
                # Fallback: split equally when operator has more than 3 countries
                country_percentages = [1 / num_countries] * num_countries

            for idx, country in enumerate(countries):
                users_to_assign = round(users_to_assign_per_operator * country_percentages[idx])

                if country not in assignment_dict:
                    assignment_dict[country] = []

                assignment_dict[country].append({
                    'operator': operator,
                    'users_to_assign': users_to_assign
                })

        print("Assignment Dictionary created successfully.")


        print("\n" + "="*80)
        print("Assigning users by country with global priority order...")
        if extra_users_country:
            print(f"Fallback countries to complete assignments: {extra_users_country}")
        else:
            print("No fallback countries configured — incomplete assignments will not be filled.")
        try:
            assigned_users, remaining_users = assign_users_by_country(
                available_users,
                assignment_dict,
                extra_users_country=extra_users_country
            )
        except Exception as error:
            print(f"Error assigning users by country: {error}")
            return f"Error: Failed to assign users by country - {error}"

        print(f"Users assigned: {len(assigned_users)}")
        print(f"Users remaining unassigned: {len(remaining_users)}")

        if not assigned_users.empty:
            print("\nAssigned users by campaign and priority:")
            assigned_priority = (
                assigned_users
                .groupby(['campaign', 'priority'], dropna=False)
                .size()
                .reset_index(name='users')
            )
            assigned_priority['_priority_key'] = assigned_priority['priority'].apply(create_priority_sort_key)
            assigned_priority['_priority_key'] = assigned_priority['_priority_key'].apply(
                lambda key: key if key is not None else (99, 99)
            )
            assigned_priority = assigned_priority.sort_values(['_priority_key', 'campaign'])
            for _, row in assigned_priority.iterrows():
                print(f"  • {row['campaign']} | {row['priority']}: {row['users']}")

        print("\n" + "="*80)
        print("User assignment process completed successfully.")
        print(f"\nFINAL ASSIGNMENT SUMMARY:")
        print(f"   Total users assigned: {len(assigned_users)}")

        # Show summary by campaign
        if not assigned_users.empty:
            print(f"\n   Assignment by campaign:")
            campaign_summary = assigned_users.groupby('campaign').agg({
                'user_id': 'count',
                'operator': 'nunique'
            }).rename(columns={'user_id': 'users', 'operator': 'operators'})
            for campaign, row in campaign_summary.iterrows():
                print(f"     • {campaign}: {row['users']} users assigned to {row['operators']} operators")
        
        print("\nAssigned users per operator (incomplete only):")
        users_per_operator_df = count_users_per_operator(assigned_users)
        users_per_operator_df = users_per_operator_df.rename(columns={'username': 'assigned_users'})

        # Add assigned countries from LP for easier operational review.
        operator_countries_df = lp[['operator', 'country']].copy()
        operator_countries_df['assigned_countries'] = operator_countries_df['country'].apply(
            lambda countries: ', '.join(sorted(set(countries))) if isinstance(countries, list) else str(countries)
        )
        operator_countries_df = operator_countries_df[['operator', 'assigned_countries']].drop_duplicates('operator')

        users_per_operator_df = users_per_operator_df.merge(
            operator_countries_df,
            on='operator',
            how='left'
        )

        incomplete_operators_df = users_per_operator_df[
            users_per_operator_df['assigned_users'] < users_to_assign_per_operator
        ].sort_values('assigned_users')

        if incomplete_operators_df.empty:
            print(f"All operators reached at least {users_to_assign_per_operator} assigned users.")
        else:
            print(incomplete_operators_df[['operator', 'assigned_countries', 'assigned_users']])
        
        # ========== CREATE ASSIGNMENT METRICS ==========
        # Create metrics DataFrame with available and assigned users per country and campaign
        print("\nCreating assignment metrics...")
        assignment_metrics = create_assignment_metrics(available_users, assigned_users, today)
        
        # Keep internal campaign codes for consistency with the rest of the system
        # assignment_metrics['campaign'] already has codes like: non_depositors, second_deposit, etc.
        
        # Convert assignment_date to datetime
        assignment_metrics['assignment_date'] = pd.to_datetime(assignment_metrics['assignment_date'], format='%Y%m%d')
        
        print("Assignment metrics created successfully.")
        print("\nAssignment Metrics:")
        print(assignment_metrics)
        
        # Save assignment metrics locally
        print("\nSaving assignment metrics to local file...")
        try:
            assignment_metrics.to_excel(f'./data/Assignment_Metrics_{today}.xlsx', index=False)
            print("Assignment metrics saved to local file.")
        except Exception as error:
            print(f"Error saving metrics to local file: {error}")
            # Continue even if local save fails
        
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

        # Sort for operator consumption: operator first, then highest priority to lowest.
        assigned_users['_priority_key'] = assigned_users['priority'].apply(create_priority_sort_key)
        assigned_users['_priority_key'] = assigned_users['_priority_key'].apply(
            lambda key: key if key is not None else (99, 99)
        )
        assigned_users = assigned_users.sort_values(
            ['operator', '_priority_key', 'campaign_name', 'user_id'],
            ascending=[True, True, True, True]
        )

        # Reorder columns
        column_order = [
            'assignment_date',
            'operator', 
            'campaign_name',
            'campaign_details',
            'priority',
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
            'DailyAssignment': assigned_users[['assignment_date', 'operator', 'campaign_name', 'user_id',
            'username', 'firstLast_name', 'phone', 'level', 'register_currency', 'last_activity',
            'campaign_details', 'priority']],
            'AssignmentMetrics': assignment_metrics[['assignment_date', 'campaign', 'available_users',
            'assigned_users', 'unassigned_users', 'country', 'priority']]
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


