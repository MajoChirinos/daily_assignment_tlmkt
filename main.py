import pandas as pd
import os
from datetime import datetime, timedelta

from google.cloud import bigquery
from google.auth import default

# Local project modules
from src.extract import read_bigquery_table, get_data, get_data_hist
from src.config import Config
from src.transform import (
    normalize_country_to_currency,
    normalize_campaign_to_code,
    normalize_campaign_to_display,
    create_campaign_dataframes,
    create_priority_sort_key,
    assign_users_by_country,
    count_users_per_operator,
    create_assignment_metrics,
    build_discard_from_hist,
)
from src.load import CreateAndLoad_BQ


def run_daily_assignment(request) -> str:
    try:
        creds, project = default(scopes=[
            'https://www.googleapis.com/auth/cloud-platform',
            'https://www.googleapis.com/auth/drive.readonly',
        ])


        # ========== ASSIGNMENT CONFIGURATION ==========
        # Read configuration file
        try:
            conf = read_bigquery_table('conf_parameters_v2', credentials=creds)
            conf = conf.dropna(subset=['variable', 'value', 'type'], how='all')
        except Exception as error:
            print(f"Error reading configuration from BigQuery: {error}")
            return f"Error: Failed to read configuration - {error}"

        # Create configuration object
        try:
            config = Config(conf)
        except Exception as error:
            print(f"Error loading configuration parameters: {error}")
            return f"Error: Failed to load configuration - {error}"

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
        campaigns_to_filter_by_currency = config.campaigns_to_filter_by_currency
        extra_users_country = getattr(config, 'extra_users_country', [])

        #print(f"exclude_email_mkt_users: {exclude_email_mkt_users}")

        # ========== DATES TO USE ==========
        today_dt = datetime.now()
        today_midnight = datetime(today_dt.year, today_dt.month, today_dt.day)
        today = datetime.strftime(today_midnight, '%Y-%m-%d').replace('-', '')
        yesterday_str = datetime.strftime(today_midnight - timedelta(days=1), '%Y-%m-%d')
        # days_ago_to_discard stays as int here; fetch_cutoff_str is computed after reading segments_tables

        # ========== DATA PREPARATION ==========
        # Segment tables to assign (read early to build per-campaign discard windows)
        try:
            segments_tables = read_bigquery_table('conf_segments_to_consult', credentials=creds)
        except Exception as error:
            print(f"Error reading segment tables from BigQuery: {error}")
            return f"Error: Failed to read segment tables - {error}"

        campaigns_to_assign = segments_tables['table_name'].tolist()
        source_table_to_campaign_label = dict(zip(segments_tables['table_name'], segments_tables['campaign_label']))

        # Build per-campaign discard window: fallback to global days_ago_to_discard
        campaign_discard_map = {}
        campaign_control_map = {}
        for _, seg_row in segments_tables.iterrows():
            label = seg_row['campaign_label']
            val = seg_row.get('days_ago_to_discard', '')
            if pd.notna(val) and str(val).strip():
                campaign_discard_map[label] = int(float(val))
            else:
                campaign_discard_map[label] = days_ago_to_discard

            ctrl_val = seg_row.get('control_group_percent', '')
            if pd.notna(ctrl_val) and str(ctrl_val).strip():
                campaign_control_map[label] = float(ctrl_val)
            else:
                campaign_control_map[label] = 0.0

        max_discard_days = max([days_ago_to_discard] + list(campaign_discard_map.values()))
        fetch_cutoff_dt = today_midnight - timedelta(days=max_discard_days)
        fetch_cutoff_str = datetime.strftime(fetch_cutoff_dt, '%Y-%m-%d')
        print(f"History fetch window: {max_discard_days} days back ({fetch_cutoff_str})")
        print(f"Per-campaign discard windows: { {k: v for k, v in campaign_discard_map.items()} }")

        # LP-TLMKT processing
        try:
            lp = read_bigquery_table('lp_tlmkt_current', credentials=creds)
        except Exception as error:
            print(f"Error reading lp_tlmkt_current from BigQuery: {error}")
            return f"Error: Failed to read lp_tlmkt_current - {error}"
        
        lp = lp[(lp['job_title'] == 'Ejecutivo de Televentas') & (lp['status'] == 'Activo')].copy()
        lp.rename(columns={'full_name': 'operator',
                        'dotpanel_username': 'user_dotpanel'}, inplace=True)

        if 'corporate_email' in lp.columns:
            lp.rename(columns={'corporate_email': 'op_email'}, inplace=True)
            lp['op_email'] = lp['op_email'].astype('string').str.strip()
            lp['op_email'] = lp['op_email'].where(
                lp['op_email'].notna() & ~lp['op_email'].isin(['', 'nan', 'None']), None
            ).astype(object)
            lp.loc[lp['op_email'].isna(), 'op_email'] = None
        else:
            lp['op_email'] = None

        if 'country' not in lp.columns:
            return "Error: lp_tlmkt_current is missing country column"

        lp['country'] = lp['country'].fillna('').astype(str).str.upper().str.split(r',\s*')
        original_lp_countries = (
            lp[['country']]
            .explode('country')['country']
            .dropna()
            .astype(str)
            .str.upper()
            .unique()
            .tolist()
        )
        try:
            lp['country'] = lp['country'].apply(
                lambda countries: [
                    normalize_country_to_currency(country)
                    for country in countries
                    if str(country).strip()
                ]
            )
            extra_users_country = [
                normalize_country_to_currency(country)
                for country in extra_users_country
                if str(country).strip()
            ]
        except Exception as error:
            print(f"Error normalizing country codes: {error}")
            return f"Error: Failed to normalize country codes - {error}"

        # Historical assignment users from telemarketing
        try:
            daily_assigment_hist = get_data_hist('tlmkt_DailyAssignment', fetch_cutoff_str, credentials=creds)
        except Exception as error:
            print(f"Error getting historical telemarketing data from BigQuery: {error}")
            return f"Error: Failed to get historical telemarketing data - {error}"

        print(f"Telemarketing historical users loaded: {daily_assigment_hist.shape[0]}")
        daily_assigment_hist['campaign_name'] = daily_assigment_hist['campaign_name'].apply(normalize_campaign_to_code)
        # Exclude today's assignments from discard; keep yesterday and older.
        daily_assigment_hist = daily_assigment_hist[daily_assigment_hist['assignment_date'] < today_midnight]

        # Build users_to_discard per campaign using each campaign's specific lookback window
        try:
            users_to_discard = build_discard_from_hist(
                daily_assigment_hist, campaign_discard_map, today_midnight, days_ago_to_discard
            )
        except Exception as error:
            print(f"Error building discard list from telemarketing history: {error}")
            return f"Error: Failed to build discard list - {error}"

        if exclude_email_mkt_users:
            try:
                email_mkt_hist = get_data_hist('email_mkt_DailyAssignment', fetch_cutoff_str, credentials=creds)
                print(f"Email marketing historical users loaded: {email_mkt_hist.shape[0]}")
                if not email_mkt_hist.empty:
                    email_mkt_hist['campaign_name'] = email_mkt_hist['campaign_name'].apply(normalize_campaign_to_code)
                    # Exclude today's assignments from discard; keep yesterday and older.
                    email_mkt_hist = email_mkt_hist[email_mkt_hist['assignment_date'] < today_midnight]
                    email_users_to_discard = build_discard_from_hist(
                        email_mkt_hist, campaign_discard_map, today_midnight, days_ago_to_discard
                    )
                    users_to_discard = pd.concat([users_to_discard, email_users_to_discard], ignore_index=True).drop_duplicates()
            except Exception as error:
                print(f"Warning: Could not load email marketing history, continuing with TLMKT only: {error}")
        else:
            print("Skipping email marketing historical load (exclude_email_mkt_users=False)")

        print(f"Total users to discard: {users_to_discard.shape[0]}")

        # Data extraction to assign
        print("Extracting data to assign...")
        try:
            available_users = get_data(campaigns_to_assign, currencies_to_filter, credentials=creds)
        except Exception as error:
            print(f"Error extracting data from BigQuery: {error}")
            return f"Error: Failed to extract data - {error}"
        print("\nData extracted successfully")
        if not available_users.empty:
            available_users['campaign_label'] = (
                available_users['source_table']
                .map(source_table_to_campaign_label)
                .fillna(available_users['campaign_name'])
            )

        if not available_users.empty and 'source_table' in available_users.columns:
            print("\nDownloaded users by table and campaign (before config filters):")
            downloaded_summary = (
                available_users
                .groupby(['source_table', 'campaign_name'])
                .size()
                .reset_index(name='users')
                .sort_values(['source_table', 'users'], ascending=[True, False])
            )
            for _, row in downloaded_summary.iterrows():
                print(f"  • {row['source_table']} | {row['campaign_name']}: {row['users']} users")



        if campaigns_to_filter:
            print(f"Filtering campaigns from config: {campaigns_to_filter}")
            removed_global = available_users[available_users['campaign_name'].isin(campaigns_to_filter)]
            if not removed_global.empty:
                removed_global_summary = removed_global['campaign_name'].value_counts()
                for campaign, count in removed_global_summary.items():
                    print(f"  - Removed by global campaign filter | {campaign}: {count} users")
            available_users = available_users[~available_users['campaign_name'].isin(campaigns_to_filter)]
            print(f"Users after campaign filter: {available_users.shape[0]}")

        if campaigns_to_filter_by_currency:
            print("\nFiltering campaigns by currency:")
            for currency, campaigns in campaigns_to_filter_by_currency.items():
                print(f"  - {currency}: {campaigns}")
            for currency, campaigns in campaigns_to_filter_by_currency.items():
                mask = (
                    (available_users['register_currency'] == currency) &
                    (available_users['campaign_name'].isin(campaigns))
                )
                removed = mask.sum()
                if removed > 0:
                    removed_breakdown = (
                        available_users[mask]
                        .groupby('campaign_name')
                        .size()
                        .sort_values(ascending=False)
                    )
                    for campaign, campaign_count in removed_breakdown.items():
                        print(f"    · Removed {campaign_count} users from campaign '{campaign}' in {currency}")
                available_users = available_users[~mask]
                print(f"  [{currency}] Removed {removed} users from campaigns {campaigns}")
            print(f"\nUsers after currency-campaign filter: {available_users.shape[0]}")

        # Remove users contacted within each campaign's lookback window
        print(f"\nDiscarding previously contacted users (per-campaign windows, up to {yesterday_str})")
        available_users = available_users.merge(
            users_to_discard, 
            on=['user_id', 'campaign_name'], 
            how='left', 
            indicator=True
        )

        discarded_by_campaign = (
            available_users[available_users['_merge'] == 'both']
            .groupby('campaign_name')
            .size()
            .sort_values(ascending=False)
        )
        if not discarded_by_campaign.empty:
            print("Users discarded by campaign due to contact history:")
            for campaign, count in discarded_by_campaign.items():
                print(f"  • {campaign}: {count} users discarded")

        available_users = available_users[available_users['_merge'] == 'left_only'].drop(columns=['_merge'])

        before_dedup = available_users.shape[0]
        available_users['_priority_key'] = available_users['priority'].apply(create_priority_sort_key)
        available_users['_priority_key'] = available_users['_priority_key'].apply(
            lambda key: key if key is not None else (99, 99)
        )
        available_users = available_users.sort_values(['user_id', '_priority_key'], kind='stable')
        available_users = available_users.drop_duplicates(subset=['user_id'], keep='first')
        available_users = available_users.drop(columns=['_priority_key'])
        dedup_removed = before_dedup - available_users.shape[0]
        if dedup_removed > 0:
            print(f"Duplicate users removed by priority order: {dedup_removed}")

        print(f"\nAvailable users for assignment: {available_users.shape[0]}")

        # ========== CONTROL GROUP RESERVATION ==========
        # Reserve a fixed percentage per campaign BEFORE assignment.
        # Seed is date-based (YYYYMMDD) so re-running on the same day yields identical results.
        control_seed = int(today)
        control_users = available_users.head(0).copy()
        active_control_campaigns = {k: v for k, v in campaign_control_map.items() if v > 0}
        if active_control_campaigns:
            print(f"\nReserving control groups (random seed={control_seed}):")
            control_indices = []
            for campaign_label, pct in active_control_campaigns.items():
                campaign_pool = available_users[available_users['campaign_name'] == campaign_label]
                n_control = int(len(campaign_pool) * pct)
                if n_control > 0:
                    reserved = campaign_pool.sample(n=n_control, random_state=control_seed)
                    control_indices.extend(reserved.index.tolist())
                    print(f"  • {campaign_label}: {n_control}/{len(campaign_pool)} users reserved as control ({pct:.0%})")
                else:
                    print(f"  • {campaign_label}: 0 users reserved (pool too small for {pct:.0%})")
            if control_indices:
                control_users = available_users.loc[control_indices].copy()
            available_users = available_users.drop(index=control_indices)
            print(f"Available users after control group reservation: {available_users.shape[0]}")
        else:
            print("\nNo control groups configured — all available users eligible for assignment.")

        # Summary of available users by campaign (after discarding contacted users)
        print("\nAvailable users by campaign (after discarding contacted users):")
        campaign_summary = available_users['campaign_name'].value_counts()
        for campaign, count in campaign_summary.items():
            print(f"  • {campaign}: {count} users")
        
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

            # Round all countries except the last; give the last the exact remainder
            # so the operator's total always equals users_to_assign_per_operator,
            # whatever value that parameter has (e.g. 50, 100, etc.).
            remaining_to_assign = users_to_assign_per_operator
            for idx, country in enumerate(countries):
                if idx < num_countries - 1:
                    users_to_assign = round(users_to_assign_per_operator * country_percentages[idx])
                else:
                    users_to_assign = remaining_to_assign
                remaining_to_assign -= users_to_assign

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
        assigned_users['group_flag'] = 'TREATMENT'

        control_users = control_users.copy()
        if not control_users.empty:
            control_users['campaign'] = control_users['campaign_name']
            control_users['operator'] = None
            control_users['group_flag'] = 'CONTROL'

        daily_assignment_users = pd.concat([assigned_users, control_users], ignore_index=True)

        if not daily_assignment_users.empty:
            print("\nDaily Assignment group summary by campaign:")
            group_summary = (
                daily_assignment_users
                .groupby(['campaign_name', 'group_flag'])
                .size()
                .unstack(fill_value=0)
            )
            for campaign, row in group_summary.iterrows():
                control_count = row.get('CONTROL', 0)
                treatment_count = row.get('TREATMENT', 0)
                print(f"  • {campaign}: CONTROL={control_count}, TREATMENT={treatment_count}")

        # Convert internal codes to Spanish names for BigQuery
        daily_assignment_users['campaign_name'] = daily_assignment_users['campaign_name'].apply(normalize_campaign_to_display)

        # Ensure campaign_details column exists (NULL for regular campaigns, filled for external campaigns)
        if 'campaign_details' not in daily_assignment_users.columns:
            daily_assignment_users['campaign_details'] = None

        # Apply data type transformations
        daily_assignment_users['assignment_date'] = pd.to_datetime(daily_assignment_users['assignment_date'])
        daily_assignment_users['user_id'] = daily_assignment_users['user_id'].astype('Int64')
        daily_assignment_users['level'] = daily_assignment_users['level'].astype('Int64')
        daily_assignment_users['phone'] = daily_assignment_users['phone'].astype(str)
        daily_assignment_users['last_activity'] = pd.to_datetime(daily_assignment_users['last_activity'])
        daily_assignment_users['experiment_id'] = None
        daily_assignment_users['channel'] = 'TLMKT'
        daily_assignment_users['user_campaign_id'] = (
            daily_assignment_users['user_id'].astype(str)
            + '_'
            + daily_assignment_users['campaign_label'].astype(str)
            + '_'
            + today
        )

        # Add corporate email from LP, tied to each operator.
        operator_email_map = (
            lp[['operator', 'op_email']]
            .dropna(subset=['operator'])
            .drop_duplicates(subset=['operator'], keep='first')
        )
        daily_assignment_users = daily_assignment_users.merge(operator_email_map, on='operator', how='left')

        # Sort for operator consumption: operator first, then highest priority to lowest.
        daily_assignment_users['_priority_key'] = daily_assignment_users['priority'].apply(create_priority_sort_key)
        daily_assignment_users['_priority_key'] = daily_assignment_users['_priority_key'].apply(
            lambda key: key if key is not None else (99, 99)
        )
        daily_assignment_users = daily_assignment_users.sort_values(
            ['operator', '_priority_key', 'campaign_name', 'user_id'],
            ascending=[True, True, True, True],
            na_position='last'
        )

        # Reorder columns
        column_order = [
            'assignment_date',
            'operator',
            'op_email', 
            'campaign_name',
            'campaign_details',
            'priority',
            'user_id',
            'username',
            'firstLast_name',
            'phone',
            'level',
            'register_currency',
            'last_activity',
            'group_flag',
            'experiment_id',
            'channel',
            'user_campaign_id'
        ]

        daily_assignment_users = daily_assignment_users[column_order]

        # Save assignment locally
        print("\nSaving assignment to local file...")
        try:
            daily_assignment_users.to_excel(f'./data/Telemarketing_Assignment_{today}.xlsx', index=False)
            print("Assignment saved to local file.")
        except Exception as error:
            print(f"Error saving to local file: {error}")
            # Continue even if local save fails

        # Save assignment to BigQuery
        # Create BigQuery client
        bq_client = bigquery.Client(credentials=creds, project=project)

        # Create Assignment data dictionary
        dict_tlmkt_assignment = {
            'DailyAssignment': daily_assignment_users[['assignment_date', 'operator', 'campaign_name', 'user_id',
            'username', 'firstLast_name', 'phone', 'level', 'register_currency', 'last_activity',
            'campaign_details', 'priority', 'op_email', 'group_flag', 'experiment_id', 'channel',
            'user_campaign_id']],
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
        return f"Error: Unexpected error - {error}"'group_flag',



