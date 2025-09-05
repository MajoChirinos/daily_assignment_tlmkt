import pandas as pd
import numpy as np
import re
from collections import defaultdict
from typing import List, Dict


def normalize_campaign_to_code(campaign_str):
    """
    Converts Spanish campaign names to internal codes.
    Used when processing input data.
    
    Args:
        campaign_str (str): Campaign name in Spanish
        
    Returns:
        str: Internal campaign code
    """
    # Patterns to convert Spanish → code
    pattern_map = {
        r'No Depositantes': 'non_depositors',
        r'Reactivación': 'reactivation',
        r'Segundo Depósito': 'second_deposit',
        r'Tercer Depósito': 'third_deposit',
        r'Rejected': 'rejected'
    }
    
    for pat, val in pattern_map.items():
        campaign_str = re.sub(pat, val, campaign_str)
    return campaign_str


def normalize_campaign_to_display(campaign_str):
    """
    Converts internal codes to Spanish campaign names.
    Used before sending data to BigQuery or for display.
    
    Args:
        campaign_str (str): Internal campaign code
        
    Returns:
        str: Campaign name in Spanish
    """
    # Mapping to convert code → Spanish
    code_to_display = {
        'non_depositors': 'No Depositantes',
        'reactivation': 'Reactivación',
        'second_deposit': 'Segundo Depósito',
        'third_deposit': 'Tercer Depósito',
        'rejected': 'Rejected'
    }
    
    return code_to_display.get(campaign_str, campaign_str)


def analyze_user_distribution_by_currency(df):
    """
    Analyzes the DataFrame by the 'register_currency' column, counting users per currency and calculating
    the percentage of users for each currency.

    Parameters:
    - df: Input DataFrame with columns 'register_currency' and 'user_id'.

    Returns:
    - DataFrame with the 'register_currency' column, the user count per currency ('user_id'), and the percentage
      of users per currency.
    """
    # Group by 'register_currency' and count users
    df_group = df.groupby('register_currency')['user_id'].count().reset_index()
    
    # Calculate the percentage of users per currency
    df_group['percentage'] = (df_group['user_id'] / df_group['user_id'].sum() * 100).round(1)
    
    return df_group


def count_users_per_operator(df):
    """
    Counts the number of 'username' records per operator.

    Parameters:
    - df: DataFrame with columns 'operator' and 'username'.

    Returns:
    - DataFrame with the count of 'username' records per operator.
    """
    users_by_operator = df.groupby(['operator'])['username'].count().reset_index()
    return users_by_operator


def count_users_by_campaign_and_operator(df): 
    """
    Counts the number of 'username' records by campaign and operator.

    Parameters:
    - df: DataFrame with columns 'campaign_name' and 'operator'.

    Returns:
    - DataFrame with the count of 'username' records per campaign and operator.
    """
    users_by_campaing_n_operator = df.groupby(['campaign_name', 'operator'])['username'].count().reset_index()
    return users_by_campaing_n_operator


def analyze_operator_currency_distribution(df):
    """
    Analyzes operator and currency distribution, counting the number of 'username' records.

    Parameters:
    - df: DataFrame with columns 'operator', 'register_currency', and 'username'.

    Returns:
    - DataFrame with the count of 'username' records per operator and currency, sorted by operator.
    """
    users_by_ope_currency = df.groupby(['operator', 'register_currency'])['username'].count().reset_index()
    users_by_ope_currency = users_by_ope_currency.sort_values(by=['operator']).reset_index(drop=True)
    return users_by_ope_currency


def check_duplicates(df):
    """
    Checks for duplicates in the DataFrame by 'user_id'.
    
    Parameters:
    - df: DataFrame with the records to check.
    
    Returns:
    - DataFrame with the duplicates found.
    """
    # Check for duplicates by 'user_id'
    duplicates = df[df.duplicated(subset=['user_id'], keep=False)]
    
    # Show the duplicates found, sorted by 'user_id'
    duplicates = duplicates.sort_values('user_id')
    
    return duplicates


def get_operator_campaign_summary(df):
    """
    Gets a comprehensive summary of operators with their assigned campaigns and user distribution,
    showing currency distribution details.

    Parameters:
    - df: DataFrame with columns 'operator', 'campaign', 'register_currency' and 'username'.

    Returns:
    - DataFrame with:
        * operator: Operator name.
        * campaign: Campaign assigned to the operator.
        * username_count: Number of users assigned per campaign.
        * currency_distribution: Dictionary showing currency distribution per campaign.
    """
    # Group by operator and campaign, counting the number of users
    grouped = df.groupby(['operator', 'campaign', 'register_currency']).size().reset_index(name='username_count')

    # Create a dictionary of currency distribution by operator and campaign
    currency_distribution = (
        grouped.groupby(['operator', 'campaign'])
        .apply(lambda x: x.set_index('register_currency')['username_count'].to_dict())
        .reset_index(name='currency_distribution')
    )

    # Combine results with total users by operator and campaign
    result = (
        grouped.groupby(['operator', 'campaign'])['username_count']
        .sum()
        .reset_index()
        .merge(currency_distribution, on=['operator', 'campaign'], how='left')
    )

    # Sort results by operator and campaign
    result = result.sort_values(by=['operator', 'campaign']).reset_index(drop=True)

    return result


def count_users_by_campaign_and_currency(df):
    """
    Counts users by campaign and currency.

    Args:
        df (pd.DataFrame): DataFrame with 'campaign' and 'register_currency' columns.

    Returns:
        pd.DataFrame: DataFrame with columns:
            - 'campaign': Campaign name.
            - 'register_currency': Currency.
            - 'user_count': Number of users per campaign and currency.
    """
    grouped = df.groupby(['campaign', 'register_currency']).size().reset_index(name='user_count')
    return grouped


def count_operators_per_campaign(df):
    """
    Counts the number of unique operators per campaign.

    Parameters:
    - df: DataFrame with 'campaign' and 'operator' columns.

    Returns:
    - DataFrame with columns:
        * 'campaign': Campaign name.
        * 'unique_operators': Number of unique operators per campaign.
    """
    result = df.groupby('campaign')['operator'].nunique().reset_index()
    result.rename(columns={'operator': 'unique_operators'}, inplace=True)
    return result


def create_campaign_dataframes(available_users):
    """
    Create dictionary of DataFrames by campaign
    
    Args:
        available_users (pd.DataFrame): DataFrame with all available users
    
    Returns:
        dict: Diccionario con {campaign_name: DataFrame}
    """
    campaign_dfs = {}
    unique_campaigns = available_users['campaign_name'].unique()
    
    for campaign in unique_campaigns:
        campaign_dfs[campaign] = available_users[available_users['campaign_name'] == campaign].copy()
        print(f"{campaign}_df: {len(campaign_dfs[campaign])} users")
    
    return campaign_dfs


def calculate_remaining_assignments_dict(assigned_users_df, assignment_dict):
    """
    Calculates how many users each operator still needs to be assigned in each campaign and returns a dictionary.

    Args:
        assigned_users_df (pd.DataFrame): DataFrame with current assignments. Must contain columns:
            - 'campaign': Campaign name.
            - 'operator': Operator name.
            - 'user_id': ID of the assigned user.
        assignment_dict (dict): Dictionary with campaigns as keys and operators with users to assign as values.
            Ejemplo:
            {
                'campaign_1': [{'operator': 'operator_1', 'users_to_assign': 100}, ...],
                ...
            }

    Returns:
        dict: Diccionario con la estructura:
            {
                'campaign_1': [{'operator': 'operator_1', 'users_to_assign': 10}, ...],
                ...
            }
    """
    # Create a DataFrame from assignment_dict
    assignment_list = []
    for campaign, operators_info in assignment_dict.items():
        for operator_info in operators_info:
            assignment_list.append({
                'campaign': campaign,
                'operator': operator_info['operator'],
                'users_to_assign': operator_info['users_to_assign']
            })
    assignment_df = pd.DataFrame(assignment_list)

    # Count assigned users by campaign and operator
    assigned_counts = (
        assigned_users_df.groupby(['campaign', 'operator'])
        .size()
        .reset_index(name='users_assigned')
    )

    # Combine the assignments DataFrame with assigned users counts
    result_df = assignment_df.merge(assigned_counts, on=['campaign', 'operator'], how='left')

    # Fill NaN in 'users_assigned' with 0 (for operators without assigned users yet)
    result_df['users_assigned'] = result_df['users_assigned'].fillna(0).astype(int)

    # Calculate remaining users to assign
    result_df['users_remaining'] = result_df['users_to_assign'] - result_df['users_assigned']

    # Create the dictionary with the requested structure
    remnant_assignment_dict = defaultdict(list)
    for _, row in result_df.iterrows():
        if row['users_remaining'] > 0:  # Only include operators with remaining users
            remnant_assignment_dict[row['campaign']].append({
                'operator': row['operator'],
                'users_to_assign': row['users_remaining']
            })

    return dict(remnant_assignment_dict)


def assign_currencies(assignment_dict, currency_list, campaign_dfs, max_percent=None, split_percentage=False):
    """
    Unified function to assign users of different currency types equitably among operators.
    
    Args:
        assignment_dict (dict): Dictionary with campaigns as keys and operators with users to assign as values.
        currency_list (list): List of currencies to filter and assign.
        campaign_dfs (dict): Dictionary with {campaign_name: DataFrame}
        max_percent (float, optional): Maximum percentage of users to assign. If None, no limit.
        split_percentage (bool): If True, splits the percentage among currencies in the list.
                                If False, uses the total percentage for all currencies combined.
    
    Returns:
        pd.DataFrame: DataFrame with assignments made.
        pd.DataFrame: DataFrame with remaining unassigned users.
    """
    # Seed to ensure repeatability
    np.random.seed(42)
    
    assigned_users_list = []
    remaining_users_list = []

    for campaign, operators_info in assignment_dict.items():
        # Get campaign DataFrame from dictionary
        if campaign not in campaign_dfs:
            print(f"DataFrame for campaign {campaign} not found. Skipping...")
            continue

        campaign_df = campaign_dfs[campaign]

        # Filter users of specified currencies
        currency_users = campaign_df[campaign_df['register_currency'].isin(currency_list)]
        
        # Randomize DataFrame for random assignment
        currency_users = currency_users.sample(frac=1, random_state=42).reset_index(drop=True)

        if len(operators_info) == 0:
            print(f"No operators for campaign {campaign}. Skipping...")
            continue

        # If no percentage limit, assign without restrictions (relevant_currencies case)
        if max_percent is None:
            # Create a dictionary to track how many users each operator has received
            operator_assignments = {op_info['operator']: 0 for op_info in operators_info}
            operator_index = 0  # Index to assign users circularly among operators

            while not currency_users.empty:
                # Get current operator
                current_op_info = operators_info[operator_index]
                operator = current_op_info['operator']
                users_to_assign = current_op_info['users_to_assign']

                # Check how many users this operator already has assigned
                already_assigned = operator_assignments[operator]

                # Calculate how many more users this operator can receive
                remaining_limit = users_to_assign - already_assigned

                if remaining_limit <= 0:
                    # This operator has reached its limit, move to next
                    operator_index = (operator_index + 1) % len(operators_info)
                    
                    # If we've completed a full cycle and no operator can receive more users, exit
                    if operator_index == 0 and all(
                        operator_assignments[op_info['operator']] >= op_info['users_to_assign']
                        for op_info in operators_info
                    ):
                        break
                    continue

                # Always assign one by one for equitable distribution
                num_users_to_assign = 1

                # Assign users
                assigned = currency_users.iloc[:num_users_to_assign].copy()
                assigned['campaign'] = campaign
                assigned['operator'] = operator
                assigned_users_list.append(assigned)

                # Update assignment counter
                operator_assignments[operator] += num_users_to_assign

                # Remove assigned users
                currency_users = currency_users.iloc[num_users_to_assign:]

                # Advance to next operator circularly
                operator_index = (operator_index + 1) % len(operators_info)

        else:
            # Assignment with percentage limits
            if split_percentage:
                # Split percentage among currencies (priority_currencies, big_currencies behavior)
                num_currencies = len(currency_list)
                max_percent_per_currency = max_percent / num_currencies if num_currencies > 0 else 0

                # Create a dictionary to track how many users of each currency each operator has received
                operator_assignments = {op_info['operator']: {currency: 0 for currency in currency_list} for op_info in operators_info}

                # Assign users by currency
                for currency in currency_list:
                    currency_specific_users = currency_users[currency_users['register_currency'] == currency].copy()
                    operator_index = 0  # Index to assign users circularly among operators

                    while not currency_specific_users.empty:
                        # Get current operator
                        current_op_info = operators_info[operator_index]
                        operator = current_op_info['operator']
                        users_to_assign = current_op_info['users_to_assign']

                        # Calculate user limit for this currency and operator
                        max_users_per_currency = int(users_to_assign * max_percent_per_currency)

                        # Check how many users of this currency this operator already has assigned
                        already_assigned = operator_assignments[operator][currency]
                        
                        # Calculate how many more users this operator can receive for this currency
                        remaining_limit = max_users_per_currency - already_assigned

                        if remaining_limit <= 0:
                            # This operator has reached its limit for this currency, move to next
                            operator_index = (operator_index + 1) % len(operators_info)
                            
                            # If we've completed a full cycle and no operator can receive more users, exit
                            if operator_index == 0 and all(
                                operator_assignments[op_info['operator']][currency] >= int(op_info['users_to_assign'] * max_percent_per_currency)
                                for op_info in operators_info
                            ):
                                break
                            continue

                        # Determine how many users to assign
                        if len(currency_specific_users) < len(operators_info):
                            # If there are fewer users than operators, assign one to each operator until exhausted
                            num_users_to_assign = 1
                        else:
                            # Assign up to limit or one user at a time for equitable distribution
                            num_users_to_assign = min(len(currency_specific_users), remaining_limit, 1)

                        # Assign users
                        assigned = currency_specific_users.iloc[:num_users_to_assign].copy()
                        assigned['campaign'] = campaign
                        assigned['operator'] = operator
                        assigned_users_list.append(assigned)

                        # Update assignment counter
                        operator_assignments[operator][currency] += num_users_to_assign

                        # Remove assigned users
                        currency_specific_users = currency_specific_users.iloc[num_users_to_assign:]

                        # Advance to next operator circularly
                        operator_index = (operator_index + 1) % len(operators_info)

            else:
                # Use total percentage for all currencies combined (small_currencies behavior)
                # Create a dictionary to track how many users each operator has received
                operator_assignments = {op_info['operator']: 0 for op_info in operators_info}
                operator_index = 0  # Index to assign users circularly among operators

                while not currency_users.empty:
                    # Get current operator
                    current_op_info = operators_info[operator_index]
                    operator = current_op_info['operator']
                    users_to_assign = current_op_info['users_to_assign']

                    # Calculate total user limit for this operator
                    max_users_total = int(users_to_assign * max_percent)

                    # Check how many users this operator already has assigned
                    already_assigned = operator_assignments[operator]

                    # Calculate how many more users this operator can receive
                    remaining_limit = max_users_total - already_assigned

                    if remaining_limit <= 0:
                        # This operator has reached its limit, move to next
                        operator_index = (operator_index + 1) % len(operators_info)
                        
                        # If we've completed a full cycle and no operator can receive more users, exit
                        if operator_index == 0 and all(
                            operator_assignments[op_info['operator']] >= int(op_info['users_to_assign'] * max_percent)
                            for op_info in operators_info
                        ):
                            break
                        continue

                    # Always assign one by one for equitable distribution
                    num_users_to_assign = 1

                    # Assign users
                    assigned = currency_users.iloc[:num_users_to_assign].copy()
                    assigned['campaign'] = campaign
                    assigned['operator'] = operator
                    assigned_users_list.append(assigned)

                    # Update assignment counter
                    operator_assignments[operator] += num_users_to_assign

                    # Remove assigned users
                    currency_users = currency_users.iloc[num_users_to_assign:]

                    # Advance to next operator circularly
                    operator_index = (operator_index + 1) % len(operators_info)

        # Save remaining users
        remaining_currency_users = campaign_df[campaign_df['register_currency'].isin(currency_list)]
        
        # Remove users that were already assigned
        assigned_user_ids = []
        for assigned_df in assigned_users_list:
            if not assigned_df.empty and assigned_df['campaign'].iloc[0] == campaign:
                assigned_user_ids.extend(assigned_df['user_id'].tolist())
        
        remaining_currency_users = remaining_currency_users[~remaining_currency_users['user_id'].isin(assigned_user_ids)]
        
        if not remaining_currency_users.empty:
            remaining_currency_users = remaining_currency_users.copy()
            remaining_currency_users['campaign'] = campaign
            remaining_users_list.append(remaining_currency_users)

    # Combine all assignments and remaining users into DataFrames
    assigned_users_df = pd.concat(assigned_users_list, ignore_index=True) if assigned_users_list else pd.DataFrame()
    remaining_users_df = pd.concat(remaining_users_list, ignore_index=True) if remaining_users_list else pd.DataFrame()

    return assigned_users_df, remaining_users_df


def complete_assignments(remaining_users_df, remaining_assignments_dict, extra_users_campaign, priority_currencies, relevant_currencies):
    """
    Completes user assignment for operators, prioritizing users from the same campaign and then
    using users from campaigns in `extra_users_campaign`. Also prioritizes assignment of currencies
    in `priority_currencies` and `relevant_currencies`.

    Args:
        remaining_users_df (pd.DataFrame): DataFrame with unassigned users of the day. Must contain columns:
            - 'campaign': Campaign name.
            - 'user_id': User ID.
            - 'register_currency': User currency.
        remaining_assignments_dict (dict): Dictionary with campaigns as keys and operators with remaining users to assign.
        extra_users_campaign (list): List of campaigns to complete assignment with, in priority order.
        priority_currencies (list): List of priority currencies to assign first.
        relevant_currencies (list): List of relevant currencies to assign after priority ones.

    Returns:
        pd.DataFrame: DataFrame with assignments made.
        pd.DataFrame: DataFrame with remaining unassigned users.
    """
    # Seed to ensure repeatability
    np.random.seed(42)
    
    assigned_users_list = []
    # Keep a copy of available users that gets updated after each assignment
    available_users = remaining_users_df.copy()
    
    # Randomize DataFrame for random assignment
    available_users = available_users.sample(frac=1, random_state=42).reset_index(drop=True)

    for campaign, operators_info in remaining_assignments_dict.items():
        if len(operators_info) == 0:
            print(f"No operators for campaign {campaign}. Skipping...")
            continue

        # Create a dictionary to track how many users each operator has received
        operator_assignments = {op_info['operator']: 0 for op_info in operators_info}

        # Assign users by operator circularly
        operator_index = 0  # Index to assign users circularly among operators

        while not available_users.empty:
            # Get current operator
            current_op_info = operators_info[operator_index]
            operator = current_op_info['operator']
            users_to_assign = current_op_info['users_to_assign']

            # Check how many users this operator already has assigned
            already_assigned = operator_assignments[operator]

            # Calculate how many more users this operator can receive
            remaining_limit = users_to_assign - already_assigned

            if remaining_limit <= 0:
                # This operator has reached its limit, move to next
                operator_index = (operator_index + 1) % len(operators_info)
                
                # If we've completed a full cycle and no operator can receive more users, exit
                if operator_index == 0 and all(
                    operator_assignments[op_info['operator']] >= op_info['users_to_assign']
                    for op_info in operators_info
                ):
                    break
                continue

            # Search for available users in priority order
            user_to_assign = None
            
            # 1. Prioritize users from same campaign with priority currencies
            campaign_priority_users = available_users[
                (available_users['campaign'] == campaign) & 
                (available_users['register_currency'].isin(priority_currencies))
            ]
            if not campaign_priority_users.empty:
                user_to_assign = campaign_priority_users.iloc[0]
            
            # 2. Users from same campaign with relevant currencies
            elif not campaign_priority_users.empty or True:  # Always check this condition
                campaign_relevant_users = available_users[
                    (available_users['campaign'] == campaign) & 
                    (available_users['register_currency'].isin(relevant_currencies))
                ]
                if not campaign_relevant_users.empty:
                    user_to_assign = campaign_relevant_users.iloc[0]
            
            # 3. Any user from same campaign
            if user_to_assign is None:
                campaign_users = available_users[available_users['campaign'] == campaign]
                if not campaign_users.empty:
                    user_to_assign = campaign_users.iloc[0]
            
            # 4. Users from extra campaigns with priority currencies
            if user_to_assign is None:
                extra_priority_users = available_users[
                    (available_users['campaign'].isin(extra_users_campaign)) & 
                    (available_users['register_currency'].isin(priority_currencies))
                ]
                if not extra_priority_users.empty:
                    user_to_assign = extra_priority_users.iloc[0]
            
            # 5. Users from extra campaigns with relevant currencies
            if user_to_assign is None:
                extra_relevant_users = available_users[
                    (available_users['campaign'].isin(extra_users_campaign)) & 
                    (available_users['register_currency'].isin(relevant_currencies))
                ]
                if not extra_relevant_users.empty:
                    user_to_assign = extra_relevant_users.iloc[0]
            
            # 6. Any user from extra campaigns
            if user_to_assign is None:
                extra_users_df = available_users[available_users['campaign'].isin(extra_users_campaign)]
                if not extra_users_df.empty:
                    user_to_assign = extra_users_df.iloc[0]
            
            # If no users available, exit the loop
            if user_to_assign is None:
                break

            # Create assignment record
            assigned = user_to_assign.to_frame().T.copy()
            assigned['campaign'] = campaign
            assigned['operator'] = operator
            assigned_users_list.append(assigned)

            # Update assignment counter
            operator_assignments[operator] += 1

            # Remove assigned user from available users DataFrame
            user_id_to_remove = user_to_assign['user_id']
            available_users = available_users[available_users['user_id'] != user_id_to_remove]

            # Advance to next operator circularly
            operator_index = (operator_index + 1) % len(operators_info)

    # Combine all assignments
    assigned_users_df = pd.concat(assigned_users_list, ignore_index=True) if assigned_users_list else pd.DataFrame()
    
    return assigned_users_df, available_users


