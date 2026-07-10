import pandas as pd
import numpy as np
import re
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict


# Country/currency code lookup used to normalize LP country values
# to the register_currency codes present in user data.
_COUNTRY_TO_CURRENCY = {
    'VE': 'VES', 'VES': 'VES', 'VENEZUELA': 'VES',
    'CL': 'CLP', 'CLP': 'CLP', 'CHILE': 'CLP',
    'PE': 'PEN', 'PEN': 'PEN', 'PERU': 'PEN', 'PERÚ': 'PEN',
    'EC': 'USD', 'ECUADOR': 'USD',
    'US': 'USD', 'USA': 'USD', 'USD': 'USD',
    'GT': 'GTQ', 'GTQ': 'GTQ', 'GUATEMALA': 'GTQ',
    'HN': 'HNL', 'HNL': 'HNL', 'HONDURAS': 'HNL',
    'MX': 'MXN', 'MXN': 'MXN', 'MEXICO': 'MXN', 'MÉXICO': 'MXN',
    'CR': 'CRC', 'CRC': 'CRC', 'COSTARICA': 'CRC', 'COSTA RICA': 'CRC',
}


def normalize_country_to_currency(value: str) -> str:
    """
    Converts a country name, ISO-2 code, or currency code to the
    register_currency code used in user data.

    Falls back to the uppercased input if no mapping is found,
    so unknown values pass through without raising.

    Args:
        value (str): Country name, ISO-2 code, or currency code (case-insensitive).

    Returns:
        str: Corresponding register_currency code (e.g. 'PEN', 'CLP').

    Examples:
        >>> normalize_country_to_currency('PE')
        'PEN'
        >>> normalize_country_to_currency('Chile')
        'CLP'
        >>> normalize_country_to_currency('XYZ')  # unknown → passthrough
        'XYZ'
    """
    text = str(value).strip().upper()
    return _COUNTRY_TO_CURRENCY.get(text, text)


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


def create_priority_sort_key(priority_value):
    """
    Creates a sortable tuple from priority string for proper ordering.
    
    Priority format: LEVEL-NUMBER (e.g., ULTRA-1, ALTA-2, MEDIA-3, BAJA-1)
    - ULTRA has highest priority (1)
    - ALTA has second priority (2)
    - MEDIA has third priority (3)
    - BAJA has lowest priority (4)
    - Within each level, lower numbers have higher priority (1 > 2 > 3)
    
    Args:
        priority_value (str): Priority string in format "LEVEL-NUMBER"
        
    Returns:
        tuple: (level_rank, number) for sorting. Returns None for invalid/null values.
    """
    if pd.isna(priority_value) or not priority_value:
        return None  # Nulls will maintain their random position
    
    # Priority level mapping
    priority_levels = {
        'ULTRA': 1,
        'ALTA': 2,
        'MEDIA': 3,
        'BAJA': 4
    }
    
    try:
        # Split by hyphen: "ULTRA-1" -> ["ULTRA", "1"]
        parts = str(priority_value).split('-')
        if len(parts) == 2:
            level = parts[0].strip().upper()
            number = int(parts[1].strip())
            level_rank = priority_levels.get(level, None)
            if level_rank is not None:
                return (level_rank, number)
        return None  # Invalid format
    except (ValueError, AttributeError):
        return None  # Error parsing


def sort_by_priority(df):
    """
    Sorts a DataFrame by priority column, maintaining random order within same priority.
    Rows with null/invalid priority values maintain their original random position.
    
    Args:
        df (pd.DataFrame): DataFrame with 'priority' column
        
    Returns:
        pd.DataFrame: Sorted DataFrame (rows with priority first, then rows without)
    """
    if df.empty or 'priority' not in df.columns:
        return df
    
    # Separate rows with valid priority from those without
    df_copy = df.copy()
    df_copy['_has_priority'] = df_copy['priority'].apply(
        lambda x: create_priority_sort_key(x) is not None
    )
    
    # Split DataFrames
    df_with_priority = df_copy[df_copy['_has_priority']].copy()
    df_without_priority = df_copy[~df_copy['_has_priority']].copy()
    
    # Sort only the ones with valid priority
    if not df_with_priority.empty:
        df_with_priority['_priority_sort_key'] = df_with_priority['priority'].apply(create_priority_sort_key)
        df_with_priority = df_with_priority.sort_values('_priority_sort_key', kind='stable')
        df_with_priority = df_with_priority.drop(columns=['_priority_sort_key', '_has_priority'])
    else:
        df_with_priority = df_with_priority.drop(columns=['_has_priority'])
    
    # Clean up the without priority DataFrame
    if not df_without_priority.empty:
        df_without_priority = df_without_priority.drop(columns=['_has_priority'])
    
    # Concatenate: prioritized first, then random order for the rest
    df_sorted = pd.concat([df_with_priority, df_without_priority], ignore_index=True)
    
    return df_sorted


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
    Create dictionary of DataFrames by campaign, pre-sorted by priority.
    
    Each campaign DataFrame is:
    1. Randomized first (to break ties within same priority)
    2. Sorted by priority (ULTRA-1, ULTRA-2, ALTA-1, etc.)
    
    This ensures that when filtered by currency later, the priority order is maintained.
    
    Args:
        available_users (pd.DataFrame): DataFrame with all available users
    
    Returns:
        dict: Dictionary with {campaign_name: DataFrame sorted by priority}
    """
    np.random.seed(42)  # Ensure repeatability
    
    campaign_dfs = {}
    unique_campaigns = available_users['campaign_name'].unique()
    
    for campaign in unique_campaigns:
        campaign_df = available_users[available_users['campaign_name'] == campaign].copy()
        
        # First randomize to break ties within same priority
        campaign_df = campaign_df.sample(frac=1, random_state=42).reset_index(drop=True)
        
        # Then sort by campaign priority (ULTRA-1, ULTRA-2, ALTA-1, etc.)
        # This ensures ALL ULTRA-1 users (across all currencies) come before ULTRA-2
        campaign_df = sort_by_priority(campaign_df)
        
        campaign_dfs[campaign] = campaign_df
    
    return campaign_dfs


def build_discard_from_hist(
    hist_df: pd.DataFrame,
    campaign_discard_map: Dict[str, int],
    today_midnight: datetime,
    global_days_ago: int,
) -> pd.DataFrame:
    """
    Builds the set of (user_id, campaign_name) pairs to discard from assignment,
    applying a per-campaign lookback window defined in campaign_discard_map.

    For each campaign in campaign_discard_map the function keeps only history
    records within that campaign's window.  Any campaign present in hist_df but
    NOT in campaign_discard_map falls back to the global_days_ago window.

    Args:
        hist_df (pd.DataFrame): Historical assignment records with at least
            columns ['user_id', 'campaign_name', 'assignment_date'].
            assignment_date must be a timezone-naive datetime column and must
            already exclude today (assignment_date < today_midnight).
        campaign_discard_map (Dict[str, int]): Mapping of campaign_label → days.
            Built from the segments_to_consult sheet column days_ago_to_discard.
        today_midnight (datetime): Start of today with no time component
            (datetime(year, month, day)) used as the reference point for cutoffs.
        global_days_ago (int): Fallback lookback in days for campaigns not
            present in campaign_discard_map.

    Returns:
        pd.DataFrame: Deduplicated DataFrame with columns ['user_id', 'campaign_name']
            representing users that must be excluded from today's assignment.
    """
    parts = []

    # Per-campaign filtered slices
    for campaign_label, days in campaign_discard_map.items():
        cutoff = today_midnight - timedelta(days=days)
        campaign_hist = hist_df[
            (hist_df['campaign_name'] == campaign_label) &
            (hist_df['assignment_date'] >= cutoff)
        ][['user_id', 'campaign_name']]
        parts.append(campaign_hist)

    # Campaigns in the history that are not in the map → use global window
    known_labels = set(campaign_discard_map.keys())
    global_cutoff = today_midnight - timedelta(days=global_days_ago)
    other_hist = hist_df[
        (~hist_df['campaign_name'].isin(known_labels)) &
        (hist_df['assignment_date'] >= global_cutoff)
    ][['user_id', 'campaign_name']]
    parts.append(other_hist)

    return pd.concat(parts, ignore_index=True).drop_duplicates()


def assign_users_by_country(available_users, assignment_dict, extra_users_country=None):
    """
    Assign users equitably by country, preserving global priority order within each country.
    If users from a target country are exhausted, fills using countries listed in
    extra_users_country (in the same order).

    Args:
        available_users (pd.DataFrame): DataFrame with available users.
            Must contain: 'register_currency', 'campaign_name', 'priority'.
        assignment_dict (dict): Dictionary with countries as keys and operator quotas as values.
            Example:
            {
                'VES': [{'operator': 'op_1', 'users_to_assign': 130}, ...],
                ...
            }
        extra_users_country (list[str], optional): Fallback countries used to complete
            assignments when target-country users are not available.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: (assigned_users_df, remaining_users_df)
    """
    np.random.seed(42)

    assigned_users_list = []
    if extra_users_country is None:
        extra_users_country = []

    # Global randomized pool to break ties, then sorted by priority.
    # We will always pick first match to preserve priority inside each country filter.
    available_pool = available_users.sample(frac=1, random_state=42).reset_index(drop=True)
    available_pool = sort_by_priority(available_pool)

    for country, operators_info in assignment_dict.items():
        if not operators_info:
            continue

        operator_assignments = {op_info['operator']: 0 for op_info in operators_info}
        operator_index = 0

        while True:
            current_op_info = operators_info[operator_index]
            operator = current_op_info['operator']
            users_to_assign = current_op_info['users_to_assign']

            remaining_limit = users_to_assign - operator_assignments[operator]
            if remaining_limit <= 0:
                operator_index = (operator_index + 1) % len(operators_info)
                if operator_index == 0 and all(
                    operator_assignments[op_info['operator']] >= op_info['users_to_assign']
                    for op_info in operators_info
                ):
                    break
                continue

            # 1) Try target country first
            target_mask = available_pool['register_currency'] == country
            if target_mask.any():
                candidate_idx = available_pool[target_mask].index[0]
            else:
                # 2) Fallback countries in configured priority order
                candidate_idx = None
                for fallback_country in extra_users_country:
                    fallback_mask = available_pool['register_currency'] == fallback_country
                    if fallback_mask.any():
                        candidate_idx = available_pool[fallback_mask].index[0]
                        break

                if candidate_idx is None:
                    # No candidates left for this target-country quota
                    break

            assigned = available_pool.loc[[candidate_idx]].copy()
            assigned['campaign'] = assigned['campaign_name']
            assigned['operator'] = operator
            assigned_users_list.append(assigned)

            operator_assignments[operator] += 1
            available_pool = available_pool.drop(index=candidate_idx)
            operator_index = (operator_index + 1) % len(operators_info)

    if assigned_users_list:
        assigned_users_df = pd.concat(assigned_users_list, ignore_index=True)
    else:
        assigned_users_df = available_users.head(0).copy()
        assigned_users_df['campaign'] = pd.Series(dtype='object')
        assigned_users_df['operator'] = pd.Series(dtype='object')

    remaining_users_df = available_pool.copy()
    if not remaining_users_df.empty:
        remaining_users_df['campaign'] = remaining_users_df['campaign_name']

    return assigned_users_df, remaining_users_df


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
        # Note: campaign_df is already sorted by priority from create_campaign_dataframes()
        # so we just filter without re-sorting to maintain global priority order
        currency_users = campaign_df[campaign_df['register_currency'].isin(currency_list)].copy()

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
    
    # Sort by campaign priority without re-randomizing
    # (users were already randomized when campaign_dfs were created)
    # This ensures ULTRA-1 users are assigned before ULTRA-2, etc.
    available_users = sort_by_priority(available_users)

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


def create_assignment_metrics(available_users, assigned_users, assignment_date):
    """
    Creates a DataFrame with daily metrics showing available and assigned users per
    priority, country, and campaign.
    
    Args:
        available_users (pd.DataFrame): DataFrame with all available users before assignment.
            Must contain 'priority', 'register_currency', and 'campaign_name' columns.
        assigned_users (pd.DataFrame): DataFrame with all assigned users.
            Must contain 'priority', 'register_currency', and 'campaign_name' columns.
        assignment_date (str): Date of the assignment in 'YYYYMMDD' format
    
    Returns:
        pd.DataFrame: DataFrame with columns:
            - assignment_date: Date of the assignment
            - priority: Priority bucket
            - country: Country code (register_currency)
            - campaign: Campaign name
            - available_users: Number of users available for assignment
            - assigned_users: Number of users actually assigned
            - unassigned_users: Number of users that were available but not assigned
    """
    # Available users by country and campaign
    available_metrics = (
        available_users
        .groupby(['priority', 'register_currency', 'campaign_name'], dropna=False)
        .size()
        .reset_index(name='available_users')
    )

    # Assigned users by country and campaign
    assigned_metrics = (
        assigned_users
        .groupby(['priority', 'register_currency', 'campaign_name'], dropna=False)
        .size()
        .reset_index(name='assigned_users')
    )

    # Merge both metrics and fill missing values
    metrics_df = available_metrics.merge(
        assigned_metrics,
        on=['priority', 'register_currency', 'campaign_name'],
        how='outer'
    )
    metrics_df['available_users'] = metrics_df['available_users'].fillna(0).astype(int)
    metrics_df['assigned_users'] = metrics_df['assigned_users'].fillna(0).astype(int)
    metrics_df['unassigned_users'] = metrics_df['available_users'] - metrics_df['assigned_users']

    # Rename and order columns as requested (country after assignment_date)
    metrics_df = metrics_df.rename(columns={
        'register_currency': 'country',
        'campaign_name': 'campaign'
    })

    metrics_df.insert(0, 'assignment_date', assignment_date)
    metrics_df = metrics_df[['assignment_date', 'country', 'priority', 'campaign', 'available_users', 'assigned_users', 'unassigned_users']]
    # Sort by country, then real priority order (ULTRA-1, ULTRA-2, ALTA-1, ...), then campaign.
    metrics_df = metrics_df.assign(_priority_key=metrics_df['priority'].apply(create_priority_sort_key))
    metrics_df['_priority_key'] = metrics_df['_priority_key'].apply(
        lambda key: key if key is not None else (99, 99)
    )
    metrics_df = metrics_df.sort_values(['country', '_priority_key', 'campaign']).drop(columns=['_priority_key']).reset_index(drop=True)

    return metrics_df


