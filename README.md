# Daily Assignment - Telemarketing

**English** | [Español](README.es.md)

## Description
Automated daily user assignment system for telemarketing operators. The system distributes users equitably among operators considering:

- **Campaign distribution**: Each operator can handle 1-3 specific campaigns
- **Currency balancing**: Intelligent distribution by currency types (priority, small, large, relevant)
- **Contacted user exclusion**: Avoids contacting users recently contacted via telemarketing or email marketing based on configuration
- **Proportional assignment algorithm**: Percentage distribution based on number of assigned campaigns

## Project Structure

```
daily_assignment_tlmkt/
├── data/                          # Assignment output files (Excel)
├── src/
│   ├── config.py                  # Dynamic configuration class
│   ├── extract.py                 # BigQuery and Google Sheets extraction
│   ├── transform.py               # Assignment algorithms and normalization
│   ├── load.py                    # BigQuery data loading
│   └── __pycache__/              # Python compiled files
├── .env                          # Environment variables (not in repo)
├── .gitignore                    # Version control exclusions
├── main.py                       # Main executable (Cloud Run compatible)
├── test_main.py                  # Local testing script
├── requirements.txt              # Project dependencies
├── iam-account-xxxx-xxxx.json    # Service credentials for BigQuery (not in repo)
├── xxxxxx-xxxxxxxxxxxx.json  # Service credentials for Sheets (not in repo)
└── README.md                     # Project documentation
```

## System Configuration

### Config Class
The `Config` class dynamically manages all system parameters from Google Sheets:

```python
class Config:
    """
    Manages configuration parameters from DataFrame.
    
    Automatically converts data types:
    - int: Integer numbers (days, quantities)
    - float: Decimal numbers (percentages)  
    - str: Text strings
    - list(str): Lists of strings separated by commas
    """
```

**Main parameters:**
- `days_ago_to_discard`: Days back to exclude users contacted by telemarketing or email marketing (e.g., 7)
- `users_to_assign_per_operator`: Base number of users per operator (e.g., 100)
- `currencies_to_filter`: List of currencies to exclude in assignment (e.g., ['USD', 'EUR', 'BRL'])
- `priority_currencies`: High priority currencies for early assignment (e.g., ['USD', 'EUR'])
- `max_priority_currencies_percent`: Maximum assignment percentage for priority currencies (e.g., 0.4 = 40%)
- `small_currencies_to_limit`: Small currencies with joint assignment percentage limit (e.g., ['JPY', 'CAD'])
- `max_small_currencies_percent`: Maximum total percentage for small currencies (e.g., 0.1 = 10%)
- `big_currencies_to_limit`: Large currencies to assign with divided percentage limit (e.g., ['BRL', 'CLP'])
- `max_big_currencies_percent`: Maximum assignment percentage for large currencies (e.g., 0.3 = 30%)
- `relevant_currencies`: Relevant currencies without specific limit (e.g., ['USD', 'EUR', 'BRL'])
- `extra_users_campaign`: Additional campaigns to complete assignments (e.g., ['non_depositors'])

### Campaign Percentage System

The system uses a proportional distribution algorithm based on the number of campaigns assigned to each operator:

```python
percentages = {
    1: [1.0],           # 100% for operators with 1 campaign
    2: [0.7, 0.3],      # 70% and 30% for operators with 2 campaigns  
    3: [0.5, 0.3, 0.2]  # 50%, 30%, and 20% for operators with 3 campaigns
}
```

**Assignment logic:**
- **1 campaign**: The operator receives 100% of their assigned users in that campaign
- **2 campaigns**: The main campaign receives 70%, the secondary 30%
- **3 campaigns**: Distribution 50%-30%-20% in priority order

**Practical example:**
If an operator should receive 100 users and manages 3 campaigns:
- Campaign 1: 50 users (50%)
- Campaign 2: 30 users (30%)  
- Campaign 3: 20 users (20%)

## Installation and Configuration

### Prerequisites
```bash
# Python 3.8+
# Google Cloud CLI configured
# BigQuery credentials
```

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Authentication
```bash
# Using CLI credentials (recommended for development)
gcloud auth application-default login

# Or configure environment variable for production
export GOOGLE_APPLICATION_CREDENTIALS="path/to/service-account.json"
```

### Environment Variables for Cloud Run
Configure these variables in Cloud Run console:
- `SHEET_CREDENTIALS`: JSON string of Google Sheets service account credentials

## Usage

### Local Testing (test_main.py)
The `test_main.py` script allows you to test the Cloud Run-ready `main.py` locally by simulating the Cloud Run environment:

```python
# test_main.py configures:
# 1. BigQuery credentials: service-account.json
# 2. Sheets credentials: SHEET_CREDENTIALS environment variable
# 3. Mock request object for Cloud Run compatibility

python test_main.py
```

**Purpose**: Test the exact code that will run in Cloud Run without deploying. Useful for:
- Validating assignment logic before deployment
- Debugging credential issues locally
- Testing configuration changes
- Verifying data extraction and transformation

**Output**: Complete execution log with final result status.

### Cloud Run Deployment
```bash
# Deploy to Cloud Run
gcloud run deploy daily-assignment-tlmkt \
  --source . \
  --region southamerica-west1 \
  --allow-unauthenticated

# Set environment variable in Cloud Run console:
# SHEET_CREDENTIALS = {JSON content from Google Sheets service account}
```

**Entry point**: `run_daily_assignment(request)` function in main.py

## Recent Improvements (2026)

### Enhanced Error Handling
- **Individual table error handling**: If a BigQuery table doesn't exist or is empty, the process continues with remaining tables
- **Comprehensive try-except blocks**: All critical operations wrapped with error handling
- **Informative error messages**: Clear logging for debugging in Cloud Run

### Data Loading Safety
- **`delete_today` parameter**: Control whether to replace or prevent duplicate data
  - `True`: Replaces today's data if it exists
  - `False`: Prevents loading if data already exists for today (recommended for production)
- **Smart deletion verification**: Checks if data exists before attempting deletion
- **Clear status messages**: Logs show exactly what happened with the data

### Additional Features
- **`campaign_details` column**: Support for external campaign metadata
- **Table download tracking**: Shows which tables are being downloaded (* table_name)
- **Empty table detection**: Warns when tables exist but contain no data (⚠️)

## Process Flow

### 1. **Configuration and Credentials**
- Load configuration from Google Sheets using `Config` class
- Establish CLI credentials for BigQuery and Google Sheets
- Define dates and filtering parameters

### 2. **Data Extraction (Extract)**
- **Active operators**: List from Google Sheet 'LP_TLMKT'
- **Available users**: BigQuery segments according to configuration
- **Assignment history**: Recently contacted users via telemarketing (`tlmkt_DailyAssignment`) and email marketing (`email_mkt_DailyAssignment`)
- **Campaign configuration**: Dynamic system parameters

### 3. **Transformation and Assignment (Transform)**
- **User filtering**: Exclusion of users recently contacted by telemarketing or email marketing
- **Campaign normalization**: Conversion between internal codes and Spanish names
- **DataFrame creation per campaign**: Organization of available users
- **4-phase assignment algorithm**:
  1. **Priority currencies** (with divided percentage limit)
  2. **Small currencies** (with total percentage limit)
  3. **Big currencies** (with divided percentage limit)
  4. **Relevant currencies** (no limit, complete to target)
- **Assignment completion**: Use of extra users from other campaigns

### 4. **Data Loading (Load)**
- **Local file**: Excel with daily assignments
- **BigQuery**: Historical assignment table
- **Final normalization**: Conversion of codes to Spanish names

## Assignment Algorithms

### Distribution by Currency Type

1. **Priority Currencies** (`max_priority_currencies_percent`, `split_percentage=True`)
   - Percentage limit divided among currencies in the list
   - Circular equitable assignment among operators

2. **Small Currencies** (`max_small_currencies_percent`, `split_percentage=False`)  
   - Total percentage limit for all small currencies combined
   - Proportional distribution without division by currency

3. **Big Currencies** (`max_big_currencies_percent`, `split_percentage=True`)
   - Similar to priority currencies, limit divided among currencies
   - Balanced assignment per operator

4. **Relevant Currencies** (no limit)
   - Assignment to complete operator quotas
   - No percentage restrictions

### Completion Algorithm
- **Priority 1**: Users from the same campaign with priority currencies
- **Priority 2**: Users from the same campaign with relevant currencies  
- **Priority 3**: Any user from the same campaign
- **Priority 4**: Users from extra campaigns with priority currencies
- **Priority 5**: Users from extra campaigns with relevant currencies
- **Priority 6**: Any user from extra campaigns

## Output Examples

### Distribution by Currency
| Currency | Users | Percentage |
|----------|-------|------------|
| USD      | 8,532 | 54.6%      |
| EUR      | 3,247 | 20.8%      |
| BRL      | 2,186 | 14.0%      |
| CLP      | 1,693 | 10.8%      |

### Assignment by Operator
| Operator | Campaign | Currency | Assigned Users |
|----------|----------|----------|----------------|
| Ana García | Non Depositors | USD | 42 |
| Luis Pérez | Reactivation | EUR | 38 |
| María López | Second Deposit | BRL | 35 |

### Final Assignment Summary
```
Operator          Assigned Users
Ana García                    95
Luis Pérez                    98  
María López                   97
Carlos Ruiz                  102
Total assigned users:        392
```

## Monitoring and Logs

The system generates detailed real-time logs:

```
Extracting data to assign...
Data extracted successfully
Discarding users contacted since 2025-08-20
Available users for assignment: 15658

Creating assignment dictionary...
Assignment Dictionary created successfully.

Assigning Priority Currencies...
Assigning Small Currencies...
Assigning Big Currencies...  
Updating Assignment Dictionary...
Assigning Relevant Currencies...
Completing Assignment with Additional Users...
Assignment completed.

Saving assignment to local file...
Assignment saved to local file.
Loading data to BigQuery...
Data loaded to BigQuery successfully.
```

## Configuration Files

### Required Google Sheets:
1. **Daily_Assignment_Configuration** (Sheet 0): System parameters
2. **Daily_Assignment_Configuration** (Sheet 1): Segment tables
3. **LP_TLMKT**: Active operators list

### Configuration structure:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| users_to_assign_per_operator | 100 | int |
| priority_currencies | USD,EUR | list(str) |
| max_priority_currencies_percent | 0.4 | float |

## Maintenance and Administration

### Update Operators
Edit Google Sheet 'LP_TLMKT':
- **Name and Surname**: Full name of operator
- **DotPanel User**: System username  
- **Campaign**: Comma-separated list of campaigns
- **Position**: "Sales Executive"
- **Status**: "Active" to include in assignments

### Modify System Parameters
Edit Google Sheet 'Daily_Assignment_Configuration':
- Change currency percentages
- Adjust exclusion days
- Modify number of users per operator
- Add new currencies to lists

### Add New Campaigns
1. Include table in segment configuration sheet
2. Update normalization in `transform.py`:
   ```python
   pattern_map = {
       r'New Campaign': 'new_campaign',
       # ... existing patterns
   }
   ```

### Common Troubleshooting
- **Credentials error**: Verify `gcloud auth list`
- **Missing data**: Review Google Sheets configuration
- **Unbalanced assignments**: Adjust percentages in configuration

