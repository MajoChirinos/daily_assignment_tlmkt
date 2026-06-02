# Daily Assignment - Telemarketing

**English** | [Español](README.es.md)

## Description
Automated daily user assignment system for telemarketing operators. The current process distributes users equitably by **country** and prioritizes the highest-priority users first within each country.

The system now considers:

- **Country-based distribution**: Operators are assigned one or more countries from `LP_TLMKT`
- **Priority ordering**: Users are assigned from highest to lowest priority (`ULTRA-1`, `ULTRA-2`, `ALTA-1`, ...)
- **Fallback completion**: Optional fallback countries can fill incomplete quotas when a country runs out of users
- **Contacted user exclusion**: Excludes users contacted from `days_ago_to_discard` back to yesterday based on configuration
- **Config-driven behavior**: Assignment and filtering are controlled by Google Sheets

## Operational References

- **Assignment configuration spreadsheet**: [Daily_Assignment_Configuration](https://docs.google.com/spreadsheets/d/1h7FemF3zjIMCjTwo4DPKrNa-5eE-seV5nAW6tNrdPhU/edit?usp=sharing)
   - Sheet 0 (parameters): assignment by campaign configuration
   - Sheet 1 (segments_to_consult): segments to consult
   - Sheet 2 (parameters_v2): assignment by country configuration
- **Looker Studio report**: [Telemarketing assignment dashboard](https://datastudio.google.com/reporting/08c9ba40-b514-4715-98d1-d8b22a7587a0/page/p_q0dsmy25zd/edit)
   - Page 1: daily assignment by operator, ordered by operator and priority so the highest priority users appear first when downloaded
   - Page 2: daily available data by country and priority

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
   - bool: Boolean values (`TRUE` / `FALSE`, `1` / `0`, `yes` / `no`)
    """
```

**Main parameters:**
- `days_ago_to_discard`: Days back to exclude users contacted by telemarketing or email marketing. The discard window runs from this date up to yesterday.
- `exclude_email_mkt_users`: Whether to include email marketing history in the discard window (`TRUE` / `FALSE`)
- `users_to_assign_per_operator`: Base number of users per operator (e.g., 100)
- `currencies_to_filter`: List of currencies to exclude in extraction
- `campaigns_to_filter`: List of campaigns to exclude in extraction
- `extra_users_country`: Optional fallback countries used to complete incomplete operator quotas

### Operator Country Split

The system uses a proportional split based on the number of countries assigned to each operator:

```python
percentages = {
   1: [1.0],           # 100% for operators with 1 country
   2: [0.7, 0.3],      # 70% and 30% for operators with 2 countries  
   3: [0.5, 0.3, 0.2]  # 50%, 30%, and 20% for operators with 3 countries
}
```

**Assignment logic:**
- **1 country**: The operator receives 100% of their quota in that country
- **2 countries**: The first country receives 70%, the second 30%
- **3 countries**: Distribution 50%-30%-20% in the configured order

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
- **DataFrame creation per campaign**: Organization of available users for reporting and metrics
- **Country-based quota assignment**: Operators receive quotas by country from `LP_TLMKT`
- **1-by-1 equitable assignment**: Users are assigned one at a time per operator to keep distribution balanced
- **Priority-first selection**: Highest-priority users are assigned first within each country
- **Optional fallback countries**: `extra_users_country` can fill incomplete quotas if configured

### 4. **Data Loading (Load)**
- **Local file**: Excel with daily assignments
- **BigQuery**: Historical assignment table
- **Final normalization**: Conversion of codes to Spanish names

## Assignment Algorithms

### Country-Based Assignment

1. **Operator quota split by country**
   - Each operator receives quotas for the countries listed in `LP_TLMKT`
   - The split uses the configured percentages for 1, 2, or 3 countries

2. **Priority-first consumption**
   - Users are assigned one by one
   - Inside each country, users are consumed from highest to lowest priority
   - The ordering is based on the `priority` field (`ULTRA-1`, `ULTRA-2`, `ALTA-1`, ...)

3. **Fallback completion**
   - If a country runs out of users, the system can optionally fill the remaining quota using `extra_users_country`
   - If `extra_users_country` is empty, the quota remains incomplete

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
Discarding users contacted from 2025-08-20 to 2025-08-31
Available users for assignment: 15658

Available users by currency (after discarding contacted users):
   • VES: 9440 users
   • CLP: 1877 users
   • USD: 1699 users

Creating assignment dictionary...
Assignment Dictionary created successfully.

Assigning users by country with global priority order...
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
3. **Daily_Assignment_Configuration** (Sheet 2): Country-based assignment config
4. **LP_TLMKT**: Active operators list

### Configuration structure:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| exclude_email_mkt_users | FALSE | bool |
| users_to_assign_per_operator | 100 | int |
| currencies_to_filter | BOB | list(str) |
| campaigns_to_filter | reactivation | list(str) |
| extra_users_country | VES | list(str) |

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
- Change excluded currencies and campaigns
- Adjust exclusion days
- Modify number of users per operator
- Toggle email marketing exclusion
- Define fallback countries to fill incomplete quotas

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
- **Unbalanced assignments**: Review LP country mapping, fallback countries, and priority distribution
- **Schema mismatch**: Ensure the BQ tables include `country` and `priority` in `tlmkt_AssignmentMetrics`
- **Assignment not loading**: Confirm `load_data=True` in `main.py`

