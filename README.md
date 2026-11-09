# Daily Assignment - Telemarketing

**English** | [Español](README.es.md)

## Description
Automated daily user assignment system for telemarketing operators. The current process distributes users equitably by **country** and prioritizes the highest-priority users first within each country.

The system now considers:

- **Country-based distribution**: Operators are assigned one or more countries from `lp_tlmkt_current`
- **Priority ordering**: Users are assigned from highest to lowest priority (`ULTRA-1`, `ULTRA-2`, `ALTA-1`, ...)
- **Fallback completion**: Optional fallback countries can fill incomplete quotas when a country runs out of users
- **Base data-quality filtering**: Excludes users without `phone` or without a usable `firstLast_name` before assignment and control group reservation
- **Contacted user exclusion**: Excludes users contacted within configurable discard windows — global or per campaign — from `days_ago_to_discard` back to yesterday
- **Control group reservation**: Optional per-campaign control groups are reserved before treatment assignment using `control_group_percent`
- **Currency and campaign filtering**: Allows disabling a campaign for a specific country via `campaigns_to_filter_by_currency`
- **Config-driven behavior**: Assignment and filtering are controlled by BigQuery tables

## Operational References

- **Configuration dataset**: `mi-casino.dm_telemarketing`
   - `conf_parameters_v2`: assignment parameters
   - `conf_segments_to_consult`: segments to consult
   - `lp_tlmkt_current`: operator roster
   - `conf_priority_by_campaign`: not read by the current country-based algorithm; priorities come from segment data
- **Looker Studio report**: [Telemarketing assignment dashboard](https://datastudio.google.com/reporting/08c9ba40-b514-4715-98d1-d8b22a7587a0/page/p_q0dsmy25zd/edit)
   - Page 1: daily assignment by operator, ordered by operator and priority so the highest priority users appear first when downloaded
   - Page 2: daily available data by country and priority

## Current Behavior Snapshot

- User extraction reads all configured segment tables and skips missing or empty tables without stopping the full run.
- Base filters remove blocked currencies (`CAD`, `ARS`, `BRL`), keep levels 1-3, and remove rows with missing `phone`, null `firstLast_name`, or blank `firstLast_name`.
- The cleaned `available_users` DataFrame is used for both control users and treatment assignment, so users filtered out at extraction cannot enter either group.
- Per-campaign discard windows are built from `conf_segments_to_consult`; the largest window determines the BigQuery history fetch range.
- Control users are sampled before operator assignment, marked as `CONTROL`, and treatment users are marked as `TREATMENT`.
- The current `main.py` call uses `load_data=False`, so the run creates/validates tables and writes local Excel outputs but does not append rows to BigQuery unless this flag is changed.

## Project Structure

```
daily_assignment_tlmkt/
├── data/                          # Assignment output files (Excel)
├── src/
│   ├── config.py                  # Dynamic configuration class
│   ├── extract.py                 # BigQuery extraction
│   ├── transform.py               # Assignment algorithms and normalization
│   ├── load.py                    # BigQuery data loading
│   └── __pycache__/              # Python compiled files
├── .gitignore                    # Version control exclusions
├── main.py                       # Main executable (Cloud Run compatible)
├── test_main.py                  # Local testing script
├── test_main.example.py          # Local runner template
├── local_credentials_config.example.json # Local configuration template
├── requirements.txt              # Project dependencies
├── iam-account-xxxx-xxxx.json    # Service credentials for BigQuery (not in repo)
└── README.md                     # Project documentation
```

## System Configuration

### Config Class
The `Config` class dynamically manages all system parameters from `mi-casino.dm_telemarketing.conf_parameters_v2`:

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
      - dict(str,list(str)): Dictionary of currency → list of campaigns (format: `PEN:camp1,camp2|BOB:camp1`)
      """
```

**Main parameters:**
- `days_ago_to_discard`: Global discard window in days. Acts as fallback for campaigns without a specific value in `conf_segments_to_consult`. Also determines how far back history is fetched from BigQuery if no campaign has a larger window.
- `exclude_email_mkt_users`: Whether to include email marketing history in the discard window (`TRUE` / `FALSE`)
- `users_to_assign_per_operator`: Base number of users per operator (e.g., 100)
- `currencies_to_filter`: List of currencies to exclude in extraction
- `campaigns_to_filter`: List of campaigns to exclude globally (all countries)
- `campaigns_to_filter_by_currency`: Campaigns to exclude per specific currency. Allows turning off a campaign in one country without affecting others. Format: `PEN:sport_events,reactivation|BOB:sport_events`. Empty cell = no filter applied.
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
If an operator should receive 100 users and manages 3 countries:
- Country 1: 50 users (50%)
- Country 2: 30 users (30%)  
- Country 3: 20 users (20%)

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

### Credentials for Cloud Run
Use the Cloud Run service account through Application Default Credentials. It needs permission to query BigQuery and read the configuration, operator, segment, and history tables.

Some configuration tables are **BigQuery external tables backed by Google Sheets** (for example, `conf_parameters_v2`). BigQuery reads the Sheet using the credentials of the identity running the query, so the Cloud Run service account must have at least Viewer access to each associated Google Sheet (share the sheet with the service account email). Without that access, queries against those tables fail even if the account has dataset permissions. The code requests the `drive.readonly` scope for this purpose.

## Usage

### Local Testing (test_main.py)
Follow these steps after cloning the repository to run the full workflow on your machine:

1. Create and activate a virtual environment:

   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   ```

   On macOS or Linux, activate it with `source .venv/bin/activate`.

2. Install dependencies:

   ```shell
   python -m pip install -r requirements.txt
   ```

3. Ask the project administrator for a service account JSON key authorized to access the project. The account must be able to run queries and read the required BigQuery tables, and have Viewer access to the Google Sheets backing any external tables. Do not download or share credentials from the repository.

4. Copy the local configuration template:

   ```powershell
   Copy-Item local_credentials_config.example.json local_credentials_config.json
   ```

   On macOS or Linux, use `cp local_credentials_config.example.json local_credentials_config.json`.

5. Edit `local_credentials_config.json` and set `bigquery_credentials_file` to the path of the service account JSON key. On Windows, `/` can be used in the path, for example `C:/Users/your_user/credentials/bigquery.json`.

   ```json
   {
     "bigquery_credentials_file": "path/to/service-account-key.json"
   }
   ```

6. Run the local runner:

   ```shell
   python test_main.py
   ```

`test_main.py` uses this configuration to set `GOOGLE_APPLICATION_CREDENTIALS` and run `main.py`. **This is not an isolated test and requires Google Cloud access**: it queries BigQuery and may create the workflow's output files. No `.env` file is needed; the runner does not load one. `local_credentials_config.json` and credential files are excluded from Git.

The runner lets you exercise the local workflow before deployment, including data extraction and transformation.

### Cloud Run Deployment
```bash
# Deploy to Cloud Run
gcloud run deploy daily-assignment-tlmkt \
  --source . \
  --region southamerica-west1 \
  --allow-unauthenticated

# Grant the Cloud Run service account access to the BigQuery tables.
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
- **`group_flag` column**: Identifies `CONTROL` and `TREATMENT` rows in the daily assignment output
- **Experiment metadata**: Adds `experiment_id`, `channel`, and deterministic `user_campaign_id`
- **Table download tracking**: Shows which tables are being downloaded (* table_name)
- **Empty table detection**: Warns when tables exist but contain no data (⚠️)
- **Name validation**: Rows without `firstLast_name` are removed before any assignment logic runs

## Process Flow

### 1. **Configuration and Credentials**
- Load configuration from BigQuery using `Config` class
- Establish Application Default Credentials for BigQuery
- Define dates and filtering parameters

### 2. **Data Extraction (Extract)**
- **Active operators**: List from BigQuery table `lp_tlmkt_current`
- **Available users**: BigQuery segments according to configuration
- **Base filters**: Remove blocked currencies, unsupported levels, missing phones, and null or blank `firstLast_name`
- **Assignment history**: Recently contacted users via telemarketing (`tlmkt_DailyAssignment`) and email marketing (`email_mkt_DailyAssignment`)
- **Campaign configuration**: Dynamic system parameters

### 3. **Transformation and Assignment (Transform)**
- **User filtering**: Exclusion of users recently contacted by telemarketing or email marketing, with configurable per-campaign discard windows
- **Currency and campaign filtering**: Exclusion of specific campaigns for specific currencies/countries (`campaigns_to_filter_by_currency`)
- **Campaign normalization**: Conversion between internal codes and Spanish names
- **DataFrame creation per campaign**: Organization of available users for reporting and metrics
- **Control group reservation**: Per-campaign control users are sampled from the cleaned pool before treatment assignment
- **Country-based quota assignment**: Operators receive quotas by country from `lp_tlmkt_current`
- **1-by-1 equitable assignment**: Users are assigned one at a time per operator to keep distribution balanced
- **Priority-first selection**: Highest-priority users are assigned first within each country
- **Optional fallback countries**: `extra_users_country` can fill incomplete quotas if configured

### 4. **Data Loading (Load)**
- **Local file**: Excel with daily assignments
- **BigQuery**: Historical assignment table
- **Group labeling**: Daily output includes `CONTROL` and `TREATMENT` via `group_flag`
- **Load flag**: BigQuery append is controlled by `load_data`; current code passes `load_data=False`
- **Final normalization**: Conversion of codes to Spanish names

### BigQuery Tables

#### `tlmkt_DailyAssignment`

Current schema used by the loader:

| field | type |
|-------|------|
| assignment_date | DATETIME |
| operator | STRING |
| campaign_name | STRING |
| user_id | INTEGER |
| username | STRING |
| firstLast_name | STRING |
| phone | STRING |
| level | INTEGER |
| register_currency | STRING |
| last_activity | DATETIME |
| campaign_details | STRING |
| priority | STRING |
| op_email | STRING |
| group_flag | STRING |
| experiment_id | STRING |
| channel | STRING |
| user_campaign_id | STRING |

#### `tlmkt_AssignmentMetrics`

Current schema used by the loader:

| field | type |
|-------|------|
| assignment_date | DATETIME |
| campaign | STRING |
| available_users | INTEGER |
| assigned_users | INTEGER |
| unassigned_users | INTEGER |
| country | STRING |
| priority | STRING |

## Assignment Algorithms

### Country-Based Assignment

1. **Operator quota split by country**
   - Each operator receives quotas for the countries listed in `lp_tlmkt_current`
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
History fetch window: 21 days back (2026-06-19)
Per-campaign discard windows: {'sport_events': 7, 'non_depositors': 21, 'reactivation': 14}
Filtering campaigns by currency:
  - PEN: ['sport_events']
  [PEN] Removed 342 users from campaigns ['sport_events']
Users after currency-campaign filter: 15219
Discarding previously contacted users (per-campaign windows, up to 2026-07-09)
Available users for assignment: 15561
```

## Configuration Files

### Required BigQuery Sources:
All tables are in `mi-casino.dm_telemarketing`:
1. **conf_parameters_v2**: System parameters (`variable`, `value`, `type`)
2. **conf_segments_to_consult**: Segment tables and per-campaign controls
3. **lp_tlmkt_current**: Operator roster with English field names

`conf_priority_by_campaign` is not consumed by the current algorithm. Assignment uses the `priority` values supplied by segment tables.

Configuration tables (such as `conf_parameters_v2`) may be external tables backed by Google Sheets. To change their values, edit the source Sheet (check the "External data configuration" section of each table in BigQuery for its URI).

### Configuration structure:
| variable | value | type |
|----------|-------|------|
| days_ago_to_discard | 7 | int |
| exclude_email_mkt_users | FALSE | bool |
| users_to_assign_per_operator | 100 | int |
| currencies_to_filter | BOB | list(str) |
| campaigns_to_filter | reactivation | list(str) |
| campaigns_to_filter_by_currency | PEN:sport_events\|BOB:sport_events | dict(str,list(str)) |
| extra_users_country | VES | list(str) |

### Segments to consult structure:
| table_name | control_group_percent | description | campaign_label | days_ago_to_discard |
|---|---|---|---|---|
| tlmkt_Sport_Events | 0 | Eventos Deportivos | sport_events | 7 |
| tlmkt_Non_Depositors | 0 | No Depositantes | non_depositors | 21 |

`days_ago_to_discard` in `conf_segments_to_consult` defines the per-campaign discard window. If left null or empty, the global value from `conf_parameters_v2` is used as fallback.

`control_group_percent` defines the percentage of each campaign pool reserved as control before treatment assignment. Use `0` or leave it empty when no control group is needed.

## Maintenance and Administration

### Update Operators
Edit BigQuery table `mi-casino.dm_telemarketing.lp_tlmkt_current`:
- **full_name**: Full name, mapped internally to `operator`
- **dotpanel_username**: System username, mapped to `user_dotpanel`
- **corporate_email**: Email, mapped to `op_email`; null or blank means no email
- **country**: Comma-separated countries, preserving their quota order
- **job_title**: "Ejecutivo de Televentas"
- **status**: "Activo" to include in assignments
- **shift**, **campaign**, **notion_permission_type**: Present in the roster but not used by current assignment logic

### Modify System Parameters
Edit BigQuery table `mi-casino.dm_telemarketing.conf_parameters_v2`:
- Change excluded currencies and campaigns
- Adjust exclusion days
- Modify number of users per operator
- Toggle email marketing exclusion
- Define fallback countries to fill incomplete quotas

### Add New Campaigns
1. Include table in `conf_segments_to_consult`
2. Update normalization in `transform.py`:
   ```python
   pattern_map = {
       r'New Campaign': 'new_campaign',
       # ... existing patterns
   }
   ```

### Common Troubleshooting
- **Credentials error**: Verify `gcloud auth list`
- **Missing data**: Review BigQuery configuration tables and access permissions
- **Users without names**: Confirm source segment tables populate `firstLast_name`; extraction removes null and blank names before assignment
- **Unbalanced assignments**: Review LP country mapping, fallback countries, and priority distribution
- **Schema mismatch**: Ensure the BQ tables include `country` and `priority` in `tlmkt_AssignmentMetrics`
- **Assignment not loading**: Confirm `load_data=True` in `main.py`
