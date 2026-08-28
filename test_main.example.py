import json
import os

LOCAL_CREDENTIALS_CONFIG = 'local_credentials_config.json'

with open(LOCAL_CREDENTIALS_CONFIG, 'r') as f:
    local_config = json.load(f)

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = local_config['bigquery_credentials_file']

with open(local_config['sheets_credentials_file'], 'r') as f:
    sheet_creds = json.load(f)
os.environ['SHEET_CREDENTIALS'] = json.dumps(sheet_creds)

from main import run_daily_assignment


class MockRequest:
    pass


result = run_daily_assignment(MockRequest())
print(f"\n{'=' * 60}")
print(f"FINAL RESULT: {result}")
print(f"{'=' * 60}")