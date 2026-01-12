import os
from google.oauth2 import service_account
import json

# Configurar credenciales locales para testing
# iam-acount-bi-functions.json para BigQuery
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'iam-acount-bi-functions.json'

# parabolic-water para Google Sheets
with open('parabolic-water-352818-e036b2475893.json', 'r') as f:
    sheet_creds = json.load(f)
os.environ['SHEET_CREDENTIALS'] = json.dumps(sheet_creds)

# Importar y ejecutar main
from main import run_daily_assignment

# Simular request (Cloud Run pasa un objeto request, pero no lo usamos en el código)
class MockRequest:
    pass

result = run_daily_assignment(MockRequest())
print(f"\n{'='*60}")
print(f"FINAL RESULT: {result}")
print(f"{'='*60}")
