#!/usr/bin/env python3
"""
Quick diagnostic — tests the Airtable connection and pushes one sample record.
Run this to confirm Airtable integration is working before the full pipeline.

Usage:
    python3 test_airtable_push.py
"""
import os, json, time, requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

api_token  = os.environ.get('AIRTABLE_API_TOKEN', '').strip()
base_id    = os.environ.get('AIRTABLE_BASE_ID', '').strip()
table_name = os.environ.get('AIRTABLE_TABLE_NAME', 'Marketing Partnership Signals').strip()

print("🔑 Credentials check")
print(f"   Token : {'✅ set' if api_token else '❌ MISSING'} ({api_token[:12]}...)")
print(f"   Base  : {base_id or '❌ MISSING'}")
print(f"   Table : {table_name}")

if not api_token or not base_id:
    print("\n❌ Missing credentials — check your .env file")
    raise SystemExit(1)

headers = {
    'Authorization': f'Bearer {api_token}',
    'Content-Type': 'application/json',
}
encoded    = requests.utils.quote(table_name, safe='')
base_url   = f'https://api.airtable.com/v0/{base_id}/{encoded}'

# ── 1. Test GET (list records) ────────────────────────────────────────────────
print("\n📡 Testing GET /records ...")
try:
    resp = requests.get(base_url, headers=headers, params={'pageSize': 1}, timeout=15)
    print(f"   Status : {resp.status_code}")
    if resp.status_code == 200:
        rec_count = len(resp.json().get('records', []))
        print(f"   ✅ Connection OK — {rec_count} record(s) found")
    else:
        print(f"   ❌ Error: {resp.text}")
        raise SystemExit(1)
except requests.RequestException as e:
    print(f"   ❌ Request failed: {e}")
    raise SystemExit(1)

# ── 2. POST one test record ───────────────────────────────────────────────────
print("\n📤 Pushing 1 test record ...")
test_record = {
    "Signal ID":         "sig_test_diagnostic_001",
    "Run ID":            "run_diagnostic",
    "Company Name":      "TEST — DELETE ME",
    "Signal Type":       "funding_announcement",
    "ICP Vertical":      "ai_ml",
    "Tier 1 Confidence": 0.99,
    "Tier 2 Confidence": 0.99,
    "Context Validated": True,
    "Review Status":     "Pending",
    "Send to Attio":     False,
    "Attio Sync Status": "Not Synced",
}

payload = {'records': [{'fields': test_record}]}

try:
    resp = requests.post(base_url, headers=headers, json=payload, timeout=15)
    print(f"   Status : {resp.status_code}")
    if resp.status_code in (200, 201):
        rec_id = resp.json()['records'][0]['id']
        print(f"   ✅ Record created! ID: {rec_id}")
        print(f"\n🎉 Airtable integration is working. Check your table for 'TEST — DELETE ME'")
        print(f"   You can delete that test row from Airtable now.")
    else:
        print(f"   ❌ Failed: {resp.text}")
except requests.RequestException as e:
    print(f"   ❌ Request failed: {e}")
    raise SystemExit(1)
