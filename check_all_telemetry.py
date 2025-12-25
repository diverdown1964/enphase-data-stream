"""Check all available telemetry endpoints for the system"""
import json
import requests
from datetime import datetime

# Load tokens
with open('.tokens.json', 'r') as f:
    data = json.load(f)
    token = data['access_token']

headers = {
    'Authorization': f'Bearer {token}',
    'key': '8c8efb4bed8d29a817fe9e0be8bdf99a'
}

system_id = '5706934'
base_url = 'https://api.enphaseenergy.com/api/v4/systems'

print("=== Testing Available Telemetry Endpoints ===\n")

# Test different telemetry endpoints
endpoints = {
    'Production Meter': f'{base_url}/{system_id}/telemetry/production_meter',
    'Consumption Meter': f'{base_url}/{system_id}/telemetry/consumption_meter',
    'Battery': f'{base_url}/{system_id}/telemetry/battery',
    'Energy Import': f'{base_url}/{system_id}/energy_import_telemetry',
    'Energy Export': f'{base_url}/{system_id}/energy_export_telemetry',
    'Production Micro': f'{base_url}/{system_id}/telemetry/production_micro',
    'Latest Telemetry': f'{base_url}/{system_id}/latest_telemetry',
}

for name, url in endpoints.items():
    print(f"\n{'='*60}")
    print(f"Testing: {name}")
    print(f"{'='*60}")
    
    try:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Status: {response.status_code}")
            print(f"Keys: {list(result.keys())}")
            
            # Show sample data
            if 'intervals' in result:
                intervals = result['intervals']
                print(f"Intervals: {len(intervals)}")
                if intervals:
                    print(f"Sample interval keys: {list(intervals[0].keys())}")
                    print(f"Latest interval: {intervals[-1]}")
            else:
                print(f"Data: {result}")
                
        elif response.status_code == 404:
            print(f"❌ Status: {response.status_code} - Not available for this system")
        else:
            print(f"⚠️ Status: {response.status_code}")
            print(f"Response: {response.text[:200]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

print("\n" + "="*60)
print("Summary: Available telemetry types for your system")
print("="*60)
