"""Check the time period and structure of latest_telemetry endpoint"""
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

print("=== Latest Telemetry Snapshot Analysis ===\n")

# Get latest telemetry
r = requests.get('https://api.enphaseenergy.com/api/v4/systems/5706934/latest_telemetry', headers=headers)
result = r.json()

print("Response Keys:", list(result.keys()))
print()

# Analyze the devices
devices = result.get('devices', {})
print("Device Types:", list(devices.keys()))
print()

# Check meters
meters = devices.get('meters', [])
print(f"=== Meters ({len(meters)}) ===")
for meter in meters:
    name = meter.get('name')
    channel = meter.get('channel')
    power = meter.get('power')
    last_report = meter.get('last_report_at')
    
    if last_report:
        dt = datetime.fromtimestamp(last_report)
        print(f"{name:12} Ch{channel}: {power:5}W (reported: {dt.strftime('%Y-%m-%d %H:%M:%S')})")
    else:
        print(f"{name:12} Ch{channel}: {power}W (no report)")

print()

# Check batteries
batteries = devices.get('encharges', [])
print(f"=== Batteries ({len(batteries)}) ===")
for battery in batteries:
    name = battery.get('name')
    power = battery.get('power')
    mode = battery.get('operational_mode')
    last_report = battery.get('last_report_at')
    
    if last_report:
        dt = datetime.fromtimestamp(last_report)
        print(f"{name}: {power:4}W {mode:12} (reported: {dt.strftime('%Y-%m-%d %H:%M:%S')})")

print()
print("="*70)
print("\n📊 CONCLUSION:")
print("- latest_telemetry is a SNAPSHOT of current power levels")
print("- NOT a time range - it's the most recent readings from devices")
print("- Each device reports its last known state with timestamp")
print("- No 'intervals' - just current/latest values")
print("- Useful for: Real-time monitoring dashboards")
print()

# Calculate how recent the data is
now = datetime.now()
if meters and meters[0].get('last_report_at'):
    last_report_time = datetime.fromtimestamp(meters[0]['last_report_at'])
    age_seconds = (now - last_report_time).total_seconds()
    print(f"Data freshness: {age_seconds:.0f} seconds old")
