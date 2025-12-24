# 🚀 Quick Start Guide

## What's Already Done ✅

Your Enphase API connection is fully configured and working:

1. ✅ **OAuth Authentication**: Working with token persistence and automatic refresh
2. ✅ **API v4 Integration**: Successfully retrieving production telemetry data
3. ✅ **System Validated**: Connected to System 5706934 ("Whites Pv")
4. ✅ **Data Flow**: Getting 15-minute interval production data

**Last successful data retrieval:**
- 41 intervals of telemetry data
- Latest production: 447 Wh delivered
- All authentication tokens valid and refreshing automatically

---

## 📋 What You Need to Do Next

### Step 1: Get Microsoft Fabric Eventstream Credentials

1. **Open Microsoft Fabric** and navigate to your workspace
2. **Create or open an Eventstream**
3. **Add a source**: Select "Custom App"
4. **Copy the connection details:**
   - Event Hub connection string
   - Event Hub name (usually your Eventstream name)

The connection string looks like:
```
Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=your-key-here;EntityPath=your-eventhub-name
```

### Step 2: Update Your `.env` File

Open `.env` in this directory and replace these two lines:

```bash
# Replace these placeholder values:
EVENTHUB_CONNECTION_STRING=your_eventhub_connection_string_here
EVENTHUB_NAME=your_eventstream_name_here

# With your actual values:
EVENTHUB_CONNECTION_STRING=Endpoint=sb://...
EVENTHUB_NAME=your-actual-eventstream-name
```

**Save the file.**

### Step 3: Run the Application

```bash
# Make sure you're in the virtual environment
venv\Scripts\activate

# Run the application
python src/main.py
```

**What will happen:**
- The application will immediately poll Enphase API for production data
- Data will be sent to your Fabric Eventstream
- Polling will continue every 5 minutes (configurable)
- Press `Ctrl+C` to stop gracefully

---

## 🧪 Testing

### Test 1: Verify Enphase API Connection
```bash
python test_connection.py
```
✅ This should already work (we tested this successfully)

### Test 2: Verify v4 Telemetry Endpoint
```bash
python test_v4_telemetry.py
```
✅ This should already work (447 Wh delivered in latest test)

### Test 3: Test Complete Pipeline (after Step 2 above)
```bash
python src/main.py
```
This will test the full end-to-end flow including Eventstream delivery.

---

## 📊 What Data Is Being Sent

Each poll sends production telemetry data with:
- **System ID**: 5706934
- **Intervals**: Array of 15-minute readings
- **Metrics per interval**:
  - `end_at`: Timestamp
  - `devices_reporting`: Number of devices
  - `wh_del`: Watt-hours delivered

Example payload:
```json
{
  "system_id": 5706934,
  "granularity": "day",
  "total_devices": 1,
  "intervals": [
    {"end_at": 1766571300, "devices_reporting": 1, "wh_del": 115},
    {"end_at": 1766571600, "devices_reporting": 1, "wh_del": 275},
    ...
  ],
  "retrieved_at": "2025-12-24T15:30:00"
}
```

---

## ⚙️ Configuration Options

### Change Polling Interval

Edit `.env`:
```bash
# Poll every 1 hour (3600 seconds) - RECOMMENDED
POLL_INTERVAL=3600

# Poll every 90 minutes (more margin)
POLL_INTERVAL=5400

# Poll every 30 minutes (higher frequency, still safe)
POLL_INTERVAL=1800
```

**Rate Limits:**
- Your Enphase "Watt" plan allows: 10 hits/minute, 1000 hits/month
- 1-hour polling = 720 hits/month ✅ (within limit)
- Each poll returns all 15-minute intervals for the day

---

## 🔍 Monitoring

### Check Logs

The application logs all operations:
```
{"asctime": "2025-12-24 15:30:00", "name": "root", "levelname": "INFO", "message": "Successfully retrieved production telemetry for system 5706934"}
{"asctime": "2025-12-24 15:30:01", "name": "root", "levelname": "INFO", "message": "Data successfully sent to Eventstream"}
```

### View in Microsoft Fabric

1. Open your Eventstream in Fabric
2. Check the "Data preview" tab
3. You should see incoming events with your solar production data

---

## 🆘 Troubleshooting

### Issue: "Missing required environment variables: EVENTHUB_CONNECTION_STRING"
**Solution**: Complete Step 2 above - add your Eventstream credentials to `.env`

### Issue: Enphase authentication fails
**Solution**: Your tokens are already working! If they expire, the app will automatically refresh them using the stored refresh token in `.tokens.json`

### Issue: No data in Eventstream
**Check:**
1. Is `python src/main.py` running without errors?
2. Are logs showing "Data successfully sent to Eventstream"?
3. Is your Eventstream connection string correct?
4. Try the Eventstream "Test connection" feature in Fabric

---

## 📞 Support

- **Enphase API Documentation**: https://developer-v4.enphase.com/docs.html
- **Microsoft Fabric Eventstream**: https://learn.microsoft.com/en-us/fabric/real-time-analytics/event-streams/
- **Azure Event Hubs SDK**: https://learn.microsoft.com/en-us/python/api/overview/azure/eventhub

---

## 🎯 Summary

**You're almost done!** The hard part (Enphase API integration) is complete and working.

**Just 2 steps remaining:**
1. Get your Fabric Eventstream connection string
2. Add it to `.env`
3. Run `python src/main.py`

That's it! Your solar production data will start flowing into Microsoft Fabric.
