# Enphase to Microsoft Fabric Eventstream

An Azure Functions application that polls the Enphase Energy API for solar production data and streams it to Microsoft Fabric Eventstream via Azure Event Hubs.

## ✅ Current Status

The application is **deployed and running** as an Azure Function App, streaming solar telemetry data to Microsoft Fabric.

**What's Working:**
- ✅ Azure Functions V2 with timer trigger (every 4 hours)
- ✅ OAuth 2.0 authentication with Enphase API v4
- ✅ Token persistence with automatic refresh
- ✅ Production, consumption, battery, import, and export telemetry retrieval
- ✅ Real-time streaming to Microsoft Fabric Eventstream
- ✅ Application Insights integration for monitoring and distributed tracing
- ✅ Data stored in Fabric Eventhouse (KQL database)

**Telemetry Types Collected:**
- Production meter data
- Consumption meter data
- Battery telemetry
- Energy import telemetry
- Energy export telemetry

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Enphase API    │────▶│  Azure Function  │────▶│  Fabric Eventstream │
│  (Solar Data)   │     │  (Timer Trigger) │     │  (Event Hub)        │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
                                │                          │
                                ▼                          ▼
                        ┌──────────────────┐     ┌─────────────────────┐
                        │  App Insights    │     │  Fabric Eventhouse  │
                        │  (Monitoring)    │     │  (KQL Database)     │
                        └──────────────────┘     └─────────────────────┘
```

## Features

- ⏰ Timer-triggered Azure Function (runs every 4 hours)
- 🔄 Polls multiple Enphase telemetry endpoints
- ☁️ Streams data to Microsoft Fabric Eventstream via Azure Event Hubs
- 🔐 OAuth 2.0 authentication with automatic token refresh
- 📊 Application Insights integration with OpenCensus tracing
- 📈 Data stored in Fabric Eventhouse for KQL analysis

## Prerequisites

- Python 3.11 or higher
- Azure subscription with Function App
- Enphase Energy account with API access
- Microsoft Fabric workspace with Eventstream and Eventhouse

## Project Structure

```
enphase-data-stream/
├── function_app.py             # Azure Functions entry point (timer trigger)
├── host.json                   # Azure Functions host configuration
├── local.settings.json         # Local development settings (not in git)
├── requirements.txt            # Python dependencies
├── src/
│   ├── main.py                 # Standalone application entry point
│   ├── enphase_client.py       # Enphase API client
│   └── eventstream_sender.py   # Microsoft Fabric Eventstream sender
├── check_all_telemetry.py      # Utility: Check all telemetry endpoints
├── check_latest_snapshot.py    # Utility: Check latest data snapshot
├── QUICKSTART.md               # Quick start guide
└── README.md                   # This file
```

## Configuration

### Azure Function App Settings

| Setting | Description | Required |
|---------|-------------|----------|
| `ENPHASE_API_KEY` | Enphase API key | Yes |
| `ENPHASE_CLIENT_ID` | OAuth client ID | Yes |
| `ENPHASE_CLIENT_SECRET` | OAuth client secret | Yes |
| `ENPHASE_SYSTEM_ID` | Your Enphase system ID | Yes |
| `ENPHASE_REFRESH_TOKEN` | OAuth refresh token | Yes |
| `EVENTHUB_CONNECTION_STRING` | Azure Event Hub connection string | Yes |
| `EVENTHUB_NAME` | Event Hub/Eventstream name | Yes |
| `APPLICATIONINSIGHTS_CONNECTION_STRING` | App Insights connection | Yes |

### Local Development

1. **Clone the repository:**
   ```bash
   git clone https://github.com/diverdown1964/enphase-data-stream.git
   cd enphase-data-stream
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   venv\Scripts\activate  # Windows
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure local.settings.json:**
   ```json
   {
     "IsEncrypted": false,
     "Values": {
       "FUNCTIONS_WORKER_RUNTIME": "python",
       "AzureWebJobsStorage": "UseDevelopmentStorage=true",
       "ENPHASE_API_KEY": "your_api_key",
       "ENPHASE_CLIENT_ID": "your_client_id",
       "ENPHASE_CLIENT_SECRET": "your_client_secret",
       "ENPHASE_SYSTEM_ID": "your_system_id",
       "ENPHASE_REFRESH_TOKEN": "your_refresh_token",
       "EVENTHUB_CONNECTION_STRING": "your_connection_string",
       "EVENTHUB_NAME": "your_eventhub_name",
       "APPLICATIONINSIGHTS_CONNECTION_STRING": "your_app_insights_connection"
     }
   }
   ```

5. **Run locally:**
   ```bash
   func start
   ```

## Deployment

Deploy to Azure using VS Code Azure Functions extension or Azure CLI:

```bash
func azure functionapp publish <your-function-app-name>
```

## Data Format

The function sends JSON events to Eventstream for each telemetry type:

```json
{
  "telemetry_type": "production",
  "system_id": 5706934,
  "retrieved_at": "2025-12-25T11:34:00.647998Z",
  "end_at": 1766657700,
  "devices_reporting": 1,
  "wh_del": 447
}
```

Data is stored in the `HaleJoliSolarRaw` table in Fabric Eventhouse and can be queried using KQL.

## Querying Data in Fabric

```kql
// Get latest production readings
HaleJoliSolarRaw
| where telemetry_type == "production"
| order by end_at desc
| take 10

// Daily production summary
HaleJoliSolarRaw
| where telemetry_type == "production"
| summarize TotalWh = sum(wh_del) by bin(todatetime(end_at * 1s), 1d)
```

## Monitoring

The function integrates with Application Insights for:
- Request tracing
- Dependency tracking (API calls, Event Hub sends)
- Custom metrics and dimensions
- Error logging with context

## Enphase API Setup

1. Go to [Enphase Developer Portal](https://developer-v4.enphase.com/)
2. Create an application to get Client ID and Client Secret
3. Complete OAuth flow to obtain a refresh token
4. Note your System ID from your Enphase monitoring portal

## Microsoft Fabric Setup

1. Create an Eventstream in your Fabric workspace
2. Configure a "Custom App" source to get Event Hub credentials
3. Create an Eventhouse with a KQL database
4. Connect Eventstream to Eventhouse as a destination

## License

MIT License

## Support

- **Enphase API:** [Enphase Developer Support](https://developer-v4.enphase.com/)
- **Microsoft Fabric:** [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- **This application:** Open an issue in this repository
