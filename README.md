> **Retired 2026-11-12.** The Azure Function described below no longer exists. Solar data now
> arrives from the Kona Home Assistant's Enphase Envoy integration into the Fabric Eventhouse
> (`HaleJoli.Raw_HaleJoliAllSignals`) and is shaped by the scripts in [`kql/`](kql/):
> `Silver_Solar` (update policy), `Gold_SolarIntervals()` (the same 15-minute grain as this
> function's `SolarTelemetry` table) and `Gold_SolarTelemetryUnified()` (API history up to
> 2026-11-11 23:45 UTC, Home Assistant-derived intervals after). See [RETIREMENT.md](RETIREMENT.md)
> for what was removed and why. The `SolarTelemetry` table is kept as frozen history.

# Enphase Solar Data to Microsoft Fabric

An Azure Functions application that polls the Enphase Energy API for solar production data and ingests it directly into Microsoft Fabric Eventhouse (Kusto).

## ✅ Current Status

The application is **deployed and running** as an Azure Function App, ingesting solar telemetry data into Microsoft Fabric Eventhouse.

**What's Working:**
- ✅ Azure Functions V2 with timer trigger (every 4 hours)
- ✅ OAuth 2.0 authentication with Enphase API v4
- ✅ Automatic token refresh
- ✅ Production, consumption, battery, import, and export telemetry retrieval
- ✅ Direct ingestion to Microsoft Fabric Eventhouse via Kusto SDK
- ✅ Unified telemetry schema with all metrics in a single table
- ✅ Application Insights integration for monitoring and distributed tracing
- ✅ Managed Identity authentication to Fabric

**Telemetry Collected:**
- Production meter (solar generation)
- Consumption meter (home usage)
- Battery state (charge level, power flow)
- Grid energy import
- Grid energy export

## Architecture

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│  Enphase API    │────▶│  Azure Function  │────▶│  Fabric Eventhouse  │
│  (Solar Data)   │     │  (Timer: 4 hrs)  │     │  (Kusto Database)   │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
                                │                          
                                ▼                          
                        ┌──────────────────┐     
                        │  App Insights    │     
                        │  (Monitoring)    │     
                        └──────────────────┘     
```

## Features

- ⏰ **Timer-triggered** Azure Function (runs every 4 hours)
- 🔄 **Polls multiple** Enphase telemetry endpoints in parallel
- 📊 **Unified schema** - All telemetry types stored in a single table with consistent columns
- 🔐 **Managed Identity** authentication to Fabric Eventhouse
- 🔑 **OAuth 2.0** with automatic token refresh for Enphase API
- 📈 **Application Insights** integration with OpenCensus distributed tracing
- 🌴 **Timezone aware** - Converts UTC timestamps to local timezone (configurable)

## Project Structure

```
enphase-data-stream/
├── function_app.py             # Azure Functions entry point (timer trigger)
├── host.json                   # Azure Functions host configuration
├── local.settings.json         # Local development settings (git-ignored)
├── requirements.txt            # Python dependencies
├── src/                        # Local development modules
│   ├── main.py                 # Standalone entry point
│   ├── enphase_client.py       # Enphase API client
│   ├── kusto_client.py         # Kusto ingestion client
│   ├── eventstream_sender.py   # EventHub sender (legacy)
│   └── local_runner.py         # Local testing runner
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
| `KUSTO_CLUSTER_URI` | Fabric Eventhouse cluster URI | Yes |
| `KUSTO_DATABASE` | Kusto database name | Yes |
| `SYSTEM_TIMEZONE` | Local timezone (e.g., `Pacific/Honolulu`) | Yes |
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
   source venv/bin/activate  # macOS/Linux
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
       "AzureWebJobsStorage": "UseDevelopmentStorage=true",
       "ENPHASE_API_KEY": "your_api_key",
       "ENPHASE_CLIENT_ID": "your_client_id",
       "ENPHASE_CLIENT_SECRET": "your_client_secret",
       "ENPHASE_SYSTEM_ID": "your_system_id",
       "ENPHASE_REFRESH_TOKEN": "your_refresh_token",
       "KUSTO_CLUSTER_URI": "https://your-cluster.z7.kusto.fabric.microsoft.com",
       "KUSTO_DATABASE": "YourDatabase",
       "SYSTEM_TIMEZONE": "America/Los_Angeles",
       "APPLICATIONINSIGHTS_CONNECTION_STRING": "your_app_insights_connection"
     }
   }
   ```

5. **Run locally:**
   ```bash
   func start
   ```

## Deployment

Deploy to Azure using the Azure Functions Core Tools:

```bash
func azure functionapp publish EnphaseEnergyPolling --python
```

## Data Schema

The function ingests data into a unified `SolarTelemetry` table:

| Column | Type | Description |
|--------|------|-------------|
| `Timestamp` | datetime | Reading timestamp (UTC) |
| `LocalTime` | datetime | Reading timestamp (local timezone) |
| `ProductionWh` | real | Solar production (Wh) |
| `ProductionW` | real | Solar production power (W) |
| `ConsumptionWh` | real | Home consumption (Wh) |
| `ConsumptionW` | real | Home consumption power (W) |
| `BatteryPercent` | real | Battery state of charge (%) |
| `BatteryPowerW` | real | Battery power flow (W, positive=charging) |
| `GridImportWh` | real | Energy imported from grid (Wh) |
| `GridExportWh` | real | Energy exported to grid (Wh) |
| `SystemId` | string | Enphase system identifier |

## Querying Data in Fabric

```kql
// Get latest readings
SolarTelemetry
| order by Timestamp desc
| take 10

// Daily energy summary
SolarTelemetry
| summarize 
    TotalProduction = sum(ProductionWh),
    TotalConsumption = sum(ConsumptionWh),
    TotalImport = sum(GridImportWh),
    TotalExport = sum(GridExportWh)
  by bin(LocalTime, 1d)

// Battery usage patterns
SolarTelemetry
| where BatteryPercent > 0
| summarize AvgCharge = avg(BatteryPercent) by bin(LocalTime, 1h)
| render timechart
```

## Monitoring

The function integrates with Application Insights for:
- **Request tracing** - Each function invocation is tracked
- **Dependency tracking** - API calls to Enphase and Kusto are traced
- **Custom dimensions** - Duration, record counts, and system ID
- **Error logging** - Exceptions with full context

View logs in Azure Portal:
- **Function App → Application Insights → Live Metrics** for real-time data
- **Application Insights → Transaction search** to find specific executions

## Enphase API Setup

1. Go to [Enphase Developer Portal](https://developer-v4.enphase.com/)
2. Create an application to get Client ID and Client Secret
3. Complete OAuth flow to obtain a refresh token
4. Note your System ID from your Enphase monitoring portal

## Microsoft Fabric Setup

1. Create an Eventhouse in your Fabric workspace
2. Create a KQL database
3. Grant the Function App's Managed Identity access to the database
4. Note the cluster URI from the Eventhouse overview page

### Grant Managed Identity Access

In your Kusto database, run:
```kql
.add database ['YourDatabase'] ingestors ('aadapp=<function-app-managed-identity-id>')
```

## License

MIT License

## Support

- **Enphase API:** [Enphase Developer Support](https://developer-v4.enphase.com/)
- **Microsoft Fabric:** [Microsoft Fabric Documentation](https://learn.microsoft.com/fabric/)
- **Azure Functions:** [Azure Functions Python Guide](https://learn.microsoft.com/azure/azure-functions/functions-reference-python)
