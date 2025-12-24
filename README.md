# Enphase to Microsoft Fabric Eventstream

A Python application that polls the Enphase Energy API for solar production data and streams it to Microsoft Fabric Eventstream in real-time.

## ✅ Current Status

The application is **ready to stream data** once you configure Microsoft Fabric Eventstream credentials.

**What's Working:**
- ✅ OAuth 2.0 authentication with Enphase API v4
- ✅ Token persistence with automatic refresh
- ✅ Production telemetry data retrieval (15-minute intervals)
- ✅ System discovery and validation (System: 5706934 "Whites Pv")
- ✅ V4 API integration with proper authentication headers

**Last Test Results:**
- System ID: 5706934
- Total Devices: 1
- Latest Production: 447 Wh delivered
- Retrieved 41 intervals of telemetry data

**⚠️ To Complete Setup:**
You need to add your Microsoft Fabric Eventstream credentials to the `.env` file (see instructions below).

## Features

- 🔄 Continuous polling of Enphase Energy API
- ☁️ Streams data to Microsoft Fabric Eventstream via Azure Event Hubs
- 🔐 OAuth 2.0 authentication with Enphase API
- 📊 JSON structured logging
- ⚡ Configurable polling intervals
- 🛡️ Graceful shutdown handling

## Prerequisites

- Python 3.8 or higher
- Enphase Energy account with API access
- Microsoft Fabric workspace with Eventstream configured

## Enphase API Setup

1. **Get Enphase API Credentials:**
   - Go to [Enphase Developer Portal](https://developer-v4.enphase.com/)
   - Create an account or sign in
   - Create a new application to get your Client ID and Client Secret
   - Note your System ID from your Enphase monitoring portal

2. **API Access:**
   - You'll need to register for API access through Enphase
   - Ensure you have the appropriate API tier for your polling frequency

## Microsoft Fabric Eventstream Setup

1. **Create an Eventstream:**
   - In your Microsoft Fabric workspace, create a new Eventstream
   - Configure the Eventstream source as "Custom App"
   - Copy the Event Hub connection string and Event Hub name

2. **Connection String:**
   - The connection string should look like:
     ```
     Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key>;EntityPath=<eventhub-name>
     ```

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd enphase-data-stream
   ```

2. **Create a virtual environment:**
   ```bash
   python -m venv venv
   
   # Windows
   venv\Scripts\activate
   
   # Linux/Mac
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure environment variables:**
   ```bash
   # Copy the example file
   cp .env.example .env
   
   # Edit .env with your credentials
   ```

5. **Edit the `.env` file with your credentials:**
   ```env
   # Enphase API Configuration
   ENPHASE_API_KEY=your_api_key_here
   ENPHASE_CLIENT_ID=your_client_id_here
   ENPHASE_CLIENT_SECRET=your_client_secret_here
   ENPHASE_SYSTEM_ID=your_system_id_here

   # Microsoft Fabric Eventstream Configuration
   EVENTHUB_CONNECTION_STRING=your_eventhub_connection_string_here
   EVENTHUB_NAME=your_eventstream_name_here

   # Polling Configuration (in seconds)
   POLL_INTERVAL=300
   ```

## Usage

### Run the application:

```bash
python src/main.py
```

### Run with custom poll interval:

```bash
# Set in .env file or override with environment variable
POLL_INTERVAL=60 python src/main.py
```

### Graceful shutdown:

Press `Ctrl+C` or send a SIGTERM signal. The application will complete the current operation and close connections gracefully.

## Project Structure

```
enphase-data-stream/
├── src/
│   ├── main.py                 # Main application entry point
│   ├── enphase_client.py       # Enphase API client
│   └── eventstream_sender.py   # Microsoft Fabric Eventstream sender
├── .env.example                # Example environment configuration
├── .gitignore                  # Git ignore file
├── requirements.txt            # Python dependencies
└── README.md                   # This file
```

## Data Format

The application sends JSON data to Eventstream with the following structure from Enphase API v4 telemetry:

```json
{
  "system_id": 5706934,
  "granularity": "day",
  "total_devices": 1,
  "start_at": 1766570400,
  "end_at": 1766607947,
  "intervals": [
    {
      "end_at": 1766570400,
      "devices_reporting": 1,
      "wh_del": 0
    },
    {
      "end_at": 1766571300,
      "devices_reporting": 1,
      "wh_del": 115
    }
  ],
  "retrieved_at": "2025-12-24T15:30:00"
}
```

**Fields:**
- `system_id`: Your Enphase system identifier
- `granularity`: Time interval granularity (e.g., "day" for 15-minute intervals)
- `total_devices`: Number of production meters reporting
- `start_at`: Unix timestamp of data range start
- `end_at`: Unix timestamp of data range end
- `intervals`: Array of production readings
  - `end_at`: Unix timestamp for this interval
  - `devices_reporting`: Number of devices reporting in this interval
  - `wh_del`: Watt-hours delivered in this interval
- `retrieved_at`: ISO timestamp when data was retrieved

## Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `ENPHASE_API_KEY` | Enphase API key | Yes |
| `ENPHASE_CLIENT_ID` | OAuth client ID | Yes |
| `ENPHASE_CLIENT_SECRET` | OAuth client secret | Yes |
| `ENPHASE_SYSTEM_ID` | Your Enphase system ID | Yes |
| `EVENTHUB_CONNECTION_STRING` | Azure Event Hub connection string | Yes |
| `EVENTHUB_NAME` | Event Hub/Eventstream name | Yes |
| `POLL_INTERVAL` | Polling interval in seconds (default: 300) | No |

### Polling Interval Recommendations

- **Real-time monitoring:** 60-300 seconds
- **Historical analysis:** 600-1800 seconds
- **API rate limits:** Check Enphase API documentation for your tier

## Logging

The application uses JSON structured logging for easy integration with log aggregation systems. Logs include:

- Timestamp
- Log level
- Logger name
- Message
- Additional context fields

Example log entry:
```json
{
  "asctime": "2025-12-24 10:30:00,123",
  "name": "root",
  "levelname": "INFO",
  "message": "Successfully retrieved production data for system 123456"
}
```

## Troubleshooting

### Authentication Errors
- Verify your Enphase API credentials are correct
- Check that your API key has not expired
- Ensure your client ID and secret are valid

### Connection Errors
- Verify your Event Hub connection string is correct
- Check that the Event Hub name matches your Eventstream
- Ensure your network allows outbound connections to Azure

### No Data Being Sent
- Check the application logs for errors
- Verify your system ID is correct
- Ensure your Enphase system is online and producing data

## Development

### Running Tests
```bash
# Add tests in the future
pytest
```

### Code Formatting
```bash
# Install development dependencies
pip install black flake8

# Format code
black src/

# Check linting
flake8 src/
```

## License

MIT License - See LICENSE file for details

## Support

For issues related to:
- **Enphase API:** Contact Enphase Developer Support
- **Microsoft Fabric:** Check Microsoft Fabric documentation
- **This application:** Open an issue in this repository

## Acknowledgments

- Enphase Energy for their API
- Microsoft for Fabric Eventstream platform
