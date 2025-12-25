"""
Azure Functions V2 - Enphase Solar Data Poller
Polls Enphase API every 4 hours and sends telemetry to Fabric Eventstream
"""
import azure.functions as func
import logging
import os
import json
import time
from datetime import datetime, timezone

import requests
from azure.eventhub import EventHubProducerClient, EventData
from opencensus.ext.azure.log_exporter import AzureLogHandler
from opencensus.ext.azure.trace_exporter import AzureExporter
from opencensus.trace import config_integration
from opencensus.trace.samplers import AlwaysOnSampler
from opencensus.trace.tracer import Tracer

app = func.FunctionApp()

# Enphase API configuration
ENPHASE_BASE_URL = "https://api.enphaseenergy.com/api/v4"
ENPHASE_TOKEN_URL = "https://api.enphaseenergy.com/oauth/token"

# Configure OpenCensus for distributed tracing
config_integration.trace_integrations(['requests'])

def get_tracer():
    """Get configured tracer for Application Insights"""
    connection_string = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
    if connection_string:
        return Tracer(
            exporter=AzureExporter(connection_string=connection_string),
            sampler=AlwaysOnSampler()
        )
    return Tracer(sampler=AlwaysOnSampler())

def get_logger():
    """Get logger configured with Application Insights"""
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        connection_string = os.environ.get("APPLICATIONINSIGHTS_CONNECTION_STRING")
        if connection_string:
            logger.addHandler(AzureLogHandler(connection_string=connection_string))
    return logger


class EnphaseClient:
    """Client for interacting with Enphase Energy API v4"""
    
    def __init__(self, api_key: str, client_id: str, client_secret: str, 
                 system_id: str, refresh_token: str, logger=None, tracer=None):
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.system_id = system_id
        self.refresh_token = refresh_token
        self.access_token = None
        self.logger = logger or logging.getLogger(__name__)
        self.tracer = tracer
        
    def _refresh_access_token(self):
        """Get new access token using refresh token"""
        start_time = time.time()
        try:
            response = requests.post(
                ENPHASE_TOKEN_URL,
                auth=(self.client_id, self.client_secret),
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.refresh_token
                }
            )
            response.raise_for_status()
            data = response.json()
            self.access_token = data["access_token"]
            if "refresh_token" in data:
                self.refresh_token = data["refresh_token"]
            
            duration_ms = (time.time() - start_time) * 1000
            self.logger.info("Token refreshed successfully", extra={
                'custom_dimensions': {
                    'operation': 'token_refresh',
                    'duration_ms': duration_ms,
                    'system_id': self.system_id
                }
            })
            return self.access_token
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            self.logger.error(f"Token refresh failed: {e}", extra={
                'custom_dimensions': {
                    'operation': 'token_refresh',
                    'duration_ms': duration_ms,
                    'error': str(e)
                }
            })
            raise
    
    def _get_headers(self) -> dict:
        """Get headers with API key and access token"""
        if not self.access_token:
            self._refresh_access_token()
        return {
            "Authorization": f"Bearer {self.access_token}",
            "key": self.api_key
        }
    
    def _make_request(self, endpoint: str, params: dict = None) -> dict:
        """Make authenticated request with retry on 401"""
        url = f"{ENPHASE_BASE_URL}/systems/{self.system_id}/{endpoint}"
        start_time = time.time()
        
        for attempt in range(2):
            response = requests.get(url, headers=self._get_headers(), params=params)
            if response.status_code == 401 and attempt == 0:
                self.logger.info("Token expired, refreshing...", extra={
                    'custom_dimensions': {'endpoint': endpoint, 'attempt': attempt}
                })
                self._refresh_access_token()
                continue
            
            duration_ms = (time.time() - start_time) * 1000
            
            if response.ok:
                self.logger.info(f"API call successful: {endpoint}", extra={
                    'custom_dimensions': {
                        'endpoint': endpoint,
                        'status_code': response.status_code,
                        'duration_ms': duration_ms,
                        'system_id': self.system_id
                    }
                })
            else:
                self.logger.warning(f"API call failed: {endpoint}", extra={
                    'custom_dimensions': {
                        'endpoint': endpoint,
                        'status_code': response.status_code,
                        'duration_ms': duration_ms,
                        'response_text': response.text[:500]
                    }
                })
            
            response.raise_for_status()
            return response.json()
        
        raise Exception("Failed after token refresh")
    
    def get_production_data(self, granularity: str = "week") -> dict:
        """Get production meter telemetry"""
        return self._make_request("telemetry/production_meter", {"granularity": granularity})
    
    def get_consumption_data(self, granularity: str = "week") -> dict:
        """Get consumption meter telemetry"""
        return self._make_request("telemetry/consumption_meter", {"granularity": granularity})
    
    def get_battery_data(self, granularity: str = "week") -> dict:
        """Get battery telemetry"""
        return self._make_request("telemetry/battery", {"granularity": granularity})
    
    def get_import_data(self, granularity: str = "week") -> dict:
        """Get energy import telemetry"""
        return self._make_request("energy_import_telemetry", {"granularity": granularity})
    
    def get_export_data(self, granularity: str = "week") -> dict:
        """Get energy export telemetry"""
        return self._make_request("energy_export_telemetry", {"granularity": granularity})
    
    def get_all_telemetry(self, granularity: str = "week") -> dict:
        """Get all available telemetry types"""
        results = {}
        
        endpoints = [
            ("production", self.get_production_data),
            ("consumption", self.get_consumption_data),
            ("battery", self.get_battery_data),
            ("import", self.get_import_data),
            ("export", self.get_export_data),
        ]
        
        for name, method in endpoints:
            try:
                results[name] = method(granularity)
                self.logger.info(f"Retrieved {name} telemetry", extra={
                    'custom_dimensions': {'telemetry_type': name, 'system_id': self.system_id}
                })
            except Exception as e:
                self.logger.warning(f"Failed to get {name} telemetry: {e}", extra={
                    'custom_dimensions': {'telemetry_type': name, 'error': str(e)}
                })
                results[name] = None
        
        return results


def send_to_eventstream(connection_string: str, eventhub_name: str, events: list, logger=None):
    """Send events to Fabric Eventstream via Event Hub"""
    logger = logger or logging.getLogger(__name__)
    start_time = time.time()
    
    producer = EventHubProducerClient.from_connection_string(
        conn_str=connection_string,
        eventhub_name=eventhub_name
    )
    
    try:
        with producer:
            event_data_batch = producer.create_batch()
            for event in events:
                event_data_batch.add(EventData(json.dumps(event)))
            producer.send_batch(event_data_batch)
            
        duration_ms = (time.time() - start_time) * 1000
        logger.info(f"Sent {len(events)} events to Eventstream", extra={
            'custom_dimensions': {
                'operation': 'send_to_eventstream',
                'event_count': len(events),
                'duration_ms': duration_ms,
                'eventhub_name': eventhub_name
            }
        })
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        logger.error(f"Failed to send events to Eventstream: {e}", extra={
            'custom_dimensions': {
                'operation': 'send_to_eventstream',
                'event_count': len(events),
                'duration_ms': duration_ms,
                'error': str(e)
            }
        })
        raise


def process_telemetry(telemetry_type: str, data: dict, retrieved_at: str) -> list:
    """Process telemetry data into individual events"""
    events = []
    
    if not data:
        return events
    
    system_id = data.get("system_id")
    
    # Handle different response structures from various endpoints
    intervals = data.get("intervals", [])
    
    for interval in intervals:
        # Check if interval is a dict (most endpoints) or needs wrapping
        if isinstance(interval, dict):
            event = {
                "telemetry_type": telemetry_type,
                "system_id": system_id,
                "retrieved_at": retrieved_at,
                **interval
            }
        else:
            # Some endpoints may return simple values
            event = {
                "telemetry_type": telemetry_type,
                "system_id": system_id,
                "retrieved_at": retrieved_at,
                "value": interval
            }
        events.append(event)
    
    # Also handle 'readings' key (used by battery endpoint)
    readings = data.get("readings", [])
    for reading in readings:
        if isinstance(reading, dict):
            event = {
                "telemetry_type": telemetry_type,
                "system_id": system_id,
                "retrieved_at": retrieved_at,
                **reading
            }
        else:
            event = {
                "telemetry_type": telemetry_type,
                "system_id": system_id,
                "retrieved_at": retrieved_at,
                "value": reading
            }
        events.append(event)
    
    return events


@app.timer_trigger(schedule="0 0 */4 * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=True)
def enphase_poller(myTimer: func.TimerRequest) -> None:
    """Timer-triggered function to poll Enphase and send to Eventstream"""
    
    # Initialize instrumented logger and tracer
    logger = get_logger()
    tracer = get_tracer()
    
    execution_start = time.time()
    retrieved_at = datetime.now(timezone.utc).isoformat()
    invocation_id = os.environ.get("INVOCATION_ID", retrieved_at)
    
    with tracer.span(name="enphase_poller") as span:
        span.add_attribute("invocation_id", invocation_id)
        
        if myTimer.past_due:
            logger.warning("Timer is past due!", extra={
                'custom_dimensions': {'invocation_id': invocation_id, 'past_due': True}
            })
        
        logger.info("EnphasePoller function started", extra={
            'custom_dimensions': {'invocation_id': invocation_id, 'retrieved_at': retrieved_at}
        })
        
        # Get configuration from environment
        api_key = os.environ.get("ENPHASE_API_KEY")
        client_id = os.environ.get("ENPHASE_CLIENT_ID")
        client_secret = os.environ.get("ENPHASE_CLIENT_SECRET")
        system_id = os.environ.get("ENPHASE_SYSTEM_ID")
        refresh_token = os.environ.get("ENPHASE_REFRESH_TOKEN")
        connection_string = os.environ.get("EVENTHUB_CONNECTION_STRING")
        eventhub_name = os.environ.get("EVENTHUB_NAME")
        
        # Validate configuration
        missing = []
        for name, value in [
            ("ENPHASE_API_KEY", api_key),
            ("ENPHASE_CLIENT_ID", client_id),
            ("ENPHASE_CLIENT_SECRET", client_secret),
            ("ENPHASE_SYSTEM_ID", system_id),
            ("ENPHASE_REFRESH_TOKEN", refresh_token),
            ("EVENTHUB_CONNECTION_STRING", connection_string),
            ("EVENTHUB_NAME", eventhub_name),
        ]:
            if not value:
                missing.append(name)
        
        if missing:
            logger.error(f"Missing configuration: {', '.join(missing)}", extra={
                'custom_dimensions': {'missing_config': missing, 'invocation_id': invocation_id}
            })
            return
        
        span.add_attribute("system_id", system_id)
        
        try:
            # Initialize Enphase client with instrumentation
            client = EnphaseClient(
                api_key=api_key,
                client_id=client_id,
                client_secret=client_secret,
                system_id=system_id,
                refresh_token=refresh_token,
                logger=logger,
                tracer=tracer
            )
            
            # Get all telemetry
            with tracer.span(name="fetch_all_telemetry"):
                all_telemetry = client.get_all_telemetry(granularity="week")
            
            # Process into events
            all_events = []
            telemetry_counts = {}
            for telemetry_type, data in all_telemetry.items():
                if data:
                    events = process_telemetry(telemetry_type, data, retrieved_at)
                    all_events.extend(events)
                    telemetry_counts[telemetry_type] = len(events)
                    logger.info(f"Processed {len(events)} {telemetry_type} events", extra={
                        'custom_dimensions': {
                            'telemetry_type': telemetry_type,
                            'event_count': len(events),
                            'invocation_id': invocation_id
                        }
                    })
            
            # Send to Eventstream
            if all_events:
                with tracer.span(name="send_to_eventstream"):
                    send_to_eventstream(connection_string, eventhub_name, all_events, logger)
                
                execution_duration_ms = (time.time() - execution_start) * 1000
                logger.info(f"Successfully sent {len(all_events)} total events", extra={
                    'custom_dimensions': {
                        'total_events': len(all_events),
                        'telemetry_counts': json.dumps(telemetry_counts),
                        'execution_duration_ms': execution_duration_ms,
                        'invocation_id': invocation_id,
                        'system_id': system_id
                    }
                })
            else:
                logger.warning("No events to send", extra={
                    'custom_dimensions': {'invocation_id': invocation_id}
                })
                
        except Exception as e:
            execution_duration_ms = (time.time() - execution_start) * 1000
            logger.error(f"Error in EnphasePoller: {e}", extra={
                'custom_dimensions': {
                    'error': str(e),
                    'error_type': type(e).__name__,
                    'execution_duration_ms': execution_duration_ms,
                    'invocation_id': invocation_id
                }
            })
            raise
        
        logger.info("EnphasePoller function completed", extra={
            'custom_dimensions': {
                'invocation_id': invocation_id,
                'execution_duration_ms': (time.time() - execution_start) * 1000
            }
        })
