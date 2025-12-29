"""
Azure Functions V2 - Enphase Solar Data Poller
Polls Enphase API and writes unified telemetry directly to Fabric Eventhouse (Kusto)
"""
import azure.functions as func
import logging
import os
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
from zoneinfo import ZoneInfo

import requests
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.identity import DefaultAzureCredential
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
    
    def get_production_data(self, start_at: int = None, end_at: int = None) -> dict:
        """Get production meter telemetry"""
        params = {"granularity": "week"}
        if start_at:
            params["start_at"] = start_at
        if end_at:
            params["end_at"] = end_at
        data = self._make_request("telemetry/production_meter", params)
        data['retrieved_at'] = datetime.now(timezone.utc).isoformat()
        return data
    
    def get_consumption_data(self, start_at: int = None, end_at: int = None) -> dict:
        """Get consumption meter telemetry"""
        params = {"granularity": "week"}
        if start_at:
            params["start_at"] = start_at
        if end_at:
            params["end_at"] = end_at
        data = self._make_request("telemetry/consumption_meter", params)
        data['retrieved_at'] = datetime.now(timezone.utc).isoformat()
        return data
    
    def get_battery_data(self, start_at: int = None, end_at: int = None) -> dict:
        """Get battery telemetry"""
        params = {"granularity": "week"}
        if start_at:
            params["start_at"] = start_at
        if end_at:
            params["end_at"] = end_at
        data = self._make_request("telemetry/battery", params)
        data['retrieved_at'] = datetime.now(timezone.utc).isoformat()
        return data
    
    def get_import_data(self, start_at: int = None, end_at: int = None) -> dict:
        """Get energy import telemetry"""
        params = {"granularity": "week"}
        if start_at:
            params["start_at"] = start_at
        if end_at:
            params["end_at"] = end_at
        data = self._make_request("energy_import_telemetry", params)
        data['retrieved_at'] = datetime.now(timezone.utc).isoformat()
        return data
    
    def get_export_data(self, start_at: int = None, end_at: int = None) -> dict:
        """Get energy export telemetry"""
        params = {"granularity": "week"}
        if start_at:
            params["start_at"] = start_at
        if end_at:
            params["end_at"] = end_at
        data = self._make_request("energy_export_telemetry", params)
        data['retrieved_at'] = datetime.now(timezone.utc).isoformat()
        return data


class FabricKustoClient:
    """Client for writing to Fabric Eventhouse (Kusto) using managed identity"""
    
    DEFAULT_TIMEZONE = 'Pacific/Honolulu'
    
    def __init__(self, cluster_uri: str, database: str, system_timezone: str = None, logger=None):
        self.cluster_uri = cluster_uri
        self.database = database
        self.system_timezone = system_timezone or self.DEFAULT_TIMEZONE
        self._tz = ZoneInfo(self.system_timezone)
        self._client: Optional[KustoClient] = None
        self.logger = logger or logging.getLogger(__name__)
    
    def _get_client(self) -> KustoClient:
        """Get or create the Kusto client with managed identity auth"""
        if self._client is None:
            # Use DefaultAzureCredential for managed identity in Azure Functions
            credential = DefaultAzureCredential()
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                self.cluster_uri, credential
            )
            self._client = KustoClient(kcsb)
            self.logger.info(f"Connected to Kusto cluster: {self.cluster_uri}")
        return self._client
    
    def _utc_to_local(self, utc_dt: datetime) -> datetime:
        """Convert UTC datetime to local time"""
        if utc_dt is None:
            return None
        utc_aware = utc_dt.replace(tzinfo=timezone.utc)
        local_dt = utc_aware.astimezone(self._tz)
        return local_dt.replace(tzinfo=None)
    
    def get_latest_end_at(self, system_id: int) -> Optional[int]:
        """Get the most recent end_at timestamp from SolarTelemetry"""
        query = f"""
        SolarTelemetry
        | where system_id == {system_id}
        | summarize max(end_at)
        """
        try:
            client = self._get_client()
            response = client.execute(self.database, query)
            
            for row in response.primary_results[0]:
                max_end_at = row[0]
                if max_end_at is not None:
                    self.logger.info(f"Latest end_at: {max_end_at}")
                    return int(max_end_at)
            
            self.logger.info(f"No existing data for system {system_id}")
            return None
        except KustoServiceError as e:
            self.logger.error(f"Kusto query error: {e}")
            raise
    
    def get_existing_end_ats(self, system_id: int, start_at: int, end_at: int) -> set:
        """Get set of existing end_at timestamps in a date range for deduplication"""
        start_dt = datetime.utcfromtimestamp(start_at)
        end_dt = datetime.utcfromtimestamp(end_at)
        query = f"""
        SolarTelemetry
        | where system_id == {system_id}
        | where reading_time >= datetime({start_dt.isoformat()}Z) and reading_time < datetime({end_dt.isoformat()}Z)
        | project end_at
        """
        try:
            client = self._get_client()
            response = client.execute(self.database, query)
            return set(int(row[0]) for row in response.primary_results[0] if row[0] is not None)
        except KustoServiceError as e:
            self.logger.error(f"Kusto query error: {e}")
            raise
    
    def ingest_unified_telemetry(self, system_id: int, retrieved_at: datetime, 
                                  merged_intervals: List[Dict]) -> int:
        """Ingest merged telemetry data into the unified SolarTelemetry table"""
        client = self._get_client()
        ingested = 0
        
        for interval in merged_intervals:
            end_at = interval.get('end_at')
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            reading_time_local = self._utc_to_local(reading_time)
            
            # Extract all measures with defaults
            production_wh = interval.get('production_wh', 0.0)
            production_devices = interval.get('production_devices', 0)
            consumption_wh = interval.get('consumption_wh', 0.0)
            consumption_devices = interval.get('consumption_devices', 0)
            battery_charge_wh = interval.get('battery_charge_wh', 0.0)
            battery_discharge_wh = interval.get('battery_discharge_wh', 0.0)
            battery_soc_percent = interval.get('battery_soc_percent', 0.0)
            battery_devices = interval.get('battery_devices', 0)
            grid_import_wh = interval.get('grid_import_wh', 0.0)
            grid_export_wh = interval.get('grid_export_wh', 0.0)
            
            command = f""".ingest inline into table SolarTelemetry <|
{system_id},{end_at},{reading_time.isoformat() if reading_time else ''},{retrieved_at.isoformat()},{production_wh},{production_devices},{consumption_wh},{consumption_devices},{battery_charge_wh},{battery_discharge_wh},{battery_soc_percent},{battery_devices},{grid_import_wh},{grid_export_wh},{reading_time_local.isoformat() if reading_time_local else ''}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                self.logger.error(f"Failed to ingest telemetry row: {e}")
        
        self.logger.info(f"Ingested {ingested} unified telemetry rows")
        return ingested
    
    def close(self):
        """Close the client connection"""
        if self._client:
            self._client.close()
            self._client = None


def flatten_intervals(raw_intervals: List, is_nested: bool = False) -> List[Dict]:
    """Flatten nested interval structure if needed"""
    if is_nested and raw_intervals and isinstance(raw_intervals[0], list):
        return raw_intervals[0]
    return raw_intervals


def merge_intervals(all_data: Dict[str, Dict]) -> List[Dict]:
    """Merge all telemetry types into unified interval records by end_at timestamp"""
    merged: Dict[int, Dict] = defaultdict(lambda: {
        'end_at': None,
        'production_wh': 0.0,
        'production_devices': 0,
        'consumption_wh': 0.0,
        'consumption_devices': 0,
        'battery_charge_wh': 0.0,
        'battery_discharge_wh': 0.0,
        'battery_soc_percent': 0.0,
        'battery_devices': 0,
        'grid_import_wh': 0.0,
        'grid_export_wh': 0.0,
    })
    
    # Process production
    for interval in all_data.get('production', {}).get('intervals', []):
        end_at = interval.get('end_at')
        if end_at:
            merged[end_at]['end_at'] = end_at
            merged[end_at]['production_wh'] = interval.get('wh_del', 0.0)
            merged[end_at]['production_devices'] = interval.get('devices_reporting', 0)
    
    # Process consumption
    for interval in all_data.get('consumption', {}).get('intervals', []):
        end_at = interval.get('end_at')
        if end_at:
            merged[end_at]['end_at'] = end_at
            merged[end_at]['consumption_wh'] = interval.get('enwh', 0.0)
            merged[end_at]['consumption_devices'] = interval.get('devices_reporting', 0)
    
    # Process battery
    for interval in all_data.get('battery', {}).get('intervals', []):
        end_at = interval.get('end_at')
        if end_at:
            merged[end_at]['end_at'] = end_at
            charge = interval.get('charge', {})
            discharge = interval.get('discharge', {})
            soc = interval.get('soc', {})
            merged[end_at]['battery_charge_wh'] = charge.get('enwh', 0.0)
            merged[end_at]['battery_discharge_wh'] = discharge.get('enwh', 0.0)
            merged[end_at]['battery_soc_percent'] = soc.get('percent', 0.0)
            merged[end_at]['battery_devices'] = charge.get('devices_reporting', 0)
    
    # Process import (flatten nested structure)
    import_intervals = all_data.get('import', {}).get('intervals', [])
    import_intervals = flatten_intervals(import_intervals, is_nested=True)
    for interval in import_intervals:
        if isinstance(interval, dict):
            end_at = interval.get('end_at')
            if end_at:
                merged[end_at]['end_at'] = end_at
                merged[end_at]['grid_import_wh'] = interval.get('wh_imported', 0.0)
    
    # Process export (flatten nested structure)
    export_intervals = all_data.get('export', {}).get('intervals', [])
    export_intervals = flatten_intervals(export_intervals, is_nested=True)
    for interval in export_intervals:
        if isinstance(interval, dict):
            end_at = interval.get('end_at')
            if end_at:
                merged[end_at]['end_at'] = end_at
                merged[end_at]['grid_export_wh'] = interval.get('wh_exported', 0.0)
    
    # Convert to sorted list
    return [merged[end_at] for end_at in sorted(merged.keys())]


@app.timer_trigger(schedule="0 0 */4 * * *", arg_name="myTimer", run_on_startup=False,
                   use_monitor=True)
def enphase_poller(myTimer: func.TimerRequest) -> None:
    """Timer-triggered function to poll Enphase and write to Kusto (every 4 hours)"""
    
    logger = get_logger()
    tracer = get_tracer()
    
    execution_start = time.time()
    retrieved_at = datetime.now(timezone.utc)
    invocation_id = os.environ.get("INVOCATION_ID", retrieved_at.isoformat())
    
    with tracer.span(name="enphase_poller") as span:
        span.add_attribute("invocation_id", invocation_id)
        
        if myTimer.past_due:
            logger.warning("Timer is past due!", extra={
                'custom_dimensions': {'invocation_id': invocation_id, 'past_due': True}
            })
        
        logger.info("EnphasePoller function started", extra={
            'custom_dimensions': {'invocation_id': invocation_id, 'retrieved_at': retrieved_at.isoformat()}
        })
        
        # Get configuration from environment
        api_key = os.environ.get("ENPHASE_API_KEY")
        client_id = os.environ.get("ENPHASE_CLIENT_ID")
        client_secret = os.environ.get("ENPHASE_CLIENT_SECRET")
        system_id = os.environ.get("ENPHASE_SYSTEM_ID")
        refresh_token = os.environ.get("ENPHASE_REFRESH_TOKEN")
        kusto_cluster_uri = os.environ.get("KUSTO_CLUSTER_URI")
        kusto_database = os.environ.get("KUSTO_DATABASE")
        system_timezone = os.environ.get("SYSTEM_TIMEZONE", "Pacific/Honolulu")
        
        # Validate configuration
        missing = []
        for name, value in [
            ("ENPHASE_API_KEY", api_key),
            ("ENPHASE_CLIENT_ID", client_id),
            ("ENPHASE_CLIENT_SECRET", client_secret),
            ("ENPHASE_SYSTEM_ID", system_id),
            ("ENPHASE_REFRESH_TOKEN", refresh_token),
            ("KUSTO_CLUSTER_URI", kusto_cluster_uri),
            ("KUSTO_DATABASE", kusto_database),
        ]:
            if not value:
                missing.append(name)
        
        if missing:
            logger.error(f"Missing configuration: {', '.join(missing)}", extra={
                'custom_dimensions': {'missing_config': missing, 'invocation_id': invocation_id}
            })
            return
        
        system_id_int = int(system_id)
        span.add_attribute("system_id", system_id)
        
        kusto_client = None
        
        try:
            # Initialize clients
            enphase_client = EnphaseClient(
                api_key=api_key,
                client_id=client_id,
                client_secret=client_secret,
                system_id=system_id,
                refresh_token=refresh_token,
                logger=logger,
                tracer=tracer
            )
            
            kusto_client = FabricKustoClient(
                cluster_uri=kusto_cluster_uri,
                database=kusto_database,
                system_timezone=system_timezone,
                logger=logger
            )
            
            # Get latest timestamp to filter duplicates
            with tracer.span(name="get_latest_end_at"):
                latest_end_at = kusto_client.get_latest_end_at(system_id_int)
            
            if latest_end_at:
                logger.info(f"Latest data timestamp: {datetime.utcfromtimestamp(latest_end_at)}", extra={
                    'custom_dimensions': {'latest_end_at': latest_end_at, 'invocation_id': invocation_id}
                })
            
            # Determine date range for fetch
            # If we have existing data, fetch from latest timestamp to now
            # API returns ~96 intervals (~24 hours) per request
            start_at = None
            end_at = int(datetime.now(timezone.utc).timestamp())
            if latest_end_at:
                start_at = latest_end_at
                logger.info(f"Fetching from {datetime.utcfromtimestamp(start_at)} to {datetime.utcfromtimestamp(end_at)}", extra={
                    'custom_dimensions': {'start_at': start_at, 'end_at': end_at, 'invocation_id': invocation_id}
                })
            
            # Fetch all telemetry types with date range
            with tracer.span(name="fetch_all_telemetry"):
                all_data = {
                    'production': enphase_client.get_production_data(start_at=start_at, end_at=end_at),
                    'consumption': enphase_client.get_consumption_data(start_at=start_at, end_at=end_at),
                    'battery': enphase_client.get_battery_data(start_at=start_at, end_at=end_at),
                    'import': enphase_client.get_import_data(start_at=start_at, end_at=end_at),
                    'export': enphase_client.get_export_data(start_at=start_at, end_at=end_at),
                }
            
            # Log what we got
            for ttype, data in all_data.items():
                intervals = data.get('intervals', [])
                if ttype in ['import', 'export'] and intervals and isinstance(intervals[0], list):
                    intervals = intervals[0]
                logger.info(f"Fetched {len(intervals)} {ttype} intervals", extra={
                    'custom_dimensions': {'telemetry_type': ttype, 'interval_count': len(intervals)}
                })
            
            # Merge by end_at
            with tracer.span(name="merge_intervals"):
                merged = merge_intervals(all_data)
            
            logger.info(f"Merged into {len(merged)} unified intervals", extra={
                'custom_dimensions': {'merged_count': len(merged), 'invocation_id': invocation_id}
            })
            
            # Filter to only new intervals
            if latest_end_at:
                merged = [m for m in merged if m['end_at'] > latest_end_at]
                logger.info(f"After filtering: {len(merged)} new intervals", extra={
                    'custom_dimensions': {'new_count': len(merged), 'invocation_id': invocation_id}
                })
            
            # Ingest to Kusto
            if merged:
                with tracer.span(name="ingest_to_kusto"):
                    ingested = kusto_client.ingest_unified_telemetry(
                        system_id_int, retrieved_at, merged
                    )
                
                execution_duration_ms = (time.time() - execution_start) * 1000
                logger.info(f"Successfully ingested {ingested} intervals", extra={
                    'custom_dimensions': {
                        'ingested_count': ingested,
                        'execution_duration_ms': execution_duration_ms,
                        'invocation_id': invocation_id,
                        'system_id': system_id
                    }
                })
            else:
                logger.info("No new intervals to ingest", extra={
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
        finally:
            if kusto_client:
                kusto_client.close()
        
        logger.info("EnphasePoller function completed", extra={
            'custom_dimensions': {
                'invocation_id': invocation_id,
                'execution_duration_ms': (time.time() - execution_start) * 1000
            }
        })


@app.route(route="backfill", auth_level=func.AuthLevel.FUNCTION)
def backfill(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP-triggered function to backfill historical data.
    
    Query parameters:
    - days: Number of days to backfill (default: 7, max: 30)
    - delay: Seconds between API calls to avoid rate limiting (default: 2.0)
    
    Note: Enphase API returns ~96 intervals (~24 hours) per request,
    so this function fetches day-by-day.
    """
    logger = get_logger()
    tracer = get_tracer()
    
    execution_start = time.time()
    retrieved_at = datetime.now(timezone.utc)
    
    with tracer.span(name="backfill") as span:
        # Parse parameters
        try:
            days = int(req.params.get('days', 7))
            days = min(days, 30)  # Cap at 30 days
            delay = float(req.params.get('delay', 2.0))
        except ValueError as e:
            return func.HttpResponse(f"Invalid parameter: {e}", status_code=400)
        
        span.add_attribute("days", days)
        span.add_attribute("delay", delay)
        
        logger.info(f"Backfill started for {days} days", extra={
            'custom_dimensions': {'days': days, 'delay': delay}
        })
        
        # Get configuration
        api_key = os.environ.get("ENPHASE_API_KEY")
        client_id = os.environ.get("ENPHASE_CLIENT_ID")
        client_secret = os.environ.get("ENPHASE_CLIENT_SECRET")
        system_id = os.environ.get("ENPHASE_SYSTEM_ID")
        refresh_token = os.environ.get("ENPHASE_REFRESH_TOKEN")
        kusto_cluster_uri = os.environ.get("KUSTO_CLUSTER_URI")
        kusto_database = os.environ.get("KUSTO_DATABASE")
        system_timezone = os.environ.get("SYSTEM_TIMEZONE", "Pacific/Honolulu")
        
        # Validate configuration
        missing = []
        for name, value in [
            ("ENPHASE_API_KEY", api_key),
            ("ENPHASE_CLIENT_ID", client_id),
            ("ENPHASE_CLIENT_SECRET", client_secret),
            ("ENPHASE_SYSTEM_ID", system_id),
            ("ENPHASE_REFRESH_TOKEN", refresh_token),
            ("KUSTO_CLUSTER_URI", kusto_cluster_uri),
            ("KUSTO_DATABASE", kusto_database),
        ]:
            if not value:
                missing.append(name)
        
        if missing:
            return func.HttpResponse(f"Missing configuration: {', '.join(missing)}", status_code=500)
        
        system_id_int = int(system_id)
        kusto_client = None
        total_ingested = 0
        days_processed = 0
        errors = []
        
        try:
            enphase_client = EnphaseClient(
                api_key=api_key,
                client_id=client_id,
                client_secret=client_secret,
                system_id=system_id,
                refresh_token=refresh_token,
                logger=logger,
                tracer=tracer
            )
            
            kusto_client = FabricKustoClient(
                cluster_uri=kusto_cluster_uri,
                database=kusto_database,
                system_timezone=system_timezone,
                logger=logger
            )
            
            # Process day by day (API returns ~24 hours per request)
            now = datetime.now(timezone.utc)
            
            for day_offset in range(days, 0, -1):
                day_start = now - timedelta(days=day_offset)
                day_end = day_start + timedelta(days=1)
                
                start_at = int(day_start.timestamp())
                end_at = int(day_end.timestamp())
                
                try:
                    logger.info(f"Fetching day {day_offset} days ago: {day_start.date()}", extra={
                        'custom_dimensions': {'day_offset': day_offset, 'date': str(day_start.date())}
                    })
                    
                    # Fetch all telemetry for this day
                    all_data = {
                        'production': enphase_client.get_production_data(start_at=start_at, end_at=end_at),
                        'consumption': enphase_client.get_consumption_data(start_at=start_at, end_at=end_at),
                        'battery': enphase_client.get_battery_data(start_at=start_at, end_at=end_at),
                        'import': enphase_client.get_import_data(start_at=start_at, end_at=end_at),
                        'export': enphase_client.get_export_data(start_at=start_at, end_at=end_at),
                    }
                    
                    # Merge intervals
                    merged = merge_intervals(all_data)
                    
                    if merged:
                        # Get existing timestamps for this range
                        existing = kusto_client.get_existing_end_ats(system_id_int, start_at, end_at)
                        
                        # Filter to new intervals only
                        new_intervals = [m for m in merged if m['end_at'] not in existing]
                        
                        if new_intervals:
                            ingested = kusto_client.ingest_unified_telemetry(
                                system_id_int, retrieved_at, new_intervals
                            )
                            total_ingested += ingested
                            logger.info(f"Ingested {ingested} intervals for {day_start.date()}", extra={
                                'custom_dimensions': {'date': str(day_start.date()), 'ingested': ingested}
                            })
                    
                    days_processed += 1
                    
                    # Rate limiting delay
                    if day_offset > 1:
                        time.sleep(delay)
                        
                except Exception as e:
                    error_msg = f"Error fetching {day_start.date()}: {str(e)}"
                    errors.append(error_msg)
                    logger.error(error_msg, extra={'custom_dimensions': {'date': str(day_start.date())}})
                    # Extra delay on error (rate limiting)
                    time.sleep(delay * 3)
            
            execution_duration_ms = (time.time() - execution_start) * 1000
            
            result = {
                "status": "completed",
                "days_requested": days,
                "days_processed": days_processed,
                "total_ingested": total_ingested,
                "execution_duration_ms": execution_duration_ms,
                "errors": errors
            }
            
            logger.info(f"Backfill completed: {total_ingested} intervals ingested", extra={
                'custom_dimensions': result
            })
            
            return func.HttpResponse(
                json.dumps(result, indent=2),
                mimetype="application/json",
                status_code=200
            )
            
        except Exception as e:
            logger.error(f"Backfill failed: {e}", extra={'custom_dimensions': {'error': str(e)}})
            return func.HttpResponse(f"Backfill failed: {e}", status_code=500)
        finally:
            if kusto_client:
                kusto_client.close()
