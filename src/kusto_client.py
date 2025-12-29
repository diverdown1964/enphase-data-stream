"""
Kusto Client for Fabric Eventhouse
Provides read and write operations for solar telemetry data
"""
import os
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List
from zoneinfo import ZoneInfo
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder
from azure.kusto.data.exceptions import KustoServiceError
from azure.identity import InteractiveBrowserCredential

logger = logging.getLogger(__name__)


class FabricKustoClient:
    """Client for reading from and writing to Fabric Eventhouse (Kusto)"""
    
    # Table name mapping for telemetry types
    TABLE_MAP = {
        'production': 'SolarProduction',
        'consumption': 'SolarConsumption',
        'battery': 'SolarBattery',
        'import': 'SolarImport',
        'export': 'SolarExport'
    }
    
    # Default timezone for systems (can be overridden per system)
    DEFAULT_TIMEZONE = 'Pacific/Honolulu'
    
    def __init__(self, cluster_uri: str, database: str, system_timezone: str = None):
        """
        Initialize Kusto client for Fabric Eventhouse
        
        Args:
            cluster_uri: The Kusto cluster URI (e.g., https://xxx.z7.kusto.fabric.microsoft.com)
            database: The database name
            system_timezone: IANA timezone string (e.g., 'Pacific/Honolulu')
        """
        self.cluster_uri = cluster_uri
        self.database = database
        self.system_timezone = system_timezone or self.DEFAULT_TIMEZONE
        self._tz = ZoneInfo(self.system_timezone)
        self._query_client: Optional[KustoClient] = None
    
    def _utc_to_local(self, utc_dt: datetime) -> datetime:
        """Convert UTC datetime to local time"""
        if utc_dt is None:
            return None
        # Make it timezone-aware (UTC), then convert to local
        utc_aware = utc_dt.replace(tzinfo=timezone.utc)
        local_dt = utc_aware.astimezone(self._tz)
        # Return as naive datetime (for Kusto compatibility)
        return local_dt.replace(tzinfo=None)
        
    def _get_query_client(self) -> KustoClient:
        """Get or create the Kusto query client with interactive auth"""
        if self._query_client is None:
            # Use interactive browser auth for local development
            logger.info("Authenticating with interactive browser...")
            credential = InteractiveBrowserCredential()
            kcsb = KustoConnectionStringBuilder.with_azure_token_credential(
                self.cluster_uri, credential
            )
            self._query_client = KustoClient(kcsb)
            logger.info(f"Connected to Kusto cluster: {self.cluster_uri}")
        return self._query_client
    
    def get_latest_end_at(self, telemetry_type: str, system_id: int) -> Optional[int]:
        """
        Get the most recent end_at timestamp for a specific telemetry type
        
        Args:
            telemetry_type: Type of telemetry (production, consumption, battery, import, export)
            system_id: The Enphase system ID
            
        Returns:
            The latest end_at value (Unix timestamp in seconds), or None if no data exists
        """
        table_name = self.TABLE_MAP.get(telemetry_type)
        if not table_name:
            raise ValueError(f"Unknown telemetry type: {telemetry_type}")
        
        query = f"""
        {table_name}
        | where system_id == {system_id}
        | summarize max(end_at)
        """
        
        try:
            client = self._get_query_client()
            response = client.execute(self.database, query)
            
            # Get the result
            for row in response.primary_results[0]:
                max_end_at = row[0]
                if max_end_at is not None:
                    logger.info(f"Latest {telemetry_type} end_at: {max_end_at} ({datetime.fromtimestamp(max_end_at)})")
                    return int(max_end_at)
            
            logger.info(f"No existing {telemetry_type} data found for system {system_id}")
            return None
            
        except KustoServiceError as e:
            logger.error(f"Kusto query error: {e}")
            raise
    
    def get_all_latest_end_at(self, system_id: int) -> Dict[str, Optional[int]]:
        """
        Get the most recent end_at for all telemetry types in one call
        
        Args:
            system_id: The Enphase system ID
            
        Returns:
            Dictionary mapping telemetry type to latest end_at (or None)
        """
        result = {}
        
        for telemetry_type in self.TABLE_MAP.keys():
            try:
                result[telemetry_type] = self.get_latest_end_at(telemetry_type, system_id)
            except Exception as e:
                logger.warning(f"Failed to get latest {telemetry_type}: {e}")
                result[telemetry_type] = None
        
        return result
    
    def ingest_production(self, system_id: int, retrieved_at: datetime, intervals: List[Dict]) -> int:
        """
        Ingest production telemetry data
        
        Args:
            system_id: The Enphase system ID
            retrieved_at: When the data was retrieved
            intervals: List of interval dictionaries from Enphase API
            
        Returns:
            Number of rows ingested
        """
        client = self._get_query_client()
        ingested = 0
        
        for interval in intervals:
            # Convert interval to row format
            end_at = interval.get('end_at')
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            devices_reporting = interval.get('devices_reporting', 0)
            wh_del = interval.get('wh_del', 0.0)
            
            # Use inline ingestion via .ingest inline command
            command = f""".ingest inline into table SolarProduction <|
{system_id},{retrieved_at.isoformat()},{end_at},{reading_time.isoformat() if reading_time else ''},{devices_reporting},{wh_del}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                logger.error(f"Failed to ingest production row: {e}")
        
        logger.info(f"Ingested {ingested} production rows")
        return ingested
    
    def ingest_consumption(self, system_id: int, retrieved_at: datetime, intervals: List[Dict]) -> int:
        """Ingest consumption telemetry data"""
        client = self._get_query_client()
        ingested = 0
        
        for interval in intervals:
            end_at = interval.get('end_at')
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            devices_reporting = interval.get('devices_reporting', 0)
            enwh = interval.get('enwh', 0.0)
            
            command = f""".ingest inline into table SolarConsumption <|
{system_id},{retrieved_at.isoformat()},{end_at},{reading_time.isoformat() if reading_time else ''},{devices_reporting},{enwh}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                logger.error(f"Failed to ingest consumption row: {e}")
        
        logger.info(f"Ingested {ingested} consumption rows")
        return ingested
    
    def ingest_battery(self, system_id: int, retrieved_at: datetime, intervals: List[Dict]) -> int:
        """Ingest battery telemetry data"""
        client = self._get_query_client()
        ingested = 0
        
        for interval in intervals:
            end_at = interval.get('end_at')
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            
            # Battery has charge, discharge, and state-of-charge data
            charge = interval.get('charge', {})
            discharge = interval.get('discharge', {})
            soc = interval.get('soc', {})
            
            charge_enwh = charge.get('enwh', 0.0)
            charge_devices = charge.get('devices_reporting', 0)
            discharge_enwh = discharge.get('enwh', 0.0)
            discharge_devices = discharge.get('devices_reporting', 0)
            soc_percent = soc.get('percent', 0.0)
            soc_devices = soc.get('devices_reporting', 0)
            
            command = f""".ingest inline into table SolarBattery <|
{system_id},{retrieved_at.isoformat()},{end_at},{reading_time.isoformat() if reading_time else ''},{charge_enwh},{charge_devices},{discharge_enwh},{discharge_devices},{soc_percent},{soc_devices}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                logger.error(f"Failed to ingest battery row: {e}")
        
        logger.info(f"Ingested {ingested} battery rows")
        return ingested
    
    def ingest_import(self, system_id: int, retrieved_at: datetime, intervals: List) -> int:
        """Ingest grid import telemetry data"""
        client = self._get_query_client()
        ingested = 0
        
        for interval in intervals:
            # Import data is a dict with 'end_at' and 'wh_imported'
            if isinstance(interval, dict):
                end_at = interval.get('end_at')
                wh_imported = interval.get('wh_imported', 0.0)
            else:
                logger.warning(f"Unknown import interval format: {interval}")
                continue
                
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            
            command = f""".ingest inline into table SolarImport <|
{system_id},{retrieved_at.isoformat()},{end_at},{reading_time.isoformat() if reading_time else ''},{wh_imported}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                logger.error(f"Failed to ingest import row: {e}")
        
        logger.info(f"Ingested {ingested} import rows")
        return ingested
    
    def ingest_export(self, system_id: int, retrieved_at: datetime, intervals: List) -> int:
        """Ingest grid export telemetry data"""
        client = self._get_query_client()
        ingested = 0
        
        for interval in intervals:
            # Export data is a dict with 'end_at' and 'wh_exported'
            if isinstance(interval, dict):
                end_at = interval.get('end_at')
                wh_exported = interval.get('wh_exported', 0.0)
            else:
                logger.warning(f"Unknown export interval format: {interval}")
                continue
                
            reading_time = datetime.utcfromtimestamp(end_at) if end_at else None
            
            command = f""".ingest inline into table SolarExport <|
{system_id},{retrieved_at.isoformat()},{end_at},{reading_time.isoformat() if reading_time else ''},{wh_exported}"""
            
            try:
                client.execute(self.database, command)
                ingested += 1
            except KustoServiceError as e:
                logger.error(f"Failed to ingest export row: {e}")
        
        logger.info(f"Ingested {ingested} export rows")
        return ingested
    
    def ingest_telemetry(self, telemetry_type: str, system_id: int, 
                         retrieved_at: datetime, intervals: List[Dict]) -> int:
        """
        Generic telemetry ingestion dispatcher
        
        Args:
            telemetry_type: Type of telemetry
            system_id: Enphase system ID
            retrieved_at: When data was retrieved
            intervals: List of interval dictionaries
            
        Returns:
            Number of rows ingested
        """
        ingest_methods = {
            'production': self.ingest_production,
            'consumption': self.ingest_consumption,
            'battery': self.ingest_battery,
            'import': self.ingest_import,
            'export': self.ingest_export
        }
        
        method = ingest_methods.get(telemetry_type)
        if not method:
            raise ValueError(f"Unknown telemetry type: {telemetry_type}")
        
        return method(system_id, retrieved_at, intervals)
    
    def close(self):
        """Close the client connections"""
        if self._query_client:
            self._query_client.close()
            self._query_client = None
        logger.info("Kusto client closed")
    
    # ==================== Unified SolarTelemetry Methods ====================
    
    def get_latest_telemetry_end_at(self, system_id: int) -> Optional[int]:
        """
        Get the most recent end_at timestamp from the unified SolarTelemetry table
        
        Args:
            system_id: The Enphase system ID
            
        Returns:
            The latest end_at value (Unix timestamp in seconds), or None if no data exists
        """
        query = f"""
        SolarTelemetry
        | where system_id == {system_id}
        | summarize max(end_at)
        """
        
        try:
            client = self._get_query_client()
            response = client.execute(self.database, query)
            
            for row in response.primary_results[0]:
                max_end_at = row[0]
                if max_end_at is not None:
                    logger.info(f"Latest telemetry end_at: {max_end_at} ({datetime.utcfromtimestamp(max_end_at)})")
                    return int(max_end_at)
            
            logger.info(f"No existing telemetry data found for system {system_id}")
            return None
            
        except KustoServiceError as e:
            logger.error(f"Kusto query error: {e}")
            raise
    
    def ingest_unified_telemetry(self, system_id: int, retrieved_at: datetime, 
                                  merged_intervals: List[Dict]) -> int:
        """
        Ingest merged telemetry data into the unified SolarTelemetry table
        
        Args:
            system_id: The Enphase system ID
            retrieved_at: When the data was retrieved
            merged_intervals: List of merged interval dictionaries containing all measures
            
        Returns:
            Number of rows ingested
        """
        client = self._get_query_client()
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
                logger.error(f"Failed to ingest telemetry row: {e}")
        
        logger.info(f"Ingested {ingested} unified telemetry rows")
        return ingested
