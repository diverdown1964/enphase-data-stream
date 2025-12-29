"""
Local Runner for Enphase Data Collection with Kusto Deduplication
Queries Kusto for the latest timestamp and only fetches new data from Enphase API

Supports two modes:
- Legacy: 5 separate bronze tables (--legacy flag)
- Unified: Single SolarTelemetry table with merged rows (default)
"""
import os
import sys
import logging
import argparse
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from collections import defaultdict
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(__file__))

from enphase_client import EnphaseClient
from kusto_client import FabricKustoClient

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IncrementalEnphaseFetcher:
    """
    Fetches only new Enphase telemetry data by querying Kusto for the latest timestamp
    and using that as the start_at parameter for API calls.
    """
    
    def __init__(self):
        """Initialize the fetcher with Enphase and Kusto clients"""
        # Load environment variables
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(env_path)
        
        # Token file is in project root
        token_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.tokens.json')
        
        # Initialize Enphase client
        self.enphase_client = EnphaseClient(
            api_key=os.getenv("ENPHASE_API_KEY", ""),
            client_id=os.getenv("ENPHASE_CLIENT_ID"),
            client_secret=os.getenv("ENPHASE_CLIENT_SECRET"),
            system_id=os.getenv("ENPHASE_SYSTEM_ID"),
            auth_code=os.getenv("ENPHASE_AUTH_CODE"),
            token_file=token_file
        )
        
        self.system_id = int(os.getenv("ENPHASE_SYSTEM_ID"))
        
        # Initialize Kusto client
        cluster_uri = os.getenv("KUSTO_CLUSTER_URI", "https://trd-1z0ryh7rvnmjd3dvsz.z7.kusto.fabric.microsoft.com")
        database = os.getenv("KUSTO_DATABASE", "HaleJoli")
        
        self.kusto_client = FabricKustoClient(cluster_uri, database)
        
        logger.info(f"Initialized IncrementalEnphaseFetcher for system {self.system_id}")
        logger.info(f"Kusto database: {database}")
    
    def _filter_new_intervals(self, intervals: List[Dict], latest_end_at: Optional[int]) -> List[Dict]:
        """
        Filter intervals to only include those after the latest known timestamp
        
        Args:
            intervals: List of interval dictionaries from Enphase API
            latest_end_at: The latest end_at value from Kusto (or None if no data)
            
        Returns:
            Filtered list of new intervals only
        """
        if latest_end_at is None:
            # No existing data, return all intervals
            return intervals
        
        new_intervals = [
            interval for interval in intervals
            if interval.get('end_at', 0) > latest_end_at
        ]
        
        return new_intervals
    
    def fetch_and_ingest_production(self) -> int:
        """Fetch and ingest only new production data"""
        telemetry_type = 'production'
        
        # Get latest timestamp from Kusto
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        
        # Fetch data from Enphase (API returns last 7 days by default)
        data = self.enphase_client.get_production_data()
        intervals = data.get('intervals', [])
        
        logger.info(f"Fetched {len(intervals)} {telemetry_type} intervals from Enphase API")
        
        # Filter to only new intervals
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.info(f"No new {telemetry_type} intervals to ingest")
            return 0
        
        logger.info(f"Found {len(new_intervals)} new {telemetry_type} intervals")
        
        # Ingest to Kusto
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        ingested = self.kusto_client.ingest_production(self.system_id, retrieved_at, new_intervals)
        
        return ingested
    
    def fetch_and_ingest_consumption(self) -> int:
        """Fetch and ingest only new consumption data"""
        telemetry_type = 'consumption'
        
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        data = self.enphase_client.get_consumption_data()
        intervals = data.get('intervals', [])
        
        logger.info(f"Fetched {len(intervals)} {telemetry_type} intervals from Enphase API")
        
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.info(f"No new {telemetry_type} intervals to ingest")
            return 0
        
        logger.info(f"Found {len(new_intervals)} new {telemetry_type} intervals")
        
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        return self.kusto_client.ingest_consumption(self.system_id, retrieved_at, new_intervals)
    
    def fetch_and_ingest_battery(self) -> int:
        """Fetch and ingest only new battery data"""
        telemetry_type = 'battery'
        
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        data = self.enphase_client.get_battery_data()
        intervals = data.get('intervals', [])
        
        logger.info(f"Fetched {len(intervals)} {telemetry_type} intervals from Enphase API")
        
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.info(f"No new {telemetry_type} intervals to ingest")
            return 0
        
        logger.info(f"Found {len(new_intervals)} new {telemetry_type} intervals")
        
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        return self.kusto_client.ingest_battery(self.system_id, retrieved_at, new_intervals)
    
    def fetch_and_ingest_import(self) -> int:
        """Fetch and ingest only new grid import data"""
        telemetry_type = 'import'
        
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        data = self.enphase_client.get_import_data()
        raw_intervals = data.get('intervals', [])
        
        # Import/export API returns nested structure: [[{...}, {...}]] instead of [{...}, {...}]
        # Flatten if needed
        if raw_intervals and isinstance(raw_intervals[0], list):
            intervals = raw_intervals[0]
        else:
            intervals = raw_intervals
        
        logger.info(f"Fetched {len(intervals)} {telemetry_type} intervals from Enphase API")
        
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.info(f"No new {telemetry_type} intervals to ingest")
            return 0
        
        logger.info(f"Found {len(new_intervals)} new {telemetry_type} intervals")
        
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        return self.kusto_client.ingest_import(self.system_id, retrieved_at, new_intervals)
    
    def fetch_and_ingest_export(self) -> int:
        """Fetch and ingest only new grid export data"""
        telemetry_type = 'export'
        
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        data = self.enphase_client.get_export_data()
        raw_intervals = data.get('intervals', [])
        
        # Import/export API returns nested structure: [[{...}, {...}]] instead of [{...}, {...}]
        # Flatten if needed
        if raw_intervals and isinstance(raw_intervals[0], list):
            intervals = raw_intervals[0]
        else:
            intervals = raw_intervals
        
        logger.info(f"Fetched {len(intervals)} {telemetry_type} intervals from Enphase API")
        
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.info(f"No new {telemetry_type} intervals to ingest")
            return 0
        
        logger.info(f"Found {len(new_intervals)} new {telemetry_type} intervals")
        
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        return self.kusto_client.ingest_export(self.system_id, retrieved_at, new_intervals)
    
    # ==================== Unified Table Methods ====================
    
    def _flatten_intervals(self, raw_intervals: List, is_nested: bool = False) -> List[Dict]:
        """Flatten nested interval structure if needed"""
        if is_nested and raw_intervals and isinstance(raw_intervals[0], list):
            return raw_intervals[0]
        return raw_intervals
    
    def _fetch_all_telemetry(self, start_at: Optional[int] = None, 
                              end_at: Optional[int] = None) -> Dict[str, Dict]:
        """
        Fetch all 5 telemetry types from Enphase API
        
        Args:
            start_at: Optional start timestamp (for backfill)
            end_at: Optional end timestamp (for backfill)
            
        Returns:
            Dictionary mapping telemetry type to API response data
        """
        all_data = {}
        
        # Fetch production
        all_data['production'] = self.enphase_client.get_production_data(
            start_at=start_at, end_at=end_at
        )
        
        # Fetch consumption
        all_data['consumption'] = self.enphase_client.get_consumption_data(
            start_at=start_at, end_at=end_at
        )
        
        # Fetch battery
        all_data['battery'] = self.enphase_client.get_battery_data(
            start_at=start_at, end_at=end_at
        )
        
        # Fetch import
        all_data['import'] = self.enphase_client.get_import_data(
            start_at=start_at, end_at=end_at
        )
        
        # Fetch export
        all_data['export'] = self.enphase_client.get_export_data(
            start_at=start_at, end_at=end_at
        )
        
        return all_data
    
    def _merge_intervals(self, all_data: Dict[str, Dict]) -> List[Dict]:
        """
        Merge all telemetry types into unified interval records by end_at timestamp
        
        Args:
            all_data: Dictionary mapping telemetry type to API response data
            
        Returns:
            List of merged interval dictionaries with all measures
        """
        # Build a dictionary keyed by end_at timestamp
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
        import_intervals = self._flatten_intervals(import_intervals, is_nested=True)
        for interval in import_intervals:
            if isinstance(interval, dict):
                end_at = interval.get('end_at')
                if end_at:
                    merged[end_at]['end_at'] = end_at
                    merged[end_at]['grid_import_wh'] = interval.get('wh_imported', 0.0)
        
        # Process export (flatten nested structure)
        export_intervals = all_data.get('export', {}).get('intervals', [])
        export_intervals = self._flatten_intervals(export_intervals, is_nested=True)
        for interval in export_intervals:
            if isinstance(interval, dict):
                end_at = interval.get('end_at')
                if end_at:
                    merged[end_at]['end_at'] = end_at
                    merged[end_at]['grid_export_wh'] = interval.get('wh_exported', 0.0)
        
        # Convert to sorted list
        result = [merged[end_at] for end_at in sorted(merged.keys())]
        
        return result
    
    def run_unified(self) -> int:
        """
        Fetch all telemetry, merge by timestamp, and ingest to unified SolarTelemetry table
        
        Returns:
            Number of intervals ingested
        """
        logger.info("=" * 60)
        logger.info("Starting unified Enphase data fetch")
        logger.info("=" * 60)
        
        # Get latest timestamp from unified table
        latest_end_at = self.kusto_client.get_latest_telemetry_end_at(self.system_id)
        
        if latest_end_at:
            logger.info(f"Latest data: {datetime.utcfromtimestamp(latest_end_at)}")
        else:
            logger.info("No existing data - will fetch all available")
        
        # Fetch all telemetry types
        logger.info("Fetching all 5 telemetry types from Enphase API...")
        all_data = self._fetch_all_telemetry()
        
        # Log what we got
        for ttype, data in all_data.items():
            intervals = data.get('intervals', [])
            if ttype in ['import', 'export'] and intervals and isinstance(intervals[0], list):
                intervals = intervals[0]
            logger.info(f"  {ttype}: {len(intervals)} intervals")
        
        # Merge by end_at
        merged = self._merge_intervals(all_data)
        logger.info(f"Merged into {len(merged)} unified intervals")
        
        # Filter to only new intervals
        if latest_end_at:
            merged = [m for m in merged if m['end_at'] > latest_end_at]
            logger.info(f"After filtering: {len(merged)} new intervals")
        
        if not merged:
            logger.info("No new intervals to ingest")
            return 0
        
        # Get retrieved_at from any of the responses
        retrieved_at = datetime.fromisoformat(
            all_data.get('production', {}).get('retrieved_at', datetime.now().isoformat())
        )
        
        # Ingest
        ingested = self.kusto_client.ingest_unified_telemetry(
            self.system_id, retrieved_at, merged
        )
        
        logger.info("=" * 60)
        logger.info(f"Successfully ingested {ingested} unified intervals")
        logger.info("=" * 60)
        
        return ingested
    
    def backfill_unified(self, weeks: int = 4) -> int:
        """
        Backfill historical data to unified SolarTelemetry table
        
        Args:
            weeks: Number of weeks to backfill (default: 4)
            
        Returns:
            Total number of intervals ingested
        """
        logger.info("=" * 60)
        logger.info(f"Starting unified backfill for {weeks} weeks of historical data")
        logger.info("=" * 60)
        
        # First, get all existing end_at values to avoid duplicates
        existing_end_ats = self._get_existing_end_ats()
        logger.info(f"Found {len(existing_end_ats)} existing intervals to skip")
        
        total_ingested = 0
        now = datetime.now()
        
        for week_num in range(weeks, 0, -1):
            # Calculate week boundaries (going backwards)
            week_end = now - timedelta(weeks=week_num - 1)
            week_start = week_end - timedelta(weeks=1)
            
            start_at = int(week_start.timestamp())
            end_at = int(week_end.timestamp())
            
            logger.info(f"\nFetching week {weeks - week_num + 1}/{weeks}: {week_start.date()} to {week_end.date()}")
            
            try:
                # Fetch all telemetry types for this week
                all_data = self._fetch_all_telemetry(start_at=start_at, end_at=end_at)
                
                # Merge by end_at
                merged = self._merge_intervals(all_data)
                logger.info(f"  Merged {len(merged)} intervals")
                
                if not merged:
                    continue
                
                # Filter out any intervals we already have (by end_at)
                new_intervals = [m for m in merged if m['end_at'] not in existing_end_ats]
                logger.info(f"  After filtering duplicates: {len(new_intervals)} new intervals")
                
                if not new_intervals:
                    continue
                
                # Get retrieved_at
                retrieved_at = datetime.fromisoformat(
                    all_data.get('production', {}).get('retrieved_at', datetime.now().isoformat())
                )
                
                # Ingest
                ingested = self.kusto_client.ingest_unified_telemetry(
                    self.system_id, retrieved_at, new_intervals
                )
                total_ingested += ingested
                
                # Add these to our existing set so we don't re-ingest them
                existing_end_ats.update(m['end_at'] for m in new_intervals)
                
            except Exception as e:
                logger.error(f"Failed to backfill week: {e}")
        
        logger.info("\n" + "=" * 60)
        logger.info(f"Backfill complete: {total_ingested} total intervals")
        logger.info("=" * 60)
        
        return total_ingested
    
    def _get_existing_end_ats(self) -> set:
        """Get all existing end_at values from SolarTelemetry table"""
        query = f"""
        SolarTelemetry
        | where system_id == {self.system_id}
        | project end_at
        """
        try:
            client = self.kusto_client._get_query_client()
            response = client.execute(self.kusto_client.database, query)
            
            existing = set()
            for row in response.primary_results[0]:
                if row[0] is not None:
                    existing.add(int(row[0]))
            
            return existing
        except Exception as e:
            logger.warning(f"Failed to get existing end_ats: {e}")
            return set()
    
    # ==================== Legacy Multi-Table Methods ====================
    
    def run_all(self) -> Dict[str, int]:
        """
        Fetch and ingest all telemetry types
        
        Returns:
            Dictionary mapping telemetry type to number of intervals ingested
        """
        results = {}
        
        logger.info("=" * 60)
        logger.info("Starting incremental Enphase data fetch")
        logger.info("=" * 60)
        
        # Production
        try:
            results['production'] = self.fetch_and_ingest_production()
        except Exception as e:
            logger.error(f"Failed to fetch/ingest production: {e}")
            results['production'] = 0
        
        # Consumption
        try:
            results['consumption'] = self.fetch_and_ingest_consumption()
        except Exception as e:
            logger.error(f"Failed to fetch/ingest consumption: {e}")
            results['consumption'] = 0
        
        # Battery
        try:
            results['battery'] = self.fetch_and_ingest_battery()
        except Exception as e:
            logger.error(f"Failed to fetch/ingest battery: {e}")
            results['battery'] = 0
        
        # Import
        try:
            results['import'] = self.fetch_and_ingest_import()
        except Exception as e:
            logger.error(f"Failed to fetch/ingest import: {e}")
            results['import'] = 0
        
        # Export
        try:
            results['export'] = self.fetch_and_ingest_export()
        except Exception as e:
            logger.error(f"Failed to fetch/ingest export: {e}")
            results['export'] = 0
        
        # Summary
        logger.info("=" * 60)
        logger.info("Ingestion Summary:")
        total = 0
        for telemetry_type, count in results.items():
            logger.info(f"  {telemetry_type}: {count} intervals")
            total += count
        logger.info(f"  TOTAL: {total} intervals")
        logger.info("=" * 60)
        
        return results
    
    def backfill(self, weeks: int = 4) -> Dict[str, int]:
        """
        Backfill historical data by fetching in weekly chunks.
        The Enphase API allows up to 1 week of data per request.
        
        Args:
            weeks: Number of weeks to backfill (default: 4)
            
        Returns:
            Dictionary mapping telemetry type to total intervals ingested
        """
        results = {'production': 0, 'consumption': 0, 'battery': 0, 'import': 0, 'export': 0}
        
        logger.info("=" * 60)
        logger.info(f"Starting backfill for {weeks} weeks of historical data")
        logger.info("=" * 60)
        
        now = datetime.now()
        
        for week_num in range(weeks, 0, -1):
            # Calculate week boundaries (going backwards)
            week_end = now - timedelta(weeks=week_num - 1)
            week_start = week_end - timedelta(weeks=1)
            
            start_at = int(week_start.timestamp())
            end_at = int(week_end.timestamp())
            
            logger.info(f"\nFetching week {weeks - week_num + 1}/{weeks}: {week_start.date()} to {week_end.date()}")
            
            # Fetch each telemetry type for this week
            for telemetry_type in ['production', 'consumption', 'battery', 'import', 'export']:
                try:
                    count = self._backfill_telemetry_type(telemetry_type, start_at, end_at)
                    results[telemetry_type] += count
                except Exception as e:
                    logger.error(f"Failed to backfill {telemetry_type} for week: {e}")
        
        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("Backfill Summary:")
        total = 0
        for telemetry_type, count in results.items():
            logger.info(f"  {telemetry_type}: {count} intervals")
            total += count
        logger.info(f"  TOTAL: {total} intervals")
        logger.info("=" * 60)
        
        return results
    
    def _backfill_telemetry_type(self, telemetry_type: str, start_at: int, end_at: int) -> int:
        """
        Backfill a single telemetry type for a time range
        """
        # Get the latest timestamp for this type to avoid duplicates
        latest_end_at = self.kusto_client.get_latest_end_at(telemetry_type, self.system_id)
        
        # Map telemetry types to fetch methods
        fetch_methods = {
            'production': self.enphase_client.get_production_data,
            'consumption': self.enphase_client.get_consumption_data,
            'battery': self.enphase_client.get_battery_data,
            'import': self.enphase_client.get_import_data,
            'export': self.enphase_client.get_export_data,
        }
        
        ingest_methods = {
            'production': self.kusto_client.ingest_production,
            'consumption': self.kusto_client.ingest_consumption,
            'battery': self.kusto_client.ingest_battery,
            'import': self.kusto_client.ingest_import,
            'export': self.kusto_client.ingest_export,
        }
        
        # Fetch data for the time range
        data = fetch_methods[telemetry_type](start_at=start_at, end_at=end_at)
        raw_intervals = data.get('intervals', [])
        
        # Handle nested structure for import/export
        if telemetry_type in ['import', 'export'] and raw_intervals and isinstance(raw_intervals[0], list):
            intervals = raw_intervals[0]
        else:
            intervals = raw_intervals
        
        # Filter out any intervals we already have
        new_intervals = self._filter_new_intervals(intervals, latest_end_at)
        
        if not new_intervals:
            logger.debug(f"  {telemetry_type}: No new intervals in this range")
            return 0
        
        # Ingest
        retrieved_at = datetime.fromisoformat(data.get('retrieved_at', datetime.now().isoformat()))
        count = ingest_methods[telemetry_type](self.system_id, retrieved_at, new_intervals)
        
        logger.info(f"  {telemetry_type}: {count} intervals")
        return count

    def close(self):
        """Clean up resources"""
        self.kusto_client.close()


def main():
    """Entry point for local testing"""
    parser = argparse.ArgumentParser(description='Enphase Data Collector with Kusto Integration')
    parser.add_argument('--backfill', type=int, metavar='WEEKS',
                        help='Backfill historical data for specified number of weeks')
    parser.add_argument('--auto-backfill', action='store_true',
                        help='Automatically backfill if no data exists (used on first run)')
    parser.add_argument('--legacy', action='store_true',
                        help='Use legacy 5-table mode instead of unified SolarTelemetry table')
    args = parser.parse_args()
    
    fetcher = IncrementalEnphaseFetcher()
    
    try:
        if args.legacy:
            # Legacy 5-table mode
            logger.info("Running in LEGACY mode (5 separate tables)")
            if args.backfill:
                logger.info(f"Running legacy backfill for {args.backfill} weeks")
                results = fetcher.backfill(weeks=args.backfill)
                total = sum(results.values())
            elif args.auto_backfill:
                latest = fetcher.kusto_client.get_latest_end_at('production', fetcher.system_id)
                if latest is None:
                    logger.info("No existing data found - running legacy backfill (4 weeks)")
                    results = fetcher.backfill(weeks=4)
                else:
                    logger.info("Data already exists - running legacy incremental fetch")
                    results = fetcher.run_all()
                total = sum(results.values())
            else:
                results = fetcher.run_all()
                total = sum(results.values())
        else:
            # Unified SolarTelemetry mode (default)
            logger.info("Running in UNIFIED mode (SolarTelemetry table)")
            if args.backfill:
                logger.info(f"Running unified backfill for {args.backfill} weeks")
                total = fetcher.backfill_unified(weeks=args.backfill)
            elif args.auto_backfill:
                latest = fetcher.kusto_client.get_latest_telemetry_end_at(fetcher.system_id)
                if latest is None:
                    logger.info("No existing data found - running unified backfill (4 weeks)")
                    total = fetcher.backfill_unified(weeks=4)
                else:
                    logger.info("Data already exists - running unified incremental fetch")
                    total = fetcher.run_unified()
            else:
                total = fetcher.run_unified()
        
        logger.info(f"Successfully processed {total} total intervals")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
    finally:
        fetcher.close()


if __name__ == "__main__":
    main()
