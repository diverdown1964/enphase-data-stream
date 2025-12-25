"""
Enphase to Fabric Eventstream - Main Application
Polls Enphase API and streams data to Microsoft Fabric Eventstream
"""
import os
import sys
import time
import logging
import signal
from datetime import datetime
from typing import Dict
from dotenv import load_dotenv
from pythonjsonlogger import jsonlogger

from enphase_client import EnphaseClient
from eventstream_sender import EventstreamSender


# Setup logging
def setup_logging():
    """Configure logging with JSON formatter"""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(name)s %(levelname)s %(message)s',
        timestamp=True
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


# Global flag for graceful shutdown
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Shutdown signal received. Stopping...")
    running = False


class EnphaseDataStreamer:
    """Main application class for streaming Enphase data"""
    
    def __init__(self):
        """Initialize the data streamer"""
        # Load environment variables from parent directory
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
        load_dotenv(env_path)
        
        # Initialize Enphase client
        self.enphase_client = EnphaseClient(
            api_key=os.getenv("ENPHASE_API_KEY", ""),
            client_id=os.getenv("ENPHASE_CLIENT_ID"),
            client_secret=os.getenv("ENPHASE_CLIENT_SECRET"),
            system_id=os.getenv("ENPHASE_SYSTEM_ID"),
            auth_code=os.getenv("ENPHASE_AUTH_CODE")
        )
        
        # Initialize Eventstream sender
        self.eventstream_sender = EventstreamSender(
            connection_string=os.getenv("EVENTHUB_CONNECTION_STRING"),
            eventhub_name=os.getenv("EVENTHUB_NAME")
        )
        
        # Polling interval (in seconds)
        self.poll_interval = int(os.getenv("POLL_INTERVAL", 14400))
        
        logger.info("Enphase Data Streamer initialized")
        logger.info(f"Polling interval: {self.poll_interval} seconds ({self.poll_interval/3600:.1f} hours)")
    
    def _send_intervals(self, telemetry_data: Dict, telemetry_type: str) -> int:
        """
        Send all intervals for a specific telemetry type
        Deduplication is handled downstream in Fabric/KQL
        
        Args:
            telemetry_data: Full telemetry response from API
            telemetry_type: Type of telemetry (production, consumption, battery, import, export)
            
        Returns:
            Number of intervals sent
        """
        all_intervals = telemetry_data.get('intervals', [])
        
        if not all_intervals:
            logger.info(f"No {telemetry_type} intervals to send")
            return 0
        
        # Log summary
        logger.info(f"Sending {len(all_intervals)} {telemetry_type} intervals")
        
        # Send each interval individually to Eventstream
        sent_count = 0
        for interval in all_intervals:
            # Create event with interval data plus metadata
            event = {
                'system_id': telemetry_data.get('system_id'),
                'telemetry_type': telemetry_type,
                'granularity': telemetry_data.get('granularity'),
                'interval': interval,
                'retrieved_at': telemetry_data.get('retrieved_at')
            }
            
            success = self.eventstream_sender.send_event(event)
            
            if success:
                sent_count += 1
            else:
                logger.error(f"Failed to send {telemetry_type} interval at {interval.get('end_at')}")
        
        logger.info(f"Successfully sent {sent_count}/{len(all_intervals)} {telemetry_type} intervals")
        
        return sent_count
    
    def poll_and_send(self):
        """Poll Enphase API for all telemetry types and send only new intervals to Eventstream"""
        try:
            logger.info("Polling Enphase API for all telemetry types...")
            
            total_sent = 0
            
            # 1. Production telemetry
            try:
                production_data = self.enphase_client.get_production_data()
                sent = self._send_intervals(production_data, 'production')
                total_sent += sent
            except Exception as e:
                logger.error(f"Error getting production data: {e}")
            
            # 2. Consumption telemetry
            try:
                consumption_data = self.enphase_client.get_consumption_data()
                sent = self._send_intervals(consumption_data, 'consumption')
                total_sent += sent
            except Exception as e:
                logger.error(f"Error getting consumption data: {e}")
            
            # 3. Battery telemetry
            try:
                battery_data = self.enphase_client.get_battery_data()
                sent = self._send_intervals(battery_data, 'battery')
                total_sent += sent
            except Exception as e:
                logger.error(f"Error getting battery data: {e}")
            
            # 4. Grid import telemetry
            try:
                import_data = self.enphase_client.get_import_data()
                sent = self._send_intervals(import_data, 'import')
                total_sent += sent
            except Exception as e:
                logger.error(f"Error getting import data: {e}")
            
            # 5. Grid export telemetry
            try:
                export_data = self.enphase_client.get_export_data()
                sent = self._send_intervals(export_data, 'export')
                total_sent += sent
            except Exception as e:
                logger.error(f"Error getting export data: {e}")
            
            logger.info(f"Poll complete: sent {total_sent} total events to Eventstream")
                
        except Exception as e:
            logger.error(f"Error during poll and send: {e}", exc_info=True)
    
    def run(self):
        """Main run loop"""
        global running
        
        logger.info("Starting Enphase Data Streamer")
        
        # Connect to Eventstream
        self.eventstream_sender.connect()
        
        # Do initial poll immediately
        self.poll_and_send()
        
        # Main polling loop
        last_poll_time = time.time()
        
        while running:
            try:
                current_time = time.time()
                
                # Check if it's time to poll again
                if current_time - last_poll_time >= self.poll_interval:
                    self.poll_and_send()
                    last_poll_time = current_time
                
                # Sleep for a short interval to avoid busy waiting
                time.sleep(1)
                
            except KeyboardInterrupt:
                logger.info("Keyboard interrupt received")
                break
            except Exception as e:
                logger.error(f"Unexpected error in main loop: {e}", exc_info=True)
                time.sleep(5)  # Wait a bit before retrying
        
        # Cleanup
        logger.info("Shutting down...")
        self.eventstream_sender.close()
        logger.info("Shutdown complete")


def main():
    """Entry point"""
    global logger
    
    # Setup logging
    logger = setup_logging()
    
    # Load environment variables from parent directory
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
    load_dotenv(env_path)
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Validate environment variables
    required_vars = [
        "ENPHASE_API_KEY",
        "ENPHASE_CLIENT_ID", 
        "ENPHASE_CLIENT_SECRET",
        "ENPHASE_SYSTEM_ID",
        "EVENTHUB_CONNECTION_STRING",
        "EVENTHUB_NAME"
    ]
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        logger.error("Please check your .env file")
        sys.exit(1)
    
    # Create and run the streamer
    streamer = EnphaseDataStreamer()
    streamer.run()


if __name__ == "__main__":
    main()
