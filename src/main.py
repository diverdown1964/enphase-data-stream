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
        self.poll_interval = int(os.getenv("POLL_INTERVAL", 300))
        
        # Track sent intervals to avoid duplicates (store timestamps)
        self.sent_intervals = set()
        
        logger.info("Enphase Data Streamer initialized")
        logger.info(f"Polling interval: {self.poll_interval} seconds")
    
    def poll_and_send(self):
        """Poll Enphase API and send only new intervals to Eventstream"""
        try:
            logger.info("Polling Enphase API...")
            
            # Get production data (v4 telemetry with intervals)
            production_data = self.enphase_client.get_production_data()
            
            # Filter out intervals we've already sent
            all_intervals = production_data.get('intervals', [])
            new_intervals = [
                interval for interval in all_intervals
                if interval.get('end_at') not in self.sent_intervals
            ]
            
            if not new_intervals:
                logger.info("No new intervals to send (all data already sent)")
                return
            
            # Log summary
            logger.info(f"Found {len(new_intervals)} new intervals out of {len(all_intervals)} total")
            if new_intervals:
                latest_interval = new_intervals[-1]
                wh_del = latest_interval.get('wh_del', 0)
                logger.info(f"Latest production: {wh_del} Wh delivered")
            
            # Send each new interval individually to Eventstream
            sent_count = 0
            for interval in new_intervals:
                # Create event with interval data plus metadata
                event = {
                    'system_id': production_data.get('system_id'),
                    'granularity': production_data.get('granularity'),
                    'interval': interval,
                    'retrieved_at': production_data.get('retrieved_at')
                }
                
                success = self.eventstream_sender.send_event(event)
                
                if success:
                    # Mark this interval as sent
                    self.sent_intervals.add(interval.get('end_at'))
                    sent_count += 1
                else:
                    logger.error(f"Failed to send interval at {interval.get('end_at')}")
            
            logger.info(f"Successfully sent {sent_count}/{len(new_intervals)} new intervals to Eventstream")
            
            # Clean up old timestamps (keep last 24 hours worth)
            # This prevents the set from growing indefinitely
            if len(self.sent_intervals) > 200:  # ~2 days of 15-min intervals
                current_time = max(interval.get('end_at') for interval in all_intervals)
                cutoff_time = current_time - (24 * 3600)  # 24 hours ago
                self.sent_intervals = {ts for ts in self.sent_intervals if ts > cutoff_time}
                logger.info(f"Cleaned up old interval tracking (kept {len(self.sent_intervals)} recent timestamps)")
                
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
