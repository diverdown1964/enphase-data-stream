"""
Microsoft Fabric Eventstream Sender
Sends data to Microsoft Fabric Eventstream via Azure Event Hubs
"""
import json
import logging
from typing import Dict, Any
from azure.eventhub import EventHubProducerClient, EventData

logger = logging.getLogger(__name__)


class EventstreamSender:
    """Client for sending events to Microsoft Fabric Eventstream"""
    
    def __init__(self, connection_string: str, eventhub_name: str):
        """
        Initialize Eventstream sender
        
        Args:
            connection_string: Azure Event Hubs connection string
            eventhub_name: Name of the Event Hub (Eventstream)
        """
        self.connection_string = connection_string
        self.eventhub_name = eventhub_name
        self.producer = None
        
    def connect(self):
        """Establish connection to Event Hub"""
        try:
            self.producer = EventHubProducerClient.from_connection_string(
                conn_str=self.connection_string,
                eventhub_name=self.eventhub_name
            )
            logger.info(f"Connected to Event Hub: {self.eventhub_name}")
        except Exception as e:
            logger.error(f"Failed to connect to Event Hub: {e}")
            raise
    
    def send_event(self, data: Dict[str, Any]) -> bool:
        """
        Send a single event to Eventstream
        
        Args:
            data: Dictionary containing the event data
            
        Returns:
            True if successful, False otherwise
        """
        if not self.producer:
            self.connect()
        
        try:
            # Convert data to JSON string
            json_data = json.dumps(data)
            
            # Create event batch
            event_data_batch = self.producer.create_batch()
            event_data_batch.add(EventData(json_data))
            
            # Send the batch
            self.producer.send_batch(event_data_batch)
            
            logger.info(f"Successfully sent event to Eventstream")
            logger.debug(f"Event data: {json_data}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send event: {e}")
            return False
    
    def send_events_batch(self, events: list[Dict[str, Any]]) -> bool:
        """
        Send multiple events to Eventstream in a batch
        
        Args:
            events: List of dictionaries containing event data
            
        Returns:
            True if successful, False otherwise
        """
        if not self.producer:
            self.connect()
        
        try:
            # Create event batch
            event_data_batch = self.producer.create_batch()
            
            for event in events:
                json_data = json.dumps(event)
                event_data_batch.add(EventData(json_data))
            
            # Send the batch
            self.producer.send_batch(event_data_batch)
            
            logger.info(f"Successfully sent {len(events)} events to Eventstream")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send batch of events: {e}")
            return False
    
    def close(self):
        """Close the connection to Event Hub"""
        if self.producer:
            self.producer.close()
            logger.info("Closed Event Hub connection")
