"""
Enphase API Client
Handles authentication and data retrieval from Enphase Energy API
"""
import os
import json
import requests
import logging
import time
from functools import wraps
from typing import Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


def retry_on_error(max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 30.0):
    """
    Decorator that retries a function on transient errors with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        base_delay: Initial delay between retries (seconds)
        max_delay: Maximum delay between retries (seconds)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    
                    # Check if it's a retryable error
                    status_code = getattr(e.response, 'status_code', None) if hasattr(e, 'response') else None
                    
                    # Don't retry on client errors (4xx) except 429 (rate limit) and 408 (timeout)
                    if status_code and 400 <= status_code < 500 and status_code not in (429, 408):
                        logger.error(f"Non-retryable error {status_code} in {func.__name__}: {e}")
                        raise
                    
                    if attempt < max_retries:
                        # Exponential backoff with jitter
                        delay = min(base_delay * (2 ** attempt), max_delay)
                        logger.warning(f"Attempt {attempt + 1}/{max_retries + 1} failed for {func.__name__}: {e}")
                        logger.info(f"Retrying in {delay:.1f} seconds...")
                        time.sleep(delay)
                    else:
                        logger.error(f"All {max_retries + 1} attempts failed for {func.__name__}")
                        raise
                except Exception as e:
                    # Don't retry on non-request exceptions
                    logger.error(f"Non-retryable exception in {func.__name__}: {e}")
                    raise
            
            # Should not reach here, but just in case
            raise last_exception
        return wrapper
    return decorator


class EnphaseClient:
    """Client for interacting with the Enphase Energy API"""
    
    def __init__(self, api_key: str, client_id: str, client_secret: str, system_id: str, auth_code: Optional[str] = None, token_file: str = ".tokens.json"):
        """
        Initialize Enphase API client
        
        Args:
            api_key: Enphase API key (may not be needed for OAuth)
            client_id: OAuth client ID
            client_secret: OAuth client secret
            system_id: Your Enphase system ID
            auth_code: Optional authorization code to exchange for tokens
            token_file: File to persist tokens (default: .tokens.json)
        """
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self.system_id = system_id
        self.auth_code = auth_code
        self.token_file = token_file
        self.base_url = "https://api.enphaseenergy.com/api/v4"
        self.base_url_v2 = "https://api.enphaseenergy.com/api/v2"
        self.access_token: Optional[str] = None
        self.refresh_token: Optional[str] = None
        self.token_expiry: Optional[datetime] = None
        
        # Load saved tokens if available
        self._load_tokens()
        
    def _load_tokens(self):
        """Load tokens from file if available"""
        if os.path.exists(self.token_file):
            try:
                with open(self.token_file, 'r') as f:
                    data = json.load(f)
                    self.access_token = data.get("access_token")
                    self.refresh_token = data.get("refresh_token")
                    expiry_str = data.get("token_expiry")
                    if expiry_str:
                        self.token_expiry = datetime.fromisoformat(expiry_str)
                    logger.info("Loaded saved tokens from file")
            except Exception as e:
                logger.warning(f"Failed to load tokens from file: {e}")
    
    def _save_tokens(self):
        """Save tokens to file for persistence"""
        try:
            data = {
                "access_token": self.access_token,
                "refresh_token": self.refresh_token,
                "token_expiry": self.token_expiry.isoformat() if self.token_expiry else None
            }
            with open(self.token_file, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info("Saved tokens to file")
        except Exception as e:
            logger.warning(f"Failed to save tokens to file: {e}")
        
    def exchange_auth_code(self, auth_code: str) -> Dict:
        """
        Exchange authorization code for access and refresh tokens
        
        Args:
            auth_code: Authorization code from OAuth flow
            
        Returns:
            Dictionary containing token information
        """
        auth_url = "https://api.enphaseenergy.com/oauth/token"
        payload = {
            "grant_type": "authorization_code",
            "code": auth_code,
            "redirect_uri": "https://api.enphaseenergy.com/oauth/redirect_uri"
        }
        
        # Use Basic Auth with client credentials
        from requests.auth import HTTPBasicAuth
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        
        try:
            response = requests.post(auth_url, data=payload, auth=auth)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token")
            expires_in = token_data.get("expires_in", 3600)
            
            # Set token expiry (with 5 minute buffer)
            from datetime import timedelta
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
            
            logger.info("Successfully exchanged authorization code for tokens")
            
            # Save tokens for future use
            self._save_tokens()
            
            return token_data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to exchange authorization code: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def _refresh_access_token(self) -> str:
        """
        Refresh the access token using refresh token
        
        Returns:
            New access token string
        """
        if not self.refresh_token:
            raise Exception("No refresh token available")
        
        auth_url = "https://api.enphaseenergy.com/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token
        }
        
        # Use Basic Auth with client credentials
        from requests.auth import HTTPBasicAuth
        auth = HTTPBasicAuth(self.client_id, self.client_secret)
        
        try:
            response = requests.post(auth_url, data=payload, auth=auth)
            response.raise_for_status()
            
            token_data = response.json()
            self.access_token = token_data.get("access_token")
            self.refresh_token = token_data.get("refresh_token", self.refresh_token)
            expires_in = token_data.get("expires_in", 3600)
            
            # Set token expiry (with 5 minute buffer)
            from datetime import timedelta
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in - 300)
            
            logger.info("Successfully refreshed access token")
            
            # Save updated tokens
            self._save_tokens()
            
            return self.access_token
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to refresh access token: {e}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            raise
    
    def _get_access_token(self) -> str:
        """
        Get OAuth access token for Enphase API
        
        Returns:
            Access token string
        """
        # Check if we have a valid token
        if self.access_token and self.token_expiry and datetime.now() < self.token_expiry:
            logger.debug("Using cached access token")
            return self.access_token
        
        # If we have a refresh token, use it
        if self.refresh_token:
            logger.info("Access token expired, refreshing...")
            return self._refresh_access_token()
        
        # If we have an auth code and no refresh token, exchange it
        if self.auth_code:
            logger.info("No refresh token available, exchanging authorization code...")
            self.exchange_auth_code(self.auth_code)
            return self.access_token
        
        # No way to get a token
        raise Exception("No authorization code or refresh token available. Please authenticate first.")
    
    @retry_on_error(max_retries=3, base_delay=2.0, max_delay=30.0)
    def get_production_data(self, start_at: int = None, end_at: int = None) -> Dict:
        """
        Get current production data from Enphase system using v4 telemetry endpoint
        
        Args:
            start_at: Unix timestamp for start of range (optional)
            end_at: Unix timestamp for end of range (optional)
        
        Returns:
            Dictionary containing production telemetry data
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        # Add API key in header (required per v4 API docs)
        if self.api_key:
            headers["key"] = self.api_key
        
        # Use v4 telemetry endpoint
        endpoint = f"{self.base_url}/systems/{self.system_id}/telemetry/production_meter"
        
        # Add time range parameters if provided
        params = {}
        if start_at:
            params['start_at'] = start_at
        if end_at:
            params['end_at'] = end_at
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            
            # Add metadata
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved production telemetry for system {self.system_id}")
            logger.info(f"Retrieved {len(data.get('intervals', []))} intervals")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve production data: {e}")
            raise
    
    @retry_on_error(max_retries=3, base_delay=2.0, max_delay=30.0)
    def get_consumption_data(self, start_at: int = None, end_at: int = None) -> Dict:
        """
        Get consumption data from Enphase system
        
        Args:
            start_at: Unix timestamp for start of range (optional)
            end_at: Unix timestamp for end of range (optional)
        
        Returns:
            Dictionary containing consumption telemetry data
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        if self.api_key:
            headers["key"] = self.api_key
        
        endpoint = f"{self.base_url}/systems/{self.system_id}/telemetry/consumption_meter"
        
        params = {}
        if start_at:
            params['start_at'] = start_at
        if end_at:
            params['end_at'] = end_at
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved consumption telemetry for system {self.system_id}")
            logger.info(f"Retrieved {len(data.get('intervals', []))} intervals")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve consumption data: {e}")
            raise
    
    @retry_on_error(max_retries=3, base_delay=2.0, max_delay=30.0)
    def get_battery_data(self, start_at: int = None, end_at: int = None) -> Dict:
        """
        Get battery data from Enphase system
        
        Args:
            start_at: Unix timestamp for start of range (optional)
            end_at: Unix timestamp for end of range (optional)
        
        Returns:
            Dictionary containing battery telemetry data
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        if self.api_key:
            headers["key"] = self.api_key
        
        endpoint = f"{self.base_url}/systems/{self.system_id}/telemetry/battery"
        
        params = {}
        if start_at:
            params['start_at'] = start_at
        if end_at:
            params['end_at'] = end_at
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved battery telemetry for system {self.system_id}")
            logger.info(f"Retrieved {len(data.get('intervals', []))} intervals")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve battery data: {e}")
            raise
    
    @retry_on_error(max_retries=3, base_delay=2.0, max_delay=30.0)
    def get_import_data(self, start_at: int = None, end_at: int = None) -> Dict:
        """
        Get grid import data from Enphase system
        
        Args:
            start_at: Unix timestamp for start of range (optional)
            end_at: Unix timestamp for end of range (optional)
        
        Returns:
            Dictionary containing energy import telemetry data
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        if self.api_key:
            headers["key"] = self.api_key
        
        endpoint = f"{self.base_url}/systems/{self.system_id}/energy_import_telemetry"
        
        params = {}
        if start_at:
            params['start_at'] = start_at
        if end_at:
            params['end_at'] = end_at
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved import telemetry for system {self.system_id}")
            logger.info(f"Retrieved {len(data.get('intervals', []))} intervals")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve import data: {e}")
            raise
    
    @retry_on_error(max_retries=3, base_delay=2.0, max_delay=30.0)
    def get_export_data(self, start_at: int = None, end_at: int = None) -> Dict:
        """
        Get grid export data from Enphase system
        
        Args:
            start_at: Unix timestamp for start of range (optional)
            end_at: Unix timestamp for end of range (optional)
        
        Returns:
            Dictionary containing energy export telemetry data
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        if self.api_key:
            headers["key"] = self.api_key
        
        endpoint = f"{self.base_url}/systems/{self.system_id}/energy_export_telemetry"
        
        params = {}
        if start_at:
            params['start_at'] = start_at
        if end_at:
            params['end_at'] = end_at
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved export telemetry for system {self.system_id}")
            logger.info(f"Retrieved {len(data.get('intervals', []))} intervals")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve export data: {e}")
            raise
    
    def get_latest_telemetry(self) -> Dict:
        """
        Get latest real-time telemetry snapshot
        
        Returns:
            Dictionary containing current power levels and device status
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        if self.api_key:
            headers["key"] = self.api_key
        
        endpoint = f"{self.base_url}/systems/{self.system_id}/latest_telemetry"
        
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved latest telemetry for system {self.system_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve latest telemetry: {e}")
            raise
    
    def get_system_summary(self, summary_date: str = None) -> Dict:
        """
        Get system summary including current status using v4 API
        
        Args:
            summary_date: Optional date string (YYYY-MM-DD) to get lifetime energy values up to that date
        
        Returns:
            Dictionary containing system summary (includes NMI, EVSE charge/discharge power in v4)
        """
        token = self._get_access_token()
        
        headers = {
            "Authorization": f"Bearer {token}"
        }
        
        # Add API key in header (required per v4 API docs)
        if self.api_key:
            headers["key"] = self.api_key
        
        # Use v4 API endpoint (migrated from v2)
        endpoint = f"{self.base_url}/systems/{self.system_id}/summary"
        
        params = {}
        if summary_date:
            params['summary_date'] = summary_date
        
        try:
            response = requests.get(endpoint, headers=headers, params=params if params else None)
            response.raise_for_status()
            
            data = response.json()
            data["retrieved_at"] = datetime.now().isoformat()
            
            logger.info(f"Successfully retrieved system summary for {self.system_id}")
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to retrieve system summary: {e}")
            raise
