"""
Authentication for Backpack Exchange using Ed25519 signatures.
Compatible with Python 3.10 and Hummingbot patterns.
"""

import base64
import time
from typing import Any, Dict, Optional
from urllib.parse import urlencode

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSJSONRequest

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except ImportError:
    # Fallback for environments without cryptography
    ed25519 = None

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS


class BackpackAuth(AuthBase):
    """
    Backpack Exchange authentication using Ed25519 signatures.

    Implements the authentication pattern required by Backpack:
    - X-API-Key: API key
    - X-Timestamp: Unix timestamp in milliseconds
    - X-Signature: Ed25519 signature of payload
    - X-Window: Request validity window (5000ms)
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        time_provider: Optional[callable] = None
    ):
        """
        Initialize Backpack authentication.

        Args:
            api_key: Backpack API key
            api_secret: Backpack API secret (base64 encoded private key)
            time_provider: Function to get current time (for testing)
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self._time_provider = time_provider or self._get_timestamp

        if ed25519 is None:
            raise ImportError(
                "cryptography library is required for Ed25519 signatures. "
                "Install with: pip install cryptography"
            )

        # Load and validate the private key
        try:
            private_key_bytes = base64.b64decode(api_secret)
            self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key_bytes)
        except Exception as e:
            raise ValueError(f"Invalid API secret format. Expected base64 encoded Ed25519 private key: {e}")

    def _get_timestamp(self) -> int:
        """Get current timestamp in milliseconds."""
        return int(time.time() * 1000)

    def _generate_signature(self, payload: str) -> str:
        """
        Generate Ed25519 signature for the given payload.

        Args:
            payload: String to sign (timestamp + method + path + body)

        Returns:
            Base64 encoded signature
        """
        try:
            signature_bytes = self._private_key.sign(payload.encode('utf-8'))
            return base64.b64encode(signature_bytes).decode('utf-8')
        except Exception as e:
            raise ValueError(f"Failed to generate signature: {e}")

    def _build_signature_payload(
        self,
        timestamp: str,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[str] = None
    ) -> str:
        """
        Build the payload string for signature generation.

        Format: timestamp + method + path + query_string + body

        Args:
            timestamp: Request timestamp
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path
            params: Query parameters for GET requests
            data: Request body for POST requests

        Returns:
            Signature payload string
        """
        payload = f"{timestamp}{method.upper()}{path}"

        # Add query string for GET requests
        if params and method.upper() == "GET":
            query_string = urlencode(sorted(params.items()))
            if query_string:
                payload += f"?{query_string}"

        # Add body for POST requests
        if data:
            payload += data

        return payload

    def _generate_auth_headers(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate authentication headers for REST requests.

        Args:
            method: HTTP method
            path: API endpoint path
            params: Query parameters
            data: Request body

        Returns:
            Dictionary of authentication headers
        """
        timestamp = str(self._time_provider())
        payload = self._build_signature_payload(timestamp, method, path, params, data)
        signature = self._generate_signature(payload)

        return {
            "X-API-Key": self.api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
            "X-Window": str(CONSTANTS.AUTH_WINDOW_MS),
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Add authentication headers to REST request.

        Args:
            request: REST request to authenticate

        Returns:
            Authenticated request
        """
        # Extract path from URL
        if request.url.startswith("http"):
            # Full URL provided
            path = "/" + "/".join(request.url.split("/")[3:])
        else:
            # Relative path
            path = request.url if request.url.startswith("/") else f"/{request.url}"

        # Generate auth headers
        auth_headers = self._generate_auth_headers(
            method=request.method.value,
            path=path,
            params=request.params,
            data=request.data
        )

        # Add headers to request
        if request.headers is None:
            request.headers = {}
        request.headers.update(auth_headers)

        return request

    async def ws_authenticate(self, request: WSJSONRequest) -> WSJSONRequest:
        """
        Add authentication to WebSocket request.

        Args:
            request: WebSocket request to authenticate

        Returns:
            Authenticated WebSocket request
        """
        timestamp = str(self._time_provider())

        # For WebSocket auth, we typically sign a simpler payload
        auth_payload = f"{timestamp}websocket_auth"
        signature = self._generate_signature(auth_payload)

        # Add auth data to WebSocket message
        auth_data = {
            "method": "authenticate",
            "params": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
                "window": CONSTANTS.AUTH_WINDOW_MS
            }
        }

        if request.payload is None:
            request.payload = {}
        request.payload.update(auth_data)

        return request

    def get_ws_auth_message(self) -> Dict[str, Any]:
        """
        Generate WebSocket authentication message.

        Returns:
            Authentication message for WebSocket
        """
        timestamp = str(self._time_provider())
        auth_payload = f"{timestamp}websocket_auth"
        signature = self._generate_signature(auth_payload)

        return {
            "method": "authenticate",
            "params": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
                "window": CONSTANTS.AUTH_WINDOW_MS
            }
        }
