"""
Authentication for Backpack Perpetual Exchange using Ed25519 signatures.
Reuses the same authentication logic as the spot connector.
"""

import base64
import time
from typing import Any, Dict, Optional

from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSJSONRequest

try:
    from cryptography.hazmat.primitives.asymmetric import ed25519
except ImportError:
    # Fallback for environments without cryptography
    ed25519 = None


class BackpackPerpetualAuth(AuthBase):
    """
    Backpack Perpetual Exchange authentication using Ed25519 signatures.

    This is identical to the spot authentication as Backpack uses the same
    authentication mechanism for both spot and perpetual markets.

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
        Initialize Backpack Perpetual authentication.

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
            signature = self._private_key.sign(payload.encode('utf-8'))
            return base64.b64encode(signature).decode('utf-8')
        except Exception as e:
            raise ValueError(f"Failed to generate signature: {e}")

    def _build_signature_payload(
        self,
        timestamp: str,
        method: str,
        path: str,
        body: Optional[str] = None
    ) -> str:
        """
        Build the payload string for signing.

        Backpack signature format:
        - For GET/DELETE: timestamp + method + path (with query params)
        - For POST/PUT: timestamp + method + path + body

        Args:
            timestamp: Request timestamp as string
            method: HTTP method (GET, POST, etc.)
            path: Request path including query parameters
            body: Request body for POST/PUT requests

        Returns:
            Formatted payload string for signing
        """
        # Start with timestamp + method + path
        payload = f"{timestamp}{method.upper()}{path}"

        # Add body for POST/PUT requests
        if method.upper() in ["POST", "PUT"] and body:
            payload += body

        return payload

    def _generate_auth_headers(
        self,
        method: str,
        path: str,
        body: Optional[str] = None
    ) -> Dict[str, str]:
        """
        Generate authentication headers for a request.

        Returns:
            Dictionary with X-API-Key, X-Timestamp, X-Signature, X-Window headers
        """
        timestamp = str(self._time_provider())

        # Build signature payload
        signature_payload = self._build_signature_payload(
            timestamp=timestamp,
            method=method,
            path=path,
            body=body
        )

        # Generate signature
        signature = self._generate_signature(signature_payload)

        return {
            "X-API-Key": self.api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
            "X-Window": "5000",  # 5 second window for request validity
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Add authentication headers to REST request.

        Args:
            request: REST request to authenticate

        Returns:
            Authenticated request with headers added
        """
        # Extract method and path
        method = request.method.name

        # Parse URL to get path and query string
        if "?" in request.url:
            path = request.url.split("?")[0]
            query_string = request.url.split("?")[1]
            full_path = f"{path}?{query_string}"
        else:
            path = request.url
            full_path = path

        # For Backpack API, we need just the path part without the base URL
        if full_path.startswith("http"):
            # Extract path from full URL
            from urllib.parse import urlparse
            parsed = urlparse(full_path)
            full_path = parsed.path
            if parsed.query:
                full_path += f"?{parsed.query}"

        # Generate auth headers
        auth_headers = self._generate_auth_headers(
            method=method,
            path=full_path,
            body=request.data
        )

        # Add auth headers to request
        if request.headers is None:
            request.headers = {}
        request.headers.update(auth_headers)

        return request

    async def ws_authenticate(self, request: WSJSONRequest) -> WSJSONRequest:
        """
        Add authentication to WebSocket request.

        For Backpack, WebSocket authentication is done by sending an auth message
        after connection is established, not by modifying the connection request.

        Args:
            request: WebSocket request

        Returns:
            Modified request (usually unchanged for Backpack)
        """
        # Backpack authenticates via message after connection
        # No modification needed to the connection request itself
        return request

    def get_ws_auth_message(self) -> Dict[str, Any]:
        """
        Generate WebSocket authentication message.

        Returns:
            Authentication message to send after WebSocket connection
        """
        timestamp = str(self._time_provider())

        # Build auth payload for WebSocket
        auth_payload = f"{timestamp}AUTH"
        signature = self._generate_signature(auth_payload)

        return {
            "method": "auth",
            "params": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
                "window": "5000"
            }
        }
