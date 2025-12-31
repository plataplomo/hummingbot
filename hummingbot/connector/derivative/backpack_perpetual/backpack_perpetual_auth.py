"""Authentication for Backpack Perpetual Exchange using Ed25519 signatures.
Implements Backpack's instruction-based signing scheme for perpetual endpoints.
"""

import base64
import json
import logging
from typing import Any, cast
from urllib.parse import parse_qs, urlparse

from cryptography.hazmat.primitives.asymmetric import ed25519

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_constants as CONSTANTS
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest
from hummingbot.logger import HummingbotLogger


class BackpackPerpetualAuth(AuthBase):
    """Backpack Perpetual Exchange authentication using Ed25519 signatures.

    This is identical to the spot authentication as Backpack uses the same
    authentication mechanism for both spot and perpetual markets.

    Implements the authentication pattern required by Backpack:
    - X-API-Key: API key
    - X-Timestamp: Unix timestamp in milliseconds
    - X-Signature: Ed25519 signature of payload
    - X-Window: Request validity window (5000ms)
    """

    _logger = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = cast(HummingbotLogger, logging.getLogger(HummingbotLogger.logger_name_for_class(cls)))
        return cls._logger

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        time_provider: TimeSynchronizer,
    ):
        """Initialize Backpack Perpetual authentication.

        Args:
            api_key: Backpack API key
            api_secret: Backpack API secret (base64 encoded private key)
            time_provider: TimeSynchronizer for getting synchronized time
        """
        self.api_key = api_key
        self.api_secret = api_secret
        self.time_provider = time_provider

        self._private_key = None
        if api_secret:
            # Load and validate the private key
            try:
                private_key_bytes = base64.b64decode(api_secret)
                self._private_key = ed25519.Ed25519PrivateKey.from_private_bytes(private_key_bytes)
            except Exception as e:
                raise ValueError(
                    "Invalid API secret format. Expected base64 encoded Ed25519 private key: "
                    f"{e}"
                ) from e

    def _get_timestamp(self) -> int:
        """Get current timestamp in milliseconds."""
        return int(self.time_provider.time() * 1e3)

    def _generate_signature(self, payload: str) -> str:
        """Generate Ed25519 signature for the given payload.

        Args:
            payload: String to sign (timestamp + method + path + body)

        Returns:
            Base64 encoded signature
        """
        if self._private_key is None:
            raise ValueError("API secret is required to generate signed requests.")
        try:
            signature = self._private_key.sign(payload.encode("utf-8"))
            return base64.b64encode(signature).decode("utf-8")
        except Exception as e:
            raise ValueError(f"Failed to generate signature: {e}") from e

    def _get_instruction_for_endpoint(self, method: str, path: str) -> str:
        """Get the instruction string for a given method and path.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path

        Returns:
            Instruction string for signing
        """
        # Remove query parameters from path for lookup
        lookup_path = path.split("?", maxsplit=1)[0] if "?" in path else path

        # Try exact match first
        instruction = CONSTANTS.INSTRUCTION_MAP.get((method.upper(), lookup_path))

        if not instruction:
            # For unknown endpoints, generate a default instruction
            # This helps with new endpoints that might not be mapped yet
            instruction = f"{method.lower()}Query"

        return instruction

    def _build_signature_payload(
        self,
        timestamp: str,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: str | None = None,
        window: str = str(CONSTANTS.AUTH_WINDOW_MS),
    ) -> str:
        """Build the payload string for signing using instruction-based format.

        Backpack signature format:
        instruction=<instruction>&<sorted_params>&timestamp=<timestamp>&window=<window>

        Args:
            timestamp: Request timestamp as string
            method: HTTP method (GET, POST, etc.)
            path: Request path
            params: Query parameters (for GET) or parsed body parameters
            body: Request body for POST/PUT requests (JSON string)
            window: Time window for request validity

        Returns:
            Formatted payload string for signing
        """
        # Get the instruction for this endpoint
        instruction = self._get_instruction_for_endpoint(method, path)

        # Start building the payload
        payload_parts = [f"instruction={instruction}"]

        # Handle parameters based on method
        if method.upper() == "GET" and params:
            # For GET requests, add query parameters
            sorted_params = sorted(params.items())
            for key, value in sorted_params:
                if value is not None:
                    # Convert booleans to lowercase strings
                    formatted_value = value
                    if isinstance(value, bool):
                        formatted_value = "true" if value else "false"
                    payload_parts.append(f"{key}={formatted_value}")
        elif method.upper() in ["POST", "PUT", "DELETE", "PATCH"] and body:
            # For POST/PUT/DELETE, parse JSON body and add as parameters
            try:
                body_dict = json.loads(body) if isinstance(body, str) else body
                sorted_params = sorted(body_dict.items())
                for key, value in sorted_params:
                    if value is not None:
                        # Convert booleans to lowercase strings
                        formatted_value = value
                        if isinstance(value, bool):
                            formatted_value = "true" if value else "false"
                        payload_parts.append(f"{key}={formatted_value}")
            except (json.JSONDecodeError, TypeError):
                # If body is not JSON, skip parameter extraction
                pass

        # Add timestamp and window
        payload_parts.extend((f"timestamp={timestamp}", f"window={window}"))

        return "&".join(payload_parts)

    def _generate_auth_headers(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None = None,
        body: str | None = None,
    ) -> dict[str, str]:
        """Generate authentication headers for a request.

        Returns:
            Dictionary with X-API-Key, X-Timestamp, X-Signature, X-Window headers
        """
        timestamp = str(self._get_timestamp())
        window = str(CONSTANTS.AUTH_WINDOW_MS)

        # Build signature payload using instruction-based format
        signature_payload = self._build_signature_payload(
            timestamp=timestamp,
            method=method,
            path=path,
            params=params,
            body=body,
            window=window,
        )

        # Generate signature
        signature = self._generate_signature(signature_payload)

        return {
            "X-API-Key": self.api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
            "X-Window": window,
        }

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """Add authentication headers to REST request.

        Args:
            request: REST request to authenticate

        Returns:
            Authenticated request with headers added
        """
        # Get URL, defaulting to empty string if None
        url = request.url or ""

        # Extract method and path
        method = request.method.name

        # Parse URL to get path and query string
        if "?" in url:
            path = url.split("?")[0]
            query_string = url.split("?")[1]
            full_path = f"{path}?{query_string}"
        else:
            path = url
            full_path = path

        # For Backpack API, we need just the path part without the base URL
        if full_path.startswith("http"):
            # Extract path from full URL
            parsed = urlparse(full_path)
            full_path = parsed.path
            if parsed.query:
                full_path += f"?{parsed.query}"

        # Extract query parameters if present
        params = None
        clean_path = full_path
        if "?" in full_path and method == "GET":
            clean_path, query_string = full_path.split("?", 1)
            params = {k: v[0] for k, v in parse_qs(query_string).items()}
        elif request.params:
            params = dict(request.params) if request.params else None

        # Generate auth headers with instruction-based signatures
        auth_headers = self._generate_auth_headers(
            method=method,
            path=clean_path,
            params=params,
            body=request.data,
        )

        # Add auth headers to request
        if request.headers is None:
            request.headers = {}

        # Convert headers to dict if it's a Mapping
        headers_dict = dict(request.headers) if request.headers else {}
        headers_dict.update(auth_headers)
        request.headers = headers_dict

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """Add authentication to WebSocket request.

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

    def get_ws_auth_message(self) -> dict[str, Any]:
        """Generate WebSocket authentication message.

        Returns:
            Authentication message to send after WebSocket connection
        """
        timestamp = str(self._get_timestamp())
        window = str(CONSTANTS.AUTH_WINDOW_MS)

        # Build auth payload for WebSocket using instruction-based format
        # For WebSocket auth, the instruction is "subscribe"
        auth_payload = f"instruction={CONSTANTS.WS_AUTH_INSTRUCTION}&timestamp={timestamp}&window={window}"
        signature = self._generate_signature(auth_payload)

        return {
            "method": CONSTANTS.WS_AUTH_MESSAGE_METHOD,
            "params": {
                "apiKey": self.api_key,
                "timestamp": timestamp,
                "signature": signature,
                "window": window,
            },
        }
