import base64
import json
import time
import urllib.parse
from collections.abc import Mapping
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from cyberdelta.apis.backpack.models.bp_ws_payloads import BackpackWsSignatureComponents
from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class BackpackEd25519Authenticator(IAuthenticator):
    """Authenticator for Backpack API using ED25519 signature."""

    def __init__(self, api_key_b64: str, private_key_b64: str) -> None:
        """
        Initialize the authenticator with Base64-encoded keys.

        Args:
            api_key_b64: Base64-encoded public key for API authentication.
            private_key_b64: Base64-encoded private key for signing.
        """
        if not api_key_b64:
            raise ValueError("API key (Base64) cannot be empty")
        if not private_key_b64:
            raise ValueError("Private key (Base64) cannot be empty")

        self._api_key_b64 = api_key_b64

        try:
            # Decode and load the private key
            private_key_bytes = base64.b64decode(private_key_b64)
            self._ed25519_private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
        except Exception as e:
            logger.error(f"Failed to load ED25519 private key: {e}")
            raise ValueError(f"Invalid ED25519 private key: {e}") from e

        # Initialize instruction mapping for Backpack REST API endpoints
        self.INSTRUCTION_MAP: dict[tuple[str, str], str] = {
            # Account endpoints
            ("GET", "/api/v1/capital"): "balanceQuery",
            ("GET", "/api/v1/deposits"): "depositHistoryQueryAll",
            ("GET", "/api/v1/withdrawals"): "withdrawalHistoryQueryAll",
            ("POST", "/api/v1/withdraw"): "withdraw",
            # Order management endpoints
            ("POST", "/api/v1/order"): "orderExecute",
            ("DELETE", "/api/v1/order"): "orderCancel",
            ("DELETE", "/api/v1/orders"): "orderCancelAll",
            ("GET", "/api/v1/order"): "orderQuery",
            ("GET", "/api/v1/orders"): "orderHistoryQueryAll",
            # Trade endpoints
            ("GET", "/api/v1/fills"): "fillHistoryQueryAll",
            # Position endpoints
            ("GET", "/api/v1/positions"): "positionQuery",
            # System endpoints
            ("GET", "/api/v1/system"): "systemStatusQuery",
            # Market data (if signed)
            ("GET", "/api/v1/trades"): "tradeHistoryQueryAll",
            ("GET", "/api/v1/klines"): "klineQuery",
        }

    def _get_instruction_for_endpoint(self, method: str, path: str) -> str:
        """
        Get the instruction string for a given method and path.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path

        Returns:
            Instruction string for signing

        Raises:
            APIError: If instruction not found for the endpoint
        """
        method_upper = method.upper()

        # Try exact match first
        if (method_upper, path) in self.INSTRUCTION_MAP:
            return self.INSTRUCTION_MAP[(method_upper, path)]

        # Try to match path templates with variables
        for (map_method, map_path), instruction in self.INSTRUCTION_MAP.items():
            if map_method == method_upper:
                # Handle paths with variables like /api/v1/order/{orderId}
                # For now, simple prefix matching for common patterns
                if map_path.endswith("/{orderId}") and path.startswith(map_path[:-10]):
                    return instruction
                elif map_path.endswith("/{id}") and path.startswith(map_path[:-5]):
                    return instruction

        # If no match found, raise error
        raise APIError(
            f"Backpack instruction not found for {method_upper} {path}",
            code=APIErrorCode.INVALID_REQUEST.value,
        )

    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs a Backpack API request using ED25519.

        Args:
            method: The HTTP method (e.g., 'GET', 'POST').
            path: The API endpoint path (relative path).
            params: Optional dictionary of query parameters.
            data: Optional dictionary of request body data.
            headers: Optional mapping of existing headers.

        Returns:
            An AuthenticatedRequestComponents Pydantic model.

        Raises:
            APIError: If signing fails or instruction not found.
        """
        try:
            # Get instruction for this endpoint
            instruction_str = self._get_instruction_for_endpoint(method, path)

            # Generate timestamp and window
            timestamp_ms = int(time.time() * 1000)
            window_ms = 5000  # 5 second window

            # Construct content part based on method
            content_part_str = ""
            if method.upper() == "GET" and params:
                # Filter out None values and sort for consistency
                filtered_params = {k: v for k, v in params.items() if v is not None}
                if filtered_params:
                    content_part_str = urllib.parse.urlencode(sorted(filtered_params.items()))
            elif method.upper() in ["POST", "PUT", "DELETE"] and data:
                # Filter out None values and serialize to JSON
                filtered_data = {k: v for k, v in data.items() if v is not None}
                if filtered_data:
                    content_part_str = json.dumps(
                        filtered_data, separators=(",", ":"), sort_keys=True
                    )

            # Construct string to sign
            string_to_sign = (
                f"instruction={instruction_str}&{content_part_str}"
                f"&timestamp={timestamp_ms}&window={window_ms}"
            )

            # Sign the string using ED25519
            signature_bytes = self._ed25519_private_key.sign(string_to_sign.encode("utf-8"))
            signature_b64 = base64.b64encode(signature_bytes).decode("utf-8")

            # Create authentication headers
            auth_headers = {
                "X-API-Key": self._api_key_b64,
                "X-Timestamp": str(timestamp_ms),
                "X-Window": str(window_ms),
                "X-Signature": signature_b64,
            }

            # Merge with existing headers
            final_headers: dict[str, Any] = {}
            if headers:
                final_headers.update(headers)
            final_headers.update(auth_headers)

            # Add Content-Type for POST/PUT if needed
            if method.upper() in ["POST", "PUT"] and data and "Content-Type" not in final_headers:
                final_headers["Content-Type"] = "application/json; charset=utf-8"

            # Convert to string mapping for Pydantic model
            final_headers_str = {k: str(v) for k, v in final_headers.items()}

            return AuthenticatedRequestComponents(
                headers=final_headers_str,
                params=params,
                data=data,
            )

        except Exception as e:
            logger.error(f"ED25519 authentication failed for {method} {path}: {e}")
            if isinstance(e, APIError):
                raise
            raise APIError(
                f"Authentication preparation failed: {e}",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

    def get_ws_subscription_signature_components(
        self,
        subscription_type: str,
        symbol: str | None = None,
    ) -> BackpackWsSignatureComponents:
        """
        Generate ED25519 signature components for WebSocket subscription.

        Args:
            subscription_type: Type of subscription (e.g., "account", "orderbook")
            symbol: Optional symbol for market data subscriptions

        Returns:
            BackpackWsSignatureComponents model with api_key, timestamp, window, and signature
        """
        try:
            timestamp_ms = int(time.time() * 1000)
            window_ms = 5000

            # Construct message for WebSocket subscription
            if symbol:
                message_content = f"stream={subscription_type}&symbol={symbol}"
            else:
                message_content = f"stream={subscription_type}"

            string_to_sign = f"{message_content}&timestamp={timestamp_ms}&window={window_ms}"

            # Sign using ED25519
            signature_bytes = self._ed25519_private_key.sign(string_to_sign.encode("utf-8"))
            signature_b64 = base64.b64encode(signature_bytes).decode("utf-8")

            return BackpackWsSignatureComponents(
                api_key=self._api_key_b64,
                timestamp=str(timestamp_ms),
                window=str(window_ms),
                signature=signature_b64,
            )

        except Exception as e:
            logger.error(f"WebSocket signature generation failed: {e}")
            raise APIError(
                f"WebSocket signature generation failed: {e}",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e
