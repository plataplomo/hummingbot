"""Backpack Exchange API authentication module.

This module provides authentication functionality for the Backpack Exchange API,
including ED25519 signature generation for REST API requests and WebSocket subscriptions.
"""

import base64
import time
import urllib.parse
from collections.abc import Mapping
from typing import Any

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import SecretStr

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

    def __init__(self, api_key_b64_secret: SecretStr, private_key_b64_secret: SecretStr) -> None:
        """Initialize the authenticator with SecretStr-wrapped Base64-encoded keys.

        Args:
            api_key_b64_secret: SecretStr containing Base64-encoded public key for API auth.
            private_key_b64_secret: SecretStr containing Base64-encoded private key for signing.

        """
        # Get secret values and validate
        api_key_b64 = api_key_b64_secret.get_secret_value().strip()
        private_key_b64 = private_key_b64_secret.get_secret_value().strip()

        if not api_key_b64:
            raise ValueError("API key (Base64 public ED25519 key) cannot be empty")
        if not private_key_b64:
            raise ValueError("Private key (Base64 private ED25519 key) cannot be empty")

        self._api_key_b64 = api_key_b64  # Store the public key string

        try:
            # Decode and load the private key
            private_key_bytes = base64.b64decode(private_key_b64)
            self._ed25519_private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
        except Exception as e:
            logger.error(f"Failed to load ED25519 private key from Base64 string: {e}")
            raise ValueError(f"Invalid Base64 ED25519 private key: {e}") from e

        # Initialize instruction mapping for Backpack REST API endpoints
        self.INSTRUCTION_MAP: dict[tuple[str, str], str] = {
            # Account Management endpoints
            ("GET", "/api/v1/account"): "accountQuery",
            ("POST", "/api/v1/account/convertDust"): "accountConvertDust",
            # Account Limits endpoints
            ("GET", "/api/v1/account/limits/borrow"): "maxBorrowQuantity",
            ("GET", "/api/v1/account/limits/order"): "maxOrderQuantity",
            ("GET", "/api/v1/account/limits/withdrawal"): "maxWithdrawalQuantity",
            # Capital and Balance endpoints
            ("GET", "/api/v1/capital"): "balanceQuery",
            ("GET", "/api/v1/capital/collateral"): "collateralQuery",
            ("GET", "/api/v1/collateral"): "collateralQuery",
            # Position endpoints
            ("GET", "/api/v1/position"): "positionQuery",
            ("GET", "/api/v1/positions"): "positionQuery",
            ("GET", "/api/v1/borrowLend/positions"): "borrowLendPositionQuery",
            # Order Management endpoints
            ("POST", "/api/v1/order"): "orderExecute",
            ("DELETE", "/api/v1/order"): "orderCancel",
            ("DELETE", "/api/v1/orders"): "orderCancelAll",
            ("GET", "/api/v1/order"): "orderQuery",
            ("GET", "/api/v1/orders"): "orderQueryAll",
            ("GET", "/api/v1/order/{orderId}"): "orderQuery",  # Order status by ID
            # Deposit endpoints (wapi)
            ("GET", "/wapi/v1/capital/deposits"): "depositQueryAll",
            ("GET", "/wapi/v1/capital/deposit/address"): "depositAddressQuery",
            # Withdrawal endpoints (wapi)
            ("GET", "/wapi/v1/capital/withdrawals"): "withdrawalQueryAll",
            ("POST", "/wapi/v1/capital/withdraw"): "withdraw",
            # Historical Data endpoints (wapi)
            ("GET", "/wapi/v1/history/orders"): "orderHistoryQueryAll",
            ("GET", "/wapi/v1/history/fills"): "fillHistoryQueryAll",
            ("GET", "/wapi/v1/history/funding"): "fundingHistoryQueryAll",
            ("GET", "/wapi/v1/history/pnl"): "pnlHistoryQueryAll",
            ("GET", "/wapi/v1/history/settlement"): "settlementHistoryQueryAll",
            # API v1 history endpoints
            ("GET", "/api/v1/history/orders"): "orderHistoryQueryAll",
            ("GET", "/api/v1/history/fills"): "fillHistoryQueryAll",
            # Borrow/Lend History endpoints (wapi)
            ("GET", "/wapi/v1/history/borrowLend"): "borrowHistoryQueryAll",
            ("GET", "/wapi/v1/history/borrowLend/positions"): "borrowPositionHistoryQueryAll",
            ("GET", "/wapi/v1/history/interest"): "interestHistoryQueryAll",
            # Trading Data endpoints
            ("GET", "/api/v1/trades/history"): "fillHistoryQueryAll",  # Trade history
            ("GET", "/api/v1/fills"): "fillHistoryQueryAll",
            # Borrow/Lend Market endpoints
            ("GET", "/api/v1/borrowLend/markets/history"): "borrowHistoryQueryAll",
            ("POST", "/api/v1/borrowLend"): "borrowLendExecute",
            # RFQ (Request for Quote) endpoints
            ("GET", "/api/v1/rfq"): "rfqQueryAccount",  # Account RFQs
            ("GET", "/api/v1/rfq/all"): "rfqQueryAll",  # All open RFQs
            ("GET", "/api/v1/rfq/quote"): "quoteQueryAccount",  # Account quotes
            ("POST", "/api/v1/rfq"): "rfqSubmit",  # Submit RFQ
            ("POST", "/api/v1/rfq/quote"): "quoteSubmit",  # Submit quote
            # Legacy endpoints (for backward compatibility)
            ("GET", "/api/v1/deposits"): "depositQueryAll",
            ("GET", "/api/v1/withdrawals"): "withdrawalQueryAll",
            ("POST", "/api/v1/withdraw"): "withdraw",
            # System endpoints
            ("GET", "/api/v1/system"): "systemStatusQuery",
            # Market data (if signed)
            ("GET", "/api/v1/trades"): "fillHistoryQueryAll",
            ("GET", "/api/v1/klines"): "klineQuery",
        }

    def _get_instruction_for_endpoint(self, method: str, path: str) -> str:
        """Get the instruction string for a given method and path.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path or full URL

        Returns:
            Instruction string for signing

        Raises:
            APIError: If instruction not found for the endpoint

        """
        method_upper = method.upper()

        # Extract path component if a full URL is provided
        lookup_path = path
        if path.startswith(("http://", "https://")):
            from urllib.parse import urlparse

            parsed_url = urlparse(path)
            lookup_path = parsed_url.path

        # Try exact match first
        if (method_upper, lookup_path) in self.INSTRUCTION_MAP:
            return self.INSTRUCTION_MAP[(method_upper, lookup_path)]

        # Try to match path templates with variables
        for (map_method, map_path), instruction in self.INSTRUCTION_MAP.items():
            if map_method == method_upper:
                # Handle paths with variables like /api/v1/order/{orderId}
                # For now, simple prefix matching for common patterns
                if map_path.endswith("/{orderId}") and lookup_path.startswith(map_path[:-10]):
                    return instruction
                elif map_path.endswith("/{id}") and lookup_path.startswith(map_path[:-5]):
                    return instruction

        # If no match found, raise error
        raise APIError(
            f"Backpack instruction not found for {method_upper} {lookup_path}",
            code=APIErrorCode.INVALID_REQUEST.value,
        )

    def _build_content_part(
        self, method: str, params: dict[str, Any] | None, data: dict[str, Any] | None
    ) -> str:
        """Build content part for signing based on method and data."""
        if method.upper() == "GET" and params:
            filtered_params = {k: v for k, v in params.items() if v is not None}
            if filtered_params:
                stringified_params: dict[str, str] = {}
                for k, v_val in filtered_params.items():
                    if isinstance(v_val, bool):
                        # Backpack expects lowercase boolean strings for signatures
                        stringified_params[k] = "true" if v_val else "false"
                    else:
                        stringified_params[k] = str(v_val)
                return urllib.parse.urlencode(sorted(stringified_params.items()))
        elif method.upper() in ["POST", "PUT", "DELETE"] and data:
            # CRITICAL DISCOVERY: Backpack might expect ALL parameters (including POST body data)
            # to be sent as query parameters for signature generation, with empty POST body
            # This would explain why the working cassette shows "body: null"
            #
            # However, let's first try the standard approach with body data in signature
            filtered_data = {k: v for k, v in data.items() if v is not None}
            if filtered_data:
                # For POST/PUT/DELETE requests, Backpack expects the signature to be generated
                # from query string format (same as GET requests), not JSON format
                stringified_data: dict[str, str] = {}
                for k, v_val in filtered_data.items():
                    if isinstance(v_val, bool):
                        # Backpack expects lowercase boolean strings for signatures
                        stringified_data[k] = "true" if v_val else "false"
                    else:
                        stringified_data[k] = str(v_val)
                return urllib.parse.urlencode(sorted(stringified_data.items()))
        return ""

    def _build_string_to_sign(
        self, instruction_str: str, content_part_str: str, timestamp_ms: int, window_ms: int
    ) -> str:
        """Build the string to sign for authentication."""
        sign_payload_parts = [f"instruction={instruction_str}"]
        if content_part_str:
            sign_payload_parts.append(content_part_str)
        sign_payload_parts.append(f"timestamp={timestamp_ms}")
        sign_payload_parts.append(f"window={window_ms}")
        string_to_sign = "&".join(sign_payload_parts)
        return string_to_sign

    def _create_auth_headers(
        self, timestamp_ms: int, window_ms: int, signature_b64: str
    ) -> dict[str, str]:
        """Create authentication headers."""
        return {
            "X-API-Key": self._api_key_b64,
            "X-Timestamp": str(timestamp_ms),
            "X-Window": str(window_ms),
            "X-Signature": signature_b64,
        }

    def _merge_headers(
        self,
        headers: Mapping[str, Any] | None,
        auth_headers: dict[str, str],
        method: str,
        data: dict[str, Any] | None,
    ) -> dict[str, str]:
        """Merge existing headers with auth headers and add Content-Type if needed."""
        final_headers: dict[str, Any] = {}
        if headers:
            final_headers.update(headers)
        final_headers.update(auth_headers)

        if method.upper() in ["POST", "PUT", "DELETE"] and data:
            has_content_type = any(key.lower() == "content-type" for key in final_headers.keys())
            if not has_content_type:
                final_headers["Content-Type"] = "application/json; charset=utf-8"

        return {k: str(v) for k, v in final_headers.items()}

    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign a Backpack API request using ED25519.

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
            instruction_str = self._get_instruction_for_endpoint(method, path)
            timestamp_ms = int(time.time() * 1000)
            window_ms = 5000

            content_part_str = self._build_content_part(method, params, data)
            string_to_sign = self._build_string_to_sign(
                instruction_str, content_part_str, timestamp_ms, window_ms
            )

            signature_bytes = self._ed25519_private_key.sign(string_to_sign.encode("utf-8"))
            signature_b64 = base64.b64encode(signature_bytes).decode("utf-8")

            auth_headers = self._create_auth_headers(timestamp_ms, window_ms, signature_b64)
            final_headers_str = self._merge_headers(headers, auth_headers, method, data)

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
        """Generate ED25519 signature components for WebSocket subscription.

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
