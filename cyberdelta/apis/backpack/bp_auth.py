import hashlib
import hmac
import json  # For POST body serialization
import time
from typing import Any

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.models.api_error import APIError  # For raising errors
from cyberdelta.apis.models.api_error_codes import APIErrorCode  # For error codes
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)  # For logging potential issues


class BackpackHmacAuthenticator(IAuthenticator):
    """Authenticator for Backpack API using HMAC-SHA256."""

    def __init__(self, api_key: str, api_secret: str) -> None:
        """
        Initialize the authenticator with API key and secret.

        Args:
            api_key: The Backpack API key.
            api_secret: The Backpack API secret.
        """
        if not api_key:
            logger.error("API key cannot be empty for BackpackHmacAuthenticator.")
            raise ValueError("API key cannot be empty")
        if not api_secret:
            logger.error("API secret cannot be empty for BackpackHmacAuthenticator.")
            raise ValueError("API secret cannot be empty")

        self._api_key = api_key
        self._api_secret = api_secret

    def _hmac_sha256_hexdigest(self, key: bytes, msg: bytes) -> str:
        """
        Helper for HMAC-SHA256 signature generation. Returns a hex digest string.
        """
        # Pyright/Pylance cannot infer the type of hmac.new (C-extension);
        # this is a known false positive. This ignore is safe, does not affect mypy,
        # and is project-approved for stdlib cryptography edge cases.
        h: Any = hmac.new(key, msg, hashlib.sha256)  # pyright: ignore[reportUnknownMemberType]
        digest: str = h.hexdigest()
        return digest

    async def prepare_request(
        self,
        method: str,
        path: str,  # Path is not directly used in Backpack's signature, but good to have
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,  # Added type hint for headers
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs a Backpack API request using HMAC-SHA256.

        Args:
            method: The HTTP method (e.g., 'GET', 'POST').
            path: The API endpoint path (not directly used in BP signature construction).
            params: Optional dictionary of query parameters.
            data: Optional dictionary of request body data (for POST/PUT).
            headers: Optional dictionary of existing headers.

        Returns:
            An AuthenticatedRequestComponents TypedDict.

        Raises:
            APIError: If API key or secret are missing.
        """
        if not self._api_key or not self._api_secret:
            raise APIError(
                "Backpack API key and secret are required for signing requests.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        timestamp = str(int(time.time() * 1000))
        signature_payload_str = timestamp

        # Backpack's signature payload construction:
        # For GET: timestamp + sorted query string (e.g., "symbol=SOL_USDC&limit=10")
        # For POST/PUT/DELETE with JSON body: timestamp + JSON string of the body
        # For POST/PUT/DELETE with form data: timestamp + sorted form data string
        # (not handled here yet)

        # Ensure params and data are processed correctly for signature
        processed_params = params
        processed_data = data

        if method.upper() == "GET" and params:
            # Sort parameters by key for consistent signature generation
            query_string = "&".join([f"{k}={v}" for k, v in sorted(params.items())])
            signature_payload_str += query_string
        elif method.upper() in ["POST", "PUT", "DELETE"] and data:
            # Convert data to a compact JSON string without spaces for signature
            try:
                json_body_for_signature = json.dumps(data, separators=(",", ":"), sort_keys=True)
                signature_payload_str += json_body_for_signature
            except TypeError as e:
                logger.error(f"Error serializing data for signature: {e}. Data: {data}")
                raise APIError(
                    f"Failed to serialize request body for signature: {e}",
                    code=APIErrorCode.INVALID_PARAMS.value,
                    original_exception=e,
                ) from e
        # Else (e.g. POST with no body, or other methods), payload is just timestamp

        # DEBUG: Print the exact payload being signed
        print(f"[DEBUG BP_AUTH] Signing payload: '{signature_payload_str}'")

        signature = self._hmac_sha256_hexdigest(
            self._api_secret.encode("utf-8"), signature_payload_str.encode("utf-8")
        )

        auth_headers = {
            "X-Api-Key": self._api_key,
            "X-Timestamp": timestamp,
            "X-Signature": signature,
        }

        # Merge with existing headers, giving priority to auth_headers
        final_headers = {**(headers or {}), **auth_headers}
        # Ensure Content-Type for POST/PUT if data is present and not already set
        if method.upper() in ["POST", "PUT"] and data and "Content-Type" not in final_headers:
            final_headers["Content-Type"] = "application/json; charset=utf-8"

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=processed_params,  # Return original params, they are sent as query string
            data=processed_data,  # Return original data, it is sent as JSON body
        )
