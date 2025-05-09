from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from web3 import Web3

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidEip712Authenticator(IAuthenticator):
    """
    Authenticator for Hyperliquid API using EIP-712 Agent signatures.
    """

    # Standard EIP712 domain structure
    _domain_template = {
        "name": "Hyperliquid",  # As per Hyperliquid's typical agent signature
        "version": "1",
        "chainId": 0,  # Will be replaced by actual chain_id
        "verifyingContract": "0x0000000000000000000000000000000000000000",  # Standard placeholder
    }

    # EIP712 types for Agent signature
    _agent_typed_data_message_types = {
        "EIP712Domain": [
            {"name": "name", "type": "string"},
            {"name": "version", "type": "string"},
            {"name": "chainId", "type": "uint256"},
            {"name": "verifyingContract", "type": "address"},
        ],
        "Agent": [  # This structure is crucial for Hyperliquid Agent signature
            {"name": "source", "type": "string"},
            {"name": "connectionId", "type": "bytes32"},
        ],
    }

    def __init__(
        self,
        private_key_hex: str,
        wallet_address: str,
        chain_id: int,
        logger_override: logging.Logger | None = None,  # Added for testing/flexibility
    ) -> None:
        self.logger = logger_override or logger
        self._wallet_address = wallet_address
        self._chain_id = chain_id
        self._account: LocalAccount | None = None  # Store the account object
        try:
            self._account = Account.from_key(private_key_hex)
        except ValueError as e:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Invalid private key: {e}", exc_info=True
            )
            raise ValueError(f"Invalid private key: {e}") from e

        if (
            self._account is None
        ):  # DEFENSIVE CHECK: Ensure _account is not None before access. Mypy=[None] Ruff=[None]
            # This case should ideally be prevented by the previous error handling,
            # but as a safeguard:
            self.logger.error(
                "HyperliquidEip712Authenticator: Account object not created due to earlier error."
            )
            raise ValueError("Account object not created, cannot proceed.")

        if self._account.address.lower() != self._wallet_address.lower():
            self.logger.error(
                f"HyperliquidEip712Authenticator: Wallet address mismatch. "
                f"Provided: {self._wallet_address}, Derived: {self._account.address}"
            )
            raise ValueError(
                "Provided wallet address does not match the one derived from the private key."
            )
        self.logger.info(
            f"HyperliquidEip712Authenticator initialized for address: {self._wallet_address} "
            f"on chain_id: {self._chain_id}"
        )

        # Nonce strategy: use millisecond timestamp, ensuring strict increment if called rapidly.
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

        self._domain_data = self._domain_template.copy()
        self._domain_data["chainId"] = chain_id

    async def _get_next_nonce_ms(self) -> int:
        """
        Atomically generates a strictly increasing millisecond timestamp nonce.
        """
        async with self._nonce_lock:
            current_ms = int(time.time() * 1000)
            if current_ms <= self._last_nonce_ms:
                self._last_nonce_ms += 1
            else:
                self._last_nonce_ms = current_ms
            return self._last_nonce_ms

    def _generate_connection_id(self, data_payload: dict[str, Any]) -> bytes:
        """
        Generates the connectionId for EIP-712 Agent signature.
        connectionId = keccak(payload_string)
        payload_string is the JSON string of the data_payload (request body/action).
        """
        # Sort keys for deterministic output, then dump to JSON string, then encode to bytes
        # No spaces after separators for compact representation, as often expected.
        json_string = json.dumps(data_payload, sort_keys=True, separators=(",", ":"))
        return Web3.keccak(text=json_string)  # Use Web3.keccak as per HL spec

    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: str
        | dict[str, Any]
        | None,  # For Hyperliquid /exchange, this 'data' is the action payload
        headers: dict[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs a Hyperliquid API request using EIP-712 Agent signature.
        This signature is typically used for the /exchange endpoint.

        Args:
            method: The HTTP method (e.g., 'POST').
            path: The API endpoint path (e.g., '/exchange').
            params: Optional dictionary of query parameters.
            data: Dictionary of request body data (the 'action' payload for
                Hyperliquid) or JSON string.
            headers: Optional dictionary of existing headers.

        Returns:
            An AuthenticatedRequestComponents TypedDict.

        Raises:
            APIError: If data payload is missing for signing or if signing fails.
        """
        print(
            f"[DEBUG_HL_AUTH_PREPARE_REQUEST_ENTRY] data type: {type(data)}, data value: {data!r}"
        )  # DEBUG

        action_dict: dict[str, Any]
        if isinstance(data, str):
            print(f"[DEBUG_HL_AUTH] isinstance(data, str) is TRUE. data: {data!r}")  # DEBUG
            try:
                print("[DEBUG_HL_AUTH] Attempting json.loads(data)")  # DEBUG
                action_dict = json.loads(data)
                print("[DEBUG_HL_AUTH] json.loads(data) SUCCEEDED.")  # DEBUG
            except json.JSONDecodeError as e:
                print(f"[DEBUG_HL_AUTH] json.loads(data) FAILED with {e!r}")  # DEBUG
                self.logger.error(f"Invalid JSON in data payload for signing: {e}", exc_info=True)
                raise APIError(
                    f"Action data payload is an invalid JSON string: {data!r}.",
                    code=APIErrorCode.INVALID_PARAMS.value,
                    http_status=400,
                    original_exception=e,
                ) from e
        elif isinstance(data, dict):
            action_dict = data
        elif data is None:  # The /exchange endpoint actions always require a data payload
            self.logger.error(
                "HyperliquidEip712Authenticator: Data payload (action) is required "
                "for signing Hyperliquid /exchange requests."
            )
            raise APIError(
                "Data payload (action) required for Hyperliquid signed request.",
                code=APIErrorCode.INVALID_PARAMS.value,
            )

        current_nonce_ms = await self._get_next_nonce_ms()
        connection_id_bytes = self._generate_connection_id(action_dict)

        # Construct the EIP-712 message for Agent signature
        # Source "a" is commonly used for agent signatures by exchanges like Hyperliquid
        structured_data_to_sign = {
            "domain": self._domain_data,
            "message": {
                "source": "a",  # Per prompt and common convention for agent type
                "connectionId": connection_id_bytes,
            },
            "primaryType": "Agent",  # Must match the key in `types`
            "types": self._agent_typed_data_message_types,
        }

        if (
            self._account is None
        ):  # DEFENSIVE CHECK: Ensure _account is not None before signing. Mypy=[None] Ruff=[None]
            # This should not be reached if __init__ succeeded
            logger.error(
                "HyperliquidEip712Authenticator: Account not initialized, cannot sign message."
            )
            raise APIError(
                "Authenticator account not initialized.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        try:
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
            signed_message = self._account.sign_message(signable_message)
            # Mypy might complain here if it can't infer _account is definitely not None
            # despite the check. The type: ignore might be needed if a simple check isn't enough.
            # However, the explicit `if self._account is None:` check above should satisfy mypy.
            # If not, and mypy still flags attr-defined on a checked Optional, this is
            # a Mypy limitation.
            # Per RULE-RUNTIME-SAFETY-V4, the runtime check is paramount.
            # We will not use cast or ignore if the explicit check is present.

            signature_hex = signed_message.signature.hex()
        except Exception as e:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Failed to sign Hyperliquid EIP-712 "
                f"Agent message: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to sign EIP-712 Agent request: {e}",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        # Prepare headers for the authenticated request
        updated_headers = (headers or {}).copy()
        # Hyperliquid expects headers with "X-HL-" prefix for agent signature components
        updated_headers.update(
            {
                "X-HL-Timestamp": str(current_nonce_ms),  # Timestamp is used as nonce
                "X-HL-Nonce": str(current_nonce_ms),  # Nonce is also the timestamp
                "X-HL-Signature": signature_hex,
            }
        )

        # Ensure all header values are strings
        final_headers: dict[str, str] = {str(k): str(v) for k, v in updated_headers.items()}

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=params,
            data=action_dict,  # Return the parsed dict if original data was string
        )


# Example of how connectionId is formed (for reference, not part of the class):
# action = {"type": "order", "orders": [...], "grouping": "na"}
# payload_string = json.dumps(action, separators=(",", ":"), sort_keys=True)
# connection_id = Web3.keccak(text=payload_string)
#
# Then the agent signature:
# agent = {"source": "a", "connectionId": connection_id.hex()}
# EIP712 sig of agent.
