from __future__ import annotations

import asyncio
import json
import time
from typing import Any

from eth_account import Account
from eth_account.messages import encode_typed_data
from web3 import Web3

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HL_Eip712Authenticator(IAuthenticator):
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

    def __init__(self, private_key_hex: str, wallet_address: str, chain_id: int) -> None:
        """
        Initializes the HL_Eip712Authenticator.

        Args:
            private_key_hex: The private key as a hexadecimal string (with or without '0x').
            wallet_address: The wallet address associated with the private key.
            chain_id: The chain ID for the EIP-712 signature.

        Raises:
            ValueError: If the private key is invalid or wallet address is empty.
        """
        if not private_key_hex:
            logger.error("HL_Eip712Authenticator: Private key cannot be empty.")
            raise ValueError("Private key cannot be empty.")
        if not wallet_address:
            logger.error("HL_Eip712Authenticator: Wallet address cannot be empty.")
            raise ValueError("Wallet address cannot be empty.")

        self._wallet_address = Web3.to_checksum_address(wallet_address)
        self._private_key_hex = (
            private_key_hex if private_key_hex.startswith("0x") else f"0x{private_key_hex}"
        )
        try:
            self._account = Account.from_key(self._private_key_hex)
            if self._account.address != self._wallet_address:
                err_msg = (
                    f"Provided private key does not match wallet address. "
                    f"Expected: {self._wallet_address}, Got: {self._account.address}"
                )
                logger.error(f"HL_Eip712Authenticator: {err_msg}")
                raise ValueError(err_msg)
        except (ValueError, TypeError) as e:
            logger.error(f"HL_Eip712Authenticator: Invalid private key provided: {e}")
            raise ValueError(f"Invalid private key: {e}") from e

        # Nonce strategy: use millisecond timestamp, ensuring strict increment if called rapidly.
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

        self._chain_id = chain_id
        self._domain_data = self._domain_template.copy()
        self._domain_data["chainId"] = chain_id

        logger.info(
            f"HL_Eip712Authenticator initialized for address: {self._wallet_address} on chain_id: {self._chain_id}"
        )

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
        data: dict[str, Any] | None,  # For Hyperliquid /exchange, this 'data' is the action payload
        headers: dict[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs a Hyperliquid API request using EIP-712 Agent signature.
        This signature is typically used for the /exchange endpoint.

        Args:
            method: The HTTP method (e.g., 'POST').
            path: The API endpoint path (e.g., '/exchange').
            params: Optional dictionary of query parameters.
            data: Dictionary of request body data (the 'action' payload for Hyperliquid).
            headers: Optional dictionary of existing headers.

        Returns:
            An AuthenticatedRequestComponents TypedDict.

        Raises:
            APIError: If data payload is missing for signing or if signing fails.
        """
        if data is None:  # The /exchange endpoint actions always require a data payload
            logger.error(
                "HL_Eip712Authenticator: Data payload (action) is required for signing Hyperliquid /exchange requests."
            )
            raise APIError(
                "Data payload (action) is required for Hyperliquid /exchange signed requests.",
                code=APIErrorCode.INVALID_PARAMS.value,
            )

        current_nonce_ms = await self._get_next_nonce_ms()
        connection_id_bytes = self._generate_connection_id(data)

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

        try:
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
            signed_message = self._account.sign_message(signable_message)
            signature_hex = signed_message.signature.hex()
        except Exception as e:
            logger.error(
                f"HL_Eip712Authenticator: Failed to sign Hyperliquid EIP-712 Agent message: {e}",
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
            data=data,  # Params and data are returned as is
        )


# Example of how connectionId is formed (for reference, not part of the class):
# action = {"type": "order", "orders": [...], "grouping": "na"}
# payload_string = json.dumps(action, separators=(",", ":"), sort_keys=True)
# connection_id = Web3.keccak(text=payload_string)
#
# Then the agent signature:
# agent = {"source": "a", "connectionId": connection_id.hex()}
# EIP712 sig of agent.
