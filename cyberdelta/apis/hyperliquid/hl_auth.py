from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Mapping
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
        *,
        wallet_private_key: str | None = None,
        account_object: LocalAccount | None = None,  # Added account_object
        chain_id: int,
        logger: logging.Logger | None = None,  # Renamed and made optional
    ) -> None:
        self.logger = logger or get_logger(__name__)  # Use provided or get new one

        if not wallet_private_key and not account_object:
            msg = "Either wallet_private_key or account_object must be provided."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg}")
            raise ValueError(msg)
        if wallet_private_key and account_object:
            msg = "Provide either wallet_private_key or account_object, not both."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg}")
            raise ValueError(msg)

        if wallet_private_key:
            try:
                # Ensure LocalAccount is used for type consistency if Account.from_key returns it
                # or can be cast to it, or if Account.from_key actually returns LocalAccount directly.
                # For now, assume Account.from_key provides a compatible type or is LocalAccount.
                self._account: LocalAccount = Account.from_key(wallet_private_key)
            except ValueError as e:
                self.logger.error(
                    f"HyperliquidEip712Authenticator: Invalid private key: {e}", exc_info=True
                )
                raise ValueError(f"Invalid private key: {e}") from e
        elif account_object:  # account_object is guaranteed to be non-None here
            self._account = account_object
        else:
            # This case should be impossible due to the initial checks
            # but added for exhaustive handling and to satisfy linters if they get confused.
            impos_msg = "Internal error: Account could not be assigned despite checks."
            self.logger.critical(f"HyperliquidEip712Authenticator: {impos_msg}")
            raise RuntimeError(impos_msg)

        # DEFENSIVE CHECK for mypy, should be always true after logic above
        if self._account is None:  # pyright: ignore[reportUnnecessaryComparison]
            # This path should be logically unreachable if above logic is correct.
            self.logger.error(
                "HyperliquidEip712Authenticator: Account object is None after initialization logic."
            )
            # Mypy: unreachable
            raise ValueError("Account object could not be initialized.")

        self._wallet_address: str = self._account.address  # Derive address
        self._chain_id: int = chain_id

        self.logger.info(
            f"HyperliquidEip712Authenticator initialized for address: {self.wallet_address} "
            f"on chain_id: {self.chain_id}"
        )

        # Nonce strategy: use millisecond timestamp, ensuring strict increment if called rapidly.
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

        self._domain_data = self._domain_template.copy()
        self._domain_data["chainId"] = chain_id

    @property
    def wallet_address(self) -> str:
        """The Ethereum wallet address associated with this authenticator."""
        return self._wallet_address

    @property
    def chain_id(self) -> int:
        """The chain ID this authenticator is configured for."""
        return self._chain_id

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
        data: dict[str, Any] | None,
        headers: dict[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs a Hyperliquid API request using EIP-712 Agent signature.
        This signature is typically used for the /exchange endpoint.
        The 'data' for Hyperliquid /exchange (when using this EIP-712 Agent signature)
        is expected to be the dictionary payload of the specific action (e.g., order details).

        Args:
            method: The HTTP method.
            path: The API endpoint path.
            params: Optional dictionary of query parameters.
            data: The dictionary payload for the action being signed.
                  For Hyperliquid Agent EIP-712, this MUST be a dict and not None.
            headers: Optional dictionary of existing headers.

        Returns:
            An AuthenticatedRequestComponents TypedDict.

        Raises:
            APIError: If signing fails or account is not properly initialized.
            ValueError: If data is None or not a dictionary, as it's required for HL Agent sig.
        """
        if data is None:
            msg = "Invalid 'data' for Hyperliquid EIP-712 Agent signature: Must be a dictionary and not None."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg} Received: {type(data)}")
            raise ValueError(msg)

        # At this point, 'data' is confirmed to be a dict (due to type hint and above check)
        # and is the action_payload.
        action_payload: Mapping[str, Any] = data

        current_nonce_ms = await self._get_next_nonce_ms()
        connection_id_bytes = self._generate_connection_id(dict(action_payload))

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

        if self._account is None:  # DEFENSIVE CHECK: Ensure _account is not None before signing.
            # Mypy: condition-always-false, Ruff: FBT003 (`self._account is None`)
            # This should not be reached if __init__ succeeded
            self.logger.error(
                "HyperliquidEip712Authenticator: Account not initialized, cannot sign message."
            )
            # Mypy: unreachable
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
            data=dict(action_payload),  # Return the action_payload as data
        )


# Example of how connectionId is formed (for reference, not part of the class):
# action = {"type": "order", "orders": [...], "grouping": "na"}
# payload_string = json.dumps(action, separators=(",", ":"), sort_keys=True)
# connection_id = Web3.keccak(text=payload_string)
#
# Then the agent signature:
# agent = {"source": "a", "connectionId": connection_id.hex()}
# EIP712 sig of agent.
