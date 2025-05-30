from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Mapping
from typing import Any, cast

from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from web3 import Web3

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.hyperliquid.models.hl_eip712_models import (
    EIP712DomainData,
    EIP712TypeField,
    EIP712Types,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidEip712Authenticator(IAuthenticator):
    """
    Authenticator for Hyperliquid API using EIP-712 Agent signatures.
    """

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
                # or can be cast to it, or if Account.from_key actually returns
                # LocalAccount directly.
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

        # Account is guaranteed to be non-None after successful initialization above

        self._wallet_address: str = self._account.address  # Derive address
        self._chain_id: int = chain_id

        self.logger.info(
            f"HyperliquidEip712Authenticator initialized for address: {self.wallet_address} "
            f"on chain_id: {self.chain_id}"
        )

        # Nonce strategy: use millisecond timestamp, ensuring strict increment if called rapidly.
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

        # Initialize EIP-712 domain data using Pydantic model
        self._domain_data_model = EIP712DomainData(
            name="Hyperliquid",
            version="1",
            chainId=chain_id,
            verifyingContract="0x0000000000000000000000000000000000000000",
        )

        # Initialize EIP-712 types using Pydantic model
        self._eip712_types_model = EIP712Types(
            EIP712Domain=[
                EIP712TypeField(name="name", type="string"),
                EIP712TypeField(name="version", type="string"),
                EIP712TypeField(name="chainId", type="uint256"),
                EIP712TypeField(name="verifyingContract", type="address"),
            ],
            Agent=[
                EIP712TypeField(name="source", type="string"),
                EIP712TypeField(name="connectionId", type="bytes32"),
            ],
        )

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

    def _clean_order_type_fields(self, data: dict[str, Any]) -> None:
        """
        Recursively clean None values from order type structures in JSON payload.
        This is specifically for Hyperliquid API which expects order types to have
        only the active field (limit OR market), not both with one as null.
        """
        # Handle order type structures: {"limit": {...}, "market": null} -> {"limit": {...}}
        if "limit" in data and "market" in data:
            # This looks like an order type structure
            if data["limit"] is None and data["market"] is not None:
                del data["limit"]
            elif data["market"] is None and data["limit"] is not None:
                del data["market"]

        # Recursively clean nested structures
        for value in data.values():
            if isinstance(value, dict):
                # Cast to proper type since isinstance check confirms it's a dict
                self._clean_order_type_fields(cast(dict[str, Any], value))
            elif isinstance(value, list):
                list_value = cast(list[Any], value)  # type: ignore[redundant-cast]
                for item in list_value:
                    if isinstance(item, dict):
                        # Cast to proper type since isinstance check confirms it's a dict
                        self._clean_order_type_fields(cast(dict[str, Any], item))

    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
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
        if not isinstance(data, dict):
            msg = "Invalid 'data' for Hyperliquid EIP-712 Agent signature: Must be a dictionary."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg} Received type: {type(data)}")
            raise ValueError(msg)

        # At this point, 'data' is confirmed to be a dict (due to type hint and above check)
        # and is the action_payload.
        # Create a mutable copy for potential cleaning
        action_payload_dict: dict[str, Any] = dict(data)

        # Apply Hyperliquid-specific cleaning for order type fields
        # This handles cases where serialize_none_as_null=True leaves null values
        # that Hyperliquid expects to be absent
        self._clean_order_type_fields(action_payload_dict)

        action_payload: Mapping[str, Any] = action_payload_dict

        # Capture current time for timestamp BEFORE getting potentially incremented nonce
        current_timestamp_ms = int(time.time() * 1000)
        current_nonce_ms = await self._get_next_nonce_ms()
        connection_id_bytes = self._generate_connection_id(dict(action_payload))

        # Construct the EIP-712 message for Agent signature
        # Source "a" is commonly used for agent signatures by exchanges like Hyperliquid
        structured_data_to_sign = {
            "domain": self._domain_data_model.model_dump(by_alias=True),
            "message": {
                "source": "a",  # Per prompt and common convention for agent type
                "connectionId": connection_id_bytes,
            },
            "primaryType": "Agent",  # Must match the key in `types`
            "types": self._eip712_types_model.model_dump(by_alias=True),
        }

        # Account is guaranteed to be non-None after successful initialization

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
        updated_headers = dict(headers) if headers else {}
        # Hyperliquid expects headers with "X-HL-" prefix for agent signature components
        updated_headers.update(
            {
                "X-HL-Timestamp": str(current_timestamp_ms),  # Use actual captured timestamp
                "X-HL-Nonce": str(current_nonce_ms),  # Use strictly increasing nonce
                "X-HL-Signature": signature_hex,
            }
        )

        # Ensure all header values are strings
        final_headers: dict[str, str] = {str(k): str(v) for k, v in updated_headers.items()}

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=params,
            data=action_payload_dict,  # Return the cleaned action_payload as data
        )


# Example of how connectionId is formed (for reference, not part of the class):
# action = {"type": "order", "orders": [...], "grouping": "na"}
# payload_string = json.dumps(action, separators=(",", ":"), sort_keys=True)
# connection_id = Web3.keccak(text=payload_string)
#
# Then the agent signature:
# agent = {"source": "a", "connectionId": connection_id.hex()}
# EIP712 sig of agent.
