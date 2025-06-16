"""Hyperliquid EIP-712 authentication implementation."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, TypeGuard

if TYPE_CHECKING:
    pass

import msgpack
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from eth_utils.conversions import to_hex
from eth_utils.crypto import keccak
from mnemonic import Mnemonic
from pydantic import SecretStr

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.hyperliquid.models.hl_eip712_models import (
    EIP712TypeField,
    HyperliquidAgentDomainData,
    HyperliquidAgentTypes,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


def address_to_bytes(address: str) -> bytes:
    """Convert an Ethereum address string to bytes.

    Args:
        address: Ethereum address string (with or without 0x prefix)

    Returns:
        Address as bytes

    """
    return bytes.fromhex(address[2:] if address.startswith("0x") else address)


class HyperliquidEip712Authenticator(IAuthenticator):
    """Authenticator for Hyperliquid API using EIP-712 Agent signatures."""

    def __init__(
        self,
        *,
        wallet_private_key_secret: SecretStr | None = None,
        chain_id: int,
        account_object: LocalAccount | None = None,
        passphrase_secret: SecretStr | None = None,
        is_mainnet_environment: bool = True,
        logger_param: logging.Logger | None = None,
    ) -> None:
        """Initialize the Hyperliquid authenticator with wallet credentials.

        Args:
            wallet_private_key_secret: Private key for wallet (if not using account_object)
            chain_id: Ethereum chain ID for EIP-712 signing
            account_object: Pre-configured LocalAccount (alternative to private key)
            passphrase_secret: Optional passphrase for enhanced security
            is_mainnet_environment: Whether connecting to mainnet (True) or testnet (False)
            logger_param: Optional logger instance for authentication events
        """
        self.logger = logger_param or get_logger(__name__)
        self._is_mainnet_env = is_mainnet_environment
        self._chain_id = chain_id

        self._validate_auth_parameters(wallet_private_key_secret, account_object)
        self._account = self._setup_account(
            wallet_private_key_secret, account_object, passphrase_secret
        )
        self._setup_wallet_properties()
        self._setup_nonce_management()
        self._setup_eip712_configuration(chain_id)

        self.logger.info(
            f"HyperliquidEip712Authenticator initialized for address: {self.wallet_address} "
            f"on chain_id: {self.chain_id}",
        )

    def _validate_auth_parameters(
        self,
        wallet_private_key_secret: SecretStr | None,
        account_object: LocalAccount | None,
    ) -> None:
        """Validate that exactly one authentication method is provided."""
        if not wallet_private_key_secret and not account_object:
            msg = "Either wallet_private_key_secret or account_object must be provided."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg}")
            raise ValueError(msg)
        if wallet_private_key_secret and account_object:
            msg = "Provide either wallet_private_key_secret or account_object, not both."
            self.logger.error(f"HyperliquidEip712Authenticator: {msg}")
            raise ValueError(msg)

    def _setup_account(
        self,
        wallet_private_key_secret: SecretStr | None,
        account_object: LocalAccount | None,
        passphrase_secret: SecretStr | None,
    ) -> LocalAccount:
        """Setup and validate the account object."""
        if wallet_private_key_secret:
            account = self._create_account_from_private_key(wallet_private_key_secret)
            if passphrase_secret:
                self._validate_passphrase(passphrase_secret)
            return account
        elif account_object:
            return account_object
        else:
            # This case should be impossible due to the initial checks
            impos_msg = "Internal error: Account could not be assigned despite checks."
            self.logger.critical(f"HyperliquidEip712Authenticator: {impos_msg}")
            raise RuntimeError(impos_msg)

    def _create_account_from_private_key(
        self,
        wallet_private_key_secret: SecretStr,
    ) -> LocalAccount:
        """Create a LocalAccount from a private key with validation."""
        try:
            private_key_str = wallet_private_key_secret.get_secret_value().strip()
            processed_pk_str = self._process_private_key_string(private_key_str)
            self._validate_private_key_format(processed_pk_str)

            # Cryptographic validation using eth_account
            try:
                account_obj: LocalAccount = Account.from_key(processed_pk_str)
                return account_obj
            except Exception as e:
                raise ValueError(
                    f"Hyperliquid private_key is not cryptographically valid: {e}",
                ) from e

        except ValueError as e:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Invalid private key: {e}",
                exc_info=True,
            )
            raise ValueError(f"Invalid private key: {e}") from e

    def _process_private_key_string(self, private_key_str: str) -> str:
        """Process private key string by removing 0x prefix if present."""
        return private_key_str[2:] if private_key_str.startswith("0x") else private_key_str

    def _validate_private_key_format(self, processed_pk_str: str) -> None:
        """Validate that private key is a 64-character hex string."""
        if not (
            len(processed_pk_str) == 64
            and all(c in "0123456789abcdefABCDEF" for c in processed_pk_str)
        ):
            raise ValueError(
                "Hyperliquid private_key must be a 64-character hex string "
                "(with or without '0x' prefix).",
            )

    def _validate_passphrase(self, passphrase_secret: SecretStr) -> None:
        """Validate the BIP-39 passphrase if provided."""
        try:
            phrase_str = passphrase_secret.get_secret_value().strip()
            self._validate_passphrase_word_count(phrase_str)
            self._validate_passphrase_bip39(phrase_str)
        except ValueError as e:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Invalid passphrase: {e}",
                exc_info=True,
            )
            raise ValueError(f"Invalid passphrase: {e}") from e

    def _validate_passphrase_word_count(self, phrase_str: str) -> None:
        """Validate that passphrase has correct word count."""
        num_words = len(phrase_str.split())
        if num_words not in (12, 24):
            raise ValueError(
                f"Hyperliquid passphrase must consist of 12 or 24 words, got {num_words} words.",
            )

    def _validate_passphrase_bip39(self, phrase_str: str) -> None:
        """Validate that passphrase is a valid BIP-39 mnemonic."""
        try:
            mnemonic_validator = Mnemonic("english")
            if not mnemonic_validator.check(phrase_str):
                raise ValueError(
                    "Hyperliquid passphrase is not a valid BIP-39 mnemonic "
                    "(checksum or wordlist error).",
                )
        except Exception as e:
            # Handle any other exceptions from mnemonic validation
            if "not a valid BIP-39 mnemonic" not in str(e):
                raise ValueError(
                    f"Error validating Hyperliquid passphrase with mnemonic library: {e}",
                ) from e
            raise

    def _setup_wallet_properties(self) -> None:
        """Setup wallet address properties."""
        self._wallet_address: str = self._account.address

    def _setup_nonce_management(self) -> None:
        """Setup nonce management for timestamp-based nonces."""
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

    def _setup_eip712_configuration(self, chain_id: int) -> None:
        """Setup EIP-712 domain and type configurations."""
        # Initialize EIP-712 domain data for Exchange/Agent scheme (sign_l1_action)
        # Use appropriate verifying contract based on environment
        verifying_contract = (
            "0x0000000000000000000000000000000000000000"
            if self._is_mainnet_env
            else "0x0000000000000000000000000000000000000000"
        )
        self._exchange_action_domain = HyperliquidAgentDomainData(
            name="Exchange",
            version="1",
            chainId=chain_id,
            verifyingContract=verifying_contract,
        )

        # Initialize EIP-712 types for Exchange/Agent scheme
        self._exchange_action_agent_types = HyperliquidAgentTypes(
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
        """Atomically generates a strictly increasing millisecond timestamp nonce."""
        async with self._nonce_lock:
            current_ms = int(time.time() * 1000)
            if current_ms <= self._last_nonce_ms:
                self._last_nonce_ms += 1
            else:
                self._last_nonce_ms = current_ms
            return self._last_nonce_ms

    def _clean_order_type_fields(self, data: dict[str, Any]) -> None:
        """Recursively clean None values from order type structures in JSON payload.

        This is specifically for Hyperliquid API which expects order types to have
        only the active field (limit OR market), not both with one as null.
        Also removes other null fields that should be omitted.
        """
        # Handle order type structures: {"limit": {...}, "market": null} -> {"limit": {...}}
        if "limit" in data and "market" in data:
            # This looks like an order type structure
            if data["limit"] is None and data["market"] is not None:
                del data["limit"]
            elif data["market"] is None and data["limit"] is not None:
                del data["market"]

        # Remove any null fields (except for specific cases where null is meaningful)
        keys_to_remove = []
        for key, value in data.items():
            if value is None:
                keys_to_remove.append(key)

        for key in keys_to_remove:
            del data[key]

        # Recursively clean nested structures
        for value in data.values():
            if self._is_dict_str_any(value):
                # TypeGuard confirms it's a dict[str, Any]
                self._clean_order_type_fields(value)
            elif self._is_list_any(value):
                # Use TypeGuard to properly type the list
                for item in value:
                    if self._is_dict_str_any(item):
                        # TypeGuard confirms it's a dict[str, Any]
                        self._clean_order_type_fields(item)

    def _is_ethereum_address(self, value: str) -> bool:
        """Check if a string looks like an Ethereum address."""
        return len(value) == 42 and value.lower().startswith("0x")

    @staticmethod
    def _is_list_any(value: object) -> TypeGuard[list[Any]]:
        """Type guard to check if value is a list."""
        return isinstance(value, list)

    @staticmethod
    def _is_dict_str_any(value: object) -> TypeGuard[dict[str, Any]]:
        """Type guard to check if value is a dict[str, Any]."""
        return isinstance(value, dict)

    def _lowercase_addresses_in_list(self, lst: list[Any]) -> None:
        """Process a list to lowercase Ethereum addresses."""
        for i in range(len(lst)):
            item = lst[i]
            if isinstance(item, str) and self._is_ethereum_address(item):
                lst[i] = item.lower()
            elif self._is_dict_str_any(item):
                self._lowercase_addresses_in_payload(item)
            elif self._is_list_any(item):
                # Recursive call for nested lists
                self._lowercase_addresses_in_list(item)

    def _lowercase_addresses_in_payload(self, data: dict[str, Any]) -> None:
        """Recursively convert Ethereum addresses to lowercase in the payload.

        This ensures consistent hashing as addresses are case-sensitive in msgpack.

        Args:
            data: Dictionary to process (modified in place)

        """
        # Process all key-value pairs
        for key, value in data.items():
            if isinstance(value, str) and self._is_ethereum_address(value):
                # This looks like an Ethereum address
                data[key] = value.lower()
            elif self._is_dict_str_any(value):
                # Recursively process nested dictionaries
                self._lowercase_addresses_in_payload(value)
            elif self._is_list_any(value):
                # Process the list
                self._lowercase_addresses_in_list(value)

    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign a Hyperliquid API request.

        For /exchange endpoint: Uses sign_l1_action scheme with msgpack-based action_hash
        and EIP-712 Agent signature in request body (no X-HL-* headers).

        For other endpoints: Currently raises NotImplementedError.

        Args:
            method: The HTTP method.
            path: The API endpoint path.
            params: Optional dictionary of query parameters.
            data: The dictionary payload for the action being signed (required for /exchange).
            headers: Optional dictionary of existing headers.

        Returns:
            An AuthenticatedRequestComponents TypedDict.

        Raises:
            APIError: If signing fails or account is not properly initialized.
            ValueError: If data requirements are not met.
            NotImplementedError: For non-/exchange paths.

        """
        if path.endswith("/exchange"):
            return await self._prepare_exchange_request(method, path, params, data, headers)
        else:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Signing for path {path} is not implemented. "
                f"Only /exchange endpoint is currently supported.",
            )
            raise NotImplementedError(
                f"Signing for path {path} is not implemented. "
                f"Only /exchange endpoint is supported.",
            )

    async def _prepare_exchange_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign a Hyperliquid /exchange request using sign_l1_action scheme.

        This implements the SDK's sign_l1_action flow:
        1. msgpack the action payload
        2. Hash action + nonce + vault_address + expires_after with keccak
        3. Use hash as connectionId in EIP-712 Agent message
        4. Sign Agent message with "Exchange" domain
        5. Return JSON body with action, nonce, signature (no X-HL-* headers)
        """
        # Input validation
        if not isinstance(data, dict):
            msg = (
                "Invalid 'data' for Hyperliquid /exchange request: "
                "Must be a dictionary (action payload)."
            )
            self.logger.error(f"HyperliquidEip712Authenticator: {msg} Received type: {type(data)}")
            raise ValueError(msg)

        # Prepare action payload
        action_payload_dict: dict[str, Any] = dict(data)
        self._clean_order_type_fields(action_payload_dict)

        # Address lowercasing: Convert any Ethereum addresses to lowercase for consistent hashing
        self._lowercase_addresses_in_payload(action_payload_dict)

        # Generate nonce
        current_nonce_ms: int = await self._get_next_nonce_ms()

        # Define vault_address and expires_after for hash (None for standard user trades)
        vault_address_for_hash: str | None = None
        expires_after_for_hash: int | None = None

        # Calculate action_hash (mimicking SDK's action_hash function)
        msgpacked_action = msgpack.packb(action_payload_dict)
        action_hash_data_parts: list[bytes] = [msgpacked_action]
        action_hash_data_parts.append(current_nonce_ms.to_bytes(8, "big"))

        if vault_address_for_hash is not None:
            action_hash_data_parts.append(b"\x01")
            action_hash_data_parts.append(address_to_bytes(vault_address_for_hash))
        else:
            action_hash_data_parts.append(b"\x00")

        # Note: expires_after handling - if None, nothing more is added
        if expires_after_for_hash is not None:
            action_hash_data_parts.append(b"\x00")
            action_hash_data_parts.append(expires_after_for_hash.to_bytes(8, "big"))

        action_hash_input_bytes: bytes = b"".join(action_hash_data_parts)
        action_hash_bytes: bytes = keccak(action_hash_input_bytes)

        # Construct phantom_agent_message for EIP-712
        source_char: str = "a" if self._is_mainnet_env else "b"  # "a" for mainnet, "b" for testnet
        phantom_agent_message: dict[str, Any] = {
            "source": source_char,
            "connectionId": action_hash_bytes,
        }

        # Construct structured_data_to_sign for EIP-712
        structured_data_to_sign = {
            "domain": self._exchange_action_domain.model_dump(by_alias=True),
            "message": phantom_agent_message,
            "primaryType": "Agent",
            "types": self._exchange_action_agent_types.model_dump(by_alias=True),
        }

        # Sign the message
        try:
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
            signed_message_obj = self._account.sign_message(signable_message)
            signature_dict: dict[str, Any] = {
                "r": to_hex(signed_message_obj.r),
                "s": to_hex(signed_message_obj.s),
                "v": signed_message_obj.v,
            }
        except Exception as e:
            self.logger.error(
                f"HyperliquidEip712Authenticator: Failed to sign Hyperliquid Exchange "
                f"Agent message: {e}",
                exc_info=True,
            )
            raise APIError(
                f"Failed to sign EIP-712 Agent request for /exchange: {e}",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        # Construct final HTTP JSON body
        final_http_body: dict[str, Any] = {
            "action": action_payload_dict,
            "nonce": current_nonce_ms,
            "signature": signature_dict,
        }
        if vault_address_for_hash:  # Only include if set
            final_http_body["vaultAddress"] = vault_address_for_hash

        # Prepare HTTP headers (no X-HL-* auth headers for /exchange)
        final_headers: dict[str, str] = {"Content-Type": "application/json"}
        if headers:
            for key, value in headers.items():
                if key != "Content-Type":
                    final_headers[str(key)] = str(value)

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=params,
            data=final_http_body,
        )
