"""CyberDeltaEngine: Hyperliquid EIP-712 Authentication.

------------------------------------------------

This module provides authentication capabilities for the Hyperliquid exchange using
EIP-712 signing standard. It handles wallet setup, nonce management, and request
signing for both mainnet and testnet environments.

Core Features:
- EIP-712 structured data signing for exchange requests
- Secure wallet management with multiple initialization options
- Automatic nonce management with timestamp-based sequences
- Comprehensive validation and error handling

Security Notes:
- Private keys are handled securely via SecretStr and LocalAccount
- No sensitive data is logged in production
- All signatures use cryptographically secure methods
"""

from __future__ import annotations

import asyncio
import string
import time
from typing import TYPE_CHECKING, Any, Protocol

import msgpack
import structlog
from eth_account import Account
from eth_account.messages import encode_typed_data
from eth_account.signers.local import LocalAccount
from eth_utils.conversions import to_hex
from eth_utils.crypto import keccak
from mnemonic import Mnemonic
from pydantic import BaseModel, SecretStr

from cyberdelta.apis.base.authenticator_interface import (
    AuthenticatedRequestComponents,
    IAuthenticator,
)
from cyberdelta.apis.base.network_security_domain import NetworkEnvironment
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import InvalidPrivateKeyError
from cyberdelta.apis.hyperliquid.models.hl_eip712_models import (
    EIP712TypeField,
    HyperliquidAgentDomainData,
    HyperliquidAgentTypes,
    HyperliquidUsdClassTransferTypes,
    HyperliquidUserDomainData,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.exceptions.base import RequiredParameterError
from cyberdelta.exceptions.field_validation import InvalidFormatError, PassphraseFieldError
from cyberdelta.utils.typing import is_dict_str_any


if TYPE_CHECKING:
    from collections.abc import Mapping


logger = get_logger(__name__)

# Cryptographic constants
PRIVATE_KEY_HEX_LENGTH = 64  # Length of private key in hex characters
SIGNATURE_HEX_LENGTH = 66  # Length of signature components (0x + 64 hex chars)


class SignatureObject(Protocol):
    """Protocol for signature objects from eth_account signing."""

    @property
    def r(self) -> int:
        """ECDSA signature r component."""
        ...

    @property
    def s(self) -> int:
        """ECDSA signature s component."""
        ...

    @property
    def v(self) -> int:
        """ECDSA signature recovery id."""
        ...


def address_to_bytes(address: str) -> bytes:
    """Convert an Ethereum address string to bytes.

    Args:
        address: Ethereum address string (with or without 0x prefix)

    Returns:
        Address as bytes

    """
    return bytes.fromhex(address.removeprefix("0x"))


class HyperliquidEip712Authenticator(IAuthenticator):
    """Authenticator for Hyperliquid API using EIP-712 Agent signatures."""

    def __init__(
        self,
        *,
        wallet_private_key_secret: SecretStr | None = None,
        chain_id: int,
        account_object: LocalAccount | None = None,
        passphrase_secret: SecretStr | None = None,
        network_environment: NetworkEnvironment,
        logger_param: structlog.BoundLogger | None = None,
    ) -> None:
        """Initialize the Hyperliquid authenticator with wallet credentials.

        Args:
            wallet_private_key_secret: Private key for wallet (if not using account_object)
            chain_id: Ethereum chain ID for EIP-712 signing
            account_object: Pre-configured LocalAccount (alternative to private key)
            passphrase_secret: Optional passphrase for enhanced security
            network_environment: Network environment configuration
            logger_param: Optional logger instance for authentication events
        """
        self.logger = logger_param or get_logger(__name__)
        self._network_environment = network_environment
        self._chain_id = chain_id

        self._validate_auth_parameters(wallet_private_key_secret, account_object)
        self._account = self._setup_account(
            wallet_private_key_secret,
            account_object,
            passphrase_secret,
        )
        self._setup_wallet_properties()
        self._setup_nonce_management()
        self._setup_eip712_configuration(chain_id)

        self.logger.info(
            "authenticator_initialized",
            wallet_address=self.wallet_address,
            chain_id=self.chain_id,
            message="HyperliquidEip712Authenticator initialized for address: %s on chain_id: %s",
            message_args=(self.wallet_address, self.chain_id),
        )

    def _validate_auth_parameters(
        self,
        wallet_private_key_secret: SecretStr | None,
        account_object: LocalAccount | None,
    ) -> None:
        """Validate that exactly one authentication method is provided.

        Args:
            wallet_private_key_secret: Optional wallet private key
            account_object: Optional LocalAccount object

        Raises:
            ValueError: If both or neither authentication methods are provided
        """
        if not wallet_private_key_secret and not account_object:
            msg = "Either wallet_private_key_secret or account_object must be provided."
            self.logger.error(
                "auth_parameter_validation_failed",
                action="validate_auth_parameters",
                error_type="missing_credentials",
                message=f"HyperliquidEip712Authenticator: {msg}",
            )
            raise RequiredParameterError(
                parameter="wallet_private_key_secret or account_object",
                context="authentication initialization",
                exchange="Hyperliquid",
            )
        if wallet_private_key_secret and account_object:
            msg = "Provide either wallet_private_key_secret or account_object, not both."
            self.logger.error(
                "auth_parameter_validation_failed",
                action="validate_auth_parameters",
                error_type="multiple_credentials",
                message=f"HyperliquidEip712Authenticator: {msg}",
            )
            raise RequiredParameterError(
                parameter="wallet_private_key_secret or account_object",
                context="authentication initialization (only one allowed)",
                exchange="Hyperliquid",
            )

    def _setup_account(
        self,
        wallet_private_key_secret: SecretStr | None,
        account_object: LocalAccount | None,
        passphrase_secret: SecretStr | None,
    ) -> LocalAccount:
        """Setup and validate the account object.

        Args:
            wallet_private_key_secret: Optional wallet private key
            account_object: Optional LocalAccount object
            passphrase_secret: Optional passphrase for additional validation

        Returns:
            Configured LocalAccount instance

        Raises:
            RuntimeError: If account cannot be assigned despite validation
        """
        if wallet_private_key_secret:
            account = self._create_account_from_private_key(wallet_private_key_secret)
            if passphrase_secret:
                self._validate_passphrase(passphrase_secret)
            return account
        if account_object:
            return account_object
        # This case should be impossible due to the initial checks
        impos_msg = "Internal error: Account could not be assigned despite checks."
        self.logger.critical(
            "account_assignment_impossible",
            action="setup_account",
            error_type="internal_error",
            message=f"HyperliquidEip712Authenticator: {impos_msg}",
        )
        raise RuntimeError(impos_msg)

    def _create_account_from_private_key(
        self,
        wallet_private_key_secret: SecretStr,
    ) -> LocalAccount:
        """Create a LocalAccount from a private key with validation.

        Args:
            wallet_private_key_secret: The private key as a SecretStr

        Returns:
            LocalAccount instance created from the private key

        Raises:
            ValueError: If private key format is invalid or creation fails
        """
        try:
            private_key_str = wallet_private_key_secret.get_secret_value().strip()
            processed_pk_str = self._process_private_key_string(private_key_str)
            self._validate_private_key_format(processed_pk_str)

            # Cryptographic validation using eth_account
            try:
                account_obj: LocalAccount = Account.from_key(processed_pk_str)
            except Exception as e:
                raise InvalidPrivateKeyError(
                    reason=f"not cryptographically valid: {e}",
                    original_error=e,
                ) from e

        except ValueError as e:
            self.logger.exception(
                "private_key_validation_failed",
                error_details=str(e),
                message=f"HyperliquidEip712Authenticator: Invalid private key: {e!s}",
            )
            raise InvalidPrivateKeyError(
                reason=str(e),
                original_error=e,
            ) from e
        else:
            return account_obj

    def _process_private_key_string(self, private_key_str: str) -> str:
        """Process private key string by removing 0x prefix if present."""
        return private_key_str.removeprefix("0x")

    def _validate_private_key_format(self, processed_pk_str: str) -> None:
        """Validate that private key is a 64-character hex string."""
        if not (
            len(processed_pk_str) == PRIVATE_KEY_HEX_LENGTH
            and all(c in string.hexdigits for c in processed_pk_str)
        ):
            raise InvalidPrivateKeyError(
                reason="must be a 64-character hex string (with or without '0x' prefix)"
            )

    def _validate_passphrase(self, passphrase_secret: SecretStr) -> None:
        """Validate the BIP-39 passphrase if provided."""
        try:
            phrase_str = passphrase_secret.get_secret_value().strip()
            self._validate_passphrase_word_count(phrase_str)
            self._validate_passphrase_bip39(phrase_str)
        except ValueError as e:
            self.logger.exception(
                "passphrase_validation_failed",
                error_details=str(e),
                message=f"HyperliquidEip712Authenticator: Invalid passphrase: {e!s}",
            )
            raise PassphraseFieldError(
                reason=str(e),
                original_error=e,
            ) from e

    def _validate_passphrase_word_count(self, phrase_str: str) -> None:
        """Validate that passphrase has correct word count."""
        num_words = len(phrase_str.split())
        if num_words not in {12, 24}:
            raise PassphraseFieldError(
                reason="must consist of 12 or 24 words",
                word_count=num_words,
            )

    def _check_bip39_mnemonic(self, phrase_str: str, mnemonic_validator: Mnemonic) -> bool:
        """Check if passphrase is a valid BIP-39 mnemonic.

        Returns:
            True if valid, False otherwise.
        """
        return mnemonic_validator.check(phrase_str)

    def _validate_passphrase_bip39(self, phrase_str: str) -> None:
        """Validate that passphrase is a valid BIP-39 mnemonic."""
        try:
            mnemonic_validator = Mnemonic("english")
            is_valid = self._check_bip39_mnemonic(phrase_str, mnemonic_validator)
        except Exception as e:
            # Handle any other exceptions from mnemonic validation
            raise PassphraseFieldError(
                reason=f"error validating with mnemonic library: {e}",
                original_error=e,
            ) from e

        if not is_valid:
            raise PassphraseFieldError(
                reason="not a valid BIP-39 mnemonic (checksum or wordlist error)"
            )

    def _setup_wallet_properties(self) -> None:
        """Setup wallet address properties."""
        # CRITICAL: Lowercase address as recommended by SDK docs
        # "Issues with upper case characters in address fields. It is recommended to
        # lowercase any address before signing and sending."
        self._wallet_address: str = self._account.address.lower()

    def _setup_nonce_management(self) -> None:
        """Setup nonce management for timestamp-based nonces."""
        self._last_nonce_ms: int = 0
        self._nonce_lock = asyncio.Lock()

    def _setup_eip712_configuration(self, chain_id: int) -> None:
        """Setup EIP-712 domain and type configurations."""
        # Initialize EIP-712 domain data for Exchange/Agent scheme (sign_l1_action)
        # Use appropriate verifying contract based on environment
        verifying_contract = "0x0000000000000000000000000000000000000000"
        # IMPORTANT: Hyperliquid SDK hardcodes chainId to 1337 for the Exchange domain
        # regardless of mainnet/testnet
        self._exchange_action_domain = HyperliquidAgentDomainData(
            name="Exchange",
            version="1",
            chainId=1337,  # Always 1337 for Exchange domain
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

        # Initialize EIP-712 domain for user signed actions (sign_user_signed_action)
        self._user_action_domain = HyperliquidUserDomainData(
            name="HyperliquidSignTransaction",
            version="1",
            chainId=self._chain_id,  # Use actual chain ID
            verifyingContract=verifying_contract,
        )

        # Initialize EIP-712 types for USD class transfer (matches SDK exactly)
        self._usd_class_transfer_types = HyperliquidUsdClassTransferTypes(
            EIP712Domain=[
                EIP712TypeField(name="name", type="string"),
                EIP712TypeField(name="version", type="string"),
                EIP712TypeField(name="chainId", type="uint256"),
                EIP712TypeField(name="verifyingContract", type="address"),
            ],
            **{
                "HyperliquidTransaction:UsdClassTransfer": [
                    EIP712TypeField(name="hyperliquidChain", type="string"),
                    EIP712TypeField(name="amount", type="string"),
                    EIP712TypeField(name="toPerp", type="bool"),
                    EIP712TypeField(name="nonce", type="uint64"),
                ]
            },
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

    # REMOVED: Business logic methods moved to HyperliquidPayloadSigningMapper
    # This maintains proper separation of concerns:
    # - RequestBuilder: Creates validated Raw models directly (INTERNAL → RAW)
    # - PayloadSigningMapper: Handles payload conversion and cleaning for signing
    # - Authenticator: Only signs, no data transformation or business logic

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
        self.logger.error(
            "path_signing_not_implemented",
            path=path,
            message=(
                "HyperliquidEip712Authenticator: Signing for path %s is not implemented. "
                "Only /exchange endpoint is currently supported."
            ),
            message_args=(path,),
        )
        path_not_implemented_msg = (
            f"Signing for path {path} is not implemented. Only /exchange endpoint is supported."
        )
        raise NotImplementedError(path_not_implemented_msg)

    def _validate_exchange_request_data(self, data: dict[str, Any] | None) -> None:
        """Validate exchange request data format."""
        if not isinstance(data, dict):
            msg = (
                "Invalid 'data' for Hyperliquid /exchange request: "
                "Must be a dictionary (action payload)."
            )
            self.logger.error(
                "exchange_request_data_validation_failed",
                action="validate_exchange_request_data",
                data_type=str(type(data)),
                expected_type="dict",
                message=(f"HyperliquidEip712Authenticator: {msg} Received type: {type(data)}"),
            )
            data_type_error_msg = msg
            raise TypeError(data_type_error_msg)

    def _prepare_action_payload(self, data: dict[str, Any] | BaseModel) -> dict[str, Any]:
        """Prepare the action payload for signing using Pydantic model serialization.

        This method ensures all payloads go through proper Pydantic validation
        and serialization, providing consistent signing behavior.

        For Hyperliquid, we need to use the actual field names (not aliases) because
        the field names are the short ones (a, b, p, etc.) that Hyperliquid expects.
        """
        self.logger.debug(
            "action_payload_processing_started",
            action="prepare_action_payload",
            data_type=str(type(data)),
            message=f"[HL_AUTH] Input data type: {type(data)}",
        )
        self.logger.debug(
            "data_model_dump_check",
            action="prepare_action_payload",
            has_model_dump=hasattr(data, "model_dump"),
            message="[HL_AUTH] Input data has model_dump: {}".format(hasattr(data, "model_dump")),
        )

        # If the data is already a Pydantic model, serialize it directly
        if isinstance(data, BaseModel):
            return self._serialize_pydantic_model(data)

        # For dict data, ensure we match SDK behavior by removing None 'c' fields
        self.logger.debug(
            "processing_dict_data",
            action="prepare_action_payload",
            data_keys=list(data.keys()),
            message=f"[HL_AUTH] Processing dict data: {data}",
        )
        self._process_dict_orders(data)

        # Return the dict directly
        self.logger.debug(
            "dict_data_returned",
            action="prepare_action_payload",
            message="[HL_AUTH] Returning dict data",
        )
        return data

    def _serialize_pydantic_model(self, data: BaseModel) -> dict[str, Any]:
        """Serialize a Pydantic model for signing."""
        # For Hyperliquid, use by_alias=False to get the short field names (a, b, p, etc.)
        # CRITICAL: Do NOT exclude None values as Hyperliquid expects all fields
        # for msgpack order
        result = data.model_dump(by_alias=False, exclude_none=False, mode="python")
        self.logger.debug(
            "pydantic_model_serialized",
            action="serialize_pydantic_model",
            result_keys=list(result.keys()),
            message=f"[HL_AUTH] Pydantic model_dump result: {result}",
        )
        self.logger.debug(
            "model_fields_enumerated",
            action="serialize_pydantic_model",
            fields=list(result.keys()),
            message=f"[HL_AUTH] All model fields present: {list(result.keys())}",
        )

        # Process nested order objects - CRITICAL: Remove 'c' field if None to match SDK
        if result.get("orders"):
            self._process_order_items(result["orders"])

        return result

    def _process_order_items(self, orders: list[object]) -> None:
        """Process order items to ensure proper structure."""
        for i, order_item in enumerate(orders):
            # Force serialize the order item to ensure proper structure
            if isinstance(order_item, BaseModel):
                order_item_dict = order_item.model_dump(
                    by_alias=False,
                    exclude_none=True,
                    mode="python",
                )
                orders[i] = order_item_dict
                self.logger.debug(
                    "order_serialized",
                    order_index=i,
                    fields=list(order_item_dict.keys()),
                    message=(
                        f"[HL_AUTH] Serialized order {i} with fields: "
                        f"{list(order_item_dict.keys())}"
                    ),
                )
            elif is_dict_str_any(order_item):
                # order_item is now properly typed as dict[str, Any] due to TypeGuard
                self._remove_none_c_field(order_item, i)

    def _remove_none_c_field(self, order_item: dict[str, Any], index: int) -> None:
        """Remove 'c' field if it's None to match SDK behavior."""
        if "c" in order_item and order_item["c"] is None:
            del order_item["c"]
            self.logger.debug(
                "none_c_field_removed",
                action="remove_none_c_field",
                order_index=index,
                message=f"[HL_AUTH] Removed None 'c' field from order {index}",
            )
        self.logger.debug(
            "order_fields_listed",
            action="remove_none_c_field",
            order_index=index,
            fields=list(order_item.keys()),
            message=f"[HL_AUTH] Order {index} fields: {list(order_item.keys())}",
        )

    def _process_dict_orders(self, data: dict[str, Any]) -> None:
        """Process orders in dict data to remove None 'c' fields."""
        if data.get("orders"):
            for i, order_item in enumerate(data["orders"]):
                if is_dict_str_any(order_item):
                    # order_item is now properly typed as dict[str, Any] due to TypeGuard
                    self._remove_none_c_field(order_item, i)

    def _compute_action_hash(
        self,
        action_payload_dict: dict[str, Any],
        current_nonce_ms: int,
    ) -> bytes:
        """Compute action hash using msgpack and keccak."""
        try:
            # Log the exact payload structure being msgpacked
            self.logger.debug(
                "action_payload_structure_logged",
                action="compute_action_hash",
                payload_keys=list(action_payload_dict.keys()),
                message=(f"[HL_AUTH] Action payload keys: {list(action_payload_dict.keys())}"),
            )
            if action_payload_dict.get("orders"):
                order = action_payload_dict["orders"][0]
                self.logger.debug(
                    "first_order_structure_logged",
                    action="compute_action_hash",
                    order_keys=list(order.keys()),
                    message=f"[HL_AUTH] First order keys: {list(order.keys())}",
                )
                # Check if 'c' field is present and its value
                if "c" in order:
                    c_value = order["c"]
                    c_type_name = type(c_value).__name__
                    self.logger.debug(
                        "c_field_value_logged",
                        c_value=c_value,
                        c_type=c_type_name,
                        message=f"[HL_AUTH] Field 'c' value: {c_value} (type: {c_type_name})",
                    )
                else:
                    self.logger.debug(
                        "c_field_missing",
                        action="compute_action_hash",
                        message="[HL_AUTH] Field 'c' is missing from order!",
                    )

            msgpacked_action: bytes = bytes(msgpack.packb(action_payload_dict))
        except Exception as e:
            self.logger.exception(
                "msgpack_serialization_failed",
                action="compute_action_hash",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to msgpack action payload: {e}",
            )
            serialize_action_failed_msg = f"Failed to serialize action payload: {e}"
            raise APIError(
                serialize_action_failed_msg,
                code=APIErrorCode.INVALID_REQUEST.value,
                original_exception=e,
            ) from e

        # Security: Only log sanitized information
        self.logger.debug(
            "action_type_logged",
            action="compute_action_hash",
            action_type=action_payload_dict.get("type", "unknown"),
            message="[HL_AUTH] Action type: {}".format(action_payload_dict.get("type", "unknown")),
        )
        self.logger.debug(
            "msgpack_size_logged",
            action="compute_action_hash",
            msgpack_size_bytes=len(msgpacked_action),
            message=f"[HL_AUTH] Msgpacked action size: {len(msgpacked_action)} bytes",
        )

        # Build hash input components
        action_hash_data_parts: list[bytes] = [msgpacked_action]
        action_hash_data_parts.append(current_nonce_ms.to_bytes(8, "big"))
        self.logger.debug(
            "nonce_logged",
            action="compute_action_hash",
            nonce=current_nonce_ms,
            message=f"[HL_AUTH] Nonce: {current_nonce_ms}",
        )

        # Add vault address (None for standard user trades)
        vault_address_for_hash: str | None = None
        if vault_address_for_hash is not None:
            action_hash_data_parts.extend((b"\x01", address_to_bytes(vault_address_for_hash)))
        else:
            action_hash_data_parts.append(b"\x00")

        # Add expires_after if present
        expires_after_for_hash: int | None = None
        if expires_after_for_hash is not None:
            action_hash_data_parts.extend((b"\x00", expires_after_for_hash.to_bytes(8, "big")))

        action_hash_input_bytes = b"".join(action_hash_data_parts)
        self.logger.debug(
            "action_hash_input_size_logged",
            action="compute_action_hash",
            input_size_bytes=len(action_hash_input_bytes),
            message=(f"[HL_AUTH] Action hash input size: {len(action_hash_input_bytes)} bytes"),
        )

        try:
            return keccak(action_hash_input_bytes)
        except Exception as e:
            self.logger.exception(
                "action_hash_computation_failed",
                action="compute_action_hash",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to compute action hash: {e}",
            )
            compute_action_hash_failed_msg = f"Failed to compute action hash: {e}"
            raise APIError(
                compute_action_hash_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

    def _create_phantom_agent_message(self, action_hash_bytes: bytes) -> dict[str, Any]:
        """Create phantom agent message for EIP-712 signing."""
        source_char = "a" if self._network_environment.is_mainnet() else "b"
        phantom_agent_message = {
            "source": source_char,
            "connectionId": action_hash_bytes,
        }
        self.logger.debug(
            "phantom_agent_source_set",
            action="create_phantom_agent_message",
            source_char=source_char,
            is_mainnet=self._network_environment.is_mainnet(),
            message=f"[HL_AUTH] Phantom agent source: {source_char}",
        )
        return phantom_agent_message

    def _validate_hex_format(self, hex_value: str, field_name: str) -> None:
        """Validate hex format for signature component."""
        if not hex_value.startswith("0x") or len(hex_value) != SIGNATURE_HEX_LENGTH:
            raise InvalidFormatError(
                field_name=field_name,
                expected_format="0x + 64 hex characters",
                actual_value=hex_value,
                reason=f"got length {len(hex_value)}",
            )

    def _format_signature_components(self, signed_message_obj: SignatureObject) -> dict[str, Any]:
        """Format signature components with proper validation and padding."""
        # Validate signature components
        if (
            not hasattr(signed_message_obj, "r")
            or not hasattr(signed_message_obj, "s")
            or not hasattr(signed_message_obj, "v")
        ):
            invalid_signature_components_msg = (
                "Invalid signature object: missing r, s, or v components"
            )
            raise APIError(
                invalid_signature_components_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # Convert signature components to hex with proper padding
        try:
            r_hex: str = to_hex(signed_message_obj.r)
            s_hex: str = to_hex(signed_message_obj.s)

            # Pad to 64 hex characters if needed (0x + 64 chars = 66 total)
            if len(r_hex) < SIGNATURE_HEX_LENGTH:
                r_hex = "0x" + r_hex[2:].zfill(64)
            if len(s_hex) < SIGNATURE_HEX_LENGTH:
                s_hex = "0x" + s_hex[2:].zfill(64)

            # Validate hex format outside try block
            self._validate_hex_format(r_hex, "r")
            self._validate_hex_format(s_hex, "s")

            signature_dict = {
                "r": r_hex,
                "s": s_hex,
                "v": signed_message_obj.v,
            }

            # Security: Log signature creation success without exposing values
            self.logger.debug(
                "signature_components_generated",
                action="format_signature_components",
                message="[HL_AUTH] Signature components generated successfully",
            )
            self.logger.debug(
                "signature_v_value_logged",
                action="format_signature_components",
                v_value=signed_message_obj.v,
                message=f"[HL_AUTH] Signature v value: {signed_message_obj.v}",
            )
            self.logger.debug(
                "signing_account_logged",
                action="format_signature_components",
                account_address=self._account.address,
                message=f"[HL_AUTH] Signing account: {self._account.address}",
            )

        except Exception as e:
            self.logger.exception(
                "signature_formatting_failed",
                action="format_signature_components",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to format signature components: {e}",
            )
            format_signature_failed_msg = f"Failed to format signature: {e}"
            raise APIError(
                format_signature_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e
        else:
            return signature_dict

    async def _prepare_exchange_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign a Hyperliquid /exchange request using appropriate signing scheme."""
        # Input validation
        self._validate_exchange_request_data(data)
        # DEFENSIVE CHECK: Ensure data is not None after validation
        if data is None:
            raise RequiredParameterError(
                parameter="data",
                context="exchange request validation",
                exchange="Hyperliquid",
            )

        # Prepare action payload and determine signing scheme
        action_payload_dict = self._prepare_action_payload(data)
        action_type = action_payload_dict.get("type")

        # Route to appropriate signing scheme based on action type
        if action_type == "usdClassTransfer":
            return await self._prepare_user_signed_request(action_payload_dict, params, headers)
        return await self._prepare_l1_signed_request(action_payload_dict, params, headers)

    async def _prepare_l1_signed_request(
        self,
        action_payload_dict: dict[str, Any],
        params: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign using sign_l1_action scheme (original method)."""
        current_nonce_ms = await self._get_next_nonce_ms()
        action_hash_bytes = self._compute_action_hash(action_payload_dict, current_nonce_ms)
        phantom_agent_message = self._create_phantom_agent_message(action_hash_bytes)

        # Sign the structured data
        signature_dict = self._sign_eip712_message(phantom_agent_message)

        # Construct final request components
        final_http_body = self._construct_http_body(
            action_payload_dict,
            current_nonce_ms,
            signature_dict,
        )
        final_headers = self._prepare_request_headers(headers)

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=params,
            data=final_http_body,
        )

    async def _prepare_user_signed_request(
        self,
        action_payload_dict: dict[str, Any],
        params: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepare and sign using sign_user_signed_action scheme for usdClassTransfer."""
        # Add required fields for user signed actions
        is_mainnet = self._network_environment.chain_id.is_production
        action_payload_dict["signatureChainId"] = "0x66eee"  # Fixed signature chain ID
        action_payload_dict["hyperliquidChain"] = "Mainnet" if is_mainnet else "Testnet"

        # Sign the action directly (no phantom agent or hash)
        signature_dict = self._sign_user_action(action_payload_dict)

        # Get nonce for HTTP body (different from action nonce)
        current_nonce_ms = action_payload_dict["nonce"]

        # Construct final request components
        final_http_body = self._construct_http_body(
            action_payload_dict,
            current_nonce_ms,
            signature_dict,
        )
        final_headers = self._prepare_request_headers(headers)

        return AuthenticatedRequestComponents(
            headers=final_headers,
            params=params,
            data=final_http_body,
        )

    def _sign_user_action(self, action_payload_dict: dict[str, Any]) -> dict[str, Any]:
        """Sign user action using sign_user_signed_action scheme."""
        # Extract chain_id from signatureChainId like the SDK does
        signature_chain_id = action_payload_dict.get("signatureChainId")
        if not signature_chain_id:
            missing_signature_chain_id_msg = "Missing signatureChainId in action payload"
            raise APIError(
                missing_signature_chain_id_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

        # Convert hex string to int like SDK: int(action["signatureChainId"], 16)
        chain_id = int(signature_chain_id, 16)

        # Update the existing user action domain with extracted chain_id
        # Create a copy with the correct chainId (don't modify the original)
        domain_data = self._user_action_domain.model_copy(update={"chainId": chain_id})

        # Construct structured data for EIP-712 signing
        structured_data_to_sign = {
            "domain": domain_data.model_dump(by_alias=True),
            "message": action_payload_dict,
            "primaryType": "HyperliquidTransaction:UsdClassTransfer",
            "types": self._usd_class_transfer_types.model_dump(by_alias=True),
        }

        try:
            # Generate and sign EIP-712 message
            signable_message = self._encode_and_sign_message(structured_data_to_sign)
            return self._format_signature_components(signable_message)

        except Exception as e:
            self.logger.exception(
                "user_action_signing_failed",
                action="sign_user_action",
                error_details=str(e),
                message=f"HyperliquidEip712Authenticator: Failed to sign user action: {e}",
            )
            user_action_signing_failed_msg = f"Failed to sign EIP-712 user action: {e}"
            raise APIError(
                user_action_signing_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

    def _sign_eip712_message(self, phantom_agent_message: dict[str, Any]) -> dict[str, Any]:
        """Sign the EIP-712 message and return signature components."""
        # Construct structured data for EIP-712 signing
        structured_data_to_sign = {
            "domain": self._exchange_action_domain.model_dump(by_alias=True),
            "message": phantom_agent_message,
            "primaryType": "Agent",
            "types": self._exchange_action_agent_types.model_dump(by_alias=True),
        }

        try:
            # Security: Only log domain information, not sensitive message details
            domain_dict = structured_data_to_sign["domain"]
            if isinstance(domain_dict, dict):
                self.logger.debug(
                    "eip712_domain_logged",
                    action="sign_eip712_message",
                    domain_name=domain_dict.get("name", "unknown"),
                    message=("[HL_AUTH] EIP-712 domain name: {}").format(
                        domain_dict.get("name", "unknown")
                    ),
                )
            primary_type = structured_data_to_sign.get("primaryType", "unknown")
            self.logger.debug(
                "eip712_primary_type_logged",
                action="sign_eip712_message",
                primary_type=primary_type,
                message=f"[HL_AUTH] EIP-712 primaryType: {primary_type}",
            )

            # Generate and sign EIP-712 message
            signable_message = self._encode_and_sign_message(structured_data_to_sign)
            return self._format_signature_components(signable_message)

        except Exception as e:
            self.logger.exception(
                "eip712_signing_failed",
                action="sign_eip712_message",
                error_details=str(e),
                message=(
                    f"HyperliquidEip712Authenticator: Failed to sign "
                    f"Hyperliquid Exchange Agent message: {e}"
                ),
            )
            sign_eip712_agent_failed_msg = (
                f"Failed to sign EIP-712 Agent request for /exchange: {e}"
            )
            raise APIError(
                sign_eip712_agent_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

    def _encode_and_sign_message(self, structured_data_to_sign: dict[str, Any]) -> SignatureObject:
        """Encode and sign the EIP-712 message."""
        try:
            signable_message = encode_typed_data(full_message=structured_data_to_sign)
        except Exception as e:
            self.logger.exception(
                "eip712_encoding_failed",
                action="encode_and_sign_message",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to encode EIP-712 typed data: {e}",
            )
            encode_eip712_failed_msg = f"Failed to encode EIP-712 message: {e}"
            raise APIError(
                encode_eip712_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        self.logger.debug(
            "eip712_message_encoded",
            action="encode_and_sign_message",
            message="[HL_AUTH] EIP-712 message encoded successfully",
        )

        # Debug: Try to recover address before returning
        try:
            signed_msg = self._account.sign_message(signable_message)
            # Verify recovery
            recovered = Account.recover_message(
                signable_message,
                vrs=[signed_msg.v, signed_msg.r, signed_msg.s],
            )
            self.logger.debug(
                "signing_account_info",
                action="encode_and_sign_message",
                account_address=self._account.address,
                message=f"[HL_AUTH] Signing with account: {self._account.address}",
            )
            self.logger.debug(
                "address_recovery_check",
                action="encode_and_sign_message",
                recovered_address=recovered,
                message=f"[HL_AUTH] Recovered address: {recovered}",
            )
            if recovered.lower() != self._account.address.lower():
                self.logger.error(
                    "address_recovery_mismatch",
                    action="encode_and_sign_message",
                    recovered_address=recovered,
                    signing_address=self._account.address,
                    message=(
                        f"[HL_AUTH] SIGNING ERROR: Recovered address {recovered} != "
                        f"signing address {self._account.address}"
                    ),
                )
        except Exception as e:
            self.logger.exception(
                "eip712_message_signing_failed",
                action="encode_and_sign_message",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to sign EIP-712 message: {e}",
            )
            sign_eip712_message_failed_msg = f"Failed to sign EIP-712 message: {e}"
            raise APIError(
                sign_eip712_message_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e
        else:
            return signed_msg

    def _validate_http_body_components(
        self,
        action_payload_dict: dict[str, Any],
        current_nonce_ms: int,
        signature_dict: dict[str, Any],
    ) -> None:
        """Validate components for HTTP body construction."""
        if not action_payload_dict:
            raise RequiredParameterError(
                parameter="action_payload",
                context="HTTP body construction",
                exchange="Hyperliquid",
            )
        if not signature_dict:
            raise RequiredParameterError(
                parameter="signature",
                context="HTTP body construction",
                exchange="Hyperliquid",
            )
        if current_nonce_ms <= 0:
            raise InvalidFormatError(
                field_name="nonce",
                expected_format="positive integer",
                actual_value=current_nonce_ms,
                reason="must be greater than 0",
            )

    def _construct_http_body(
        self,
        action_payload_dict: dict[str, Any],
        current_nonce_ms: int,
        signature_dict: dict[str, Any],
    ) -> dict[str, Any]:
        """Construct the final HTTP request body."""
        # Validate all required components are present
        self._validate_http_body_components(action_payload_dict, current_nonce_ms, signature_dict)

        try:
            # The SDK always includes vaultAddress and expiresAfter, even when None
            # For standard user trades, these are None
            vault_address_for_hash: str | None = None
            expires_after_for_hash: int | None = None

            final_http_body: dict[str, Any] = {
                "action": action_payload_dict,
                "nonce": current_nonce_ms,
                "signature": signature_dict,
                "vaultAddress": vault_address_for_hash,  # Include even if None
                "expiresAfter": expires_after_for_hash,  # Include even if None
            }

            # Security: Log successful body construction without sensitive data
            self.logger.debug(
                "http_body_constructed",
                action="construct_http_body",
                field_count=len(final_http_body),
                message=f"[HL_AUTH] HTTP body constructed with {len(final_http_body)} fields",
            )
        except Exception as e:
            self.logger.exception(
                "http_body_construction_failed",
                action="construct_http_body",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to construct HTTP body: {e}",
            )
            construct_body_failed_msg = f"Failed to construct request body: {e}"
            raise APIError(
                construct_body_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return final_http_body

    def _prepare_request_headers(self, headers: Mapping[str, Any] | None) -> dict[str, str]:
        """Prepare HTTP headers for the request."""
        try:
            final_headers: dict[str, str] = {"Content-Type": "application/json"}
            if headers:
                for key, value in headers.items():
                    if key != "Content-Type":  # Don't override Content-Type
                        # Validate header values are safe strings
                        if not key.strip():
                            self.logger.warning(
                                "empty_header_key_skipped",
                                action="prepare_request_headers",
                                header_key=key,
                                message=f"[HL_AUTH] Skipping empty header key: {key!r}",
                            )
                            continue
                        safe_value = str(value).strip()
                        if safe_value:  # Only add non-empty values
                            final_headers[key] = safe_value

            self.logger.debug(
                "http_headers_prepared",
                action="prepare_request_headers",
                header_count=len(final_headers),
                message=f"[HL_AUTH] Prepared {len(final_headers)} HTTP headers",
            )
        except Exception as e:
            self.logger.exception(
                "header_preparation_failed",
                action="prepare_request_headers",
                error_details=str(e),
                message=f"[HL_AUTH] Failed to prepare headers: {e}",
            )
            prepare_headers_failed_msg = f"Failed to prepare request headers: {e}"
            raise APIError(
                prepare_headers_failed_msg,
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
                original_exception=e,
            ) from e

        return final_headers
