"""CyberDeltaEngine: Hyperliquid Payload Signing Mapper.

------------------------------------------------

This module provides the HyperliquidPayloadSigningMapper class for transforming
Raw API payloads into the proper format required for EIP-712 signing.

Responsibilities:
- Convert Pydantic models to dictionary format for msgpack serialization
- Clean null/None fields that should be omitted from signing
- Normalize Ethereum addresses to lowercase for consistent hashing
- Clean order type structures (remove null limit/market fields)
- Prepare payloads for EIP-712 signature generation

Architecture Compliance:
- Uses only Raw models from cyberdelta/apis/hyperliquid/models/
- No business logic, only data transformation for signing
- Comprehensive validation at boundaries
- Clear error reporting for debugging

Security: Ensures consistent payload formatting for cryptographic signatures
to prevent signature validation failures in the trading engine.
"""

from __future__ import annotations

import logging
from typing import Any

from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidPayloadSigningMapper:
    """Maps Raw API payloads to signing format for Hyperliquid EIP-712 authentication.

    This mapper handles the RAW → SIGNING transformation boundary, ensuring:
    1. Pydantic models are converted to plain dictionaries
    2. Null/None fields are removed appropriately
    3. Ethereum addresses are normalized to lowercase
    4. Order type structures are cleaned
    5. Payload is ready for msgpack serialization and signing

    Architecture Compliance:
    - Works with Raw models from cyberdelta/apis/hyperliquid/models/
    - No business logic, only data transformation for signing
    - Comprehensive error handling
    - Clear logging for debugging signature issues
    """

    def __init__(self, logger_param: logging.Logger | None = None) -> None:
        """Initialize the payload signing mapper.

        Args:
            logger_param: Optional logger for transformation events
        """
        self.logger = logger_param or get_logger(__name__)

    def convert_payload_to_signing_format(self, data: dict[str, Any]) -> dict[str, Any]:
        """Convert payload with Pydantic models to dict format for signing.

        Architecture Compliance: Handles the RAW → SIGNING transformation with
        proper Pydantic boundary protection and validation.

        Args:
            data: Payload that may contain Pydantic models

        Returns:
            Dictionary suitable for msgpack serialization and signing

        Raises:
            TransformationError: If conversion fails
        """
        try:
            # Input validation
            if not isinstance(data, dict):
                raise TypeError(f"Expected dict, got {type(data).__name__}")
                
            payload_dict = data.copy()

            # Handle Pydantic models in the orders field
            if "orders" in payload_dict and isinstance(payload_dict["orders"], list):
                # Convert Pydantic models to dicts for signing
                orders_for_signing = []
                for i, order in enumerate(payload_dict["orders"]):
                    if hasattr(order, "model_dump"):
                        # It's a Pydantic model - convert to dict with proper field names
                        # Architecture: Use by_alias=False to get internal field names for signing
                        try:
                            order_dict = order.model_dump(by_alias=False, exclude_none=True)
                            orders_for_signing.append(order_dict)
                        except Exception as e:
                            raise TransformationError(
                                f"Failed to convert order at index {i} to dict: {e}",
                                code=APIErrorCode.TRANSFORMATION_FAILED.value,
                            ) from e
                    elif isinstance(order, dict):
                        # Already a dict - validate it has expected structure
                        if not all(key in order for key in ["a", "b", "p", "s"]):
                            raise ValueError(f"Order dict at index {i} missing required fields")
                        orders_for_signing.append(order)
                    else:
                        raise TypeError(
                            f"Invalid order type at index {i}: {type(order).__name__}"
                        )
                payload_dict["orders"] = orders_for_signing

            # Handle other potential Pydantic models in the action field
            if "action" in payload_dict and hasattr(payload_dict["action"], "model_dump"):
                try:
                    payload_dict["action"] = payload_dict["action"].model_dump(
                        by_alias=False, exclude_none=True
                    )
                except Exception as e:
                    raise TransformationError(
                        f"Failed to convert action to dict: {e}",
                        code=APIErrorCode.TRANSFORMATION_FAILED.value,
                    ) from e

            # Validate the result is serializable
            self._validate_serializable(payload_dict)
            
            return payload_dict

        except TransformationError:
            raise  # Re-raise transformation errors as-is
        except Exception as e:
            self.logger.error(f"Failed to convert payload to signing format: {e}")
            raise TransformationError(
                f"Failed to convert payload to signing format: {e}",
                code=APIErrorCode.TRANSFORMATION_FAILED.value,
                original_exception=e,
            ) from e

    def clean_raw_payload_for_signing(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Clean Raw API payload for signing by removing null fields and normalizing addresses.

        This method prepares the Raw payload for authentication by:
        1. Removing null/None fields that should be omitted from signing
        2. Converting Ethereum addresses to lowercase for consistent hashing
        3. Cleaning order type structures (remove null limit/market fields)

        Args:
            payload: Raw payload dictionary to clean

        Returns:
            Cleaned payload ready for signing

        Raises:
            TransformationError: If cleaning fails
        """
        try:
            # Make a copy to avoid modifying the original
            cleaned_payload = payload.copy()

            # Recursively clean the payload
            self._recursive_clean_payload(cleaned_payload)

            self.logger.debug("Successfully cleaned payload for signing")

            return cleaned_payload

        except Exception as e:
            self.logger.error(f"Failed to clean payload for signing: {e}")
            raise TransformationError(
                f"Failed to clean payload for signing: {e}",
                code=APIErrorCode.TRANSFORMATION_FAILED.value,
                original_exception=e,
            ) from e

    def _recursive_clean_payload(self, data: dict[str, Any]) -> None:
        """Recursively clean payload data in-place.

        Args:
            data: Dictionary to clean (modified in place)
        """
        # Clean order type structures first
        self._clean_order_type_fields(data)

        # Normalize Ethereum addresses
        self._normalize_addresses_in_payload(data)

    def _clean_order_type_fields(self, data: dict[str, Any]) -> None:
        """Remove null fields from order type structures and other null fields.

        Args:
            data: Dictionary to clean (modified in place)
        """
        # Handle order type structures: {"limit": {...}, "market": null} -> {"limit": {...}}
        if "limit" in data and "market" in data:
            if data["limit"] is None and data["market"] is not None:
                del data["limit"]
            elif data["market"] is None and data["limit"] is not None:
                del data["market"]

        # Remove any null fields
        keys_to_remove = [key for key, value in data.items() if value is None]
        for key in keys_to_remove:
            del data[key]

        # Recursively clean nested structures
        for value in data.values():
            if isinstance(value, dict):
                self._clean_order_type_fields(value)
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        self._clean_order_type_fields(item)

    def _normalize_addresses_in_payload(self, data: dict[str, Any]) -> None:
        """Normalize Ethereum addresses to lowercase in payload.

        Args:
            data: Dictionary to process (modified in place)
        """
        for key, value in data.items():
            if isinstance(value, str) and self._is_ethereum_address(value):
                data[key] = value.lower()
            elif isinstance(value, dict):
                self._normalize_addresses_in_payload(value)
            elif isinstance(value, list):
                self._normalize_addresses_in_list(value)

    def _normalize_addresses_in_list(self, lst: list[Any]) -> None:
        """Process a list to normalize Ethereum addresses.

        Args:
            lst: List to process (modified in place)
        """
        for i, item in enumerate(lst):
            if isinstance(item, str) and self._is_ethereum_address(item):
                lst[i] = item.lower()
            elif isinstance(item, dict):
                self._normalize_addresses_in_payload(item)
            elif isinstance(item, list):
                self._normalize_addresses_in_list(item)

    def _is_ethereum_address(self, value: str) -> bool:
        """Check if a string looks like an Ethereum address.

        Args:
            value: String to check

        Returns:
            True if it looks like an Ethereum address
        """
        if not isinstance(value, str):
            return False
        return len(value) == 42 and value.lower().startswith("0x")
    
    def _validate_serializable(self, data: Any) -> None:
        """Validate that data is msgpack serializable.
        
        Architecture: Boundary validation to ensure data can be signed.
        
        Args:
            data: Data to validate
            
        Raises:
            ValueError: If data contains non-serializable types
        """
        if isinstance(data, dict):
            for key, value in data.items():
                if not isinstance(key, str):
                    raise ValueError(f"Non-string dict key: {type(key).__name__}")
                self._validate_serializable(value)
        elif isinstance(data, list):
            for item in data:
                self._validate_serializable(item)
        elif not isinstance(data, (str, int, float, bool, type(None))):
            # Check for Pydantic models that weren't converted
            if hasattr(data, "model_dump"):
                raise ValueError(f"Unconverted Pydantic model: {type(data).__name__}")
            else:
                raise ValueError(f"Non-serializable type: {type(data).__name__}")
