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
from typing import Any, Protocol

from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class ModelDumpable(Protocol):
    """Protocol for objects that have a model_dump method."""

    def model_dump(self, *, by_alias: bool = False, exclude_none: bool = False) -> dict[str, Any]:
        """Dump model to dictionary."""
        ...


class HyperliquidPayloadSigningMapper:
    """Transforms Pydantic models to signing format for Hyperliquid authentication.

    This class handles the conversion of Pydantic models and data structures
    into the exact format required for Hyperliquid's EIP-712 signing process.
    It ensures proper field mapping, null value handling, and address normalization.

    Key Features:
    - Converts Pydantic models to dict format using model_dump()
    - Handles nested structures and lists of orders
    - Removes null/None fields that should be omitted from signing
    - Normalizes Ethereum addresses to lowercase for consistent hashing
    - Validates final payload is JSON serializable

    Usage:
        mapper = HyperliquidPayloadSigningMapper()
        signing_payload = mapper.convert_payload_to_signing_format(raw_data)
        cleaned_payload = mapper.clean_raw_payload_for_signing(payload)
    """

    def __init__(self, logger_param: logging.Logger | None = None) -> None:
        """Initialize the payload signing mapper.

        Args:
            logger_param: Optional logger instance. If None, creates a new logger.
        """
        self.logger = logger_param or get_logger(__name__)

    def _convert_pydantic_order_to_dict(self, order: ModelDumpable, index: int) -> dict[str, Any]:
        """Convert a Pydantic order model to dict format.

        Args:
            order: Pydantic model with model_dump() method
            index: Order index for error reporting

        Returns:
            Dictionary representation of the order
        """
        try:
            # CRITICAL: Use by_alias=True to get short field names directly
            # This ensures we get 'a', 'b', 'p', etc. instead of 'asset_index', 'is_buy', etc.
            raw_dict: dict[str, Any] = order.model_dump(by_alias=True, exclude_none=True)

            # Return the dict as-is, field ordering is preserved in Python 3.7+
            return raw_dict
        except Exception as e:
            raise TransformationError(
                f"Failed to convert order at index {index} to dict: {e}",
                code=APIErrorCode.TRANSFORMATION_FAILED.name,
            ) from e

    def _convert_aliased_order_dict(self, order: dict[str, Any]) -> dict[str, Any]:
        """Convert order dict with aliased names to short field names."""
        # Build dict with short field names
        order_dict = {
            "a": order["asset_index"],
            "b": order["is_buy"],
            "p": order["limit_px"],
            "s": order["size"],
            "r": order["reduce_only"],
            "t": order["order_type_details"],
        }

        if "client_order_id" in order and order["client_order_id"] is not None:
            order_dict["c"] = order["client_order_id"]
        return order_dict

    def _process_single_order(
        self, order: ModelDumpable | dict[str, Any], index: int
    ) -> dict[str, Any]:
        """Process a single order for signing conversion.

        Args:
            order: Order data - can be either a Pydantic model or dictionary
            index: Order index for error reporting

        Returns:
            Dictionary representation ready for signing with correct field ordering
        """
        if hasattr(order, "model_dump"):
            # It's a Pydantic model
            order_dict = self._convert_pydantic_order_to_dict(order, index)
        elif isinstance(order, dict):
            # Already a dict - check field name format
            if all(key in order for key in ["a", "b", "p", "s"]):
                # Already has short field names
                order_dict = order
            elif all(key in order for key in ["asset_index", "is_buy", "limit_px", "size"]):
                # Has aliased names - need to convert to short names
                order_dict = self._convert_aliased_order_dict(order)
            else:
                raise ValueError(f"Order dict at index {index} has unexpected field names")
        else:
            raise TypeError(f"Invalid order type at index {index}: {type(order).__name__}")

        return order_dict

    def _convert_orders_list(
        self, orders: list[ModelDumpable | dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Convert list of orders to signing format."""
        orders_for_signing: list[dict[str, Any]] = []
        for i, order in enumerate(orders):
            converted_order = self._process_single_order(order, i)
            orders_for_signing.append(converted_order)
        return orders_for_signing

    def _convert_action_field(self, payload_dict: dict[str, Any]) -> None:
        """Convert action field if it's a Pydantic model."""
        if "action" in payload_dict and hasattr(payload_dict["action"], "model_dump"):
            try:
                payload_dict["action"] = payload_dict["action"].model_dump(
                    by_alias=False, exclude_none=True
                )
            except Exception as e:
                raise TransformationError(
                    f"Failed to convert action to dict: {e}",
                    code=APIErrorCode.TRANSFORMATION_FAILED.name,
                ) from e

    def _is_cancel_payload_in_correct_format(self, data: dict[str, Any]) -> bool:
        """Check if cancel payload is already in correct format."""
        if data.get("type") != "cancel" or "cancels" not in data:
            return False
            
        if not isinstance(data["cancels"], list) or len(data["cancels"]) == 0:
            return False
            
        first_cancel = data["cancels"][0]
        return (
            isinstance(first_cancel, dict)
            and "a" in first_cancel
            and "o" in first_cancel
        )
    
    def _handle_orders_field(self, data: dict[str, Any], payload: dict[str, Any]) -> None:
        """Handle orders field conversion."""
        if "orders" not in data:
            return
            
        if isinstance(data["orders"], list):
            payload["orders"] = self._convert_orders_list(data["orders"])
        else:
            payload["orders"] = data["orders"]
    
    def _handle_action_field(self, data: dict[str, Any], payload: dict[str, Any]) -> None:
        """Handle action field conversion."""
        if "action" not in data:
            return
            
        if hasattr(data["action"], "model_dump"):
            # Convert Pydantic model to dict with aliases
            payload["action"] = data["action"].model_dump(by_alias=True, exclude_none=True)
        else:
            payload["action"] = data["action"]
    
    def _handle_cancels_field(self, data: dict[str, Any], payload: dict[str, Any]) -> None:
        """Handle cancels field conversion."""
        if "cancels" not in data:
            return
            
        if isinstance(data["cancels"], list):
            cancels_list = []
            for cancel_item in data["cancels"]:
                if hasattr(cancel_item, "model_dump"):
                    # Convert Pydantic model to dict
                    # Don't use by_alias since we already have short field names
                    cancel_dict = cancel_item.model_dump(exclude_none=True)
                    # Simple dict structure
                    cancels_list.append({"a": cancel_dict["a"], "o": cancel_dict["o"]})
                else:
                    # Already a dict - just ensure correct structure
                    cancels_list.append({"a": cancel_item["a"], "o": cancel_item["o"]})
            payload["cancels"] = cancels_list
        else:
            payload["cancels"] = data["cancels"]

    def convert_payload_to_signing_format(self, data: dict[str, Any]) -> dict[str, Any]:
        """Convert payload with Pydantic models to dict format for signing."""
        try:
            # For cancel payloads, return as-is if already in correct format
            if self._is_cancel_payload_in_correct_format(data):
                return data

            # Build payload with simple dict structure
            payload = {}

            # Add fields in the expected order
            if "type" in data:
                payload["type"] = data["type"]

            # Handle orders (for order placement)
            self._handle_orders_field(data, payload)

            # Handle action (for transfers, withdrawals, etc.)
            self._handle_action_field(data, payload)

            # Handle cancels (for order cancellations)
            self._handle_cancels_field(data, payload)

            # Add grouping last (for order placement)
            if "grouping" in data:
                payload["grouping"] = data["grouping"]

            # Add any other fields that might be present
            for key, value in data.items():
                if key not in payload:
                    payload[key] = value

            # Validate the result is serializable
            self._validate_serializable(payload)

            return payload

        except TransformationError:
            raise  # Re-raise transformation errors as-is
        except Exception as e:
            self.logger.error(f"Failed to convert payload to signing format: {e}")
            raise TransformationError(
                f"Failed to convert payload to signing format: {e}",
                code=APIErrorCode.TRANSFORMATION_FAILED.name,
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
                code=APIErrorCode.TRANSFORMATION_FAILED.name,
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
        return len(value) == 42 and value.lower().startswith("0x")

    def _validate_serializable(self, data: object) -> None:
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
        elif not isinstance(data, str | int | float | bool | type(None)):
            # Check for Pydantic models that weren't converted
            if hasattr(data, "model_dump"):
                raise ValueError(f"Unconverted Pydantic model: {type(data).__name__}")
            else:
                raise ValueError(f"Non-serializable type: {type(data).__name__}")
