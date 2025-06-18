"""CyberDeltaEngine: Hyperliquid Payload Preprocessing Mapper.

------------------------------------------------

This module provides the HyperliquidPayloadPreprocessingMapper class for preprocessing
raw API response payloads before Pydantic validation.

Responsibilities:
- Flatten nested response structures
- Map field names for consistency
- Add reasonable defaults for missing fields
- Prepare raw responses for clean Pydantic validation

Architecture Compliance:
- Handles RAW data transformation before Pydantic validation
- Keeps response handlers focused on pure validation
- No business logic, only structural transformations
- Clear separation of concerns per ERROR_HANDLING.md
"""

from typing import Any

from cyberdelta.config.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidPayloadPreprocessingMapper:
    """Preprocesses raw Hyperliquid API response payloads before validation.

    This mapper handles structural transformations that need to occur before
    Pydantic validation, ensuring response handlers remain focused on pure
    validation without business logic.

    Architecture Compliance:
    - Works at the RAW data boundary before Pydantic models
    - No business logic, only structural transformations
    - Maintains clean separation between preprocessing and validation
    """

    @staticmethod
    def _handle_list_format_response(raw_data: list[Any]) -> dict[str, Any]:
        """Handle list format order status responses."""
        from cyberdelta.apis.models.api_error import APIError
        from cyberdelta.apis.models.api_error_codes import APIErrorCode

        # Handle empty list
        if len(raw_data) == 0:
            raise APIError(
                message="Order not found (empty list).",
                code=APIErrorCode.ORDER_NOT_FOUND.value,
            )

        # Get first item from list
        status_item = raw_data[0]

        # Handle string responses in list
        if isinstance(status_item, str):
            if "Order not found" in status_item:
                raise APIError(
                    message=f"Order not found (string response: {status_item!r})",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                    metadata={"original_response_item": status_item},
                )
            else:
                raise APIError(
                    message=f"Unexpected order status response: {status_item}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    metadata={"original_response_item": status_item},
                )

        # Handle non-dict items
        if not isinstance(status_item, dict):
            raise APIError(
                message=f"Order status response list: expected dict, "
                f"got {type(status_item).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                metadata={"original_response_item": status_item},
            )

        return status_item

    @staticmethod
    def _handle_string_or_none_response(raw_data: str | None) -> None:
        """Handle string or None responses by raising appropriate errors."""
        from cyberdelta.apis.models.api_error import APIError
        from cyberdelta.apis.models.api_error_codes import APIErrorCode

        if isinstance(raw_data, str):
            if "Order not found" in raw_data:
                raise APIError(
                    message=f"Order not found (direct string: {raw_data!r})",
                    code=APIErrorCode.ORDER_NOT_FOUND.value,
                    metadata={"original_response": raw_data},
                )
            else:
                raise APIError(
                    message=f"Invalid order status response format: got string {raw_data!r}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                )
        elif raw_data is None:
            raise APIError(
                message="Order status response is None",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

    @staticmethod
    def _process_nested_order_structure(order_wrapper: dict[str, Any]) -> dict[str, Any]:
        """Process nested order structure and flatten it."""
        # Check if we have the deeply nested structure
        if "order" in order_wrapper and isinstance(order_wrapper["order"], dict):
            # Extract the actual order data
            inner_order = order_wrapper["order"]

            # Map 'coin' to 'asset' if asset is not present
            if "coin" in inner_order and "asset" not in inner_order:
                inner_order["asset"] = inner_order["coin"]

            # Add status fields from the wrapper if they exist
            if "status" in order_wrapper:
                inner_order["status"] = order_wrapper["status"]
            if "statusTimestamp" in order_wrapper:
                inner_order["statusTimestamp"] = order_wrapper["statusTimestamp"]

            # Add reasonable defaults for missing fields
            if "remainingSz" not in inner_order and "sz" in inner_order:
                # Only add default if status indicates it's an open order
                status = inner_order.get("status", "").lower()
                if status in ["open", "pending", "untriggered"]:
                    inner_order["remainingSz"] = inner_order["sz"]

            return {"order": inner_order}
        else:
            # The wrapper itself might be the order data
            return {"order": order_wrapper}

    @staticmethod
    def preprocess_order_status_response(
        raw_data: dict[str, Any] | list[Any] | str | None,
    ) -> dict[str, Any]:
        """Preprocess order status response to flatten nested structure."""
        from cyberdelta.apis.models.api_error import APIError
        from cyberdelta.apis.models.api_error_codes import APIErrorCode

        # Handle list format
        if isinstance(raw_data, list):
            raw_data = HyperliquidPayloadPreprocessingMapper._handle_list_format_response(raw_data)

        # Handle string or None responses
        elif isinstance(raw_data, str | type(None)):
            HyperliquidPayloadPreprocessingMapper._handle_string_or_none_response(raw_data)
            # This will always raise, so we never reach here

        # Validate we have a dict after preprocessing
        if not isinstance(raw_data, dict):
            raise APIError(
                message=f"Order status response: expected dict after preprocessing, "
                f"got {type(raw_data).__name__}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        # Handle the nested structure
        if "order" in raw_data and isinstance(raw_data["order"], dict):
            return HyperliquidPayloadPreprocessingMapper._process_nested_order_structure(
                raw_data["order"]
            )

        # If we don't have the expected nested structure, check if it's already flattened
        if "oid" in raw_data and "asset" in raw_data:
            # It's already a flat order object, wrap it
            return {"order": raw_data}

        # Return as-is if it's already in the expected format
        return raw_data

    @staticmethod
    def preprocess_user_state_response(raw_data: dict[str, Any]) -> dict[str, Any]:
        """Preprocess user state response if needed.

        Currently, user state responses don't require preprocessing,
        but this method is here for consistency and future needs.

        Args:
            raw_data: Raw response from Hyperliquid user state endpoint

        Returns:
            Preprocessed data (currently unchanged)
        """
        return raw_data

    @staticmethod
    def preprocess_open_orders_response(raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Preprocess open orders response if needed.

        Currently, open orders responses don't require preprocessing,
        but this method is here for consistency and future needs.

        Args:
            raw_data: Raw response from Hyperliquid open orders endpoint

        Returns:
            Preprocessed data (currently unchanged)
        """
        return raw_data
