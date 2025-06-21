"""Unit tests for HyperliquidResponseHandler exchange and trading response functionality."""

import copy
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusObject,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_response_handler"]


class TestHandleExchangeResponse:
    """Tests for HyperliquidResponseHandler.handle_exchange_response."""

    def test_valid(self, valid_raw_exchange_response: dict[str, Any]) -> None:
        """Test handling a valid raw exchange response."""
        raw_data = valid_raw_exchange_response
        response: HyperliquidRawExchangeResponse = (
            HyperliquidResponseHandler.handle_exchange_response(
                cast("ParsedJsonResponse", raw_data),
                action_type="order",
            )
        )
        assert isinstance(response, HyperliquidRawExchangeResponse)
        assert response.status == "ok"
        assert response.data is not None
        assert response.data.type == "order"
        assert len(response.data.statuses) == 2
        status1 = response.data.statuses[0]
        assert isinstance(status1, HyperliquidRawExchangeStatusObject)
        assert status1.resting is not None
        assert status1.resting.oid == 12345
        status2 = response.data.statuses[1]
        assert isinstance(status2, str)
        assert status2 == "canceled"

    def test_validation_error_missing_status(self) -> None:
        """Test exchange response dict missing required 'status' field."""
        raw_data = {"data": {"type": "order", "statuses": [{"resting": {"oid": 12345}}]}}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast("ParsedJsonResponse", raw_data),
                action_type="order",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "status" in str(exc_info.value.original_exception)

    def test_validation_error_bad_status_value(self) -> None:
        """Test exchange response with status != 'ok'."""
        raw_data = {"status": "error", "error": "Invalid order size"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast("ParsedJsonResponse", raw_data),
                action_type="order",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "status" in str(exc_info.value.original_exception)
        assert "Input should be 'ok'" in str(exc_info.value.original_exception)

    def test_validation_error_ok_missing_data(self) -> None:
        """Test exchange response status='ok' but missing 'data' field."""
        raw_data = {"status": "ok", "data": "not a valid data structure"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast("ParsedJsonResponse", raw_data),
                action_type="order",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid exchange (order) response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "data" in str(exc_info.value.original_exception)
        assert "Input should be a valid dictionary" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self) -> None:
        """Test exchange response with wrong top-level type (list instead of dict)."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_exchange_response(
                cast("ParsedJsonResponse", raw_data),
                action_type="order",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected exchange (order) response format: expected dict" in exc_info.value.message
        )
        assert "got <class 'list'>" in exc_info.value.message


class TestHandleQueryOrderHistoryResponse:
    """Tests for HyperliquidResponseHandler.handle_historical_orders_response."""

    def test_valid(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
    ) -> None:
        """Test handling a valid raw order history response."""
        raw_data = [valid_raw_historical_order_response, valid_raw_historical_order_response.copy()]
        response_list: list[HyperliquidRawHistoricalOrderResponse] = (
            HyperliquidResponseHandler.handle_historical_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
            )
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 2
        assert isinstance(response_list[0], HyperliquidRawHistoricalOrderResponse)
        assert response_list[0].order.oid == 7001

    def test_invalid_item_type_in_list(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
    ) -> None:
        """Test list containing a non-dict item. Handler should skip it."""
        raw_data = [valid_raw_historical_order_response.copy(), "not_an_order_dict"]
        # Handler skips invalid items, so no exception is raised
        response_list = HyperliquidResponseHandler.handle_historical_orders_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
        )
        assert isinstance(response_list, list)
        assert len(response_list) == 1  # Only the valid item remains
        assert isinstance(response_list[0], HyperliquidRawHistoricalOrderResponse)

    def test_item_validation_error(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
    ) -> None:
        """Test list where the item fails model validation (e.g., missing order.oid).

        Handler should raise.
        """
        # Use deepcopy to ensure modifications to invalid_item don't affect other copies
        invalid_item = copy.deepcopy(valid_raw_historical_order_response)
        # Ensure 'order' and 'oid' exist before trying to delete, and that 'order' is a dict
        if (
            "order" in invalid_item
            and isinstance(invalid_item["order"], dict)
            and "oid" in invalid_item["order"]
        ):
            del invalid_item["order"]["oid"]
        else:
            pytest.fail(
                "Fixture valid_raw_historical_order_response does not have expected "
                "'order'.'oid' structure or 'order' is not a dict.",
            )

        # Also use deepcopy for the "valid" item in the list to ensure it's pristine
        raw_data = [
            copy.deepcopy(valid_raw_historical_order_response),  # First item is a clean copy
            invalid_item,  # Second item is the modified one (missing oid)
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_historical_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid historicalOrders (for {user_address}) response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Field required" in str(exc_info.value.original_exception)
        assert "order.oid" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, user_address: str) -> None:
        """Test order history response with wrong top-level type (dict instead of list)."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_historical_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid historicalOrders (for {user_address}) response from exchange"
            in exc_info.value.message
        )
        assert "Expected a list of orders, got dict" in exc_info.value.message


class TestHandleInfoOrderStatusResponse:
    """Tests for HyperliquidResponseHandler.handle_info_order_status_response."""

    def test_valid(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
        order_id: int,
    ) -> None:
        """Test handling a valid order status response (list with one dict item)."""
        # The API returns a list containing the order status dict
        raw_data = [valid_raw_historical_order_response]
        response = HyperliquidResponseHandler.handle_info_order_status_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            order_id=order_id,
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.oid == 7001
        assert response.status == "filled"

    def test_order_not_found_string_direct(self, user_address: str, order_id: int) -> None:
        """Test handling 'Order not found' string directly."""
        raw_data = "Order not found"
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        expected_message = f"Order not found (direct string: {raw_data!r})"
        assert exc_info.value.message == expected_message
        assert exc_info.value.metadata == {"original_response": "Order not found"}

    def test_order_not_found_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling ['Order not found'] list."""
        raw_data = ["Order not found"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        # The handler identifies the string item within the list, so the message reflects that.
        # Original raw_data is a list: ["Order not found"]. status_item becomes "Order not found".
        expected_message_detail = "(string response: 'Order not found')"
        assert expected_message_detail in exc_info.value.message, (
            f"Detail '{expected_message_detail}' not in msg '{exc_info.value.message}'"
        )
        assert exc_info.value.metadata == {"original_response_item": "Order not found"}

    def test_order_not_found_empty_list(self, user_address: str, order_id: int) -> None:
        """Test handling [] empty list response."""
        raw_data: ParsedJsonResponse = []  # Using ParsedJsonResponse
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                raw_data,
                user_address=user_address,
                order_id=order_id,  # No cast
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert exc_info.value.message == "Order not found (empty list)."

    def test_order_not_found_none(self, user_address: str, order_id: int) -> None:
        """Test handling None response."""
        raw_data = None
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.message == "Order status response is None"

    def test_unexpected_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling unexpected string inside the list."""
        raw_data = ["Some other error string"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.message == "Unexpected order status response: Some other error string"
        assert exc_info.value.metadata == {"original_response_item": "Some other error string"}

    def test_unexpected_item_type_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling non-dict, non-string item inside the list."""
        raw_data = [12345]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert exc_info.value.message == "Order status response list: expected dict, got int"
        assert exc_info.value.metadata == {"original_response_item": 12345}

    def test_validation_error_in_list_item(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
        order_id: int,
    ) -> None:
        """Test list where the item fails model validation (e.g., missing status)."""
        invalid_item = valid_raw_historical_order_response.copy()
        del invalid_item["order"]["oid"]
        raw_data = [invalid_item]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"order status (user: {user_address}, order: {order_id})" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order.oid" in str(exc_info.value.original_exception)
