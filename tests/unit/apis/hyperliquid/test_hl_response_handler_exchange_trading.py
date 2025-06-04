"""Unit tests for HyperliquidResponseHandler exchange and trading response functionality."""

import copy
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_processed_exchange_responses import (
    HyperliquidErrorStatus,
    HyperliquidSuccessfulOrderStatus,
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
                cast("RawJsonResponse", raw_data),
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
                cast("RawJsonResponse", raw_data),
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
                cast("RawJsonResponse", raw_data),
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
                cast("RawJsonResponse", raw_data),
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
                cast("RawJsonResponse", raw_data),
                action_type="order",
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            "Unexpected exchange (order) response format: expected dict" in exc_info.value.message
        )
        assert "got <class 'list'>" in exc_info.value.message


class TestHandleQueryOrderHistoryResponse:
    """Tests for HyperliquidResponseHandler.handle_query_order_history_response."""

    def test_valid(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
    ) -> None:
        """Test handling a valid raw order history response."""
        raw_data = [valid_raw_historical_order_response, valid_raw_historical_order_response.copy()]
        response_list: list[HyperliquidRawHistoricalOrderResponse] = (
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast("RawJsonResponse", raw_data),
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
        response_list = HyperliquidResponseHandler.handle_query_order_history_response(
            cast("RawJsonResponse", raw_data),
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
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid single order history item (index 1) in query_order_history"
            f" (for {user_address}) response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "Field required" in str(exc_info.value.original_exception)
        assert "order.oid" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, user_address: str) -> None:
        """Test order history response with wrong top-level type (dict instead of list)."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_query_order_history_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected query_order_history (for {user_address}) response format: expected list"
            in exc_info.value.message
        )
        assert "got dict" in exc_info.value.message


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
            cast("RawJsonResponse", raw_data),
            user_address=user_address,
            order_id=order_id,
        )
        assert isinstance(response, HyperliquidRawHistoricalOrderResponse)
        assert response.order.oid == 7001
        assert response.order.status == "filled"

    def test_order_not_found_string_direct(self, user_address: str, order_id: int) -> None:
        """Test handling 'Order not found' string directly."""
        raw_data = "Order not found"
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        expected_message = (
            f"Order {order_id} for user {user_address} not found (direct string: {raw_data!r})"
        )
        assert exc_info.value.message == expected_message
        assert exc_info.value.metadata == {"original_response": "Order not found"}

    def test_order_not_found_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling ['Order not found'] list."""
        raw_data = ["Order not found"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
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
        raw_data: RawJsonResponse = []  # Using RawJsonResponse
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                raw_data,
                user_address=user_address,
                order_id=order_id,  # No cast
            )
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert (
            f"Order {order_id} for {user_address} not found (empty list)." in exc_info.value.message
        )

    def test_order_not_found_none(self, user_address: str, order_id: int) -> None:
        """Test handling None response."""
        raw_data = None
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"info (OrderStatus for user {user_address}, oid {order_id}) response format: "
            f"expected list or dict, got NoneType" in exc_info.value.message
        )

    def test_unexpected_string_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling unexpected string inside the list."""
        raw_data = ["Some other error string"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"(OrderStatus for user {user_address}, oid {order_id}) response: "
            f"Some other error string" in exc_info.value.message
        )
        assert exc_info.value.metadata == {"original_response_item": "Some other error string"}

    def test_unexpected_item_type_in_list(self, user_address: str, order_id: int) -> None:
        """Test handling non-dict, non-string item inside the list."""
        raw_data = [12345]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"(OrderStatus for user {user_address}, oid {order_id}) response list: "
            f"expected dict, got int" in exc_info.value.message
        )
        assert exc_info.value.metadata == {"original_response_item": 12345}

    def test_validation_error_in_list_item(
        self,
        valid_raw_historical_order_response: dict[str, Any],
        user_address: str,
        order_id: int,
    ) -> None:
        """Test list where the item fails model validation (e.g., missing status)."""
        invalid_item = valid_raw_historical_order_response.copy()
        del invalid_item["order"]["status"]
        raw_data = [invalid_item]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_order_status_response(
                cast("RawJsonResponse", raw_data),
                user_address=user_address,
                order_id=order_id,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid order status object in info (OrderStatus for user {user_address}, "
            f"oid {order_id}) response from exchange"
        ) in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "order.status" in str(exc_info.value.original_exception)


class TestProcessFirstExchangeStatus:
    """Tests for HyperliquidResponseHandler.process_first_exchange_status."""

    ACTION_DESC = "test_action"  # Common action description for these tests

    @pytest.mark.parametrize(
        "raw_status, expected_type, expected_details",
        [
            (
                {"resting": {"oid": 12345}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "resting", "oid": 12345},
            ),
            (
                {"filled": {"oid": 67890, "totalSz": "1.0", "avgPx": "3000.0"}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "filled", "oid": 67890, "total_sz": "1.0", "avg_px": "3000.0"},
            ),
            (
                {"canceled": {"oid": 54321}},
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "canceled", "oid": 54321},
            ),
            (
                "canceled",
                HyperliquidSuccessfulOrderStatus,
                {"status_type": "canceled_str"},
            ),
            (
                {"error": "Insufficient margin"},
                HyperliquidErrorStatus,
                {"message": "Insufficient margin"},
            ),
        ],
    )
    def test_valid_statuses(
        self,
        raw_status: RawJsonResponse,
        expected_type: type[HyperliquidSuccessfulOrderStatus | HyperliquidErrorStatus],
        expected_details: dict[str, Any],
    ) -> None:
        """Test processing various valid raw status objects and strings."""
        result = HyperliquidResponseHandler.process_first_exchange_status(
            raw_status,
            action_description=self.ACTION_DESC,
        )
        assert isinstance(result, expected_type)

        if isinstance(result, HyperliquidSuccessfulOrderStatus):
            assert result.status_type == expected_details["status_type"]
            if result.status_type in ["resting", "filled", "canceled"]:
                assert result.oid == expected_details["oid"]
            if result.status_type == "filled":
                assert result.total_sz == expected_details["total_sz"]
                assert result.avg_px == expected_details["avg_px"]
            # For "canceled_str", only type and status_type are asserted

        elif isinstance(result, HyperliquidErrorStatus):  # pyright: ignore[reportUnnecessaryIsInstance]
            assert result.message == expected_details["message"]

    @pytest.mark.parametrize(
        "invalid_raw_status, expected_exception_message_part_template",
        [
            (12345, "Invalid status type for {action_desc}: <class 'int'>"),
            ({}, "Unknown status structure for {action_desc}: {{}}"),
            (
                {"unknown_key": "value"},
                "Unknown status structure for {action_desc}: {{'unknown_key': 'value'}}",
            ),
            (["list_item"], "Invalid status type for {action_desc}: <class 'list'>"),
        ],
    )
    def test_invalid_status_structures(
        self,
        invalid_raw_status: RawJsonResponse,
        expected_exception_message_part_template: str,
    ) -> None:
        """Test invalid or unrecognized status structures."""
        expected_message = expected_exception_message_part_template.format(
            action_desc=self.ACTION_DESC,
        )
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.process_first_exchange_status(
                invalid_raw_status,
                action_description=self.ACTION_DESC,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert expected_message in exc_info.value.message

    def test_comprehensive_status_edge_cases(self) -> None:
        """Test additional edge cases for exchange status processing."""
        # Test empty dict
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.process_first_exchange_status(
                {},
                action_description=self.ACTION_DESC,
            )
        assert "Unknown status structure" in exc_info.value.message

        # Test dict with multiple keys (should use first recognized one)
        multi_key_status = cast(
            "RawJsonResponse",
            {"resting": {"oid": 999}, "filled": {"oid": 888}},
        )
        result = HyperliquidResponseHandler.process_first_exchange_status(
            multi_key_status,
            action_description=self.ACTION_DESC,
        )
        assert isinstance(result, HyperliquidSuccessfulOrderStatus)
        assert result.status_type == "resting"
        assert result.oid == 999

        # Test error with additional fields
        error_status = cast("RawJsonResponse", {"error": "Rate limit exceeded", "code": 429})
        result = HyperliquidResponseHandler.process_first_exchange_status(
            error_status,
            action_description=self.ACTION_DESC,
        )
        assert isinstance(result, HyperliquidErrorStatus)
        assert result.message == "Rate limit exceeded"
