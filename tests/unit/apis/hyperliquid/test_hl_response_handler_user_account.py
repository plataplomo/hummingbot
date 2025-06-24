"""Unit tests for HyperliquidResponseHandler user and account response functionality."""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawOpenOrdersResponse
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFill,
    HyperliquidRawUserFillsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawClearinghouseState,
)
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_response_handler"]


class TestHandleInfoUserStateResponse:
    """Tests for HyperliquidResponseHandler.handle_info_user_state_response."""

    def test_valid(self, valid_raw_user_state: dict[str, Any], user_address: str) -> None:
        """Test handling a valid user state response."""
        raw_data = valid_raw_user_state
        response: HyperliquidRawClearinghouseState = (
            HyperliquidResponseHandler.handle_info_user_state_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        )
        assert isinstance(response, HyperliquidRawClearinghouseState)
        assert len(response.asset_positions) == 1
        assert response.asset_positions[0].asset == "ETH-PERP"
        assert response.withdrawable == "4700.0"

    def test_validation_error_missing_asset_positions(self, user_address: str) -> None:
        """Test user state response missing asset_positions field."""
        raw_data = {
            "crossMaintenanceMarginUsed": "30.0",
            "withdrawable": "4700.0",
        }  # Missing assetPositions
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_state_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (user state for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "assetPositions" in str(exc_info.value.original_exception)

    def test_validation_error_invalid_margin_summary(self, user_address: str) -> None:
        """Test user state response with invalid margin summary structure."""
        raw_data: dict[str, Any] = {
            "assetPositions": [],
            "crossMaintenanceMarginUsed": "30.0",
            "marginSummary": "invalid_structure",  # Should be a dict
            "withdrawable": "4700.0",
        }
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_state_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (user state for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, user_address: str) -> None:
        """Test user state response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_state_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (user state for {user_address}) response format:"
            in exc_info.value.message
        )
        assert "expected dict, got list" in exc_info.value.message


class TestHandleInfoOpenOrdersResponse:
    """Tests for HyperliquidResponseHandler.handle_info_open_orders_response."""

    def test_valid(self, valid_raw_open_order_item: dict[str, Any], user_address: str) -> None:
        """Test handling a valid open orders response."""
        raw_data = [valid_raw_open_order_item, valid_raw_open_order_item.copy()]
        response: HyperliquidRawOpenOrdersResponse = (
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        )
        assert isinstance(response, HyperliquidRawOpenOrdersResponse)
        assert len(response.items) == 2
        assert response.items[0].coin == "ETH-PERP"
        assert response.items[0].oid == 6001

    def test_empty_orders_list(self, user_address: str) -> None:
        """Test handling empty open orders response."""
        raw_data: list[Any] = []
        response = HyperliquidResponseHandler.handle_info_open_orders_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert isinstance(response, HyperliquidRawOpenOrdersResponse)
        assert len(response.items) == 0

    def test_validation_error_invalid_order_item(self, user_address: str) -> None:
        """Test open orders response with invalid order item."""
        invalid_order = {"asset": "ETH-PERP"}  # Missing required fields
        raw_data = [invalid_order]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (open orders for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_item_type_in_list(self, user_address: str) -> None:
        """Test open orders response with non-dict item in list."""
        raw_data = ["not_an_order_dict"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (open orders for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, user_address: str) -> None:
        """Test open orders response with wrong top-level type."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (open orders for {user_address}) response format:"
            in exc_info.value.message
        )
        assert "expected list, got dict" in exc_info.value.message

    def test_open_orders_with_mixed_valid_invalid_items(
        self,
        user_address: str,
        valid_raw_open_order_item: dict[str, Any],
    ) -> None:
        """Test open orders response with mix of valid and invalid items."""
        invalid_order = {"asset": "ETH-PERP"}  # Missing required fields
        raw_data = [
            valid_raw_open_order_item.copy(),  # Valid
            invalid_order,  # Invalid
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (open orders for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)


class TestHandleInfoUserFillsResponse:
    """Tests for HyperliquidResponseHandler.handle_info_user_fills_response."""

    def test_valid(self, valid_raw_user_fill: dict[str, Any], user_address: str) -> None:
        """Test handling a valid user fills response."""
        raw_data = [valid_raw_user_fill, valid_raw_user_fill.copy()]
        response: HyperliquidRawUserFillsResponse = (
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        )
        assert isinstance(response, HyperliquidRawUserFillsResponse)
        assert len(response.root) == 2
        assert isinstance(response.root[0], HyperliquidRawUserFill)
        assert response.root[0].coin == "ETH-PERP"
        assert response.root[0].tid == 1001

    def test_empty_fills_list(self, user_address: str) -> None:
        """Test handling empty user fills response."""
        raw_data: list[Any] = []
        response = HyperliquidResponseHandler.handle_info_user_fills_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert isinstance(response, HyperliquidRawUserFillsResponse)
        assert len(response.root) == 0

    def test_validation_error_invalid_fill_item(self, user_address: str) -> None:
        """Test user fills response with invalid fill item."""
        invalid_fill = {"coin": "ETH"}  # Missing required fields
        raw_data = [invalid_fill]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (user fills for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_item_type_in_list(self, user_address: str) -> None:
        """Test user fills response with non-dict item in list."""
        raw_data = ["not_a_fill_dict"]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (user fills for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, user_address: str) -> None:
        """Test user fills response with wrong top-level type."""
        raw_data = {"invalid": "data"}
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Unexpected info (user fills for {user_address}) response format:"
            in exc_info.value.message
        )
        assert "expected list, got dict" in exc_info.value.message


class TestUserAccountEdgeCases:
    """Tests for additional edge cases in user and account response handling."""

    def test_user_state_with_empty_asset_positions(self, user_address: str) -> None:
        """Test user state response with empty asset positions array."""
        raw_data: dict[str, Any] = {
            "assetPositions": [],  # Empty positions
            "crossMaintenanceMarginUsed": "0.0",
            "crossMarginSummary": {
                "accountValue": "5000.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "5000.0",
            },
            "marginSummary": {
                "accountValue": "5000.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "5000.0",
            },
            "isolatedMaintenanceMarginUsed": "0.0",
            "isolatedMarginSummary": {
                "accountValue": "0.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "0.0",
            },
            "withdrawable": "5000.0",
        }
        response = HyperliquidResponseHandler.handle_info_user_state_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert len(response.asset_positions) == 0
        assert response.withdrawable == "5000.0"

    def test_user_state_with_missing_optional_fields(self, user_address: str) -> None:
        """Test user state response with minimal required fields only."""
        raw_data: dict[str, Any] = {
            "assetPositions": [],
            "crossMaintenanceMarginUsed": "0.0",
            "crossMarginSummary": {
                "accountValue": "1000.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "1000.0",
            },
            "marginSummary": {
                "accountValue": "1000.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "1000.0",
            },
            "isolatedMaintenanceMarginUsed": "0.0",
            "isolatedMarginSummary": {
                "accountValue": "0.0",
                "totalMarginUsed": "0.0",
                "totalNtlPos": "0.0",
                "totalRawUsd": "0.0",
            },
            "withdrawable": "1000.0",
        }
        response = HyperliquidResponseHandler.handle_info_user_state_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert response.withdrawable == "1000.0"
        assert response.cross_maintenance_margin_used == "0.0"

    def test_user_fills_with_mixed_valid_invalid_items(
        self,
        user_address: str,
        valid_raw_user_fill: dict[str, Any],
    ) -> None:
        """Test user fills response with mix of valid and invalid items."""
        invalid_fill = {"coin": "ETH"}  # Missing required fields
        raw_data = [
            valid_raw_user_fill.copy(),  # Valid
            invalid_fill,  # Invalid
        ]
        with pytest.raises(APIError) as exc_info:
            HyperliquidResponseHandler.handle_info_user_fills_response(
                cast("ParsedJsonResponse", raw_data),
                user_address=user_address,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert (
            f"Invalid info (user fills for {user_address}) response from exchange:"
            in exc_info.value.message
        )
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_user_state_with_complex_asset_positions(self, user_address: str) -> None:
        """Test user state response with multiple asset positions."""
        raw_data = {
            "assetPositions": [
                {
                    "asset": "BTC-PERP",
                    "position": {
                        "coin": "BTC-PERP",
                        "szi": "0.1",
                        "entryPx": "45000.0",
                        "leverage": {"type": "isolated", "value": 5},
                        "liquidationPx": "40000.0",
                        "marginUsed": "900.0",
                        "maxLeverage": 25,
                        "positionValue": "4500.0",
                        "returnOnEquity": "0.1",
                        "unrealizedPnl": "500.0",
                    },
                },
                {
                    "asset": "ETH-PERP",
                    "position": {
                        "coin": "ETH-PERP",
                        "szi": "2.0",
                        "entryPx": "3000.0",
                        "leverage": {"type": "cross", "value": 10},
                        "liquidationPx": "2700.0",
                        "marginUsed": "600.0",
                        "maxLeverage": 50,
                        "positionValue": "6000.0",
                        "returnOnEquity": "0.05",
                        "unrealizedPnl": "200.0",
                    },
                },
            ],
            "crossMaintenanceMarginUsed": "60.0",
            "crossMarginSummary": {
                "accountValue": "15000.0",
                "totalMarginUsed": "1500.0",
                "totalNtlPos": "10500.0",
                "totalRawUsd": "13500.0",
            },
            "marginSummary": {
                "accountValue": "15000.0",
                "totalMarginUsed": "1500.0",
                "totalNtlPos": "10500.0",
                "totalRawUsd": "13500.0",
            },
            "isolatedMaintenanceMarginUsed": "45.0",
            "isolatedMarginSummary": {
                "accountValue": "5000.0",
                "totalMarginUsed": "900.0",
                "totalNtlPos": "4500.0",
                "totalRawUsd": "4100.0",
            },
            "withdrawable": "12000.0",
        }
        response = HyperliquidResponseHandler.handle_info_user_state_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert len(response.asset_positions) == 2
        assert response.asset_positions[0].asset == "BTC-PERP"
        assert response.asset_positions[1].asset == "ETH-PERP"
        assert response.withdrawable == "12000.0"

    def test_open_orders_with_trigger_orders(self, user_address: str) -> None:
        """Test open orders response containing trigger orders."""
        # The openOrders endpoint returns simplified order format
        trigger_order = {
            "coin": "BTC-PERP",
            "limitPx": "46000.0",
            "oid": 7001,
            "side": "A",
            "sz": "0.1",
            "timestamp": 1678889700000,
            "origSz": "0.1",
        }
        raw_data = [trigger_order]
        response = HyperliquidResponseHandler.handle_info_open_orders_response(
            cast("ParsedJsonResponse", raw_data),
            user_address=user_address,
            status_code=200,
        )
        assert len(response.items) == 1
        assert response.items[0].coin == "BTC-PERP"
        assert response.items[0].oid == 7001
        assert response.items[0].limit_px == "46000"
