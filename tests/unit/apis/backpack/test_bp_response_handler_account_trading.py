"""Unit tests for BackpackAccountResponseHandler account and trading response functionality."""

from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.utils.typing import ParsedJsonResponse


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.conftest_response_handler"]

# Type aliases for clarity
type RawJsonPrim = str | int | float | bool | None
type RawJson = dict[str, RawJson] | list[RawJson] | RawJsonPrim
type RawJsonResponse = RawJson


class TestHandleGetBalancesResponse:
    """Tests for BackpackAccountResponseHandler.handle_get_balances_response."""

    def test_valid(self, valid_raw_balances: dict[str, Any]) -> None:
        """Test handling a valid raw balances response."""
        balances: dict[str, BackpackRawBalance] = (
            BackpackAccountResponseHandler.handle_get_balances_response(
                cast("ParsedJsonResponse", valid_raw_balances),
                status_code=200,
            )
        )
        assert isinstance(balances, dict)
        assert len(balances) == 2

        # Check SOL balance
        assert "SOL" in balances
        sol_balance = balances["SOL"]
        assert isinstance(sol_balance, BackpackRawBalance)
        assert sol_balance.available == "10.5"
        assert sol_balance.locked == "2.0"
        assert sol_balance.staked == "0"

        # Check USDC balance
        assert "USDC" in balances
        usdc_balance = balances["USDC"]
        assert isinstance(usdc_balance, BackpackRawBalance)
        assert usdc_balance.available == "1000.0"
        assert usdc_balance.locked == "50.0"
        assert usdc_balance.staked == "0"

    def test_empty_balances(self) -> None:
        """Test handling empty balances response."""
        raw_data: dict[str, Any] = {}
        balances = BackpackAccountResponseHandler.handle_get_balances_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert isinstance(balances, dict)
        assert len(balances) == 0

    def test_validation_error_invalid_balance_item(self) -> None:
        """Test balances response with invalid balance item."""
        raw_data = {
            "SOL": {
                "asset": "SOL",
                # Missing 'available' field
                "total": "12.5",
            },
        }
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_balances_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "balance details for SOL" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "available" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self) -> None:
        """Test balances response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_balances_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message

    def test_invalid_balance_item_raises_error(self) -> None:
        """Test that invalid balance items raise APIError with centralized validation."""
        raw_data = {
            "SOL": {
                "available": "10.5",
                "locked": "2.0",
                "staked": "0.0",
            },
            "INVALID": "not_a_dict",  # Invalid item
        }
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_balances_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "balances for asset 'INVALID'" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message


class TestHandleGetPositionsResponse:
    """Tests for BackpackAccountResponseHandler.handle_get_positions_response."""

    def test_valid(self, valid_raw_positions: list[dict[str, Any]]) -> None:
        """Test handling a valid raw positions response."""
        positions: list[BackpackRawPosition] = (
            BackpackAccountResponseHandler.handle_get_positions_response(
                cast("ParsedJsonResponse", valid_raw_positions),
                None,
                status_code=200,
            )
        )
        assert isinstance(positions, list)
        assert len(positions) == 2

        # Check first position
        pos1 = positions[0]
        assert isinstance(pos1, BackpackRawPosition)
        assert pos1.symbol == "SOL_USDC"
        assert pos1.break_even_price == "131.00"
        assert pos1.entry_price == "130.00"
        assert pos1.est_liquidation_price == "120.00"
        assert pos1.mark_price == "135.00"
        assert pos1.net_cost == "325.00"
        assert pos1.net_quantity == "2.5"
        assert pos1.pnl_realized == "10.00"
        assert pos1.pnl_unrealized == "12.50"
        assert pos1.position_id == "pos123"

        # Check second position
        pos2 = positions[1]
        assert pos2.symbol == "BTC_USDT"
        assert pos2.net_quantity == "-0.1"  # Short position
        assert pos2.position_id == "pos456"

    def test_empty_positions_list(self) -> None:
        """Test handling empty positions response."""
        raw_data: list[Any] = []
        positions = BackpackAccountResponseHandler.handle_get_positions_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert isinstance(positions, list)
        assert len(positions) == 0

    def test_invalid_position_item_raises_error(self) -> None:
        """Test that invalid position items raise APIError with centralized validation."""
        valid_position = {
            "symbol": "SOL_USDC",
            "breakEvenPrice": "131.00",
            "entryPrice": "130.00",
            "estLiquidationPrice": "120.00",
            "imf": "0.1",
            "imfFunction": {"base": "0.005", "factor": "0.000001"},
            "markPrice": "135.00",
            "mmf": "0.05",
            "mmfFunction": {"base": "0.002", "factor": "0.0000005"},
            "netCost": "325.00",
            "netQuantity": "2.5",
            "netExposureQuantity": "2.5",
            "netExposureNotional": "337.50",
            "pnlRealized": "10.00",
            "pnlUnrealized": "12.50",
            "cumulativeFundingPayment": "-0.50",
            "userId": 1,
            "positionId": "pos123",
            "cumulativeInterest": "0.0",
            "subaccountId": 0,
        }
        raw_data = [valid_position, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_positions_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "positions (all) item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self) -> None:
        """Test positions response with missing required field."""
        invalid_position = {
            "symbol": "SOL_USDC",
            "break_even_price": "131.00",
            # Missing 'entry_price' field
            "est_liquidation_price": "120.00",
            "imf": "0.1",
            "mark_price": "135.00",
            "mmf": "0.05",
            "net_cost": "325.00",
            "net_quantity": "2.5",
            "netExposureQuantity": "2.5",
            "netExposureNotional": "337.50",
            "pnl_realized": "10.00",
            "pnl_unrealized": "12.50",
            "cumulativeFundingPayment": "-0.50",
            "userId": 1,
            "position_id": "pos123",
            "cumulativeInterest": "0.0",
        }
        raw_data = [invalid_position]
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_positions_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "positions" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test positions response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_positions_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetAccountInfoResponse:
    """Tests for BackpackAccountResponseHandler.handle_get_account_info_response."""

    def test_valid(self, valid_raw_account_summary: dict[str, Any]) -> None:
        """Test handling a valid raw account summary response."""
        summary: BackpackRawAccountSummary = (
            BackpackAccountResponseHandler.handle_get_account_info_response(
                cast("ParsedJsonResponse", valid_raw_account_summary),
                status_code=200,
            )
        )
        assert isinstance(summary, BackpackRawAccountSummary)
        assert summary.auto_borrow_settlements is True
        assert summary.auto_lend is False
        assert summary.auto_realize_pnl is True
        assert summary.auto_repay_borrows is True
        assert summary.borrow_limit == Decimal("100000.0")
        assert summary.futures_maker_fee == Decimal("0.0002")
        assert summary.futures_taker_fee == Decimal("0.0005")
        assert summary.leverage_limit == Decimal("20.0")
        assert summary.limit_orders == 50
        assert summary.liquidating is False
        assert summary.position_limit == Decimal("500000.0")
        assert summary.spot_maker_fee == Decimal("0.0008")
        assert summary.spot_taker_fee == Decimal("0.0010")
        assert summary.trigger_orders == 20

    def test_validation_error_missing_field(self) -> None:
        """Test account summary response missing required field."""
        raw_data = {
            "auto_borrow_settlements": True,
            "auto_lend": False,
            # Missing 'auto_realize_pnl' field
            "auto_repay_borrows": True,
            "borrow_limit": "100000.0",
            "futures_maker_fee": "0.0002",
            "futures_taker_fee": "0.0005",
            "leverage_limit": "20.0",
            "limit_orders": 50,
            "liquidating": False,
            "position_limit": "500000.0",
            "spot_maker_fee": "0.0008",
            "spot_taker_fee": "0.0010",
            "trigger_orders": 20,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_account_info_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "account info" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test account summary response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackAccountResponseHandler.handle_get_account_info_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message


class TestHandleGetOpenOrdersResponse:
    """Tests for BackpackTradingResponseHandler.handle_get_open_orders_response."""

    def test_valid(self, valid_raw_open_orders: list[dict[str, Any]]) -> None:
        """Test handling a valid raw open orders response."""
        orders: list[BackpackRawOrder] = (
            BackpackTradingResponseHandler.handle_get_open_orders_response(
                cast("ParsedJsonResponse", valid_raw_open_orders),
                None,
                status_code=200,
            )
        )
        assert isinstance(orders, list)
        assert len(orders) == 2

        # Check first order
        order1 = orders[0]
        assert isinstance(order1, BackpackRawOrder)
        assert order1.id == "987654321"
        assert order1.clientId == "clientOrder001"
        assert order1.symbol == "SOL_USDC"
        assert order1.side == "buy"
        assert order1.orderType == "LIMIT"
        assert order1.quantity == "10.0"
        assert order1.price == "140.00"
        assert order1.timeInForce == "GTC"
        assert order1.status == "NEW"
        assert order1.executedQuantity == "0"
        assert order1.avgFillPrice is None

        # Check second order
        order2 = orders[1]
        assert order2.id == "order002"
        assert order2.symbol == "BTC_USDT"
        assert order2.side == "sell"

    def test_empty_orders_list(self) -> None:
        """Test handling empty open orders response."""
        raw_data: list[Any] = []
        orders = BackpackTradingResponseHandler.handle_get_open_orders_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert isinstance(orders, list)
        assert len(orders) == 0

    def test_invalid_order_item_raises_error(self) -> None:
        """Test that invalid order items raise APIError with centralized validation."""
        valid_order = {
            "id": "987654321",
            "clientId": "clientOrder001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "140.00",
            "timeInForce": "GTC",
            "status": "NEW",
            "createdAt": 1678886405000,
            "executedQuantity": "0",
            "avgFillPrice": None,
        }
        raw_data = [valid_order, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "open orders (all) item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self) -> None:
        """Test open orders response with missing required field."""
        invalid_order = {
            "id": "987654321",
            "clientId": "clientOrder001",
            "symbol": "SOL_USDC",
            # Missing 'side' field
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "140.00",
            "timeInForce": "GTC",
            "status": "NEW",
            "createdAt": 1678886405000,
            "executedQuantity": "0",
        }
        raw_data = [invalid_order]
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "open orders" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test open orders response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_open_orders_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetOrderHistoryResponse:
    """Tests for BackpackTradingResponseHandler.handle_get_order_history_response."""

    def test_valid(self, valid_raw_order_history: list[dict[str, Any]]) -> None:
        """Test handling a valid raw order history response."""
        orders: list[BackpackRawOrder] = (
            BackpackTradingResponseHandler.handle_get_order_history_response(
                cast("ParsedJsonResponse", valid_raw_order_history),
                None,
                status_code=200,
            )
        )
        assert isinstance(orders, list)
        assert len(orders) == 2

        # Check first historical order
        order1 = orders[0]
        assert isinstance(order1, BackpackRawOrder)
        assert order1.id == "histOrder001"
        assert order1.status == "FILLED"
        assert order1.executedQuantity == "10.0"
        assert order1.avgFillPrice == "140.00"

        # Check second historical order
        order2 = orders[1]
        assert order2.id == "histOrder002"
        assert order2.symbol == "BTC_USDT"
        assert order2.side == "sell"
        assert order2.orderType == "MARKET"
        assert order2.price is None  # Market order
        assert order2.timeInForce == "IOC"
        assert order2.status == "FILLED"

    def test_empty_order_history_list(self) -> None:
        """Test handling empty order history response."""
        raw_data: list[Any] = []
        orders = BackpackTradingResponseHandler.handle_get_order_history_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert isinstance(orders, list)
        assert len(orders) == 0

    def test_invalid_order_item_raises_error(self) -> None:
        """Test that invalid order history items raise APIError with centralized validation."""
        valid_order = {
            "id": "histOrder001",
            "clientId": "clientOrder001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "140.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678886000000,
            "executedQuantity": "10.0",
            "avgFillPrice": "140.00",
        }
        raw_data = [valid_order, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_order_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "order history (all) item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self) -> None:
        """Test order history response with missing required field."""
        invalid_order = {
            "id": "histOrder001",
            "clientId": "clientOrder001",
            # Missing 'symbol' field
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "140.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678886000000,
            "executedQuantity": "10.0",
            "avgFillPrice": "140.00",
        }
        raw_data = [invalid_order]
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_order_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "order history" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test order history response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_order_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetTradeHistoryResponse:
    """Tests for BackpackAccountResponseHandler.handle_get_trade_history_response."""

    def test_valid(self, valid_raw_trade_history: list[dict[str, Any]]) -> None:
        """Test handling a valid raw trade history response."""
        trades: list[BackpackRawPublicTrade] = (
            BackpackTradingResponseHandler.handle_get_trade_history_response(
                cast("ParsedJsonResponse", valid_raw_trade_history),
                None,
                status_code=200,
            )
        )
        assert isinstance(trades, list)
        assert len(trades) == 2

        # Check first trade
        trade1 = trades[0]
        assert isinstance(trade1, BackpackRawPublicTrade)
        assert trade1.symbol == "SOL_USDC"
        assert trade1.price == "141.00"
        assert trade1.quantity == "1.5"
        assert trade1.time == 1678886000000
        assert trade1.id == "tradeHist001"
        assert trade1.order_id == "histOrderX001"

        # Check second trade
        trade2 = trades[1]
        assert trade2.id == "tradeHist002"
        assert trade2.price == "141.05"
        assert trade2.quantity == "0.75"

    def test_empty_trade_history_list(self) -> None:
        """Test handling empty trade history response."""
        raw_data: list[Any] = []
        trades = BackpackTradingResponseHandler.handle_get_trade_history_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert isinstance(trades, list)
        assert len(trades) == 0

    def test_invalid_trade_item_raises_error(self) -> None:
        """Test that invalid trade history items raise APIError with centralized validation."""
        valid_trade = {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886000000,
            "id": "tradeHist001",
            "order_id": "histOrderX001",
        }
        raw_data = [valid_trade, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_trade_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "trade history (all) item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self) -> None:
        """Test trade history response with missing required field."""
        invalid_trade = {
            "symbol": "SOL_USDC",
            # Missing 'price' field
            "qty": "1.5",
            "time": 1678886000000,
            "id": "tradeHist001",
            "order_id": "histOrderX001",
        }
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_trade_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "trade history" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self) -> None:
        """Test trade history response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_trade_history_response(
                cast("ParsedJsonResponse", raw_data),
                None,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetOrderStatusResponse:
    """Tests for BackpackTradingResponseHandler.handle_get_order_status_response."""

    def test_valid(self, valid_raw_order_status: dict[str, Any], order_id: str) -> None:
        """Test handling a valid raw order status response."""
        order: BackpackRawOrder = BackpackTradingResponseHandler.handle_get_order_status_response(
            cast("ParsedJsonResponse", valid_raw_order_status),
            order_id,
            status_code=200,
        )
        assert isinstance(order, BackpackRawOrder)
        assert order.id == "statusOrder123"
        assert order.clientId == "clientStatus001"
        assert order.side == "sell"
        assert order.price == "142.00"
        assert order.status == "FILLED"
        assert order.executedQuantity == "10.0"
        assert order.avgFillPrice == "142.00"

    def test_validation_error_missing_field(self, order_id: str) -> None:
        """Test order status response missing required field."""
        raw_data = {
            "id": "statusOrder123",
            "clientId": "clientStatus001",
            "symbol": "SOL_USDC",
            # Missing 'side' field
            "orderType": "LIMIT",
            "quantity": "10.0",
            "price": "142.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678889000000,
            "executedQuantity": "10.0",
            "avgFillPrice": "142.00",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                order_id,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"order status (id={order_id})" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, order_id: str) -> None:
        """Test order status response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackTradingResponseHandler.handle_get_order_status_response(
                cast("ParsedJsonResponse", raw_data),
                order_id,
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message


class TestAccountTradingEdgeCases:
    """Tests for additional edge cases in account and trading response handling."""

    def test_balance_with_zero_amounts(self) -> None:
        """Test balance response with zero amounts."""
        raw_data = {
            "ZERO": {
                "available": "0.0",
                "locked": "0.0",
                "staked": "0.0",
            },
        }
        balances = BackpackAccountResponseHandler.handle_get_balances_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert len(balances) == 1
        assert balances["ZERO"].available == "0.0"
        assert balances["ZERO"].locked == "0.0"
        assert balances["ZERO"].staked == "0.0"

    def test_position_with_negative_values(self) -> None:
        """Test position response with negative values (short position)."""
        raw_data = [
            {
                "symbol": "BTC_USDT",
                "breakEvenPrice": "54900.00",
                "entryPrice": "55000.00",
                "estLiquidationPrice": "60000.00",
                "imf": "0.1",
                "imfFunction": {"base": "0.005", "factor": "0.000001"},
                "markPrice": "54000.00",
                "mmf": "0.05",
                "mmfFunction": {"base": "0.002", "factor": "0.0000005"},
                "netCost": "-5500.00",  # Negative cost for short
                "netQuantity": "-0.1",  # Negative quantity for short
                "netExposureQuantity": "-0.1",
                "netExposureNotional": "-5400.00",
                "pnlRealized": "50.00",
                "pnlUnrealized": "100.00",
                "cumulativeFundingPayment": "1.20",
                "userId": 1,
                "positionId": "pos456",
                "cumulativeInterest": "0.0",
                "subaccountId": 0,
            },
        ]
        positions = BackpackAccountResponseHandler.handle_get_positions_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert len(positions) == 1
        position = positions[0]
        assert position.net_cost == "-5500.00"
        assert position.net_quantity == "-0.1"

    def test_order_with_null_price(self) -> None:
        """Test order response with null price (market order)."""
        raw_data = [
            {
                "id": "market123",
                "clientId": "clientMarket001",
                "symbol": "SOL_USDC",
                "side": "buy",
                "orderType": "MARKET",
                "quantity": "5.0",
                "price": None,  # Market order has no price
                "timeInForce": "IOC",
                "status": "FILLED",
                "createdAt": 1678886405000,
                "executedQuantity": "5.0",
                "avgFillPrice": "140.25",
            },
        ]
        orders = BackpackTradingResponseHandler.handle_get_open_orders_response(
            cast("ParsedJsonResponse", raw_data),
            None,
            status_code=200,
        )
        assert len(orders) == 1
        order = orders[0]
        assert order.price is None
        assert order.orderType == "MARKET"
        assert order.avgFillPrice == "140.25"

    def test_account_summary_with_edge_values(self) -> None:
        """Test account summary response with edge case values."""
        raw_data = {
            "auto_borrow_settlements": False,
            "auto_lend": True,
            "auto_realize_pnl": False,
            "auto_repay_borrows": False,
            "borrow_limit": "0.0",  # Zero limit
            "futures_maker_fee": "0.0000",  # Zero fee
            "futures_taker_fee": "0.0001",
            "leverage_limit": "1.0",  # Minimum leverage
            "limit_orders": 0,  # No limit orders
            "liquidating": True,  # Account in liquidation
            "position_limit": "1.0",  # Minimum position limit
            "spot_maker_fee": "0.0000",
            "spot_taker_fee": "0.0001",
            "trigger_orders": 0,  # No trigger orders
        }
        summary = BackpackAccountResponseHandler.handle_get_account_info_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert summary.borrow_limit == Decimal("0.0")
        assert summary.leverage_limit == Decimal("1.0")
        assert summary.limit_orders == 0
        assert summary.liquidating is True
