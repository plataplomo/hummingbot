"""Unit tests for BackpackResponseHandler."""

import copy
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_funding import BackpackRawFundingRate
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import BackpackRawPosition
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# --- Type Aliases for Raw JSON and Parametrized Tests ---

type RawJsonPrim = str | int | float | bool | None
type RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
type RawJsonResponse = RawJson  # Alias for clarity in handler signatures

type HandlerMethodType = Callable[..., Any]
type HandlerArgsSpecType = dict[str, Any]
type InvalidDataForTestType = RawJsonResponse  # For top-level type/validation tests
type ModificationDetailsType = dict[str, Any]
# More specific type for list item tests where data starts as list/dict before modification
# Reverting this change as it caused incompatibility
# InvalidListDataForTestType: TypeAlias = list[Any] | dict[str, Any]

type InvalidTypeTestCaseType = tuple[
    HandlerMethodType, InvalidDataForTestType, str, HandlerArgsSpecType, str
]
type ValidationErrorTestCaseType = tuple[
    HandlerMethodType, str, ModificationDetailsType, str, HandlerArgsSpecType, str
]
type ListItemTestCaseType = tuple[HandlerMethodType, str, str]

# More specific type alias for the _list_item_error_cases tuples
type ListItemErrorTestCaseStructure = tuple[
    HandlerMethodType,  # The handler method to test
    str,  # Name of the fixture providing valid raw list/dict data
    ModificationDetailsType,  # Describes how to modify an item in the list/dict
    str,  # Expected substring in the log message or error
    HandlerArgsSpecType,  # Arguments for the handler method
    str,  # Context string format for error messages
    bool,  # True if a warning log is expected, False for APIError
]

# --- Fixtures ---


@pytest.fixture
def symbol_spot() -> str:
    return "SOL_USDC"


@pytest.fixture
def symbol_perp() -> str:
    return "SOL-PERP"


@pytest.fixture
def symbol_any() -> str:
    """Generic symbol fixture for tests not specific to spot/perp."""
    return "GENERIC_SYMBOL"


@pytest.fixture
def order_id() -> str:
    return "987654321"


@pytest.fixture
def client_id() -> str:
    return "clientOrder001"


@pytest.fixture
def valid_raw_ticker(symbol_spot: str) -> dict[str, Any]:
    return {
        "symbol": symbol_spot,
        "price": "140.50",
        "bid": "140.49",
        "ask": "140.51",
        "volume": "500000.0",
        "time": 1678886400000,
    }


@pytest.fixture
def valid_raw_order_book(symbol_spot: str) -> dict[str, Any]:
    return {
        "bids": [["140.10", "10"], ["140.00", "20"]],
        "asks": [["140.20", "15"], ["140.30", "25"]],
        "lastUpdateId": "update123",
        "timestamp": 1678886401000,
    }


@pytest.fixture
def valid_raw_trade_item(symbol_spot: str) -> dict[str, Any]:
    return {
        "symbol": symbol_spot,
        "price": "141.00",
        "qty": "1.5",
        "time": 1678886402000,
        "id": "1001",
        "orderId": "order123",
    }


@pytest.fixture
def valid_raw_recent_trades(valid_raw_trade_item: dict[str, Any]) -> list[dict[str, Any]]:
    item1 = valid_raw_trade_item.copy()
    item2 = valid_raw_trade_item.copy()
    item2["id"] = "1002"
    item2["price"] = "141.01"
    item2["qty"] = "0.5"
    item2["time"] = 1678886403000
    item2["orderId"] = "order124"
    return [item1, item2]


@pytest.fixture
def valid_raw_balance_item() -> dict[str, Any]:
    return {
        "asset": "SOL",
        "available": "10.5",
        "total": "12.5",
    }


@pytest.fixture
def valid_raw_balances(valid_raw_balance_item: dict[str, Any]) -> dict[str, Any]:
    usdc_item = valid_raw_balance_item.copy()
    usdc_item["asset"] = "USDC"
    usdc_item["available"] = "1000.0"
    usdc_item["total"] = "1050.0"
    return {"SOL": valid_raw_balance_item, "USDC": usdc_item}


@pytest.fixture
def valid_raw_position_item(symbol_spot: str) -> dict[str, Any]:
    return {
        "symbol": symbol_spot,
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
    }


@pytest.fixture
def valid_raw_positions(valid_raw_position_item: dict[str, Any]) -> list[dict[str, Any]]:
    item2 = valid_raw_position_item.copy()
    item2["symbol"] = "BTC_USDT"
    item2["breakEvenPrice"] = "54900.00"
    item2["entryPrice"] = "55000.00"
    item2["estLiquidationPrice"] = "60000.00"
    item2["markPrice"] = "54000.00"
    item2["netCost"] = "-5500.00"
    item2["netQuantity"] = "-0.1"
    item2["netExposureQuantity"] = "-0.1"
    item2["netExposureNotional"] = "-5400.00"
    item2["pnlRealized"] = "50.00"
    item2["pnlUnrealized"] = "100.00"
    item2["cumulativeFundingPayment"] = "1.20"
    item2["positionId"] = "pos456"
    return [valid_raw_position_item, item2]


@pytest.fixture
def valid_raw_order(order_id: str, client_id: str, symbol_spot: str) -> dict[str, Any]:
    return {
        "id": order_id,
        "clientId": client_id,
        "symbol": symbol_spot,
        "side": "buy",
        "orderType": "LIMIT",
        "quantity": "10.0",
        "price": "140.00",
        "timeInForce": "GTC",
        "status": "NEW",
        "createdAt": 1678886405000,
        "executedQuantity": "0",
        "avgFillPrice": None,  # Often present, even if 0 executed
    }


@pytest.fixture
def valid_raw_open_orders(valid_raw_order: dict[str, Any]) -> list[dict[str, Any]]:
    item2 = valid_raw_order.copy()
    item2["id"] = "order002"
    item2["symbol"] = "BTC_USDT"
    item2["side"] = "sell"
    item2["quantity"] = "0.1"
    item2["price"] = "56000.00"
    item2["createdAt"] = 1678886411000
    return [valid_raw_order, item2]


@pytest.fixture
def valid_raw_funding_rate(symbol_perp: str) -> dict[str, Any]:
    return {
        "symbol": symbol_perp,
        "rate": "0.000123",
        "markPrice": "140.00",
        "indexPrice": "139.90",
        "time": 1678887000000,
    }


@pytest.fixture
def valid_raw_account_summary() -> dict[str, Any]:
    return {
        "autoBorrowSettlements": True,
        "autoLend": False,
        "autoRealizePnl": True,
        "autoRepayBorrows": True,
        "borrowLimit": "100000.0",
        "futuresMakerFee": "0.0002",
        "futuresTakerFee": "0.0005",
        "leverageLimit": "20.0",
        "limitOrders": 50,
        "liquidating": False,
        "positionLimit": "500000.0",
        "spotMakerFee": "0.0008",
        "spotTakerFee": "0.0010",
        "triggerOrders": 20,
    }


@pytest.fixture
def valid_raw_withdrawal() -> dict[str, Any]:
    return {
        "id": 12345,
        "blockchain": "Solana",
        "quantity": "100.0",
        "fee": "0.01",
        "symbol": "USDC",
        "status": "confirmed",
        "toAddress": "SOLANA_ADDRESS_HERE",
        "createdAt": "2023-03-15T10:00:00.000Z",
        "isInternal": False,
    }


@pytest.fixture
def valid_raw_order_history(valid_raw_order: dict[str, Any]) -> list[dict[str, Any]]:
    item1 = valid_raw_order.copy()
    item1["id"] = "histOrder001"
    item1["status"] = "FILLED"
    item1["executedQuantity"] = "10.0"
    item1["avgFillPrice"] = "140.00"
    item1["createdAt"] = 1678886000000

    item2 = valid_raw_order.copy()
    item2["id"] = "histOrder002"
    item2["symbol"] = "BTC_USDT"
    item2["side"] = "sell"
    item2["orderType"] = "MARKET"
    item2["quantity"] = "0.2"
    item2["price"] = None  # Market order might not have price
    item2["timeInForce"] = "IOC"
    item2["status"] = "FILLED"
    item2["executedQuantity"] = "0.2"
    item2["avgFillPrice"] = "55950.00"
    item2["createdAt"] = 1678886100000

    return [item1, item2]


@pytest.fixture
def valid_raw_trade_history(symbol_spot: str) -> list[dict[str, Any]]:
    # This fixture is structured for BackpackRawTrade, aligning with handler's return type
    trade1 = {
        "symbol": symbol_spot,
        "price": "141.00",
        "qty": "1.5",  # qty for BackpackRawTrade
        "time": 1678886000000,  # int timestamp
        "id": "tradeHist001",  # id for BackpackRawTrade
        "orderId": "histOrderX001",
    }
    trade2 = {
        "symbol": symbol_spot,
        "price": "141.05",
        "qty": "0.75",  # qty for BackpackRawTrade
        "time": 1678886001000,  # int timestamp
        "id": "tradeHist002",  # id for BackpackRawTrade
        "orderId": "histOrderX002",
    }
    return [trade1, trade2]


@pytest.fixture
def valid_raw_market_data() -> list[list[Any]]:
    return [
        [1678886400000, "138.0", "139.5", "137.5", "139.0", "1000.0"],
        [1678886460000, "139.0", "140.0", "138.5", "139.8", "1200.0"],
    ]


@pytest.fixture
def valid_raw_historical_trades(symbol_spot: str) -> list[dict[str, Any]]:
    # This fixture should provide data structured for BackpackRawTrade
    trade1 = {
        "id": "1001",  # Explicitly string
        "symbol": symbol_spot,
        "price": "135.00",
        "qty": "2.0",  # Alias for BackpackRawTrade.quantity
        "time": 1678880000000,
        "orderId": "histOrderA",
    }
    trade2 = {
        "id": "1002",  # Explicitly string
        "symbol": symbol_spot,
        "price": "135.10",
        "qty": "1.0",  # Alias for BackpackRawTrade.quantity
        "time": 1678880100000,
        "orderId": "histOrderB",
    }
    return [trade1, trade2]


@pytest.fixture
def valid_raw_order_status(valid_raw_order: dict[str, Any]) -> dict[str, Any]:
    order_copy = valid_raw_order.copy()
    order_copy["id"] = "statusOrder123"
    order_copy["clientId"] = "clientStatus001"
    order_copy["side"] = "sell"
    order_copy["price"] = "142.00"
    order_copy["status"] = "FILLED"
    order_copy["createdAt"] = 1678889000000
    order_copy["executedQuantity"] = "10.0"  # Match original quantity
    order_copy["avgFillPrice"] = "142.00"
    return order_copy


# --- Test Classes ---


class TestHandleGetTickerResponse:
    def test_valid(self, valid_raw_ticker: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw ticker response."""
        ticker: BackpackRawTicker = BackpackResponseHandler.handle_get_ticker_response(
            cast(RawJsonResponse, valid_raw_ticker), symbol_spot, 200, {}
        )
        assert isinstance(ticker, BackpackRawTicker)
        assert ticker.symbol == symbol_spot
        assert ticker.price == "140.50"


class TestHandleGetOrderBookResponse:
    def test_valid(self, valid_raw_order_book: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw order book response."""
        order_book: BackpackRawOrderBook = BackpackResponseHandler.handle_get_order_book_response(
            cast(RawJsonResponse, valid_raw_order_book), symbol_spot, 200, {}
        )
        assert isinstance(order_book, BackpackRawOrderBook)
        assert len(order_book.bids) == 2
        assert order_book.bids[0] == ("140.10", "10")
        assert len(order_book.asks) == 2
        assert order_book.asks[0] == ("140.20", "15")


class TestHandleGetRecentTradesResponse:
    def test_valid(self, valid_raw_recent_trades: list[dict[str, Any]], symbol_spot: str) -> None:
        """Test handling a valid raw recent trades response."""
        trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_recent_trades_response(
            cast(RawJsonResponse, valid_raw_recent_trades), symbol_spot, 200, {}
        )
        assert len(trades) == 2
        assert isinstance(trades[0], BackpackRawTrade)
        assert trades[0].symbol == symbol_spot
        assert trades[0].id == "1001"
        assert trades[0].price == "141.00"
        assert trades[0].time == 1678886402000

        assert isinstance(trades[1], BackpackRawTrade)
        assert trades[1].id == "1002"
        assert trades[1].time == 1678886403000


class TestHandleGetBalancesResponse:
    def test_valid(self, valid_raw_balances: dict[str, Any]) -> None:
        """Test handling a valid raw balances response."""
        balances = BackpackResponseHandler.handle_get_balances_response(
            cast(RawJsonResponse, valid_raw_balances)
        )
        assert isinstance(balances, dict)
        assert len(balances) == 2
        assert "SOL" in balances
        assert isinstance(balances["SOL"], BackpackRawBalance)
        assert balances["SOL"].available == "10.5"
        assert "USDC" in balances
        assert balances["USDC"].available == "1000.0"
        assert balances["USDC"].total == "1050.0"


class TestHandleGetPositionsResponse:
    def test_valid(self, valid_raw_positions: list[dict[str, Any]], symbol_any: str) -> None:
        """Test handling a valid raw positions response."""
        positions = BackpackResponseHandler.handle_get_positions_response(
            cast(RawJsonResponse, valid_raw_positions), symbol_any
        )
        assert isinstance(positions, list)
        assert len(positions) == 2
        assert isinstance(positions[0], BackpackRawPosition)
        assert positions[0].symbol == "SOL_USDC"  # From fixture
        assert positions[1].net_quantity == "-0.1"


class TestHandlePlaceOrderResponse:
    def test_valid(self, valid_raw_order: dict[str, Any]) -> None:
        """Test handling a valid raw place order response."""
        order = BackpackResponseHandler.handle_place_order_response(
            cast(RawJsonResponse, valid_raw_order)
        )
        assert isinstance(order, BackpackRawOrder)
        assert order.id == valid_raw_order["id"]
        assert order.status == "NEW"


class TestHandleCancelOrderResponse:
    def test_none_input(self, order_id: str, symbol_spot: str) -> None:
        """Test handling cancel order response with None input."""
        with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
            result = BackpackResponseHandler.handle_cancel_order_response(
                None, order_id=order_id, symbol=symbol_spot
            )
            assert result is True
            mock_log.assert_not_called()

    def test_empty_dict_input(self, order_id: str, symbol_spot: str) -> None:
        """Test handling cancel order response with empty dict input."""
        with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
            result = BackpackResponseHandler.handle_cancel_order_response(
                cast(RawJsonResponse, {}), order_id=order_id, symbol=symbol_spot
            )
            assert result is True
            mock_log.assert_not_called()

    def test_unexpected_content(self, order_id: str, symbol_spot: str) -> None:
        """Test handling cancel order response with unexpected content."""
        raw_data = {"status": "cancelled but here is some unexpected data"}
        with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
            result = BackpackResponseHandler.handle_cancel_order_response(
                cast(RawJsonResponse, raw_data), order_id=order_id, symbol=symbol_spot
            )
            assert result is True
            mock_log.assert_called_once()
            assert "Received unexpected content" in mock_log.call_args[0][0]


class TestHandleGetOpenOrdersResponse:
    def test_valid(self, valid_raw_open_orders: list[dict[str, Any]], symbol_any: str) -> None:
        """Test handling a valid raw open orders response."""
        orders = BackpackResponseHandler.handle_get_open_orders_response(
            cast(RawJsonResponse, valid_raw_open_orders), symbol_any
        )
        assert isinstance(orders, list)
        assert len(orders) == 2
        assert isinstance(orders[0], BackpackRawOrder)
        assert orders[0].id == valid_raw_open_orders[0]["id"]
        assert orders[1].symbol == "BTC_USDT"


class TestHandleGetFundingRateResponse:
    def test_valid(self, valid_raw_funding_rate: dict[str, Any], symbol_perp: str) -> None:
        """Test handling a valid raw funding rate response."""
        funding_rate: BackpackRawFundingRate = (
            BackpackResponseHandler.handle_get_funding_rate_response(
                cast(RawJsonResponse, valid_raw_funding_rate), symbol_perp, 200, {}
            )
        )
        assert isinstance(funding_rate, BackpackRawFundingRate)
        assert funding_rate.symbol == symbol_perp
        assert funding_rate.funding_rate == "0.000123"
        assert funding_rate.mark_price == "140.00"
        assert funding_rate.index_price == "139.90"
        assert funding_rate.time == 1678887000000


class TestHandleGetAccountInfoResponse:
    def test_valid(self, valid_raw_account_summary: dict[str, Any]) -> None:
        """Test handling a valid raw account info response."""
        account_info = BackpackResponseHandler.handle_get_account_info_response(
            cast(RawJsonResponse, valid_raw_account_summary)
        )
        assert isinstance(account_info, BackpackRawAccountSummary)
        assert account_info.leverage_limit == Decimal("20.0")
        assert account_info.limit_orders == 50
        assert account_info.auto_lend is False


class TestHandleWithdrawResponse:
    def test_valid(self, valid_raw_withdrawal: dict[str, Any]) -> None:
        """Test handling a valid raw withdraw response."""
        withdraw_response = BackpackResponseHandler.handle_withdraw_response(
            cast(RawJsonResponse, valid_raw_withdrawal)
        )
        assert isinstance(withdraw_response, BackpackRawWithdrawalResponse)
        assert withdraw_response.id == 12345
        assert withdraw_response.status == "confirmed"
        assert withdraw_response.created_at == datetime(2023, 3, 15, 10, 0, 0, tzinfo=UTC)


class TestHandleGetOrderHistoryResponse:
    def test_valid(self, valid_raw_order_history: list[dict[str, Any]], symbol_any: str) -> None:
        """Test handling a valid raw order history response."""
        orders = BackpackResponseHandler.handle_get_order_history_response(
            cast(RawJsonResponse, valid_raw_order_history), symbol_any
        )
        assert isinstance(orders, list)
        assert len(orders) == 2
        assert isinstance(orders[0], BackpackRawOrder)
        assert orders[0].id == "histOrder001"
        assert orders[0].status == "FILLED"
        assert orders[1].symbol == "BTC_USDT"


class TestHandleGetTradeHistoryResponse:
    def test_valid(self, valid_raw_trade_history: list[dict[str, Any]], symbol_spot: str) -> None:
        """Test handling a valid raw trade history response."""
        trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_trade_history_response(
            cast(RawJsonResponse, valid_raw_trade_history), symbol_spot
        )
        assert isinstance(trades, list)
        assert len(trades) == 2
        assert isinstance(trades[0], BackpackRawTrade)
        assert trades[0].id == "tradeHist001"
        assert trades[0].symbol == symbol_spot
        assert trades[0].price == "141.00"
        assert trades[0].time == 1678886000000

        assert isinstance(trades[1], BackpackRawTrade)
        assert trades[1].id == "tradeHist002"
        assert trades[1].symbol == symbol_spot
        assert trades[1].price == "141.05"
        assert trades[1].time == 1678886001000


class TestHandleGetMarketDataResponse:
    def test_valid(self, valid_raw_market_data: list[list[Any]], symbol_spot: str) -> None:
        """Test handling a valid raw market data (klines) response."""
        klines: list[RawJson] = BackpackResponseHandler.handle_get_market_data_response(
            cast(RawJsonResponse, valid_raw_market_data), symbol_spot, "1m", 200, {}
        )
        assert isinstance(klines, list)
        assert len(klines) == 2
        assert isinstance(klines[0], list)
        assert klines[0][0] == 1678886400000
        assert isinstance(klines[1], list)
        assert klines[1][4] == "139.8"


class TestHandleGetHistoricalTradesResponse:
    def test_valid(
        self, valid_raw_historical_trades: list[dict[str, Any]], symbol_spot: str
    ) -> None:
        """Test handling a valid raw historical trades response."""
        trades: list[BackpackRawTrade] = (
            BackpackResponseHandler.handle_get_historical_trades_response(
                cast(RawJsonResponse, valid_raw_historical_trades), symbol_spot, 200, {}
            )
        )
        assert isinstance(trades, list)
        assert len(trades) == 2

        assert isinstance(trades[0], BackpackRawTrade)
        assert trades[0].id == "1001"
        assert trades[0].order_id == "histOrderA"
        assert trades[0].symbol == symbol_spot
        assert trades[0].price == "135.00"
        assert trades[0].quantity == "2.0"
        assert trades[0].time == 1678880000000

        assert isinstance(trades[1], BackpackRawTrade)
        assert trades[1].id == "1002"
        assert trades[1].order_id == "histOrderB"
        assert trades[1].symbol == symbol_spot
        assert trades[1].price == "135.10"
        assert trades[1].quantity == "1.0"
        assert trades[1].time == 1678880100000


class TestHandleGetOrderStatusResponse:
    def test_valid(self, valid_raw_order_status: dict[str, Any], order_id: str) -> None:
        """Test handling a valid raw order status response."""
        valid_raw_order_status["id"] = order_id
        order = BackpackResponseHandler.handle_get_order_status_response(
            cast(RawJsonResponse, valid_raw_order_status),
            identifier=order_id,
        )
        assert isinstance(order, BackpackRawOrder)
        assert order.id == order_id
        assert order.status == "FILLED"

    def test_none_input(self, order_id: str) -> None:
        """Test handling order status response when input is None (order not found)."""
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_order_status_response(None, identifier=order_id)
        assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
        assert f"Order {order_id} not found" in exc_info.value.message


# --- Parametrized Invalid Type Tests ---

_invalid_type_test_cases: list[InvalidTypeTestCaseType] = [
    (
        BackpackResponseHandler.handle_get_ticker_response,
        ["invalid"],
        "dict",
        {"symbol": "symbol_spot", "status_code": 400, "headers": {}},
        "ticker ({symbol}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_order_book_response,
        ["invalid"],
        "dict",
        {"symbol": "symbol_spot", "status_code": 400, "headers": {}},
        "order book ({symbol}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_recent_trades_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_spot", "status_code": 400, "headers": {}},
        "recent trades ({symbol}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_balances_response,
        ["invalid"],
        "dict",
        {},
        "balances",
    ),
    (
        BackpackResponseHandler.handle_get_positions_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_any"},
        "positions ({symbol})",
    ),
    (
        BackpackResponseHandler.handle_place_order_response,
        ["invalid"],
        "dict",
        {},
        "place order response",
    ),
    (
        BackpackResponseHandler.handle_get_open_orders_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_any"},
        "open orders ({symbol})",
    ),
    (
        BackpackResponseHandler.handle_get_funding_rate_response,
        ["invalid"],
        "dict",
        {"symbol": "symbol_perp", "status_code": 400, "headers": {}},
        "funding rate ({symbol}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_account_info_response,
        ["invalid"],
        "dict",
        {},
        "account info",
    ),
    (
        BackpackResponseHandler.handle_withdraw_response,
        ["invalid"],
        "dict",
        {},
        "withdraw response",
    ),
    (
        BackpackResponseHandler.handle_get_order_history_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_any"},
        "order history ({symbol})",
    ),
    (
        BackpackResponseHandler.handle_get_trade_history_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_spot"},
        "trade history/fills ({symbol})",
    ),
    (
        BackpackResponseHandler.handle_get_market_data_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_spot", "timeframe": "1m", "status_code": 400, "headers": {}},
        "market data (klines {symbol}, {timeframe}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_historical_trades_response,
        {"error": "expected list"},
        "list",
        {"symbol": "symbol_spot", "status_code": 400, "headers": {}},
        "historical trades ({symbol}) - Status: 400",
    ),
    (
        BackpackResponseHandler.handle_get_order_status_response,
        ["invalid"],
        "dict",
        {"identifier": "order_id"},
        "order status (id={identifier})",
    ),
]


@pytest.mark.parametrize(
    (
        "handler_method, invalid_data, expected_container_type, handler_args_spec, "
        "context_format_string"
    ),
    _invalid_type_test_cases,
)
def test_handler_invalid_top_level_type(
    handler_method: HandlerMethodType,
    invalid_data: InvalidDataForTestType,
    expected_container_type: str,
    handler_args_spec: HandlerArgsSpecType,
    context_format_string: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test handlers raise APIError for incorrect top-level data type."""
    actual_handler_args: dict[str, Any] = {}
    for arg_name, value_or_fixture_name in handler_args_spec.items():
        if isinstance(value_or_fixture_name, str):
            try:
                actual_handler_args[arg_name] = request.getfixturevalue(value_or_fixture_name)
            except (pytest.FixtureLookupError, AttributeError):
                actual_handler_args[arg_name] = value_or_fixture_name
        else:
            actual_handler_args[arg_name] = value_or_fixture_name

    # Format context string
    final_context_string = context_format_string
    try:
        # Attempt to format using only the args present in the spec
        format_args = {
            k: actual_handler_args[k] for k in handler_args_spec if k in actual_handler_args
        }
        final_context_string = context_format_string.format(**format_args)
    except KeyError:
        pass  # Keep original if formatting fails

    with pytest.raises(APIError) as exc_info:
        handler_method(invalid_data, **actual_handler_args)

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    # Adjust expected message based on handler specifics
    if handler_method is BackpackResponseHandler.handle_place_order_response:
        expected_message_part = (
            f"Unexpected {final_context_string} format: expected {expected_container_type}"
        )
    else:
        expected_message_part = (
            f"Unexpected {final_context_string} response format: expected {expected_container_type}"
        )
    assert expected_message_part in exc_info.value.message
    assert (
        f"got {type(invalid_data)}" in exc_info.value.message
        or f"got {type(invalid_data).__name__}" in exc_info.value.message
    )


# --- Parametrized Validation Error Tests (Dict Handlers) ---

_validation_error_test_cases_dict: list[ValidationErrorTestCaseType] = [
    (
        BackpackResponseHandler.handle_get_ticker_response,
        "valid_raw_ticker",
        {"remove_field": "time"},
        "time",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "ticker ({symbol}) - Status: 200",
    ),
    (
        BackpackResponseHandler.handle_get_order_book_response,
        "valid_raw_order_book",
        {"change_nested_field": ["bids", 0, 0], "new_value": "invalid_price"},
        "Cannot convert 'invalid_price' to Decimal",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "order book ({symbol}) - Status: 200",
    ),
    (
        BackpackResponseHandler.handle_place_order_response,
        "valid_raw_order",
        {"remove_field": "side"},
        "side",
        {},
        "place order response",
    ),
    (
        BackpackResponseHandler.handle_get_funding_rate_response,
        "valid_raw_funding_rate",
        {"remove_field": "rate"},
        "rate",
        {"symbol": "symbol_perp", "status_code": 200, "headers": {}},
        "funding rate ({symbol}) - Status: 200",
    ),
    (
        BackpackResponseHandler.handle_get_account_info_response,
        "valid_raw_account_summary",
        {"remove_field": "leverageLimit"},
        "leverageLimit",
        {},
        "account info",
    ),
    (
        BackpackResponseHandler.handle_withdraw_response,
        "valid_raw_withdrawal",
        {"remove_field": "fee"},
        "fee",
        {},
        "withdraw response",
    ),
    (
        BackpackResponseHandler.handle_get_order_status_response,
        "valid_raw_order_status",
        {"remove_field": "side"},
        "side",
        {"identifier": "order_id"},
        "order status (id={identifier})",
    ),
    # Example of checking balance item validation
    (
        BackpackResponseHandler.handle_get_balances_response,
        "valid_raw_balances",
        {"remove_nested_field": ["USDC", "available"]},
        "available",
        {},
        "balance details for USDC",
    ),
]


@pytest.mark.parametrize(
    (
        "handler_method, valid_data_fixture_name, modification_details, "
        "expected_error_substring, handler_args_spec, context_format_string"
    ),
    _validation_error_test_cases_dict,
)
def test_handler_validation_error_dict(
    handler_method: HandlerMethodType,
    valid_data_fixture_name: str,
    modification_details: ModificationDetailsType,
    expected_error_substring: str,
    handler_args_spec: HandlerArgsSpecType,
    context_format_string: str,
    request: pytest.FixtureRequest,
) -> None:
    """Test handlers for dict responses raise validation errors for malformed data."""
    valid_data = request.getfixturevalue(valid_data_fixture_name)
    invalid_data = copy.deepcopy(valid_data)

    # Apply modification
    if "remove_field" in modification_details:
        field_to_remove = modification_details["remove_field"]
        if isinstance(invalid_data, dict) and field_to_remove in invalid_data:
            del invalid_data[field_to_remove]
    elif "change_nested_field" in modification_details:
        path: list[str | int] = modification_details["change_nested_field"]
        new_value = modification_details["new_value"]
        temp: Any = invalid_data
        try:
            for i, key_or_index in enumerate(path):
                if i == len(path) - 1:
                    temp[key_or_index] = new_value
                else:
                    temp = temp[key_or_index]
        except (KeyError, IndexError, TypeError) as e:
            pytest.fail(f"Failed to apply nested change: {path}. Error: {e}")
    elif "remove_nested_field" in modification_details:
        # Rename to avoid Mypy no-redef error
        remove_path: list[str | int] = modification_details["remove_nested_field"]
        remove_temp: Any = invalid_data
        try:
            for i, key_or_index in enumerate(remove_path):
                if i == len(remove_path) - 1:
                    del remove_temp[key_or_index]
                else:
                    remove_temp = remove_temp[key_or_index]
        except (KeyError, IndexError, TypeError) as e:
            pytest.fail(f"Failed to apply nested removal: {remove_path}. Error: {e}")

    # Resolve handler args
    actual_handler_args: dict[str, Any] = {}
    for arg_name, value_or_fixture_name in handler_args_spec.items():
        if isinstance(value_or_fixture_name, str):
            try:
                actual_handler_args[arg_name] = request.getfixturevalue(value_or_fixture_name)
            except (pytest.FixtureLookupError, AttributeError):
                actual_handler_args[arg_name] = value_or_fixture_name
        else:
            actual_handler_args[arg_name] = value_or_fixture_name

    # Format context string
    final_context_string = context_format_string
    try:
        format_args = {
            k: actual_handler_args[k] for k in handler_args_spec if k in actual_handler_args
        }
        final_context_string = context_format_string.format(**format_args)
    except KeyError:
        pass

    # Perform test
    with pytest.raises(APIError) as exc_info:
        handler_method(cast(RawJsonResponse, invalid_data), **actual_handler_args)

    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert f"Invalid {final_context_string} response from exchange:" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError | ValueError)
    assert expected_error_substring in str(exc_info.value.original_exception)


# --- Parametrized List Item Error Tests ---

# (handler_method, valid_list_fixture, item_modification,
#  expected_error_substring, args_spec, item_context_string,
#  expect_warning_log)
_list_item_error_cases: list[ListItemErrorTestCaseStructure] = [
    # get_recent_trades: Invalid item type
    (
        BackpackResponseHandler.handle_get_recent_trades_response,
        "valid_raw_recent_trades",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "recent trades ({symbol}) - Status: 200",
        True,
    ),
    # get_recent_trades: Item validation error
    (
        BackpackResponseHandler.handle_get_recent_trades_response,
        "valid_raw_recent_trades",
        {"modify_item": {"index": 1, "remove_field": "price"}},
        "price",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "single trade item in recent trades ({symbol}) - Status: 200",
        False,
    ),
    # get_balances: Invalid value type (value in dict is not dict)
    (
        BackpackResponseHandler.handle_get_balances_response,
        "valid_raw_balances",
        {"modify_item": {"key": "USDC", "new_value": "not_a_dict"}},
        "Skipping non-dict balance details",
        {},
        "balances",
        True,
    ),
    # get_positions: Invalid item type
    (
        BackpackResponseHandler.handle_get_positions_response,
        "valid_raw_positions",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_any"},
        "positions ({symbol})",
        True,
    ),
    # get_positions: Item validation error
    (
        BackpackResponseHandler.handle_get_positions_response,
        "valid_raw_positions",
        {"modify_item": {"index": 1, "remove_field": "symbol"}},
        "symbol",
        {"symbol": "symbol_any"},
        "single position item in positions ({symbol})",
        False,
    ),
    # get_open_orders: Invalid item type
    (
        BackpackResponseHandler.handle_get_open_orders_response,
        "valid_raw_open_orders",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_any"},
        "open orders ({symbol})",
        True,
    ),
    # get_open_orders: Item validation error
    (
        BackpackResponseHandler.handle_get_open_orders_response,
        "valid_raw_open_orders",
        {"modify_item": {"index": 1, "remove_field": "side"}},
        "side",
        {"symbol": "symbol_any"},
        "single open order item in open orders ({symbol})",
        False,
    ),
    # get_order_history: Invalid item type
    (
        BackpackResponseHandler.handle_get_order_history_response,
        "valid_raw_order_history",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_any"},
        "order history ({symbol})",
        True,
    ),
    # get_order_history: Item validation error
    (
        BackpackResponseHandler.handle_get_order_history_response,
        "valid_raw_order_history",
        {"modify_item": {"index": 1, "remove_field": "orderType"}},
        "orderType",
        {"symbol": "symbol_any"},
        "single order history item in order history ({symbol})",
        False,
    ),
    # get_trade_history: Invalid item type
    (
        BackpackResponseHandler.handle_get_trade_history_response,
        "valid_raw_trade_history",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_spot"},
        "trade history ({symbol})",
        True,
    ),
    # get_trade_history: Item validation error
    (
        BackpackResponseHandler.handle_get_trade_history_response,
        "valid_raw_trade_history",
        {"modify_item": {"index": 0, "remove_field": "price"}},
        "price",
        {"symbol": "symbol_spot"},
        "single trade item in trade history/fills ({symbol})",
        False,
    ),
    (
        BackpackResponseHandler.handle_get_trade_history_response,
        "valid_raw_trade_history",
        {"modify_item": {"index": 1, "change_field": "qty", "new_value": "not_a_decimal"}},
        "qty",
        {"symbol": "symbol_spot"},
        "single trade item in trade history/fills ({symbol})",
        False,
    ),
    # get_market_data: Invalid item type (non-list)
    (
        BackpackResponseHandler.handle_get_market_data_response,
        "valid_raw_market_data",
        {"insert_invalid_item": {"error": "not a list"}, "index": 1},
        "Skipping non-list item",
        {"symbol": "symbol_spot", "timeframe": "1m", "status_code": 200, "headers": {}},
        "market data (klines {symbol}, {timeframe}) - Status: 200",
        True,
    ),
    # get_market_data: Item validation error (wrong length)
    # Note: The current handler doesn't validate kline item structure/length strictly.
    # This test would need model adjustments to be effective.
    # get_historical_trades: Invalid item type
    (
        BackpackResponseHandler.handle_get_historical_trades_response,
        "valid_raw_historical_trades",
        {"insert_invalid_item": "not_a_dict", "index": 1},
        "Skipping non-dict item",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "historical trades ({symbol}) - Status: 200",
        True,
    ),
    # get_historical_trades: Item validation error
    (
        BackpackResponseHandler.handle_get_historical_trades_response,
        "valid_raw_historical_trades",
        {"modify_item": {"index": 1, "remove_field": "price"}},
        "price",
        {"symbol": "symbol_spot", "status_code": 200, "headers": {}},
        "single historical trade item in historical trades ({symbol}) - Status: 200",
        False,
    ),
]


@pytest.mark.parametrize(
    (
        "handler_method, valid_data_fixture, item_modification, "
        "expected_log_or_error, handler_args_spec, context_format_string, "
        "expect_warning_log"
    ),
    _list_item_error_cases,
)
def test_handler_list_item_errors(
    handler_method: HandlerMethodType,
    valid_data_fixture: str,
    item_modification: dict[str, Any],
    expected_log_or_error: str,
    handler_args_spec: HandlerArgsSpecType,
    context_format_string: str,
    expect_warning_log: bool,
    request: pytest.FixtureRequest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Tests handlers for list responses handle invalid items (type or validation)."""
    valid_data: RawJsonResponse = request.getfixturevalue(valid_data_fixture)
    invalid_data: RawJsonResponse = copy.deepcopy(valid_data)

    # Apply modification to create invalid item
    if "insert_invalid_item" in item_modification:
        item = item_modification["insert_invalid_item"]
        index = item_modification.get("index", 0)
        if isinstance(invalid_data, list):
            list_data = cast(list[Any], invalid_data)  # Cast after check
            list_data.insert(index, item)
    elif "modify_item" in item_modification:
        mod_details = item_modification["modify_item"]
        if isinstance(invalid_data, list):
            list_data = cast(list[Any], invalid_data)  # Cast after check
            index = mod_details.get("index", 0)
            if index < len(list_data):
                item_to_mod: Any = list_data[index]
                if isinstance(item_to_mod, dict):
                    # Cast to dict[str, Any] to satisfy Pyright for dict_item
                    dict_item = cast(dict[str, Any], item_to_mod)
                    if "remove_field" in mod_details:
                        del dict_item[mod_details["remove_field"]]
                    elif "change_field" in mod_details:
                        dict_item[mod_details["change_field"]] = mod_details["new_value"]
        elif isinstance(invalid_data, dict):
            dict_data = cast(dict[str, Any], invalid_data)  # Cast after check
            key = mod_details.get("key")
            if key and key in dict_data:
                if "new_value" in mod_details:
                    dict_data[key] = mod_details["new_value"]

    # Resolve handler args
    actual_handler_args: dict[str, Any] = {}
    for arg_name, value_or_fixture_name in handler_args_spec.items():
        if isinstance(value_or_fixture_name, str):
            try:
                actual_handler_args[arg_name] = request.getfixturevalue(value_or_fixture_name)
            except (pytest.FixtureLookupError, AttributeError):
                actual_handler_args[arg_name] = value_or_fixture_name
        else:
            actual_handler_args[arg_name] = value_or_fixture_name

    # Format context string
    final_context_string = context_format_string
    try:
        format_args = {
            k: actual_handler_args[k] for k in handler_args_spec if k in actual_handler_args
        }
        final_context_string = context_format_string.format(**format_args)
    except KeyError:
        pass

    if expect_warning_log:
        # Test for warning log and correct return value (usually filtered list)
        # Remove the patch, rely on caplog to capture logs from the handler's logger.
        # Ensure the logger in bp_response_handler is configured to emit warnings
        # that caplog can capture.

        # Call the handler method directly
        result = handler_method(invalid_data, **actual_handler_args)

        # Check that the result is the filtered list/dict (or appropriate type)
        if isinstance(valid_data, list):
            assert isinstance(result, list)
        elif isinstance(valid_data, dict):
            assert isinstance(result, dict)

        # Check log message using caplog.records
        log_found = any(expected_log_or_error in record.getMessage() for record in caplog.records)
        if not log_found:
            # Using getattr to safely access node.name, or provide a default.
            node_name = getattr(getattr(request, "node", None), "name", "Unknown Test Node")
            print(f"Test {node_name} failed. Expected log: '{expected_log_or_error}'")
            print(f"Captured logs:\\n{caplog.text}")
        assert log_found
    else:
        # Test for APIError with specific validation message
        with pytest.raises(APIError) as exc_info:
            # Cast removed as redundant
            handler_method(invalid_data, **actual_handler_args)

        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid {final_context_string} response from exchange:" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert expected_log_or_error in str(exc_info.value.original_exception)


# --- Cleanup: Remove old standalone tests ---
# (The original test functions like test_handle_get_ticker_response_valid,
# test_handle_get_ticker_response_invalid_type, etc., are now replaced
# by the class-based tests and parametrized tests above)
