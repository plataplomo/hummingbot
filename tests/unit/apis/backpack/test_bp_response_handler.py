"""Unit tests for BackpackResponseHandler."""

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


def test_handle_get_ticker_response_valid() -> None:
    """Test handling a valid raw ticker response."""
    raw_data = {
        "symbol": "SOL_USDC",
        "price": "140.50",
        "quantity": "100.0",
        "quoteQuantity": "14050.0",
        "bestBid": "140.49",
        "bestAsk": "140.51",
        "open": "138.00",
        "high": "142.00",
        "low": "137.50",
        "close": "140.50",
        "firstId": "1000",
        "lastId": "1100",
        "bidQuantity": "50.0",
        "askQuantity": "60.0",
        "volume": "500000.0",
        "quoteVolume": "70250000.0",
        "trades": 100,
        "timestamp": 1678886400000,
        "priceChange": "2.50",
        "priceChangePercent": "1.81",
        "lastQuantity": "10.0",
    }
    ticker: BackpackRawTicker = BackpackResponseHandler.handle_get_ticker_response(
        cast(Any, raw_data), "SOL_USDC"
    )
    assert isinstance(ticker, BackpackRawTicker)
    assert ticker.symbol == "SOL_USDC"
    assert ticker.price == "140.50"


def test_handle_get_ticker_response_invalid_type() -> None:
    """Test handling a ticker response with an invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_ticker_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message


def test_handle_get_ticker_response_validation_error() -> None:
    """Test handling a ticker response with missing required fields."""
    raw_data = {"symbol": "SOL_USDC"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_ticker_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for ticker" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_order_book_response_valid() -> None:
    """Test handling a valid raw order book response."""
    raw_data = {
        "symbol": "SOL_USDC",
        "bids": [["140.10", "10"], ["140.00", "20"]],
        "asks": [["140.20", "15"], ["140.30", "25"]],
        "time": 1678886401000,
    }
    order_book: BackpackRawOrderBook = BackpackResponseHandler.handle_get_order_book_response(
        cast(Any, raw_data), "SOL_USDC"
    )
    assert isinstance(order_book, BackpackRawOrderBook)
    assert len(order_book.bids) == 2
    assert order_book.bids[0] == ["140.10", "10"]  # type: ignore[comparison-overlap]
    assert len(order_book.asks) == 2
    assert order_book.asks[0] == ["140.20", "15"]  # type: ignore[comparison-overlap]


def test_handle_get_order_book_response_invalid_type() -> None:
    """Test handling an order book response with an invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_book_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "order book" in exc_info.value.message


def test_handle_get_order_book_response_validation_error() -> None:
    """Test handling an order book response with missing/invalid fields."""
    raw_data = {
        "symbol": "SOL_USDC",
        "bids": [["140.10", "10"], ["invalid_price", "20"]],  # Invalid price string
        "asks": [["140.20", "15"]],
        "time": 1678886401000,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_book_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for order book" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_recent_trades_response_valid() -> None:
    """Test handling a valid raw recent trades response (list of trades)."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "1001",
            "orderId": "order123",
        },
        {
            "symbol": "SOL_USDC",
            "price": "141.01",
            "qty": "0.5",
            "time": 1678886403000,
            "id": "1002",
            "orderId": "order124",
        },
    ]
    trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_recent_trades_response(
        cast(Any, raw_data), "SOL_USDC"
    )
    assert isinstance(trades, list)
    assert len(trades) == 2
    assert isinstance(trades[0], BackpackRawTrade)
    assert trades[0].id == "1001"
    assert trades[1].order_id == "order124"
    assert trades[1].quantity == "0.5"


def test_handle_get_recent_trades_response_invalid_type() -> None:
    """Test handling recent trades response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_recent_trades_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "recent trades" in exc_info.value.message


def test_handle_get_recent_trades_response_invalid_item_type() -> None:
    """Test handling recent trades list containing a non-dict item."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "1001",
            "orderId": "order123",
        },
        "not_a_dict",  # Invalid item type
    ]
    # It should log a warning and skip the invalid item
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_recent_trades_response(
            cast(Any, raw_data), "SOL_USDC"
        )
        assert len(trades) == 1  # Only the valid trade should be returned
        assert trades[0].id == "1001"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_recent_trades_response_item_validation_error() -> None:
    """Test handling recent trades list with an item failing validation."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "1001",
            "orderId": "order123",
        },
        {
            "symbol": "SOL_USDC",
            # Missing required 'price' field
            "qty": "0.5",
            "time": 1678886403000,
            "id": "1002",
            "orderId": "order124",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_recent_trades_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single trade item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_balances_response_valid() -> None:
    """Test handling a valid raw balances response (dict of asset balances)."""
    raw_data = {
        "SOL": {
            "asset": "SOL",
            "available": "10.5",
            "total": "12.5",
        },
        "USDC": {
            "asset": "USDC",
            "available": "1000.0",
            "total": "1050.0",
        },
    }
    balances: dict[str, BackpackRawBalance] = BackpackResponseHandler.handle_get_balances_response(
        cast(Any, raw_data)
    )
    assert isinstance(balances, dict)
    assert len(balances) == 2
    assert "SOL" in balances
    assert isinstance(balances["SOL"], BackpackRawBalance)
    assert balances["SOL"].available == "10.5"
    assert "USDC" in balances
    assert balances["USDC"].available == "1000.0"
    assert balances["USDC"].total == "1050.0"


def test_handle_get_balances_response_invalid_type() -> None:
    """Test handling balances response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_balances_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "balances" in exc_info.value.message


def test_handle_get_balances_response_invalid_value_type() -> None:
    """Test handling balances dict containing a value that is not a dict."""
    raw_data = {"SOL": {"asset": "SOL", "available": "10.0", "total": "10.0"}, "USDC": "not_a_dict"}
    # It should log a warning and skip the invalid item
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        balances: dict[str, BackpackRawBalance] = (
            BackpackResponseHandler.handle_get_balances_response(cast(Any, raw_data))
        )
        assert len(balances) == 1  # Only the valid balance should be returned
        assert "SOL" in balances
        assert "USDC" not in balances
        mock_log.assert_called_once()
        assert "Skipping non-dict balance details" in mock_log.call_args[0][0]


def test_handle_get_balances_response_item_validation_error() -> None:
    """Test handling balances dict with a value dict failing validation."""
    raw_data = {
        "SOL": {"asset": "SOL", "available": "10.5", "total": "12.5"},
        "USDC": {"asset": "USDC", "total": "50.0"},
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_balances_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for balance details for USDC" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_positions_response_valid() -> None:
    """Test handling a valid raw positions response (list of position dicts)."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "breakEvenPrice": "131.00",
            "entryPrice": "130.00",
            "estLiquidationPrice": "120.00",
            "imf": "0.1",
            "imfFunction": {"a": "1", "b": "0", "c": "0", "maxImf": "1"},
            "markPrice": "135.00",
            "mmf": "0.05",
            "mmfFunction": {"a": "1", "b": "0", "c": "0", "maxMmf": "1"},
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
        },
        {
            "symbol": "BTC_USDT",
            "breakEvenPrice": "54900.00",
            "entryPrice": "55000.00",
            "estLiquidationPrice": "60000.00",
            "imf": "0.2",
            "imfFunction": {"a": "1", "b": "0", "c": "0", "maxImf": "1"},
            "markPrice": "54000.00",
            "mmf": "0.1",
            "mmfFunction": {"a": "1", "b": "0", "c": "0", "maxMmf": "1"},
            "netCost": "-5500.00",
            "netQuantity": "-0.1",
            "netExposureQuantity": "-0.1",
            "netExposureNotional": "-5400.00",
            "pnlRealized": "50.00",
            "pnlUnrealized": "100.00",
            "cumulativeFundingPayment": "1.20",
            "userId": 1,
            "positionId": "pos456",
            "cumulativeInterest": "0.0",
        },
    ]
    positions: list[BackpackRawPosition] = BackpackResponseHandler.handle_get_positions_response(
        cast(Any, raw_data), "all"
    )
    assert isinstance(positions, list)
    assert len(positions) == 2
    assert isinstance(positions[0], BackpackRawPosition)
    assert positions[0].symbol == "SOL_USDC"
    assert positions[1].net_quantity == "-0.1"


def test_handle_get_positions_response_invalid_type() -> None:
    """Test handling positions response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_positions_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "positions" in exc_info.value.message


def test_handle_get_positions_response_invalid_item_type() -> None:
    """Test handling positions list containing a non-dict item."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "breakEvenPrice": "131.00",
            "entryPrice": "130.00",
            "estLiquidationPrice": "120.00",
            "imf": "0.1",
            "imfFunction": {"a": "1", "b": "0", "c": "0", "maxImf": "1"},
            "markPrice": "135.00",
            "mmf": "0.05",
            "mmfFunction": {"a": "1", "b": "0", "c": "0", "maxMmf": "1"},
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
        },
        "not_a_dict",  # Invalid item type
    ]
    # It should log a warning and skip the invalid item
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        positions: list[BackpackRawPosition] = (
            BackpackResponseHandler.handle_get_positions_response(cast(Any, raw_data), "all")
        )
        assert len(positions) == 1  # Only the valid position should be returned
        assert positions[0].symbol == "SOL_USDC"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_positions_response_item_validation_error() -> None:
    """Test handling positions list with an item failing validation."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "breakEvenPrice": "131.00",
            "entryPrice": "130.00",
            "estLiquidationPrice": "120.00",
            "imf": "0.1",
            "imfFunction": {"a": "1", "b": "0", "c": "0", "maxImf": "1"},
            "markPrice": "135.00",
            "mmf": "0.05",
            "mmfFunction": {"a": "1", "b": "0", "c": "0", "maxMmf": "1"},
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
        },
        {
            # Missing required 'symbol' field
            "breakEvenPrice": "54900.00",
            "entryPrice": "55000.00",
            "estLiquidationPrice": "60000.00",
            "imf": "0.2",
            "imfFunction": {"a": "1", "b": "0", "c": "0", "maxImf": "1"},
            "markPrice": "54000.00",
            "mmf": "0.1",
            "mmfFunction": {"a": "1", "b": "0", "c": "0", "maxMmf": "1"},
            "netCost": "-5500.00",
            "netQuantity": "-0.1",
            "netExposureQuantity": "-0.1",
            "netExposureNotional": "-5400.00",
            "pnlRealized": "50.00",
            "pnlUnrealized": "100.00",
            "cumulativeFundingPayment": "1.20",
            "userId": 1,
            "positionId": "pos456",
            "cumulativeInterest": "0.0",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_positions_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single position item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_place_order_response_valid() -> None:
    """Test handling a valid raw place order response."""
    raw_data = {
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
    }
    order: BackpackRawOrder = BackpackResponseHandler.handle_place_order_response(
        cast(Any, raw_data)
    )
    assert isinstance(order, BackpackRawOrder)
    assert order.id == "987654321"
    assert order.status == "NEW"


def test_handle_place_order_response_invalid_type() -> None:
    """Test handling place order response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_place_order_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "place order response" in exc_info.value.message


def test_handle_place_order_response_validation_error() -> None:
    """Test handling place order response dict failing validation."""
    raw_data = {
        "id": "987654321",
        "symbol": "SOL_USDC",
        # Missing required fields like 'side', 'orderType', 'quantity'
        "status": "NEW",
        "createdAt": 1678886405000,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_place_order_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for place order response" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_cancel_order_response_none_input() -> None:
    """Test handling cancel order response with None input."""
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        result = BackpackResponseHandler.handle_cancel_order_response(
            None, order_id="123", symbol="SOL_USDC"
        )
        assert result is True
        mock_log.assert_not_called()


def test_handle_cancel_order_response_empty_dict_input() -> None:
    """Test handling cancel order response with empty dict input."""
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        result = BackpackResponseHandler.handle_cancel_order_response(
            cast(Any, {}), order_id="123", symbol="SOL_USDC"
        )
        assert result is True
        mock_log.assert_not_called()


def test_handle_cancel_order_response_unexpected_content() -> None:
    """Test handling cancel order response with unexpected content."""
    raw_data = {"status": "cancelled but here is some unexpected data"}
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        result = BackpackResponseHandler.handle_cancel_order_response(
            cast(Any, raw_data), order_id="123", symbol="SOL_USDC"
        )
        assert result is True
        mock_log.assert_called_once()
        assert "Received unexpected content" in mock_log.call_args[0][0]


def test_handle_get_open_orders_response_valid() -> None:
    """Test handling a valid raw open orders response (list of order dicts)."""
    raw_data = [
        {
            "id": "order001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "5.0",
            "price": "139.00",
            "timeInForce": "GTC",
            "status": "OPEN",
            "createdAt": 1678886410000,
            "executedQuantity": "1.0",
        },
        {
            "id": "order002",
            "symbol": "BTC_USDT",
            "side": "sell",
            "orderType": "LIMIT",
            "quantity": "0.1",
            "price": "56000.00",
            "timeInForce": "GTC",
            "status": "OPEN",
            "createdAt": 1678886411000,
            "executedQuantity": "0",
        },
    ]
    orders: list[BackpackRawOrder] = BackpackResponseHandler.handle_get_open_orders_response(
        cast(Any, raw_data), "all"
    )
    assert isinstance(orders, list)
    assert len(orders) == 2
    assert isinstance(orders[0], BackpackRawOrder)
    assert orders[0].id == "order001"
    assert orders[1].symbol == "BTC_USDT"


def test_handle_get_open_orders_response_invalid_type() -> None:
    """Test handling open orders response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_open_orders_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "open orders" in exc_info.value.message


def test_handle_get_open_orders_response_invalid_item_type() -> None:
    """Test handling open orders list containing a non-dict item."""
    raw_data = [
        {
            "id": "order001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "5.0",
            "price": "139.00",
            "timeInForce": "GTC",
            "status": "OPEN",
            "createdAt": 1678886410000,
            "executedQuantity": "1.0",
        },
        "not_an_order_dict",
    ]
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        orders: list[BackpackRawOrder] = BackpackResponseHandler.handle_get_open_orders_response(
            cast(Any, raw_data), "all"
        )
        assert len(orders) == 1
        assert orders[0].id == "order001"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_open_orders_response_item_validation_error() -> None:
    """Test handling open orders list with an item failing validation."""
    raw_data = [
        {
            "id": "order001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "5.0",
            "price": "139.00",
            "timeInForce": "GTC",
            "status": "OPEN",
            "createdAt": 1678886410000,
            "executedQuantity": "1.0",
        },
        {
            "id": "order002",
            "symbol": "BTC_USDT",
            # Missing required 'side' field
            "orderType": "LIMIT",
            "quantity": "0.1",
            "price": "56000.00",
            "timeInForce": "GTC",
            "status": "OPEN",
            "createdAt": 1678886411000,
            "executedQuantity": "0",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_open_orders_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single open order item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_funding_rate_response_valid() -> None:
    """Test handling a valid raw funding rate response."""
    raw_data = {
        "symbol": "SOL-PERP",
        "fundingRate": "0.000123",
        "fundingTime": 1678887000000,
        "nextFundingRate": "0.000125",
        "nextFundingTime": 1678890600000,
        "openInterest": "150000.5",
        "openInterestValue": "21000070.25",
    }
    funding_rate: BackpackRawFundingRate = BackpackResponseHandler.handle_get_funding_rate_response(
        cast(Any, raw_data), "SOL-PERP"
    )
    assert isinstance(funding_rate, BackpackRawFundingRate)
    assert funding_rate.symbol == "SOL-PERP"
    assert funding_rate.funding_rate == "0.000123"


def test_handle_get_funding_rate_response_invalid_type() -> None:
    """Test handling funding rate response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_funding_rate_response(cast(Any, raw_data), "SOL-PERP")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "funding rate" in exc_info.value.message


def test_handle_get_funding_rate_response_validation_error() -> None:
    """Test handling funding rate response dict failing validation."""
    raw_data = {
        "symbol": "SOL-PERP",
        # Missing required 'fundingRate' field
        "fundingTime": 1678887000000,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_funding_rate_response(cast(Any, raw_data), "SOL-PERP")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for funding rate" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_account_info_response_valid() -> None:
    """Test handling a valid raw account info response."""
    raw_data = {
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
    account_info: BackpackRawAccountSummary = (
        BackpackResponseHandler.handle_get_account_info_response(cast(Any, raw_data))
    )
    assert isinstance(account_info, BackpackRawAccountSummary)
    assert account_info.leverage_limit == Decimal("20.0")
    assert account_info.limit_orders == 50
    assert account_info.auto_lend is False


def test_handle_get_account_info_response_invalid_type() -> None:
    """Test handling account info response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_account_info_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "account info" in exc_info.value.message


def test_handle_get_account_info_response_validation_error() -> None:
    """Test handling account info response dict failing validation."""
    raw_data = {
        "autoBorrowSettlements": True,
        "autoLend": False,
        # Missing required 'leverageLimit' field
        "limitOrders": 50,
        "liquidating": False,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_account_info_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for account info" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_withdraw_response_valid() -> None:
    """Test handling a valid raw withdraw response."""
    raw_data = {
        "id": 12345,
        "blockchain": "Solana",
        "quantity": "100.0",
        "fee": "0.01",
        "symbol": "USDC",
        "status": "confirmed",
        "toAddress": "SOLANA_ADDRESS_HERE",
        "createdAt": "2023-03-15T10:00:00.000Z",
        "isInternal": False,
        "transactionHash": "TX_HASH_HERE",
    }
    withdraw_response: BackpackRawWithdrawalResponse = (
        BackpackResponseHandler.handle_withdraw_response(cast(Any, raw_data))
    )
    assert isinstance(withdraw_response, BackpackRawWithdrawalResponse)
    assert withdraw_response.id == 12345
    assert withdraw_response.status == "confirmed"
    assert withdraw_response.created_at == datetime(2023, 3, 15, 10, 0, 0, tzinfo=UTC)


def test_handle_withdraw_response_invalid_type() -> None:
    """Test handling withdraw response with invalid type (list instead of dict)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_withdraw_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "withdraw response" in exc_info.value.message


def test_handle_withdraw_response_validation_error() -> None:
    """Test handling withdraw response dict failing validation."""
    raw_data = {
        "id": 12345,
        "blockchain": "Solana",
        "quantity": "100.0",
        # Missing required 'fee' field
        "symbol": "USDC",
        "status": "confirmed",
        "toAddress": "SOLANA_ADDRESS_HERE",
        "createdAt": "2023-03-15T10:00:00.000Z",
        "isInternal": False,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_withdraw_response(cast(Any, raw_data))
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for withdraw response" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_order_history_response_valid() -> None:
    """Test handling a valid raw order history response (list of order dicts)."""
    raw_data = [
        {
            "id": "histOrder001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "12.0",
            "price": "138.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678886000000,
            "executedQuantity": "12.0",
            "averageExecutedPrice": "138.00",
        },
        {
            "id": "histOrder002",
            "symbol": "BTC_USDT",
            "side": "sell",
            "orderType": "MARKET",
            "quantity": "0.2",
            "timeInForce": "IOC",
            "status": "FILLED",
            "createdAt": 1678886100000,
            "executedQuantity": "0.2",
            "averageExecutedPrice": "55950.00",
        },
    ]
    orders: list[BackpackRawOrder] = BackpackResponseHandler.handle_get_order_history_response(
        cast(Any, raw_data), "all"
    )
    assert isinstance(orders, list)
    assert len(orders) == 2
    assert isinstance(orders[0], BackpackRawOrder)
    assert orders[0].id == "histOrder001"
    assert orders[0].status == "FILLED"
    assert orders[1].symbol == "BTC_USDT"


def test_handle_get_order_history_response_invalid_type() -> None:
    """Test handling order history response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_history_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "order history" in exc_info.value.message


def test_handle_get_order_history_response_invalid_item_type() -> None:
    """Test handling order history list containing a non-dict item."""
    raw_data = [
        {
            "id": "histOrder001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "12.0",
            "price": "138.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678886000000,
            "executedQuantity": "12.0",
        },
        "not_an_order_dict",
    ]
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        orders: list[BackpackRawOrder] = BackpackResponseHandler.handle_get_order_history_response(
            cast(Any, raw_data), "all"
        )
        assert len(orders) == 1
        assert orders[0].id == "histOrder001"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_order_history_response_item_validation_error() -> None:
    """Test handling order history list with an item failing validation."""
    raw_data = [
        {
            "id": "histOrder001",
            "symbol": "SOL_USDC",
            "side": "buy",
            "orderType": "LIMIT",
            "quantity": "12.0",
            "price": "138.00",
            "timeInForce": "GTC",
            "status": "FILLED",
            "createdAt": 1678886000000,
            "executedQuantity": "12.0",
        },
        {
            "id": "histOrder002",
            "symbol": "BTC_USDT",
            # Missing required 'orderType' field
            "side": "sell",
            "quantity": "0.2",
            "status": "FILLED",
            "createdAt": 1678886100000,
            "executedQuantity": "0.2",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_history_response(cast(Any, raw_data), "all")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single order history item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_trade_history_response_valid() -> None:
    """Test handling a valid raw trade history response (list of trade dicts)."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "trade1001",
            "orderId": "order123",
        },
        {
            "symbol": "SOL_USDC",
            "price": "141.01",
            "qty": "0.5",
            "time": 1678886403000,
            "id": "trade1002",
            "orderId": "order124",
        },
    ]
    trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_trade_history_response(
        cast(Any, raw_data), "SOL_USDC"
    )
    assert isinstance(trades, list)
    assert len(trades) == 2
    assert isinstance(trades[0], BackpackRawTrade)
    assert trades[0].id == "trade1001"
    assert trades[1].order_id == "order124"


def test_handle_get_trade_history_response_invalid_type() -> None:
    """Test handling trade history response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_trade_history_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "trade history" in exc_info.value.message


def test_handle_get_trade_history_response_invalid_item_type() -> None:
    """Test handling trade history list containing a non-dict item."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "trade1001",
            "orderId": "order123",
        },
        "not_a_dict",
    ]
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_trade_history_response(
            cast(Any, raw_data), "SOL_USDC"
        )
        assert len(trades) == 1
        assert trades[0].id == "trade1001"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_trade_history_response_item_validation_error() -> None:
    """Test handling trade history list with an item failing validation."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "trade1001",
            "orderId": "order123",
        },
        {
            "symbol": "SOL_USDC",
            # Missing required 'price' field
            "qty": "0.5",
            "time": 1678886403000,
            "id": "trade1002",
            "orderId": "order124",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_trade_history_response(cast(Any, raw_data), "SOL_USDC")
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single trade history item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_market_data_response_valid() -> None:
    """Test handling a valid raw market data (klines) response."""
    raw_data = [
        [1678886400000, "138.0", "139.5", "137.5", "139.0", "1000.0"],  # Example kline data
        [1678886460000, "139.0", "140.0", "138.5", "139.8", "1200.0"],
    ]
    klines: list[Any] = BackpackResponseHandler.handle_get_market_data_response(
        cast(Any, raw_data), "SOL_USDC", "1m"
    )
    assert isinstance(klines, list)
    assert len(klines) == 2
    assert isinstance(klines[0], list)
    assert klines[0][0] == 1678886400000
    assert klines[1][4] == "139.8"


def test_handle_get_market_data_response_invalid_type() -> None:
    """Test handling market data response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_market_data_response(
            cast(Any, raw_data), "SOL_USDC", "1m"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "market data (klines" in exc_info.value.message


def test_handle_get_market_data_response_invalid_item_type() -> None:
    """Test handling market data list containing a non-list item."""
    raw_data = [
        [1678886400000, "138.0", "139.5", "137.5", "139.0", "1000.0"],
        {"error": "not a list"},  # Invalid item
    ]
    # It should log a warning and skip the invalid item
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        klines: list[Any] = BackpackResponseHandler.handle_get_market_data_response(
            cast(Any, raw_data), "SOL_USDC", "1m"
        )
        assert len(klines) == 1
        assert klines[0][0] == 1678886400000
        mock_log.assert_called_once()
        assert "Skipping non-list item" in mock_log.call_args[0][0]


def test_handle_get_historical_trades_response_valid() -> None:
    """Test handling a valid raw historical trades response (list of trade dicts)."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "135.00",
            "qty": "2.0",
            "time": 1678880000000,
            "id": "histTrade001",
            "orderId": "histOrderA",
        },
        {
            "symbol": "SOL_USDC",
            "price": "135.10",
            "qty": "1.0",
            "time": 1678880100000,
            "id": "histTrade002",
            "orderId": "histOrderB",
        },
    ]
    trades: list[BackpackRawTrade] = BackpackResponseHandler.handle_get_historical_trades_response(
        cast(Any, raw_data), "SOL_USDC"
    )
    assert isinstance(trades, list)
    assert len(trades) == 2
    assert isinstance(trades[0], BackpackRawTrade)
    assert trades[0].id == "histTrade001"
    assert trades[1].order_id == "histOrderB"


def test_handle_get_historical_trades_response_invalid_type() -> None:
    """Test handling historical trades response with invalid type (dict instead of list)."""
    raw_data = {"error": "expected list"}
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_historical_trades_response(
            cast(Any, raw_data), "SOL_USDC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected list" in exc_info.value.message
    assert "historical trades" in exc_info.value.message


def test_handle_get_historical_trades_response_invalid_item_type() -> None:
    """Test handling historical trades list containing a non-dict item."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "135.00",
            "qty": "2.0",
            "time": 1678880000000,
            "id": "histTrade001",
            "orderId": "histOrderA",
        },
        "not_a_dict",
    ]
    with patch("cyberdelta.apis.backpack.bp_response_handler.logger.warning") as mock_log:
        trades: list[BackpackRawTrade] = (
            BackpackResponseHandler.handle_get_historical_trades_response(
                cast(Any, raw_data), "SOL_USDC"
            )
        )
        assert len(trades) == 1
        assert trades[0].id == "histTrade001"
        mock_log.assert_called_once()
        assert "Skipping non-dict item" in mock_log.call_args[0][0]


def test_handle_get_historical_trades_response_item_validation_error() -> None:
    """Test handling historical trades list with an item failing validation."""
    raw_data = [
        {
            "symbol": "SOL_USDC",
            "price": "135.00",
            "qty": "2.0",
            "time": 1678880000000,
            "id": "histTrade001",
            "orderId": "histOrderA",
        },
        {
            "symbol": "SOL_USDC",
            "qty": "1.0",
            # Missing required 'price' field
            "time": 1678880100000,
            "id": "histTrade002",
            "orderId": "histOrderB",
        },
    ]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_historical_trades_response(
            cast(Any, raw_data), "SOL_USDC"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for single historical trade item" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


def test_handle_get_order_status_response_valid() -> None:
    """Test handling a valid raw order status response."""
    raw_data = {
        "id": "statusOrder123",
        "clientId": "clientStatus001",
        "symbol": "SOL_USDC",
        "side": "sell",
        "orderType": "LIMIT",
        "quantity": "2.0",
        "price": "142.00",
        "timeInForce": "GTC",
        "status": "FILLED",
        "createdAt": 1678889000000,
        "executedQuantity": "2.0",
        "avgFillPrice": "142.00",
    }
    order: BackpackRawOrder = BackpackResponseHandler.handle_get_order_status_response(
        cast(Any, raw_data), identifier="statusOrder123"
    )
    assert isinstance(order, BackpackRawOrder)
    assert order.id == "statusOrder123"
    assert order.status == "FILLED"


def test_handle_get_order_status_response_none_input() -> None:
    """Test handling order status response when input is None (order not found)."""
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_status_response(None, identifier="notFound123")
    assert exc_info.value.code == APIErrorCode.ORDER_NOT_FOUND.value
    assert "Order notFound123 not found" in exc_info.value.message


def test_handle_get_order_status_response_invalid_type() -> None:
    """Test handling order status response with invalid type (list instead of dict/None)."""
    raw_data = ["invalid"]
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_status_response(
            cast(Any, raw_data), identifier="invalidType123"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "expected dict" in exc_info.value.message
    assert "order status" in exc_info.value.message


def test_handle_get_order_status_response_validation_error() -> None:
    """Test handling order status response dict failing validation."""
    raw_data = {
        "id": "statusOrder123",
        "symbol": "SOL_USDC",
        # Missing required fields like 'side', 'status'
        "quantity": "2.0",
        "createdAt": 1678889000000,
    }
    with pytest.raises(APIError) as exc_info:
        BackpackResponseHandler.handle_get_order_status_response(
            cast(Any, raw_data), identifier="statusOrder123"
        )
    assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
    assert "validation failed for order status" in exc_info.value.message
    assert isinstance(exc_info.value.original_exception, ValidationError)


# All REST API handler tests are now implemented for BackpackResponseHandler.
