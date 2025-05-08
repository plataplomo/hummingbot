"""Unit tests for BackpackResponseHandler."""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawTicker
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


# TODO: Add tests for other handler methods:
# - handle_get_order_book_response (valid, invalid type, validation error)
# - handle_get_recent_trades_response (valid list, list with non-dict, list with invalid dict)
# - handle_get_balances_response (valid dict, dict with non-dict value, dict with invalid value)
# - handle_get_positions_response (valid list, list with non-dict, list with invalid dict)
# - handle_place_order_response (valid dict, invalid type, validation error)
# - handle_cancel_order_response (None input, empty dict input, unexpected content input)
# - handle_get_open_orders_response (valid list, list with non-dict, list with invalid dict)
# - handle_get_funding_rate_response (valid dict, invalid type, validation error)
# - handle_get_account_info_response (valid dict, invalid type, validation error)
# - handle_withdraw_response (valid dict, invalid type, validation error)
# - handle_get_order_history_response (valid list, list with non-dict, list with invalid dict)
# - handle_get_trade_history_response (valid list, list with non-dict, list with invalid dict)
# - handle_get_market_data_response (valid list[list], list with non-list item, list with
#   invalid kline)
# - handle_get_historical_trades_response (valid list, list with non-dict, list with invalid dict)
# - handle_get_order_status_response (valid dict, None input, invalid type, validation error)
