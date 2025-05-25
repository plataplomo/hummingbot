"""Unit tests for BackpackResponseHandler market data response functionality."""

from decimal import Decimal
from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import BackpackRawOrderBook, BackpackRawTicker
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.conftest_response_handler"]

# Type aliases for clarity
type RawJsonPrim = str | int | float | bool | None
type RawJson = dict[str, "RawJson"] | list["RawJson"] | RawJsonPrim
type RawJsonResponse = RawJson


class TestHandleGetTickerResponse:
    """Tests for BackpackResponseHandler.handle_get_ticker_response."""

    def test_valid(self, valid_raw_ticker: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw ticker response."""
        ticker: BackpackRawTicker = BackpackResponseHandler.handle_get_ticker_response(
            cast(RawJsonResponse, valid_raw_ticker), symbol_spot, 200, {}
        )
        assert isinstance(ticker, BackpackRawTicker)
        assert ticker.symbol == symbol_spot
        assert ticker.price == "140.50"
        assert ticker.bid == "140.49"
        assert ticker.ask == "140.51"
        assert ticker.volume == "500000.0"
        assert ticker.time == 1678886400000

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test ticker response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_ticker_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 400, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test ticker response missing required field."""
        raw_data = {
            # Missing 'symbol' field (required)
            "price": "140.50",
            "bid": "140.49",
            "ask": "140.51",
            "volume": "500000.0",
            "time": 1678886400000,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_ticker_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid ticker ({symbol_spot}) - Status: 200" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "symbol" in str(exc_info.value.original_exception)

    def test_validation_error_invalid_price_format(self, symbol_spot: str) -> None:
        """Test ticker response with invalid price format."""
        raw_data = {
            "symbol": symbol_spot,
            "price": "invalid_price",
            "bid": "140.49",
            "ask": "140.51",
            "volume": "500000.0",
            "time": 1678886400000,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_ticker_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_extra_fields_ignored(self, symbol_spot: str) -> None:
        """Test that extra fields in ticker response cause ValidationError due to extra='forbid'."""
        raw_data = {
            "symbol": symbol_spot,
            "price": "140.50",
            "bid": "140.49",
            "ask": "140.51",
            "volume": "500000.0",
            "time": 1678886400000,
            "extraField": "should_be_ignored",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_ticker_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)


class TestHandleGetOrderBookResponse:
    """Tests for BackpackResponseHandler.handle_get_order_book_response."""

    def test_valid(self, valid_raw_order_book: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw order book response."""
        order_book: BackpackRawOrderBook = BackpackResponseHandler.handle_get_order_book_response(
            cast(RawJsonResponse, valid_raw_order_book), symbol_spot, 200, {}
        )
        assert isinstance(order_book, BackpackRawOrderBook)
        assert len(order_book.bids) == 2
        assert order_book.bids[0] == ("140.10", "10")
        assert order_book.bids[1] == ("140.00", "20")
        assert len(order_book.asks) == 2
        assert order_book.asks[0] == ("140.20", "15")
        assert order_book.asks[1] == ("140.30", "25")
        assert order_book.last_update_id == "update123"
        assert order_book.timestamp == 1678886401000

    def test_empty_order_book(self, symbol_spot: str) -> None:
        """Test handling order book with empty bids and asks."""
        raw_data: dict[str, Any] = {
            "bids": [],
            "asks": [],
            "lastUpdateId": "update456",
            "timestamp": 1678886401000,
        }
        order_book = BackpackResponseHandler.handle_get_order_book_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert len(order_book.bids) == 0
        assert len(order_book.asks) == 0
        assert order_book.last_update_id == "update456"

    def test_validation_error_invalid_bid_format(self, symbol_spot: str) -> None:
        """Test order book response with invalid bid format."""
        raw_data = {
            "bids": [["140.10", "10"], ["invalid_price", "20"]],  # Invalid price
            "asks": [["140.20", "15"]],
            "lastUpdateId": "update123",
            "timestamp": 1678886401000,
        }
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_order_book_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid order book ({symbol_spot}) - Status: 200" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test order book response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_order_book_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 400, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message


class TestHandleGetRecentTradesResponse:
    """Tests for BackpackResponseHandler.handle_get_recent_trades_response."""

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
        assert trades[0].quantity == "1.5"
        assert trades[0].time == 1678886402000
        assert trades[0].order_id == "order123"

        assert isinstance(trades[1], BackpackRawTrade)
        assert trades[1].id == "1002"
        assert trades[1].price == "141.01"
        assert trades[1].quantity == "0.5"
        assert trades[1].time == 1678886403000

    def test_empty_trades_list(self, symbol_spot: str) -> None:
        """Test handling empty recent trades response."""
        raw_data: list[Any] = []
        trades = BackpackResponseHandler.handle_get_recent_trades_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert isinstance(trades, list)
        assert len(trades) == 0

    def test_invalid_trade_item_skipped(
        self, symbol_spot: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that invalid trade items are skipped with warning."""
        valid_trade = {
            "symbol": symbol_spot,
            "price": "141.00",
            "qty": "1.5",
            "time": 1678886402000,
            "id": "1001",
            "orderId": "order123",
        }
        raw_data = [valid_trade, "not_a_dict"]  # Invalid item
        trades = BackpackResponseHandler.handle_get_recent_trades_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert len(trades) == 1  # Only valid trade processed
        assert trades[0].id == "1001"

        # Check that warning was logged
        log_found = any(
            "Skipping non-dict item" in record.getMessage() for record in caplog.records
        )
        assert log_found

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test recent trades response with missing required field."""
        invalid_trade = {
            "symbol": symbol_spot,
            "price": "141.00",
            # Missing 'qty' field
            "time": 1678886402000,
            "id": "1001",
            "orderId": "order123",
        }
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_recent_trades_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "single trade item" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test recent trades response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_recent_trades_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 400, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetMarketDataResponse:
    """Tests for BackpackResponseHandler.handle_get_market_data_response."""

    def test_valid(self, valid_raw_market_data: list[list[Any]], symbol_spot: str) -> None:
        """Test handling a valid raw market data (klines) response."""
        klines: list[BackpackRawKline] = BackpackResponseHandler.handle_get_market_data_response(
            cast(RawJsonResponse, valid_raw_market_data), symbol_spot, "1m", 200, {}
        )
        assert isinstance(klines, list)
        assert len(klines) == 2
        assert isinstance(klines[0], BackpackRawKline)
        assert klines[0].start_time_ms == 1678886400000
        assert klines[0].open_price.quantize(10) == 138  # Decimal comparison
        assert klines[0].high_price.quantize(10) == Decimal("140")  # 139.5 quantizes to 140

        assert isinstance(klines[1], BackpackRawKline)
        assert klines[1].start_time_ms == 1678886460000
        assert klines[1].close_price.quantize(10) == Decimal("140")  # 139.8 quantizes to 140

    def test_empty_klines_list(self, symbol_spot: str) -> None:
        """Test handling empty market data response."""
        raw_data: list[Any] = []
        klines = BackpackResponseHandler.handle_get_market_data_response(
            cast(RawJsonResponse, raw_data), symbol_spot, "1m", 200, {}
        )
        assert isinstance(klines, list)
        assert len(klines) == 0

    def test_invalid_kline_item_skipped(
        self, symbol_spot: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that invalid kline items are skipped with warning."""
        valid_kline = [
            1678886400000,
            "138.0",
            "139.5",
            "137.5",
            "139.0",
            "1000.0",
            1678886459999,
            "500000.0",
            100,
            "250000.0",
            "125000.0",
            "0",
        ]
        raw_data = [valid_kline, {"not": "a_list"}]  # Invalid item
        klines = BackpackResponseHandler.handle_get_market_data_response(
            cast(RawJsonResponse, raw_data), symbol_spot, "1m", 200, {}
        )
        assert len(klines) == 1  # Only valid kline processed

        # Check that warning was logged
        log_found = any(
            "Skipping non-list kline item" in record.getMessage() for record in caplog.records
        )
        assert log_found

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test market data response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_market_data_response(
                cast(RawJsonResponse, raw_data), symbol_spot, "1m", 400, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetHistoricalTradesResponse:
    """Tests for BackpackResponseHandler.handle_get_historical_trades_response."""

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

    def test_empty_historical_trades_list(self, symbol_spot: str) -> None:
        """Test handling empty historical trades response."""
        raw_data: list[Any] = []
        trades = BackpackResponseHandler.handle_get_historical_trades_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert isinstance(trades, list)
        assert len(trades) == 0

    def test_invalid_trade_item_skipped(
        self, symbol_spot: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that invalid historical trade items are skipped with warning."""
        valid_trade = {
            "id": "1001",
            "symbol": symbol_spot,
            "price": "135.00",
            "qty": "2.0",
            "time": 1678880000000,
            "orderId": "histOrderA",
        }
        raw_data = [valid_trade, "not_a_dict"]  # Invalid item
        trades = BackpackResponseHandler.handle_get_historical_trades_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert len(trades) == 1  # Only valid trade processed
        assert trades[0].id == "1001"

        # Check that warning was logged
        log_found = any(
            "Skipping non-dict item" in record.getMessage() for record in caplog.records
        )
        assert log_found

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test historical trades response with missing required field."""
        invalid_trade = {
            "id": "1001",
            "symbol": symbol_spot,
            # Missing 'price' field
            "qty": "2.0",
            "time": 1678880000000,
            "orderId": "histOrderA",
        }
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_historical_trades_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "single historical trade item" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "price" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test historical trades response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackResponseHandler.handle_get_historical_trades_response(
                cast(RawJsonResponse, raw_data), symbol_spot, 400, {}
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestMarketDataEdgeCases:
    """Tests for additional edge cases in market data response handling."""

    def test_ticker_with_zero_values(self, symbol_spot: str) -> None:
        """Test ticker response with zero/null values."""
        raw_data = {
            "symbol": symbol_spot,
            "price": "0.0",
            "bid": "0.0",
            "ask": "0.0",
            "volume": "0.0",
            "time": 0,
        }
        ticker = BackpackResponseHandler.handle_get_ticker_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert ticker.price == "0.0"
        assert ticker.volume == "0.0"
        assert ticker.time == 0

    def test_order_book_with_single_level(self, symbol_spot: str) -> None:
        """Test order book response with single bid/ask level."""
        raw_data = {
            "bids": [["140.10", "10"]],
            "asks": [["140.20", "15"]],
            "lastUpdateId": "update789",
            "timestamp": 1678886401000,
        }
        order_book = BackpackResponseHandler.handle_get_order_book_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert len(order_book.bids) == 1
        assert len(order_book.asks) == 1
        assert order_book.bids[0] == ("140.10", "10")
        assert order_book.asks[0] == ("140.20", "15")

    def test_trades_with_precision_values(self, symbol_spot: str) -> None:
        """Test trades response with high precision decimal values."""
        trade_item = {
            "symbol": symbol_spot,
            "price": "141.123456789",
            "qty": "1.000000001",
            "time": 1678886402000,
            "id": "precision_test",
            "orderId": "order_precision",
        }
        raw_data = [trade_item]
        trades = BackpackResponseHandler.handle_get_recent_trades_response(
            cast(RawJsonResponse, raw_data), symbol_spot, 200, {}
        )
        assert len(trades) == 1
        assert trades[0].price == "141.123456789"
        assert trades[0].quantity == "1.000000001"

    def test_klines_with_missing_optional_fields(self, symbol_spot: str) -> None:
        """Test klines response that handles optional fields gracefully."""
        # Using minimum required fields for kline
        minimal_kline = [
            1678886400000,  # start_time_ms
            "138.0",  # open_price
            "139.5",  # high_price
            "137.5",  # low_price
            "139.0",  # close_price
            "1000.0",  # volume
            1678886459999,  # end_time_ms
            "500000.0",  # quote_volume
            100,  # trade_count
            "250000.0",  # taker_buy_volume
            "125000.0",  # taker_buy_quote_volume
            "0",  # ignore field
        ]
        raw_data = [minimal_kline]
        klines = BackpackResponseHandler.handle_get_market_data_response(
            cast(RawJsonResponse, raw_data), symbol_spot, "1m", 200, {}
        )
        assert len(klines) == 1
        assert klines[0].start_time_ms == 1678886400000
        assert klines[0].trade_count == 100
