"""Unit tests for BackpackMarketDataResponseHandler market data response functionality."""

from decimal import Decimal
from typing import Any, cast

import pytest
import structlog.testing
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarket,
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawPublicTrade,
    BackpackRawRecentPublicTrade,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.utils.typing import ParsedJsonResponse


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.backpack.conftest_response_handler"]

# Type aliases for clarity
type RawJsonPrim = str | int | float | bool | None
type RawJson = dict[str, RawJson] | list[RawJson] | RawJsonPrim
type RawJsonResponse = RawJson


class TestHandleGetTickerResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_ticker_response."""

    def test_valid(self, valid_raw_ticker: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw ticker response."""
        ticker: BackpackRawTicker = BackpackMarketDataResponseHandler.handle_get_ticker_response(
            cast("ParsedJsonResponse", valid_raw_ticker),
            symbol_spot,
            200,
            {},
        )
        assert isinstance(ticker, BackpackRawTicker)
        assert ticker.symbol == symbol_spot
        assert ticker.first_price == "140.00"
        assert ticker.last_price == "140.50"
        assert ticker.high == "141.00"
        assert ticker.low == "139.50"
        assert ticker.volume == "500000.0"
        assert ticker.trades == "1250"

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test ticker response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_ticker_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test ticker response missing required field."""
        raw_data = {
            # Missing 'symbol' field (required)
            "firstPrice": "140.00",
            "lastPrice": "140.50",
            "high": "141.00",
            "low": "139.50",
            "priceChange": "0.50",
            "priceChangePercent": "0.36",
            "volume": "500000.0",
            "quoteVolume": "70250000.0",
            "trades": "1250",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_ticker_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid ticker ({symbol_spot}) - Status: 200" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "symbol" in str(exc_info.value.original_exception)

    def test_validation_error_invalid_price_format(self, symbol_spot: str) -> None:
        """Test ticker response with invalid price format."""
        raw_data = {
            "symbol": symbol_spot,
            "firstPrice": "140.00",
            "lastPrice": "invalid_price",
            "high": "141.00",
            "low": "139.50",
            "priceChange": "0.50",
            "priceChangePercent": "0.36",
            "volume": "500000.0",
            "quoteVolume": "70250000.0",
            "trades": "1250",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_ticker_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_extra_fields_ignored(self, symbol_spot: str) -> None:
        """Test that extra fields in ticker response cause ValidationError due to extra='forbid'."""
        raw_data = {
            "symbol": symbol_spot,
            "firstPrice": "140.00",
            "lastPrice": "140.50",
            "high": "141.00",
            "low": "139.50",
            "priceChange": "0.50",
            "priceChangePercent": "0.36",
            "volume": "500000.0",
            "quoteVolume": "70250000.0",
            "trades": "1250",
            "extraField": "should_be_ignored",
        }
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_ticker_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert isinstance(exc_info.value.original_exception, ValidationError)


class TestHandleGetOrderBookResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_order_book_response."""

    def test_valid(self, valid_raw_order_book: dict[str, Any], symbol_spot: str) -> None:
        """Test handling a valid raw order book response."""
        order_book: BackpackRawOrderBook = (
            BackpackMarketDataResponseHandler.handle_get_order_book_response(
                cast("ParsedJsonResponse", valid_raw_order_book),
                symbol_spot,
                200,
                {},
            )
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
        order_book = BackpackMarketDataResponseHandler.handle_get_order_book_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
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
            BackpackMarketDataResponseHandler.handle_get_order_book_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid order book ({symbol_spot}) - Status: 200" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test order book response with wrong top-level type."""
        raw_data = ["invalid"]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_order_book_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message


class TestHandleGetRecentTradesResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_recent_trades_response."""

    def test_valid(self, valid_raw_recent_trades: list[dict[str, Any]], symbol_spot: str) -> None:
        """Test handling a valid raw recent trades response."""
        trades: list[BackpackRawRecentPublicTrade] = (
            BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
                cast("ParsedJsonResponse", valid_raw_recent_trades),
                symbol_spot,
                200,
                {},
            )
        )
        assert len(trades) == 2
        assert isinstance(trades[0], BackpackRawRecentPublicTrade)
        assert trades[0].id == 1001
        assert trades[0].price == "141.00"
        assert trades[0].quantity == "1.5"
        assert trades[0].quote_quantity == "211.50"
        assert trades[0].timestamp == 1678886402000
        assert trades[0].is_buyer_maker is False

        assert isinstance(trades[1], BackpackRawRecentPublicTrade)
        assert trades[1].id == 1002
        assert trades[1].price == "141.01"
        assert trades[1].quantity == "0.5"
        assert trades[1].timestamp == 1678886403000

    def test_empty_trades_list(self, symbol_spot: str) -> None:
        """Test handling empty recent trades response."""
        raw_data: list[Any] = []
        trades = BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert isinstance(trades, list)
        assert len(trades) == 0

    def test_invalid_trade_item_raises_error(
        self,
        symbol_spot: str,
    ) -> None:
        """Test that invalid trade items raise APIError with centralized validation."""
        valid_trade = {
            "id": 1001,
            "isBuyerMaker": False,
            "price": "141.00",
            "quantity": "1.5",
            "quoteQuantity": "211.50",
            "timestamp": 1678886402000,
        }
        raw_data = [valid_trade, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"recent trades ({symbol_spot}) - Status: 200 item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test recent trades response with missing required field."""
        invalid_trade = {
            "id": 1001,
            "isBuyerMaker": False,
            "price": "141.00",
            # Missing 'quantity' field
            "quoteQuantity": "211.50",
            "timestamp": 1678886402000,
        }
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "single trade item" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test recent trades response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetMarketDataResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_market_data_response."""

    def test_valid(self, valid_raw_market_data: list[list[Any]], symbol_spot: str) -> None:
        """Test handling a valid raw market data (klines) response."""
        klines: list[BackpackRawKline] = (
            BackpackMarketDataResponseHandler.handle_get_market_data_response(
                cast("ParsedJsonResponse", valid_raw_market_data),
                symbol_spot,
                "1m",
                200,
                {},
            )
        )
        assert isinstance(klines, list)
        assert len(klines) == 2
        assert isinstance(klines[0], BackpackRawKline)
        assert klines[0].start_time_ms == 1678886400000
        assert klines[0].open_price.quantize(10) == 138  # Decimal comparison
        assert klines[0].high_price.quantize(10) == Decimal(140)  # 139.5 quantizes to 140

        assert isinstance(klines[1], BackpackRawKline)
        assert klines[1].start_time_ms == 1678886460000
        assert klines[1].close_price.quantize(10) == Decimal(140)  # 139.8 quantizes to 140

    def test_empty_klines_list(self, symbol_spot: str) -> None:
        """Test handling empty market data response."""
        raw_data: list[Any] = []
        klines = BackpackMarketDataResponseHandler.handle_get_market_data_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            "1m",
            200,
            {},
        )
        assert isinstance(klines, list)
        assert len(klines) == 0

    def test_invalid_kline_item_skipped(
        self,
        symbol_spot: str,
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

        with structlog.testing.capture_logs() as captured_logs:
            klines = BackpackMarketDataResponseHandler.handle_get_market_data_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                "1m",
                200,
                {},
            )

        assert len(klines) == 1  # Only valid kline processed

        # Check that warning was captured in structured logs
        warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
        assert len(warning_logs) > 0, "Expected at least one warning log"

        # Check for the specific warning about skipping non-list kline item
        skip_logs = [log for log in warning_logs if "Skipping non-list kline item" in str(log)]
        assert len(skip_logs) > 0, (
            f"Expected warning about skipping kline item, got: {warning_logs}"
        )

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test market data response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_market_data_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                "1m",
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message


class TestHandleGetHistoricalTradesResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_historical_trades_response."""

    def test_valid(
        self,
        valid_raw_historical_trades: list[dict[str, Any]],
        symbol_spot: str,
    ) -> None:
        """Test handling a valid raw historical trades response."""
        trades: list[BackpackRawPublicTrade] = (
            BackpackMarketDataResponseHandler.handle_get_historical_trades_response(
                cast("ParsedJsonResponse", valid_raw_historical_trades),
                symbol_spot,
                200,
                {},
            )
        )
        assert isinstance(trades, list)
        assert len(trades) == 2

        assert isinstance(trades[0], BackpackRawPublicTrade)
        assert trades[0].id == "1001"
        assert trades[0].order_id == "histOrderA"
        assert trades[0].symbol == symbol_spot
        assert trades[0].price == "135.00"
        assert trades[0].quantity == "2.0"
        assert trades[0].time == 1678880000000

        assert isinstance(trades[1], BackpackRawPublicTrade)
        assert trades[1].id == "1002"
        assert trades[1].order_id == "histOrderB"
        assert trades[1].symbol == symbol_spot
        assert trades[1].price == "135.10"
        assert trades[1].quantity == "1.0"
        assert trades[1].time == 1678880100000

    def test_empty_historical_trades_list(self, symbol_spot: str) -> None:
        """Test handling empty historical trades response."""
        raw_data: list[Any] = []
        trades = BackpackMarketDataResponseHandler.handle_get_historical_trades_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert isinstance(trades, list)
        assert len(trades) == 0

    def test_invalid_trade_item_raises_error(
        self,
        symbol_spot: str,
    ) -> None:
        """Test that invalid historical trade items raise APIError with centralized validation."""
        valid_trade = {
            "id": "1001",
            "orderId": "histOrderA",
            "symbol": symbol_spot,
            "price": "135.00",
            "qty": "2.0",
            "time": 1678880000000,
        }
        raw_data = [valid_trade, "not_a_dict"]  # Invalid item
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_historical_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"historical trades ({symbol_spot}) - Status: 200 item[1]" in exc_info.value.message
        assert "expected dict, got str" in exc_info.value.message

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test historical trades response with missing required field."""
        invalid_trade = {
            "id": "1001",
            "isBuyerMaker": True,
            # Missing 'price' field
            "quantity": "2.0",
            "quoteQuantity": "270.00",
            "timestamp": 1678880000000,
        }
        raw_data = [invalid_trade]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_historical_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "single historical trade item" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)
        assert "price" in str(exc_info.value.original_exception)

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test historical trades response with wrong top-level type."""
        raw_data = {"error": "expected list"}
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_historical_trades_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                400,
                {},
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
            "firstPrice": "0.0",
            "lastPrice": "0.0",
            "high": "0.0",
            "low": "0.0",
            "priceChange": "0.0",
            "priceChangePercent": "0.0",
            "volume": "0.0",
            "quoteVolume": "0.0",
            "trades": "0",
        }
        ticker = BackpackMarketDataResponseHandler.handle_get_ticker_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert ticker.last_price == "0.0"
        assert ticker.volume == "0.0"
        assert ticker.trades == "0"

    def test_order_book_with_single_level(self, symbol_spot: str) -> None:
        """Test order book response with single bid/ask level."""
        raw_data = {
            "bids": [["140.10", "10"]],
            "asks": [["140.20", "15"]],
            "lastUpdateId": "update789",
            "timestamp": 1678886401000,
        }
        order_book = BackpackMarketDataResponseHandler.handle_get_order_book_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert len(order_book.bids) == 1
        assert len(order_book.asks) == 1
        assert order_book.bids[0] == ("140.10", "10")
        assert order_book.asks[0] == ("140.20", "15")

    def test_trades_with_precision_values(self, symbol_spot: str) -> None:
        """Test trades response with high precision decimal values."""
        trade_item = {
            "id": 12345,
            "isBuyerMaker": False,
            "price": "141.123456789",
            "quantity": "1.000000001",
            "quoteQuantity": "141.123456930",
            "timestamp": 1678886402000,
        }
        raw_data = [trade_item]
        trades = BackpackMarketDataResponseHandler.handle_get_recent_trades_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
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
        klines = BackpackMarketDataResponseHandler.handle_get_market_data_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            "1m",
            200,
            {},
        )
        assert len(klines) == 1
        assert klines[0].start_time_ms == 1678886400000
        assert klines[0].trade_count == 100


class TestHandleGetMarketsResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_markets_response."""

    def test_valid_markets_list(self) -> None:
        """Test handling a valid raw markets response."""
        raw_data = [
            {
                "symbol": "SOL_USDC",
                "baseSymbol": "SOL",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.0001",
                        "maxQuantity": "1000.0",
                        "stepSize": "0.01",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            },
            {
                "symbol": "BTC_USDC",
                "baseSymbol": "BTC",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.0001",
                        "maxQuantity": "1000.0",
                        "stepSize": "0.0001",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            },
        ]
        markets = BackpackMarketDataResponseHandler.handle_get_markets_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert isinstance(markets, list)
        assert len(markets) == 2
        assert all(isinstance(market, BackpackRawMarket) for market in markets)
        assert markets[0].symbol == "SOL_USDC"
        assert markets[0].base_symbol == "SOL"
        assert markets[0].quote_symbol == "USDC"
        assert markets[1].symbol == "BTC_USDC"

    def test_empty_markets_list(self) -> None:
        """Test handling an empty markets list."""
        raw_data: list[Any] = []
        markets = BackpackMarketDataResponseHandler.handle_get_markets_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert isinstance(markets, list)
        assert len(markets) == 0

    def test_invalid_top_level_type(self) -> None:
        """Test markets response with wrong top-level type."""
        raw_data = {"not": "a_list"}
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_markets_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected list" in exc_info.value.message
        assert "got dict" in exc_info.value.message

    def test_validation_error_in_market_item(self) -> None:
        """Test markets response with invalid market item."""
        raw_data = [
            {
                "symbol": "SOL_USDC",
                "baseSymbol": "SOL",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.0001",
                        "maxQuantity": "1000.0",
                        "stepSize": "0.01",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            },
            {
                # Missing required fields
                "symbol": "BTC_USDC",
                # Missing baseSymbol, quoteSymbol, marketType, filters, orderBookState, createdAt
            },
        ]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_markets_response(
                cast("ParsedJsonResponse", raw_data),
                status_code=200,
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "Invalid markets item 1" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_large_markets_list(self) -> None:
        """Test handling a large list of markets."""
        raw_data: list[dict[str, Any]] = [
            {
                "symbol": f"ASSET{i}_USDC",
                "baseSymbol": f"ASSET{i}",
                "quoteSymbol": "USDC",
                "marketType": "Spot",
                "filters": {
                    "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                    "quantity": {
                        "minQuantity": "0.0001",
                        "maxQuantity": "1000.0",
                        "stepSize": "0.01",
                    },
                },
                "orderBookState": "NORMAL",
                "createdAt": "2024-01-01T00:00:00.000Z",
            }
            for i in range(100)
        ]

        markets = BackpackMarketDataResponseHandler.handle_get_markets_response(
            cast("ParsedJsonResponse", raw_data),
            status_code=200,
        )
        assert len(markets) == 100
        assert all(isinstance(market, BackpackRawMarket) for market in markets)
        assert markets[0].symbol == "ASSET0_USDC"
        assert markets[99].symbol == "ASSET99_USDC"


class TestHandleGetMarketResponse:
    """Tests for BackpackMarketDataResponseHandler.handle_get_market_response."""

    def test_valid_market(self, symbol_spot: str) -> None:
        """Test handling a valid raw market response."""
        raw_data = {
            "symbol": symbol_spot,
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.01", "maxPrice": "1000000.0", "tickSize": "0.01"},
                "quantity": {"minQuantity": "0.0001", "maxQuantity": "1000.0", "stepSize": "0.01"},
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }
        market = BackpackMarketDataResponseHandler.handle_get_market_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert isinstance(market, BackpackRawMarket)
        assert market.symbol == symbol_spot
        assert market.base_symbol == "SOL"
        assert market.quote_symbol == "USDC"
        assert market.filters.price.tick_size == "0.01"
        assert market.filters.quantity.step_size == "0.01"

    def test_invalid_top_level_type(self, symbol_spot: str) -> None:
        """Test market response with wrong top-level type."""
        raw_data = ["not", "a", "dict"]
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_market_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                400,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert "expected dict" in exc_info.value.message
        assert "got list" in exc_info.value.message
        assert f"market for {symbol_spot}" in exc_info.value.message

    def test_validation_error_missing_field(self, symbol_spot: str) -> None:
        """Test market response missing required field."""
        raw_data = {
            "symbol": symbol_spot,
            "baseSymbol": "SOL",
            # Missing required quoteSymbol, marketType, filters, orderBookState, createdAt
        }
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_market_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert exc_info.value.code == APIErrorCode.INVALID_RESPONSE.value
        assert f"Invalid market for {symbol_spot}" in exc_info.value.message
        assert isinstance(exc_info.value.original_exception, ValidationError)

    def test_validation_error_invalid_field_type(self, symbol_spot: str) -> None:
        """Test market response with invalid field type."""
        raw_data: dict[str, Any] = {
            "symbol": symbol_spot,
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {
                    "minPrice": "0.01",
                    "maxPrice": "1000000.0",
                    "tickSize": "0.01",
                },
                # minQuantity should be string
                "quantity": {"minQuantity": 0.0001, "maxQuantity": "1000.0", "stepSize": "0.01"},
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }
        with pytest.raises(TypeFieldError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_market_response(
                cast("ParsedJsonResponse", raw_data),
                symbol_spot,
                200,
                {},
            )
        assert "Field 'min_quantity' must be string, got float" in str(exc_info.value)

    def test_market_with_all_optional_fields(self, symbol_spot: str) -> None:
        """Test market response with all optional fields populated."""
        raw_data = {
            "symbol": symbol_spot,
            "baseSymbol": "SOL",
            "quoteSymbol": "USDC",
            "marketType": "Spot",
            "filters": {
                "price": {"minPrice": "0.001", "maxPrice": "10000.0", "tickSize": "0.01"},
                "quantity": {"minQuantity": "0.1", "maxQuantity": "1000000.0", "stepSize": "0.01"},
            },
            "orderBookState": "NORMAL",
            "createdAt": "2024-01-01T00:00:00.000Z",
        }
        market = BackpackMarketDataResponseHandler.handle_get_market_response(
            cast("ParsedJsonResponse", raw_data),
            symbol_spot,
            200,
            {},
        )
        assert isinstance(market, BackpackRawMarket)
        assert market.symbol == symbol_spot
        assert market.filters.price.min_price == "0.001"
        assert market.filters.price.max_price == "10000.0"
        assert market.filters.quantity.min_quantity == "0.1"
        assert market.filters.quantity.max_quantity == "1000000.0"
        assert market.order_book_state == "NORMAL"

    def test_market_context_in_error_message(self) -> None:
        """Test that error messages include market context."""
        symbol = "TEST_SYMBOL"
        raw_data = "not a dict"
        with pytest.raises(APIError) as exc_info:
            BackpackMarketDataResponseHandler.handle_get_market_response(
                cast("ParsedJsonResponse", raw_data),
                symbol,
                404,
                {},
            )
        assert f"market for {symbol}" in exc_info.value.message
        assert exc_info.value.http_status == 404
