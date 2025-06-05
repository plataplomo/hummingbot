"""CyberDeltaEngine: Backpack Market Data Mapper Core Tests.

--------------------------------------------------------

Comprehensive test suite for BackpackMarketDataMapper core transformation methods.
Tests ticker, order book, trade, funding rate, and kline transformations including:
- Happy path transformations
- Error handling and edge cases
- Boundary value testing
- Data validation scenarios
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRate,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKline
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    BackpackRawTicker,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawTrade
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def mapper() -> BackpackMarketDataMapper:
    """Fixture providing a BackpackMarketDataMapper instance."""
    return BackpackMarketDataMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string."""
    return "2024-01-15T10:30:00Z"


@pytest.fixture
def test_timestamp_ms() -> int:
    """Fixture providing a consistent test timestamp in milliseconds."""
    return 1705314600000  # 2024-01-15T10:30:00Z


def create_raw_ticker(
    symbol: str = "SOL-USDC",
    price: str | None = "100.50",
    bid: str | None = "100.25",
    ask: str | None = "100.75",
    volume: str | None = "1000.0",
    time: str = "2024-01-15T10:30:00Z",
) -> BackpackRawTicker:
    """Create BackpackRawTicker instances for testing ticker transformations."""
    return BackpackRawTicker(
        symbol=symbol,
        price=price,
        bid=bid,
        ask=ask,
        volume=volume,
        time=time,
    )


def create_raw_order_book(
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    timestamp: str = "2024-01-15T10:30:00Z",
) -> BackpackRawOrderBook:
    """Create BackpackRawOrderBook instances for testing order book transformations."""
    if bids is None:
        bids = [("100.25", "10.0"), ("100.00", "5.0")]
    if asks is None:
        asks = [("100.75", "8.0"), ("101.00", "12.0")]

    return BackpackRawOrderBook(
        bids=bids,
        asks=asks,
        lastUpdateId="12345",
        timestamp=timestamp,
    )


def create_raw_trade(
    id: str = "trade123",
    symbol: str = "SOL-USDC",
    price: str = "100.50",
    qty: str = "10.0",
    time: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
) -> BackpackRawTrade:
    """Create BackpackRawTrade instances for testing trade transformations."""
    return BackpackRawTrade(
        id=id,
        symbol=symbol,
        price=price,
        qty=qty,
        time=time,
        orderId=order_id,
    )


def create_raw_funding_rate(
    symbol: str = "SOL-USDC",
    rate: str = "0.0001",
    mark_price: str = "100.50",
    index_price: str = "100.25",
    time: int = 1705314600000,
) -> BackpackRawFundingRate:
    """Create BackpackRawFundingRate instances for testing funding rate transformations."""
    return BackpackRawFundingRate(
        symbol=symbol,
        rate=rate,
        markPrice=mark_price,
        indexPrice=index_price,
        time=time,
    )


def create_raw_funding_interval_rate(
    symbol: str = "SOL-USDC",
    rate: str = "0.0001",
    time: int = 1705314600000,
) -> BackpackRawFundingIntervalRate:
    """Create BackpackRawFundingIntervalRate instances for testing interval rate transformations."""
    return BackpackRawFundingIntervalRate(
        symbol=symbol,
        rate=rate,
        time=time,
    )


def create_raw_kline(
    symbol: str = "SOL-USDC",
    start_time_ms: int = 1705314600000,
    open_price: str = "100.00",
    high_price: str = "101.00",
    low_price: str = "99.50",
    close_price: str = "100.50",
    volume: str = "1000.0",
) -> BackpackRawKline:
    """Create BackpackRawKline instances for testing kline transformations."""
    # BackpackRawKline expects a list/tuple of 12 elements in this order:
    # [start_time_ms, open_price, high_price, low_price, close_price, volume,
    #  end_time_ms, quote_volume, trade_count, taker_buy_base_volume,
    #  taker_buy_quote_volume, ignored]
    kline_data = [
        start_time_ms,  # start_time_ms (int)
        open_price,  # open_price (string)
        high_price,  # high_price (string)
        low_price,  # low_price (string)
        close_price,  # close_price (string)
        volume,  # volume (string)
        start_time_ms + 3600000,  # end_time_ms (int) - 1 hour later
        "100500.0",  # quote_volume (string)
        100,  # trade_count (int)
        "500.0",  # taker_buy_base_volume (string)
        "50250.0",  # taker_buy_quote_volume (string)
        "0",  # ignored (string)
    ]
    return BackpackRawKline.model_validate(kline_data)


class TestTickerTransformation:
    """Test cases for ticker transformation functionality."""

    def test_transform_raw_ticker_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawTicker to internal Ticker."""
        raw_ticker = create_raw_ticker(
            symbol="SOL-USDC",
            price="100.50",
            bid="100.25",
            ask="100.75",
            volume="1000.0",
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert isinstance(result, Ticker)
        assert result.symbol == "SOL-USDC"
        assert result.price == Decimal("100.50")
        assert result.bid == Decimal("100.25")
        assert result.ask == Decimal("100.75")
        assert result.volume == Decimal("1000.0")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_transform_raw_ticker_with_symbol_override(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with symbol override."""
        raw_ticker = create_raw_ticker(symbol="SOL-USDC", time=test_timestamp)

        result = mapper.transform_raw_ticker_to_internal(raw_ticker, symbol_override="BTC-USDC")

        assert result.symbol == "BTC-USDC"

    def test_transform_raw_ticker_with_none_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with None values for optional fields."""
        # Create ticker with None values by constructing directly
        raw_ticker = BackpackRawTicker(
            symbol="SOL-USDC",
            price=None,
            bid=None,
            ask=None,
            volume=None,
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price is None
        assert result.bid is None
        assert result.ask is None
        assert result.volume is None

    @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
    def test_transform_raw_ticker_with_none_timestamp(
        self,
        mock_datetime: MagicMock,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test ticker transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now

        # Create a valid raw ticker first
        raw_ticker = BackpackRawTicker(
            symbol="SOL-USDC",
            price="100.50",
            bid="100.25",
            ask="100.75",
            volume="1000.0",
            time="2024-01-15T10:30:00Z",
        )

        # Mock parse_datetime_utc to return None for timestamp parsing
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_ticker_to_internal(raw_ticker)

            assert isinstance(result, Ticker)
            assert result.timestamp == mock_now
            mock_datetime.now.assert_called_once_with(UTC)

    def test_transform_raw_ticker_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid ticker but patch parsing to cause error
        raw_ticker = create_raw_ticker(price="100.50")

        # This test relies on the actual transformation logic to cause an error
        # We'll use an invalid decimal that passes basic validation but fails transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawTicker to Ticker",
            ):
                mapper.transform_raw_ticker_to_internal(raw_ticker)

    def test_transform_raw_ticker_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with extreme decimal values."""
        raw_ticker = create_raw_ticker(
            price="0.000001",  # Very small price
            bid="999999.999999",  # Very large bid
            ask="1000000.000001",  # Very large ask
            volume="0.000000001",  # Very small volume
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal("0.000001")
        assert result.bid == Decimal("999999.999999")
        assert result.ask == Decimal("1000000.000001")
        assert result.volume == Decimal("0.000000001")

    def test_transform_raw_ticker_with_zero_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with zero values."""
        raw_ticker = create_raw_ticker(
            price="0",
            bid="0",
            ask="0",
            volume="0",
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal("0")
        assert result.bid == Decimal("0")
        assert result.ask == Decimal("0")
        assert result.volume == Decimal("0")


class TestOrderBookTransformation:
    """Test cases for order book transformation functionality."""

    def test_transform_raw_order_book_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawOrderBook to internal OrderBook."""
        raw_book = create_raw_order_book(
            bids=[("100.25", "10.0"), ("100.00", "5.0")],
            asks=[("100.75", "8.0"), ("101.00", "12.0")],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

        assert isinstance(result, OrderBook)
        assert result.symbol == "SOL-USDC"
        assert len(result.bids) == 2
        assert len(result.asks) == 2
        assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_transform_raw_order_book_empty_levels(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with empty bid/ask levels."""
        raw_book = create_raw_order_book(
            bids=[],
            asks=[],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

        assert len(result.bids) == 0
        assert len(result.asks) == 0

    def test_transform_raw_order_book_large_number_of_levels(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with large number of levels."""
        # Create 100 bid and ask levels
        bids = [(f"{100 - i * 0.01:.2f}", f"{i + 1}.0") for i in range(100)]
        asks = [(f"{101 + i * 0.01:.2f}", f"{i + 1}.0") for i in range(100)]

        raw_book = create_raw_order_book(
            bids=bids,
            asks=asks,
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

        assert len(result.bids) == 100
        assert len(result.asks) == 100
        assert result.bids[0][0] == Decimal("100.00")
        assert result.asks[0][0] == Decimal("101.00")

    @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
    def test_transform_raw_order_book_with_none_timestamp(
        self,
        mock_datetime: MagicMock,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test order book transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now

        # Create a valid raw order book and mock the timestamp parsing to return None
        raw_book = create_raw_order_book(
            bids=[("100.25", "10.0"), ("100.00", "5.0")],
            asks=[("100.75", "8.0"), ("101.00", "12.0")],
            timestamp="2024-01-15T10:30:00Z",
        )

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.timestamp == mock_now

    def test_transform_raw_order_book_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid order book and mock parsing to cause error
        raw_book = create_raw_order_book(bids=[("100.25", "10.0")])

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawOrderBook to OrderBook",
            ):
                mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

    def test_transform_raw_order_book_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with extreme price and quantity values."""
        raw_book = create_raw_order_book(
            bids=[("0.000001", "999999999.999999"), ("99999.999999", "0.000001")],
            asks=[("1000000.000001", "0.000000001"), ("1000001.000001", "1000000000.0")],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

        assert result.bids[0] == (Decimal("0.000001"), Decimal("999999999.999999"))
        assert result.asks[0] == (Decimal("1000000.000001"), Decimal("0.000000001"))

    def test_transform_raw_order_book_with_unicode_symbol(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with unicode symbol."""
        raw_book = create_raw_order_book(timestamp=test_timestamp)

        result = mapper.transform_raw_order_book_to_internal("SOL-USDC-测试", raw_book)

        assert result.symbol == "SOL-USDC-测试"


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawTrade to internal Trade."""
        raw_trade = create_raw_trade(
            id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time=test_timestamp,
            order_id="order123",
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert isinstance(result, Trade)
        assert result.id == "trade123"
        assert result.symbol == "SOL-USDC"
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.order_id == "order123"
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.executed_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_trade_missing_price(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test that missing price raises TransformationError."""
        # Cannot create BackpackRawTrade with None price, so patch parsing to return None
        raw_trade = create_raw_trade(price="100.50", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.return_value = None

            with pytest.raises(TransformationError, match="price is required for trade"):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_missing_quantity(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        # Cannot create BackpackRawTrade with None qty, so patch parsing to return None
        raw_trade = create_raw_trade(qty="10.0", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            # Return None only for quantity field
            def mock_parse_side_effect(
                value: object,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return mock parse side effect for testing."""
                if field_name == "quantity":
                    return None
                return Decimal("100.50")  # Valid for price

            mock_parse.side_effect = mock_parse_side_effect

            with pytest.raises(TransformationError, match="quantity is required for trade"):
                mapper.transform_raw_trade_to_internal(raw_trade)

    @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
    def test_transform_raw_trade_with_none_timestamp(
        self,
        mock_datetime: MagicMock,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test trade transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        mock_datetime.now.return_value = mock_now

        # Create a valid raw trade and mock the timestamp parsing to return None
        raw_trade = create_raw_trade(
            id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time="2024-01-15T10:30:00Z",
            order_id="order123",
        )

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_trade_to_internal(raw_trade)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.executed_at == mock_now

    def test_transform_raw_trade_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        raw_trade = create_raw_trade(price="100.50")

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawTrade to Trade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test trade transformation with extreme decimal values."""
        raw_trade = create_raw_trade(
            price="0.000001",  # Very small price
            qty="999999999.999999",  # Very large quantity
            time=test_timestamp,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert result.price == Decimal("0.000001")
        assert result.quantity == Decimal("999999999.999999")

    def test_transform_raw_trade_with_very_long_id(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test trade transformation with maximum allowed trade ID length."""
        long_id = "a" * 64  # 64 character ID (max allowed by BackpackRawTrade.id)
        raw_trade = create_raw_trade(
            id=long_id,
            time=test_timestamp,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert result.id == long_id


class TestFundingRateTransformation:
    """Test cases for funding rate transformation functionality."""

    def test_transform_raw_funding_rate_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFundingRate to internal FundingRate."""
        raw_funding = create_raw_funding_rate(
            symbol="SOL-USDC",
            rate="0.0001",
            mark_price="100.50",
            index_price="100.25",
            time=1705314600000,
        )

        result = mapper.transform_raw_funding_rate_to_internal(raw_funding)

        assert isinstance(result, FundingRate)
        assert result.symbol == "SOL-USDC"
        assert result.funding_rate == Decimal("0.0001")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_funding_interval_rate_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFundingIntervalRate to FundingRate."""
        raw_funding = create_raw_funding_interval_rate(
            symbol="SOL-USDC",
            rate="0.0001",
            time=1705314600000,
        )

        result = mapper.transform_raw_funding_interval_rate_to_internal(raw_funding, "SOL-USDC")

        assert isinstance(result, FundingRate)
        assert result.symbol == "SOL-USDC"
        assert result.funding_rate == Decimal("0.0001")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_funding_rate_with_extreme_rates(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test funding rate transformation with extreme rate values."""
        # Very high positive rate
        raw_funding_high = create_raw_funding_rate(rate="0.999999")
        result_high = mapper.transform_raw_funding_rate_to_internal(raw_funding_high)
        assert result_high.funding_rate == Decimal("0.999999")

        # Very low negative rate
        raw_funding_low = create_raw_funding_rate(rate="-0.999999")
        result_low = mapper.transform_raw_funding_rate_to_internal(raw_funding_low)
        assert result_low.funding_rate == Decimal("-0.999999")

        # Very small rate
        raw_funding_small = create_raw_funding_rate(rate="0.000000001")
        result_small = mapper.transform_raw_funding_rate_to_internal(raw_funding_small)
        assert result_small.funding_rate == Decimal("0.000000001")

    def test_transform_raw_funding_rate_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test that funding rate transformation errors are properly wrapped."""
        raw_funding = create_raw_funding_rate()

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawFundingRate",
            ):
                mapper.transform_raw_funding_rate_to_internal(raw_funding)


class TestKlineTransformation:
    """Test cases for kline transformation functionality."""

    def test_transform_raw_kline_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test successful transformation of BackpackRawKline to internal Candle."""
        raw_kline = create_raw_kline(
            symbol="SOL-USDC",
            start_time_ms=test_timestamp_ms,
            open_price="100.00",
            high_price="101.00",
            low_price="99.50",
            close_price="100.50",
            volume="1000.0",
        )

        result = mapper.transform_raw_kline_to_internal("SOL-USDC", "1h", raw_kline)

        assert isinstance(result, Candle)
        assert result.symbol == "SOL-USDC"
        assert result.interval == "1h"
        assert result.open == Decimal("100.00")
        assert result.high == Decimal("101.00")
        assert result.low == Decimal("99.50")
        assert result.close == Decimal("100.50")
        assert result.volume == Decimal("1000.0")

    def test_transform_raw_kline_to_internal_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw kline
        raw_kline = create_raw_kline()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawKline to Candle",
            ):
                mapper.transform_raw_kline_to_internal("SOL-USDC", "1h", raw_kline)

    def test_transform_raw_kline_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test kline transformation with extreme price and volume values."""
        raw_kline = create_raw_kline(
            start_time_ms=test_timestamp_ms,
            open_price="0.000001",  # Very small open
            high_price="999999.999999",  # Very high
            low_price="0.000000001",  # Very small low
            close_price="500000.123456",  # Large close
            volume="1000000000.999999",  # Very large volume
        )

        result = mapper.transform_raw_kline_to_internal("SOL-USDC", "1h", raw_kline)

        assert result.open == Decimal("0.000001")
        assert result.high == Decimal("999999.999999")
        assert result.low == Decimal("0.000000001")
        assert result.close == Decimal("500000.123456")
        assert result.volume == Decimal("1000000000.999999")

    def test_transform_raw_kline_with_different_intervals(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test kline transformation with different interval values."""
        raw_kline = create_raw_kline(start_time_ms=test_timestamp_ms)

        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]
        for interval in intervals:
            result = mapper.transform_raw_kline_to_internal("SOL-USDC", interval, raw_kline)
            assert result.interval == interval
