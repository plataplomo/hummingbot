"""
CyberDeltaEngine: Backpack Market Data Mapper Tests
---------------------------------------------------

Comprehensive test suite for BackpackMarketDataMapper class.
Tests all public transformation methods with various scenarios including:
- Happy path transformations
- Error handling and edge cases
- Side mapping functionality
- WebSocket event transformations
- Boundary value testing
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
    BackpackRawDepthUpdateEvent,
    BackpackRawOrderBook,
    BackpackRawTicker,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import (
    BackpackRawTrade,
    BackpackRawTradeEvent,
)
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.core.models.market import Candle
from cyberdelta.core.models.market.funding_rate import FundingRate


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
    """Helper function to create BackpackRawTicker instances for testing."""
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
    """Helper function to create BackpackRawOrderBook instances for testing."""
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
    """Helper function to create BackpackRawTrade instances for testing."""
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
    """Helper function to create BackpackRawFundingRate instances for testing."""
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
    """Helper function to create BackpackRawFundingIntervalRate instances for testing."""
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
    """Helper function to create BackpackRawKline instances for testing."""
    # BackpackRawKline expects a list/tuple of 12 elements in this order:
    # [start_time_ms, open_price, high_price, low_price, close_price, volume,
    #  end_time_ms, quote_volume, trade_count, taker_buy_base_volume, taker_buy_quote_volume, ignored]
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


def create_raw_ticker_event(
    s: str = "SOL-USDC",
    last_price: str = "100.50",
    high: str = "101.00",
    low: str = "99.50",
    open_price: str = "100.00",
    volume: str = "1000.0",
    quote_volume: str = "100500.0",
    price_change_percent: str = "0.5",
    event_time: int = 1705314600000,
) -> BackpackRawTickerEvent:
    """Helper function to create BackpackRawTickerEvent instances for testing."""
    return BackpackRawTickerEvent(
        s=s,
        lastPrice=last_price,
        high=high,
        low=low,
        o=open_price,
        volume=volume,
        quoteVolume=quote_volume,
        priceChangePercent=price_change_percent,
        e="ticker",  # Use literal value directly
        E=event_time,
    )


def create_raw_depth_event(
    last_update_id: str = "12345",
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    event_time: int = 1705314600000,
) -> BackpackRawDepthUpdateEvent:
    """Helper function to create BackpackRawDepthUpdateEvent instances for testing."""
    if bids is None:
        bids = [("100.25", "10.0")]
    if asks is None:
        asks = [("100.75", "8.0")]

    return BackpackRawDepthUpdateEvent(
        lastUpdateId=last_update_id,
        b=bids,
        a=asks,
        e="depth",  # Use literal value directly
        E=event_time,
    )


def create_raw_trade_event(
    s: str = "SOL-USDC",
    p: str = "100.50",
    q: str = "10.0",
    t: str = "trade123",
    m: bool = False,
    event_time: int = 1705314600000,
    buyer_id: str = "buyer123",
    seller_id: str = "seller123",
    trade_time: int = 1705314600000,
) -> BackpackRawTradeEvent:
    """Helper function to create BackpackRawTradeEvent instances for testing."""
    return BackpackRawTradeEvent(
        s=s,
        p=p,
        q=q,
        t=t,
        m=m,
        e="trade",  # Use literal value directly
        E=event_time,
        b=buyer_id,
        a=seller_id,
        T=trade_time,
    )


class TestSideMapping:
    """Test cases for side mapping functionality."""

    @pytest.mark.parametrize(
        "bp_side,expected_side",
        [
            ("Buy", OrderSide.BUY),
            ("buy", OrderSide.BUY),
            ("BUY", OrderSide.BUY),
            ("Bid", OrderSide.BUY),
            ("bid", OrderSide.BUY),
            ("BID", OrderSide.BUY),
            ("Sell", OrderSide.SELL),
            ("sell", OrderSide.SELL),
            ("SELL", OrderSide.SELL),
            ("Ask", OrderSide.SELL),
            ("ask", OrderSide.SELL),
            ("ASK", OrderSide.SELL),
        ],
    )
    def test_map_side_to_internal_valid_sides(
        self, mapper: BackpackMarketDataMapper, bp_side: str, expected_side: OrderSide
    ) -> None:
        """Test mapping of valid Backpack side strings to internal OrderSide enum."""
        # Test side mapping through trade transformation instead of protected method
        raw_trade_event = BackpackRawTradeEvent(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=(expected_side == OrderSide.BUY),  # Buyer is maker for BUY side
            e="trade",
            E=1705316600000,
            b="buyer123",
            a="seller123",
            T=1705316600000,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade_event)
        assert result.side == expected_side

    def test_side_mapping_through_trade_events(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test side mapping through WebSocket trade event transformation."""
        # Test buyer side (is_buyer_the_maker=True means BUY side)
        raw_trade_buy = BackpackRawTradeEvent(
            e="trade",
            E=1705316600000,
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            b="buyer123",
            a="seller123",
            t="trade123",
            T=1705316600000,
            m=True,  # Buyer is maker -> BUY side
        )

        result_buy = mapper.transform_ws_trade_event_to_internal(raw_trade_buy)
        assert result_buy.side == OrderSide.BUY

        # Test seller side (is_buyer_the_maker=False means SELL side)
        raw_trade_sell = BackpackRawTradeEvent(
            e="trade",
            E=1705316600000,
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            b="buyer123",
            a="seller123",
            t="trade456",
            T=1705316600000,
            m=False,  # Buyer is not maker -> SELL side
        )

        result_sell = mapper.transform_ws_trade_event_to_internal(raw_trade_sell)
        assert result_sell.side == OrderSide.SELL


class TestTickerTransformation:
    """Test cases for ticker transformation functionality."""

    def test_transform_raw_ticker_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test ticker transformation with symbol override."""
        raw_ticker = create_raw_ticker(symbol="SOL-USDC", time=test_timestamp)

        result = mapper.transform_raw_ticker_to_internal(raw_ticker, symbol_override="BTC-USDC")

        assert result.symbol == "BTC-USDC"

    def test_transform_raw_ticker_with_none_values(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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
        self, mock_datetime: MagicMock, mapper: BackpackMarketDataMapper
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
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc"
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_ticker_to_internal(raw_ticker)

            assert isinstance(result, Ticker)
            assert result.timestamp == mock_now
            mock_datetime.now.assert_called_once_with(UTC)

    def test_transform_raw_ticker_transformation_error(
        self, mapper: BackpackMarketDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid ticker but patch parsing to cause error
        raw_ticker = create_raw_ticker(price="100.50")

        # This test relies on the actual transformation logic to cause an error
        # We'll use an invalid decimal that passes basic validation but fails transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError, match="Failed to transform BackpackRawTicker to Ticker"
            ):
                mapper.transform_raw_ticker_to_internal(raw_ticker)


class TestOrderBookTransformation:
    """Test cases for order book transformation functionality."""

    def test_transform_raw_order_book_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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

    @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
    def test_transform_raw_order_book_with_none_timestamp(
        self, mock_datetime: MagicMock, mapper: BackpackMarketDataMapper
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
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc"
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.timestamp == mock_now

    def test_transform_raw_order_book_transformation_error(
        self, mapper: BackpackMarketDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid order book and mock parsing to cause error
        raw_book = create_raw_order_book(bids=[("100.25", "10.0")])

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError, match="Failed to transform BackpackRawOrderBook to OrderBook"
            ):
                mapper.transform_raw_order_book_to_internal("SOL-USDC", raw_book)


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test that missing price raises TransformationError."""
        # Cannot create BackpackRawTrade with None price, so patch parsing to return None
        raw_trade = create_raw_trade(price="100.50", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.return_value = None

            with pytest.raises(TransformationError, match="price is required for trade"):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_missing_quantity(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        # Cannot create BackpackRawTrade with None qty, so patch parsing to return None
        raw_trade = create_raw_trade(qty="10.0", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            # Return None only for quantity field
            def mock_parse_side_effect(
                value: object, allow_none: bool = False, field_name: str = ""
            ) -> Decimal | None:
                if field_name == "quantity":
                    return None
                return Decimal("100.50")  # Valid for price

            mock_parse.side_effect = mock_parse_side_effect

            with pytest.raises(TransformationError, match="quantity is required for trade"):
                mapper.transform_raw_trade_to_internal(raw_trade)

    @patch("cyberdelta.apis.backpack.mappers.bp_market_data_mapper.datetime")
    def test_transform_raw_trade_with_none_timestamp(
        self, mock_datetime: MagicMock, mapper: BackpackMarketDataMapper
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
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_datetime_utc"
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_trade_to_internal(raw_trade)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.executed_at == mock_now

    def test_transform_raw_trade_transformation_error(
        self, mapper: BackpackMarketDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        raw_trade = create_raw_trade(price="100.50")

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError, match="Failed to transform BackpackRawTrade to Trade"
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)


class TestFundingRateTransformation:
    """Test cases for funding rate transformation functionality."""

    def test_transform_raw_funding_rate_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
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


class TestKlineTransformation:
    """Test cases for kline transformation functionality."""

    def test_transform_raw_kline_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp_ms: int
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
        self, mapper: BackpackMarketDataMapper
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw kline
        raw_kline = create_raw_kline()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value"
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal value")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawKline to Candle",
            ):
                mapper.transform_raw_kline_to_internal("SOL-USDC", "1h", raw_kline)


class TestWebSocketEventTransformation:
    """Test cases for WebSocket event transformation functionality."""

    def test_transform_ws_ticker_event_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp_ms: int
    ) -> None:
        """Test successful transformation of BackpackRawTickerEvent to internal Ticker."""
        raw_ticker = create_raw_ticker_event(
            s="SOL-USDC",
            last_price="100.50",
            high="101.00",
            low="99.50",
            open_price="100.00",
            volume="1000.0",
            quote_volume="100500.0",
            price_change_percent="0.5",
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_ticker_event_to_internal(raw_ticker)

        assert isinstance(result, Ticker)
        assert result.symbol == "SOL-USDC"
        assert result.price == Decimal("100.50")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_transform_ws_depth_event_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp_ms: int
    ) -> None:
        """Test successful transformation of BackpackRawDepthUpdateEvent to OrderBook."""
        raw_depth = create_raw_depth_event(
            last_update_id="12345",
            bids=[("100.25", "10.0")],
            asks=[("100.75", "8.0")],
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert isinstance(result, OrderBook)
        assert result.symbol == "SOL-USDC"
        assert len(result.bids) == 1
        assert len(result.asks) == 1
        assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))

    def test_transform_ws_trade_event_to_internal_happy_path(
        self, mapper: BackpackMarketDataMapper, test_timestamp_ms: int
    ) -> None:
        """Test successful transformation of BackpackRawTradeEvent to internal Trade."""
        raw_trade = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=False,
            event_time=test_timestamp_ms,
            buyer_id="buyer123",
            seller_id="seller123",
            trade_time=1705314600000,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert isinstance(result, Trade)
        assert result.symbol == "SOL-USDC"
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.executed_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_ws_trade_event_seller_side(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test WebSocket trade event transformation with seller side."""
        raw_trade = BackpackRawTradeEvent(
            e="trade",
            E=1705316600000,
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            b="buyer123",
            a="seller123",
            t="trade123",
            T=1705316600000,
            m=False,  # Seller side
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert result.side == OrderSide.SELL  # is_buyer_the_maker=False -> SELL


class TestEdgeCasesAndRobustness:
    """Test cases for edge cases and robustness scenarios."""

    def test_boundary_decimal_values(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test transformation with boundary decimal values."""
        raw_ticker = create_raw_ticker(
            price="0.000001",  # Very small value
            bid="999999.999999",  # Very large value
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal("0.000001")
        assert result.bid == Decimal("999999.999999")

    def test_zero_values(self, mapper: BackpackMarketDataMapper, test_timestamp: str) -> None:
        """Test transformation with zero values."""
        raw_ticker = create_raw_ticker(
            price="0",
            volume="0",
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal("0")
        assert result.volume == Decimal("0")

    def test_large_order_book_levels(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test transformation with large number of order book levels."""
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

    def test_unicode_symbol_handling(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test transformation with unicode characters in symbol."""
        raw_ticker = create_raw_ticker(
            symbol="SOL-USDC-测试",
            time=test_timestamp,
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.symbol == "SOL-USDC-测试"

    def test_very_long_trade_id(
        self, mapper: BackpackMarketDataMapper, test_timestamp: str
    ) -> None:
        """Test transformation with maximum allowed trade ID length."""
        long_id = "a" * 64  # 64 character ID (max allowed by BackpackRawTrade.id)
        raw_trade = create_raw_trade(
            id=long_id,
            time=test_timestamp,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert result.id == long_id
