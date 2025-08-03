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
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.market_data.bp_candle_mapper import BackpackCandleMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_funding_rate_mapper import (
    BackpackFundingRateMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_market_mapper import BackpackMarketMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackTradeMapper
from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawMarketResponse,
    BackpackRawOrderBook,
    BackpackRawOrderBookFilters,
    BackpackRawPriceFilter,
    BackpackRawQuantityFilter,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.exceptions.data_transformation import (
    CandleTransformationError,
    FundingRateTransformationError,
    MarketTransformationError,
    OrderBookTransformationError,
    TickerTransformationError,
    TradeTransformationError,
)
from cyberdelta.core.symbols import exchanges
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.field_validation import DecimalFieldError
from cyberdelta.models import OrderBook, Ticker, Trade
from cyberdelta.models.market import Candle, Market
from cyberdelta.models.market.funding_rate import FundingRate
from cyberdelta.models.market.market import BackpackMarketDetails
from tests.common_symbols import BTC_USDC_BP, SOL_USDC_BP, SOL_USDC_PERP_BP
from tests.fixtures.time_fixtures import FreezerProtocol


class CompositeMarketDataMapper:
    """Composite mapper that provides all market data mapping functionality for testing."""

    def __init__(self) -> None:
        """Initialize the composite mapper with all sub-mappers."""
        self.ticker_mapper = BackpackTickerMapper()
        self.market_mapper = BackpackMarketMapper()
        self.order_book_mapper = BackpackOrderBookMapper()
        self.trade_mapper = BackpackTradeMapper()
        self.funding_rate_mapper = BackpackFundingRateMapper()
        self.candle_mapper = BackpackCandleMapper()

    # Delegate methods
    def transform_raw_ticker_to_internal(
        self, raw_ticker: BackpackRawTickerResponse, symbol_override: str | None = None
    ) -> Ticker:
        """Transform raw ticker to internal format.

        Args:
            raw_ticker: Raw ticker response from Backpack API.
            symbol_override: Optional symbol override for the ticker.

        Returns:
            Ticker: Transformed ticker with market data.
        """
        return self.ticker_mapper.transform_raw_ticker_to_internal(raw_ticker, symbol_override)

    def transform_raw_market_to_internal(self, raw_market: BackpackRawMarketResponse) -> Market:
        """Transform raw market to internal format.

        Args:
            raw_market: Raw market response from Backpack API.

        Returns:
            Market: Transformed market information.
        """
        return self.market_mapper.transform_raw_market_to_internal(raw_market)

    def transform_raw_order_book_to_internal(
        self, symbol: Symbol, raw_order_book: BackpackRawOrderBook
    ) -> OrderBook:
        """Transform raw order book to internal format.

        Args:
            symbol: Trading symbol object for the order book.
            raw_order_book: Raw order book from Backpack API.

        Returns:
            OrderBook: Transformed order book with bid/ask levels.
        """
        return self.order_book_mapper.transform_raw_order_book_to_internal(symbol, raw_order_book)

    def transform_raw_trade_to_internal(self, raw_trade: BackpackRawPublicTrade) -> Trade:
        """Transform raw trade to internal format.

        Args:
            raw_trade: Raw trade data from Backpack API.

        Returns:
            Trade: Transformed trade with execution details.
        """
        return self.trade_mapper.transform_raw_trade_to_internal(raw_trade)

    def transform_raw_funding_rate_to_internal(
        self, raw_funding_rate: BackpackRawFundingRateResponse
    ) -> FundingRate:
        """Transform raw funding rate to internal format.

        Args:
            raw_funding_rate: Raw funding rate response from Backpack API.

        Returns:
            FundingRate: Transformed funding rate information.
        """
        return self.funding_rate_mapper.transform_raw_funding_rate_to_internal(raw_funding_rate)

    def transform_raw_funding_interval_rate_to_internal(
        self, raw_funding_rate: BackpackRawFundingIntervalRate, symbol: Symbol
    ) -> FundingRate:
        """Transform raw funding interval rate to internal format.

        Args:
            raw_funding_rate: Raw funding interval rate from Backpack API.
            symbol: Trading symbol object for the funding rate.

        Returns:
            FundingRate: Transformed funding rate for the interval.
        """
        return self.funding_rate_mapper.transform_raw_funding_interval_rate_to_internal(
            raw_funding_rate, symbol
        )

    def transform_raw_kline_to_internal(
        self, symbol: Symbol, interval: str, raw_kline: BackpackRawKlineResponse
    ) -> Candle:
        """Transform raw kline to internal format.

        Args:
            symbol: Trading symbol object for the kline.
            interval: Time interval for the kline.
            raw_kline: Raw kline data from Backpack API.

        Returns:
            Candle: Transformed candlestick data.
        """
        return self.candle_mapper.transform_raw_kline_to_internal(symbol, interval, raw_kline)


@pytest.fixture
def mapper() -> CompositeMarketDataMapper:
    """Fixture providing a composite market data mapper instance for testing.

    Returns:
        CompositeMarketDataMapper: Mapper instance for testing.
    """
    return CompositeMarketDataMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string.

    Returns:
        str: ISO format timestamp for testing.
    """
    return "2024-01-15T10:30:00Z"


@pytest.fixture
def test_timestamp_ms() -> int:
    """Fixture providing a consistent test timestamp in milliseconds.

    Returns:
        int: Timestamp in milliseconds for testing.
    """
    return 1705314600000  # 2024-01-15T10:30:00Z


def create_raw_ticker(
    symbol: str = "SOL-USDC",
    first_price: str = "100.00",
    last_price: str = "100.50",
    high: str = "101.00",
    low: str = "99.50",
    price_change: str = "0.50",
    price_change_percent: str = "0.50",
    volume: str = "1000.0",
    quote_volume: str = "100500.0",
    trades: str = "500",
) -> BackpackRawTickerResponse:
    """Create BackpackRawTickerResponse instances for testing ticker transformations.

    Returns:
        BackpackRawTickerResponse: Raw ticker data for testing.
    """
    return BackpackRawTickerResponse(
        symbol=symbol,
        firstPrice=first_price,
        lastPrice=last_price,
        high=high,
        low=low,
        priceChange=price_change,
        priceChangePercent=price_change_percent,
        volume=volume,
        quoteVolume=quote_volume,
        trades=trades,
    )


def create_raw_order_book(
    bids: list[tuple[str, str]] | None = None,
    asks: list[tuple[str, str]] | None = None,
    timestamp: str = "2024-01-15T10:30:00Z",
) -> BackpackRawOrderBook:
    """Create BackpackRawOrderBook instances for testing order book transformations.

    Returns:
        BackpackRawOrderBook: Raw order book data for testing.
    """
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
    trade_id: str = "trade123",
    symbol: str = "SOL-USDC",
    price: str = "100.50",
    qty: str = "10.0",
    time: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
) -> BackpackRawPublicTrade:
    """Create BackpackRawPublicTrade instances for testing trade transformations.

    Returns:
        BackpackRawPublicTrade: Raw public trade data for testing.
    """
    return BackpackRawPublicTrade(
        id=trade_id,
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
) -> BackpackRawFundingRateResponse:
    """Create BackpackRawFundingRateResponse instances for testing funding rate transformations.

    Returns:
        BackpackRawFundingRateResponse: Raw funding rate data for testing.
    """
    return BackpackRawFundingRateResponse(
        symbol=symbol,
        rate=rate,
        markPrice=mark_price,
        indexPrice=index_price,
        time=time,
    )


def create_raw_funding_interval_rate(
    symbol: str = "SOL-USDC",
    funding_rate: str = "0.0001",
    interval_end_timestamp: str = "2024-01-15T10:30:00",
) -> BackpackRawFundingIntervalRate:
    """Create BackpackRawFundingIntervalRate instances for testing interval rate transformations.

    Returns:
        BackpackRawFundingIntervalRate: Raw funding interval rate data for testing.
    """
    return BackpackRawFundingIntervalRate(
        symbol=symbol,
        fundingRate=funding_rate,
        intervalEndTimestamp=interval_end_timestamp,
    )


def create_raw_kline(
    symbol: str = "SOL-USDC",
    start_time_ms: int = 1705314600000,
    open_price: str = "100.00",
    high_price: str = "101.00",
    low_price: str = "99.50",
    close_price: str = "100.50",
    volume: str = "1000.0",
) -> BackpackRawKlineResponse:
    """Create BackpackRawKlineResponse instances for testing kline transformations.

    Returns:
        BackpackRawKlineResponse: Raw kline data for testing.
    """
    # BackpackRawKlineResponse expects a list/tuple of 12 elements in this order:
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
    return BackpackRawKlineResponse.model_validate(kline_data)


def create_raw_market(
    symbol: str = SOL_USDC_BP.value,
    base_symbol: str = "SOL",
    quote_symbol: str = "USDC",
    market_type: str = "Spot",
    tick_size: str = "0.01",
    step_size: str = "0.01",
    min_price: str = "0.001",
    max_price: str = "1000000.0",
    min_quantity: str = "0.0001",
    max_quantity: str = "1000000.0",
    order_book_state: str = "NORMAL",
    created_at: str = "2024-01-01T00:00:00.000Z",
) -> BackpackRawMarketResponse:
    """Create BackpackRawMarketResponse instances for testing market transformations.

    Returns:
        BackpackRawMarketResponse: Raw market data for testing.
    """
    return BackpackRawMarketResponse(
        symbol=symbol,
        baseSymbol=base_symbol,
        quoteSymbol=quote_symbol,
        marketType=market_type,
        filters=BackpackRawOrderBookFilters(
            price=BackpackRawPriceFilter(
                minPrice=min_price,
                maxPrice=max_price,
                tickSize=tick_size,
            ),
            quantity=BackpackRawQuantityFilter(
                minQuantity=min_quantity,
                maxQuantity=max_quantity,
                stepSize=step_size,
            ),
        ),
        orderBookState=order_book_state,
        createdAt=created_at,
    )


class TestMarketTransformation:
    """Test cases for market transformation functionality."""

    def test_transform_raw_market_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test successful transformation of BackpackRawMarketResponse to internal Market."""
        raw_market = create_raw_market(
            symbol=SOL_USDC_BP.value,
            base_symbol="SOL",
            quote_symbol="USDC",
            market_type="Spot",
            tick_size="0.01",
            step_size="0.01",
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        assert isinstance(result, Market)
        assert result.symbol == SOL_USDC_BP
        assert result.market_type == "Spot"
        assert result.tick_size == Decimal("0.01")
        assert result.step_size == Decimal("0.01")
        assert result.status == "NORMAL"  # maps from order_book_state
        assert result.bp_details is not None

    def test_transform_raw_market_with_all_optional_fields(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test market transformation with all optional fields populated."""
        raw_market = create_raw_market(
            min_price="0.001",
            max_price="10000.0",
            min_quantity="0.1",
            max_quantity="1000.0",
            created_at="2024-01-15T10:30:00.000Z",
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        assert result.min_price == Decimal("0.001")
        assert result.max_price == Decimal("10000.0")
        assert result.min_quantity == Decimal("0.1")
        assert result.max_quantity == Decimal("1000.0")
        assert result.created_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_transform_raw_market_with_extreme_values(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test market transformation with extreme decimal values."""
        raw_market = create_raw_market(
            tick_size="0.000001",  # Very small tick
            step_size="0.000000001",  # Very small step
            min_price="0.000000001",  # Very small min price
            max_price="999999999.999999",  # Very large max price
            min_quantity="0.000000001",  # Very small min quantity
            max_quantity="999999999999.999999",  # Very large max quantity
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        assert result.tick_size == Decimal("0.000001")
        assert result.step_size == Decimal("0.000000001")
        assert result.min_price == Decimal("0.000000001")
        assert result.max_price == Decimal("999999999.999999")
        assert result.min_quantity == Decimal("0.000000001")
        assert result.max_quantity == Decimal("999999999999.999999")

    def test_transform_raw_market_with_none_optional_fields(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test market transformation with None optional fields."""
        # Create market with None optional fields (only maxPrice and maxQuantity can be None)
        raw_market = BackpackRawMarketResponse(
            symbol=SOL_USDC_BP.value,
            baseSymbol="SOL",
            quoteSymbol="USDC",
            marketType="Spot",
            filters=BackpackRawOrderBookFilters(
                price=BackpackRawPriceFilter(
                    minPrice="0.001",  # Required field
                    maxPrice=None,  # Optional field - can be None
                    tickSize="0.01",  # Required field
                ),
                quantity=BackpackRawQuantityFilter(
                    minQuantity="0.1",  # Required field
                    maxQuantity=None,  # Optional field - can be None
                    stepSize="0.01",  # Required field
                ),
            ),
            orderBookState="NORMAL",
            createdAt="2024-01-01T00:00:00.000Z",  # Required field
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        # Only maxPrice and maxQuantity should be None
        assert result.min_price == Decimal("0.001")
        assert result.max_price is None
        assert result.min_quantity == Decimal("0.1")
        assert result.max_quantity is None
        assert result.created_at is not None  # created_at is required

    def test_transform_raw_market_backpack_details(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that Backpack-specific details are properly created."""
        raw_market = create_raw_market(
            order_book_state="HALTED",
            created_at="2024-01-15T10:30:00.000Z",
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        assert isinstance(result.bp_details, BackpackMarketDetails)
        assert result.bp_details.order_book_state == "HALTED"
        assert result.bp_details.created_at_raw == "2024-01-15T10:30:00.000Z"
        # hl_details should be None for Backpack markets
        assert result.hl_details is None

    def test_transform_raw_market_perp_type(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test market transformation with perpetual contract type."""
        raw_market = create_raw_market(
            symbol=SOL_USDC_PERP_BP.value,
            market_type="Perpetual",
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        assert result.symbol == SOL_USDC_PERP_BP
        assert result.market_type == "Perpetual"

    def test_transform_raw_market_transformation_error(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        raw_market = create_raw_market()

        # Mock parse_decimal_value to raise an error for tick_size
        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_market_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="tickSize",
                value="0.01",
                reason="Invalid decimal value",
            )

            with pytest.raises(
                MarketTransformationError,
                match="Failed to transform RawMarket to Market",
            ):
                mapper.transform_raw_market_to_internal(raw_market)

    def test_transform_raw_market_invalid_timestamp(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test market transformation with invalid timestamp."""
        raw_market = create_raw_market(created_at="invalid-timestamp")

        # Mock parse_datetime_utc to return None for invalid timestamp
        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_market_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_market_to_internal(raw_market)

            # Should handle gracefully with None timestamp
            assert result.created_at is None

    def test_transform_raw_market_symbol_consistency(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that symbol parsing is consistent with base/quote symbols."""
        raw_market = create_raw_market(
            symbol=BTC_USDC_BP.value,
            base_symbol="BTC",
            quote_symbol="USDC",
        )

        result = mapper.transform_raw_market_to_internal(raw_market)

        # Symbol should match the specific symbol
        assert result.symbol == BTC_USDC_BP


class TestTickerTransformation:
    """Test cases for ticker transformation functionality."""

    def test_transform_raw_ticker_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawTickerResponse to internal Ticker."""
        raw_ticker = create_raw_ticker(
            symbol="SOL-USDC",
            last_price="100.50",
            volume="1000.0",
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert isinstance(result, Ticker)
        assert result.symbol == exchanges.backpack("SOL_USDC")
        assert result.price == Decimal("100.50")
        assert result.bid is None  # Not available from Backpack ticker endpoint
        assert result.ask is None  # Not available from Backpack ticker endpoint
        assert result.volume == Decimal("1000.0")
        # Timestamp is generated since API doesn't provide it
        assert result.timestamp is not None

    def test_transform_raw_ticker_with_symbol_override(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with symbol override."""
        raw_ticker = create_raw_ticker(symbol="SOL-USDC")

        result = mapper.transform_raw_ticker_to_internal(raw_ticker, symbol_override="BTC_USDC")

        assert result.symbol == exchanges.backpack("BTC_USDC")

    def test_transform_raw_ticker_with_none_values(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with zero values for optional fields."""
        # Create ticker with zero values by constructing directly
        raw_ticker = BackpackRawTickerResponse(
            symbol="SOL-USDC",
            firstPrice="0.0",
            lastPrice="0.0",
            high="0.0",
            low="0.0",
            priceChange="0.0",
            priceChangePercent="0.0",
            volume="0.0",
            quoteVolume="0.0",
            trades="0",
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        # Zero values should be converted to Decimal("0.0"), not None
        assert result.price == Decimal("0.0")
        assert result.bid is None  # Not available from Backpack ticker endpoint
        assert result.ask is None  # Not available from Backpack ticker endpoint
        assert result.volume == Decimal("0.0")

    def test_transform_raw_ticker_with_none_timestamp(
        self,
        mapper: CompositeMarketDataMapper,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test ticker transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(mock_now)

        # Create a valid raw ticker first
        raw_ticker = BackpackRawTickerResponse(
            symbol="SOL-USDC",
            firstPrice="99.50",
            lastPrice="100.50",
            high="101.00",
            low="99.00",
            priceChange="1.00",
            priceChangePercent="1.01",
            volume="1000.0",
            quoteVolume="100500.0",
            trades="50",
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert isinstance(result, Ticker)
        assert result.timestamp == mock_now

    def test_transform_raw_ticker_transformation_error(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid ticker but patch parsing to cause error
        raw_ticker = create_raw_ticker(last_price="100.50")

        # This test relies on the actual transformation logic to cause an error
        # We'll use an invalid decimal that passes basic validation but fails transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="lastPrice",
                value="100.50",
                reason="Invalid decimal",
            )

            with pytest.raises(
                TickerTransformationError,
                match="Failed to transform BackpackRawTickerResponse to Ticker",
            ):
                mapper.transform_raw_ticker_to_internal(raw_ticker)

    def test_transform_raw_ticker_with_extreme_values(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with extreme decimal values."""
        raw_ticker = create_raw_ticker(
            last_price="0.000001",  # Very small price
            volume="0.000000001",  # Very small volume
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal("0.000001")
        assert result.volume == Decimal("0.000000001")

    def test_transform_raw_ticker_with_zero_values(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test ticker transformation with zero values."""
        raw_ticker = create_raw_ticker(
            last_price="0",
            volume="0",
        )

        result = mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal(0)
        assert result.volume == Decimal(0)


class TestOrderBookTransformation:
    """Test cases for order book transformation functionality."""

    def test_transform_raw_order_book_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawOrderBook to internal OrderBook."""
        raw_book = create_raw_order_book(
            bids=[("100.25", "10.0"), ("100.00", "5.0")],
            asks=[("100.75", "8.0"), ("101.00", "12.0")],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        assert isinstance(result, OrderBook)
        assert result.symbol == SOL_USDC_BP
        assert len(result.bids) == 2
        assert len(result.asks) == 2
        assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

    def test_transform_raw_order_book_empty_levels(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with empty bid/ask levels."""
        raw_book = create_raw_order_book(
            bids=[],
            asks=[],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        assert len(result.bids) == 0
        assert len(result.asks) == 0

    def test_transform_raw_order_book_large_number_of_levels(
        self,
        mapper: CompositeMarketDataMapper,
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

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        assert len(result.bids) == 100
        assert len(result.asks) == 100
        assert result.bids[0][0] == Decimal("100.00")
        assert result.asks[0][0] == Decimal("101.00")

    def test_transform_raw_order_book_with_none_timestamp(
        self,
        mapper: CompositeMarketDataMapper,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test order book transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(mock_now)

        # Create a valid raw order book and mock the timestamp parsing to return None
        raw_book = create_raw_order_book(
            bids=[("100.25", "10.0"), ("100.00", "5.0")],
            asks=[("100.75", "8.0"), ("101.00", "12.0")],
            timestamp="2024-01-15T10:30:00Z",
        )

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.timestamp == mock_now

    def test_transform_raw_order_book_transformation_error(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid order book and mock parsing to cause error
        raw_book = create_raw_order_book(bids=[("100.25", "10.0")])

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="price",
                value="100.25",
                reason="Invalid decimal",
            )

            with pytest.raises(
                OrderBookTransformationError,
                match="Failed to transform BackpackRawOrderBook to OrderBook",
            ):
                mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

    def test_transform_raw_order_book_with_extreme_values(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with extreme price and quantity values."""
        raw_book = create_raw_order_book(
            bids=[("0.000001", "999999999.999999"), ("99999.999999", "0.000001")],
            asks=[("1000000.000001", "0.000000001"), ("1000001.000001", "1000000000.0")],
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        # Bids should be sorted by price descending (highest price first)
        assert result.bids[0] == (Decimal("99999.999999"), Decimal("0.000001"))
        assert result.bids[1] == (Decimal("0.000001"), Decimal("999999999.999999"))
        # Asks should be sorted by price ascending (lowest price first)
        assert result.asks[0] == (Decimal("1000000.000001"), Decimal("0.000000001"))
        assert result.asks[1] == (Decimal("1000001.000001"), Decimal("1000000000.0"))

    def test_transform_raw_order_book_with_unicode_symbol(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test order book transformation with unicode symbol."""
        raw_book = create_raw_order_book(timestamp=test_timestamp)

        test_symbol = exchanges.backpack("SOL_USDC_测试")
        result = mapper.transform_raw_order_book_to_internal(test_symbol, raw_book)

        assert result.symbol == test_symbol


class TestTradeTransformation:
    """Test cases for trade transformation functionality."""

    def test_transform_raw_trade_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawPublicTrade to internal Trade."""
        raw_trade = create_raw_trade(
            trade_id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time=test_timestamp,
            order_id="order123",
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert isinstance(result, Trade)
        assert result.id == "trade123"
        assert result.symbol == exchanges.backpack("SOL_USDC")
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.order_id == "order123"
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.executed_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_trade_missing_price(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test that missing price raises TransformationError."""
        # Cannot create BackpackRawPublicTrade with None price, so patch parsing to return None
        raw_trade = create_raw_trade(price="100.50", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.return_value = None

            with pytest.raises(
                TradeTransformationError,
                match="Failed to transform BackpackRawPublicTrade to Trade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_missing_quantity(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test that missing quantity raises TransformationError."""
        # Cannot create BackpackRawPublicTrade with None qty, so patch parsing to return None
        raw_trade = create_raw_trade(qty="10.0", time=test_timestamp)

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper.parse_decimal_value",
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

            with pytest.raises(
                TradeTransformationError,
                match="Failed to transform BackpackRawPublicTrade to Trade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_with_none_timestamp(
        self,
        mapper: CompositeMarketDataMapper,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test trade transformation with None timestamp uses current time."""
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(mock_now)

        # Create a valid raw trade and mock the timestamp parsing to return None
        raw_trade = create_raw_trade(
            trade_id="trade123",
            symbol="SOL-USDC",
            price="100.50",
            qty="10.0",
            time="2024-01-15T10:30:00Z",
            order_id="order123",
        )

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_trade_to_internal(raw_trade)

            # The mapper should handle invalid timestamp gracefully by using current time
            assert result.executed_at == mock_now

    def test_transform_raw_trade_transformation_error(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        raw_trade = create_raw_trade(price="100.50")

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="price",
                value="100.25",
                reason="Invalid decimal",
            )

            with pytest.raises(
                TradeTransformationError,
                match="Failed to transform BackpackRawPublicTrade to Trade",
            ):
                mapper.transform_raw_trade_to_internal(raw_trade)

    def test_transform_raw_trade_with_extreme_values(
        self,
        mapper: CompositeMarketDataMapper,
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
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test trade transformation with maximum allowed trade ID length."""
        long_id = "a" * 64  # 64 character ID (max allowed by BackpackRawPublicTrade.id)
        raw_trade = create_raw_trade(
            trade_id=long_id,
            time=test_timestamp,
        )

        result = mapper.transform_raw_trade_to_internal(raw_trade)

        assert result.id == long_id


class TestFundingRateTransformation:
    """Test cases for funding rate transformation functionality."""

    def test_transform_raw_funding_rate_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFundingRateResponse to FundingRate."""
        raw_funding = create_raw_funding_rate(
            symbol="SOL-USDC",
            rate="0.0001",
            mark_price="100.50",
            index_price="100.25",
            time=1705314600000,
        )

        result = mapper.transform_raw_funding_rate_to_internal(raw_funding)

        assert isinstance(result, FundingRate)
        assert result.symbol == exchanges.backpack("SOL_USDC")
        assert result.funding_rate == Decimal("0.0001")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_funding_interval_rate_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test successful transformation of BackpackRawFundingIntervalRate to FundingRate."""
        raw_funding = create_raw_funding_interval_rate(
            symbol="SOL-USDC",
            funding_rate="0.0001",
            interval_end_timestamp="2024-01-15T10:30:00",
        )

        result = mapper.transform_raw_funding_interval_rate_to_internal(raw_funding, SOL_USDC_BP)

        assert isinstance(result, FundingRate)
        assert result.symbol == SOL_USDC_BP
        assert result.funding_rate == Decimal("0.0001")
        assert result.timestamp == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_raw_funding_rate_with_extreme_rates(
        self,
        mapper: CompositeMarketDataMapper,
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
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that funding rate transformation errors are properly wrapped."""
        raw_funding = create_raw_funding_rate()

        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_funding_rate_mapper"
            ".parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="price",
                value="100.25",
                reason="Invalid decimal",
            )

            with pytest.raises(
                FundingRateTransformationError,
                match="Failed to transform BackpackRawFundingRateResponse to FundingRate",
            ):
                mapper.transform_raw_funding_rate_to_internal(raw_funding)


class TestKlineTransformation:
    """Test cases for kline transformation functionality."""

    def test_transform_raw_kline_to_internal_happy_path(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test successful transformation of BackpackRawKlineResponse to internal Candle."""
        raw_kline = create_raw_kline(
            symbol="SOL-USDC",
            start_time_ms=test_timestamp_ms,
            open_price="100.00",
            high_price="101.00",
            low_price="99.50",
            close_price="100.50",
            volume="1000.0",
        )

        result = mapper.transform_raw_kline_to_internal(SOL_USDC_BP, "1h", raw_kline)

        assert isinstance(result, Candle)
        assert result.symbol == SOL_USDC_BP
        assert result.interval == "1h"
        assert result.open == Decimal("100.00")
        assert result.high == Decimal("101.00")
        assert result.low == Decimal("99.50")
        assert result.close == Decimal("100.50")
        assert result.volume == Decimal("1000.0")

    def test_transform_raw_kline_to_internal_transformation_error(
        self,
        mapper: CompositeMarketDataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a valid raw kline
        raw_kline = create_raw_kline()

        # Mock parse_decimal_value to raise an error during transformation
        with patch(
            "cyberdelta.apis.backpack.mappers.market_data.bp_candle_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = DecimalFieldError(
                field_name="open_price",
                value="100.00",
                reason="Invalid decimal value",
            )

            with pytest.raises(
                CandleTransformationError,
                match="Failed to transform BackpackRawKlineResponse to Candle",
            ):
                mapper.transform_raw_kline_to_internal(SOL_USDC_BP, "1h", raw_kline)

    def test_transform_raw_kline_with_extreme_values(
        self,
        mapper: CompositeMarketDataMapper,
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

        result = mapper.transform_raw_kline_to_internal(SOL_USDC_BP, "1h", raw_kline)

        assert result.open == Decimal("0.000001")
        assert result.high == Decimal("999999.999999")
        assert result.low == Decimal("0.000000001")
        assert result.close == Decimal("500000.123456")
        assert result.volume == Decimal("1000000000.999999")

    def test_transform_raw_kline_with_different_intervals(
        self,
        mapper: CompositeMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test kline transformation with different interval values."""
        raw_kline = create_raw_kline(start_time_ms=test_timestamp_ms)

        intervals = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]
        for interval in intervals:
            result = mapper.transform_raw_kline_to_internal(SOL_USDC_BP, interval, raw_kline)
            assert result.interval == interval
