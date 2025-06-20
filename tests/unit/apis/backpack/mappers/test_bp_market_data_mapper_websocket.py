"""CyberDeltaEngine: Backpack Market Data Mapper WebSocket Tests.

------------------------------------------------------------

Comprehensive test suite for BackpackMarketDataMapper WebSocket event transformations.
Tests WebSocket ticker, depth, and trade event transformations including:
- Happy path WebSocket event transformations
- Side mapping logic for trade events
- Error handling and edge cases
- Real-time data scenarios
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawDepthUpdateEvent,
    BackpackRawTickerEvent,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTradeEvent
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import OrderBook, Ticker, Trade
from cyberdelta.core.models.enums import OrderSide
from cyberdelta.enums.exchange_names import ExchangeName


@pytest.fixture
def mapper() -> BackpackMarketDataMapper:
    """Fixture providing a BackpackMarketDataMapper instance."""
    return BackpackMarketDataMapper()


@pytest.fixture
def test_timestamp_ms() -> int:
    """Fixture providing a consistent test timestamp in milliseconds."""
    return 1705314600000  # 2024-01-15T10:30:00Z


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
    """Create BackpackRawTickerEvent instances for WebSocket testing."""
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
    """Create BackpackRawDepthUpdateEvent instances for WebSocket testing."""
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
) -> BackpackRawPublicTradeEvent:
    """Create BackpackRawPublicTradeEvent instances for WebSocket testing."""
    return BackpackRawPublicTradeEvent(
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
    """Test cases for side mapping functionality through WebSocket trade events."""

    def test_side_mapping_buyer_is_maker(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test side mapping when buyer is the maker (BUY side)."""
        raw_trade_event = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=True,  # Buyer is maker -> BUY side
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade_event)
        assert result.side == OrderSide.BUY

    def test_side_mapping_buyer_is_not_maker(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test side mapping when buyer is not the maker (SELL side)."""
        raw_trade_event = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade456",
            m=False,  # Buyer is not maker -> SELL side
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade_event)
        assert result.side == OrderSide.SELL

    def test_side_mapping_consistency_across_multiple_trades(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that side mapping is consistent across multiple trade events."""
        # Create multiple trades with different maker status
        trades_data = [
            (True, OrderSide.BUY),
            (False, OrderSide.SELL),
            (True, OrderSide.BUY),
            (False, OrderSide.SELL),
        ]

        for is_maker, expected_side in trades_data:
            raw_trade_event = create_raw_trade_event(
                t=f"trade_{is_maker}",  # Unique trade ID
                m=is_maker,
                event_time=test_timestamp_ms,
            )

            result = mapper.transform_ws_trade_event_to_internal(raw_trade_event)
            assert result.side == expected_side

    def test_side_mapping_with_different_symbols(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that side mapping works correctly with different trading symbols."""
        symbols = ["SOL-USDC", "BTC-USDC", "ETH-USDC", "DOGE-USDC"]

        for symbol in symbols:
            # Test both sides for each symbol
            for is_maker, expected_side in [(True, OrderSide.BUY), (False, OrderSide.SELL)]:
                raw_trade_event = create_raw_trade_event(
                    s=symbol,
                    t=f"trade_{symbol}_{is_maker}",
                    m=is_maker,
                    event_time=test_timestamp_ms,
                )

                result = mapper.transform_ws_trade_event_to_internal(raw_trade_event)
                assert result.side == expected_side
                assert result.symbol == symbol


class TestWebSocketTickerEventTransformation:
    """Test cases for WebSocket ticker event transformation functionality."""

    def test_transform_ws_ticker_event_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
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

    def test_transform_ws_ticker_event_with_different_symbols(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket ticker event transformation with various symbols."""
        symbols = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC"]

        for symbol in symbols:
            raw_ticker = create_raw_ticker_event(
                s=symbol,
                event_time=test_timestamp_ms,
            )

            result = mapper.transform_ws_ticker_event_to_internal(raw_ticker)
            assert result.symbol == symbol

    def test_transform_ws_ticker_event_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket ticker event transformation with extreme price values."""
        raw_ticker = create_raw_ticker_event(
            last_price="0.000001",  # Very small price
            high="999999.999999",  # Very high price
            low="0.000000001",  # Very small low
            volume="1000000000.0",  # Very large volume
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_ticker_event_to_internal(raw_ticker)

        assert result.price == Decimal("0.000001")
        # Note: We're only testing price here as the mapper may not expose all fields

    def test_transform_ws_ticker_event_with_zero_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket ticker event transformation with zero values."""
        raw_ticker = create_raw_ticker_event(
            last_price="0.0",
            high="0.0",
            low="0.0",
            volume="0.0",
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_ticker_event_to_internal(raw_ticker)

        assert result.price == Decimal("0.0")

    def test_transform_ws_ticker_event_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that WebSocket ticker event transformation errors are properly wrapped."""
        raw_ticker = create_raw_ticker_event(event_time=test_timestamp_ms)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawTickerEvent",
            ):
                mapper.transform_ws_ticker_event_to_internal(raw_ticker)

    def test_transform_ws_ticker_event_with_unicode_symbol(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket ticker event transformation with unicode symbol."""
        raw_ticker = create_raw_ticker_event(
            s="SOL-USDC-测试",
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_ticker_event_to_internal(raw_ticker)

        assert result.symbol == "SOL-USDC-测试"


class TestWebSocketDepthEventTransformation:
    """Test cases for WebSocket depth event transformation functionality."""

    def test_transform_ws_depth_event_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test successful transformation of BackpackRawDepthUpdateEvent to OrderBook."""
        raw_depth = create_raw_depth_event(
            last_update_id="12345",
            bids=[("100.25", "10.0"), ("100.00", "5.0")],
            asks=[("100.75", "8.0"), ("101.00", "12.0")],
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert isinstance(result, OrderBook)
        assert result.symbol == "SOL-USDC"
        assert len(result.bids) == 2
        assert len(result.asks) == 2
        assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))

    def test_transform_ws_depth_event_with_empty_levels(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket depth event transformation with empty bid/ask levels."""
        raw_depth = create_raw_depth_event(
            bids=[],
            asks=[],
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert len(result.bids) == 0
        assert len(result.asks) == 0

    def test_transform_ws_depth_event_with_single_level(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket depth event transformation with single bid/ask level."""
        raw_depth = create_raw_depth_event(
            bids=[("100.25", "10.0")],
            asks=[("100.75", "8.0")],
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert len(result.bids) == 1
        assert len(result.asks) == 1
        assert result.bids[0] == (Decimal("100.25"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("100.75"), Decimal("8.0"))

    def test_transform_ws_depth_event_with_multiple_levels(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket depth event transformation with multiple price levels."""
        # Create 10 bid and ask levels
        bids = [(f"{100 - i * 0.01:.2f}", f"{(i + 1) * 10}.0") for i in range(10)]
        asks = [(f"{101 + i * 0.01:.2f}", f"{(i + 1) * 8}.0") for i in range(10)]

        raw_depth = create_raw_depth_event(
            bids=bids,
            asks=asks,
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert len(result.bids) == 10
        assert len(result.asks) == 10
        assert result.bids[0] == (Decimal("100.00"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("101.00"), Decimal("8.0"))

    def test_transform_ws_depth_event_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket depth event transformation with extreme price/quantity values."""
        raw_depth = create_raw_depth_event(
            bids=[("0.000001", "999999999.999999")],
            asks=[("1000000.000001", "0.000000001")],
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

        assert result.bids[0] == (Decimal("0.000001"), Decimal("999999999.999999"))
        assert result.asks[0] == (Decimal("1000000.000001"), Decimal("0.000000001"))

    def test_transform_ws_depth_event_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that WebSocket depth event transformation errors are properly wrapped."""
        raw_depth = create_raw_depth_event(event_time=test_timestamp_ms)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawDepthUpdateEvent",
            ):
                mapper.transform_ws_depth_event_to_internal("SOL-USDC", raw_depth)

    def test_transform_ws_depth_event_with_different_symbols(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket depth event transformation with different symbols."""
        symbols = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC"]
        raw_depth = create_raw_depth_event(event_time=test_timestamp_ms)

        for symbol in symbols:
            result = mapper.transform_ws_depth_event_to_internal(symbol, raw_depth)
            assert result.symbol == symbol


class TestWebSocketTradeEventTransformation:
    """Test cases for WebSocket trade event transformation functionality."""

    def test_transform_ws_trade_event_to_internal_happy_path(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test successful transformation of BackpackRawPublicTradeEvent to internal Trade."""
        raw_trade = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=False,
            event_time=test_timestamp_ms,
            buyer_id="buyer123",
            seller_id="seller123",
            trade_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert isinstance(result, Trade)
        assert result.symbol == "SOL-USDC"
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.exchange == ExchangeName.BACKPACK.value
        assert result.executed_at == datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)
        assert result.bp_details is not None

    def test_transform_ws_trade_event_buyer_side(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with buyer side."""
        raw_trade = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=True,  # Buyer is maker -> BUY side
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert result.side == OrderSide.BUY

    def test_transform_ws_trade_event_seller_side(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with seller side."""
        raw_trade = create_raw_trade_event(
            s="SOL-USDC",
            p="100.50",
            q="10.0",
            t="trade123",
            m=False,  # Buyer is not maker -> SELL side
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert result.side == OrderSide.SELL

    def test_transform_ws_trade_event_with_extreme_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with extreme price/quantity values."""
        raw_trade = create_raw_trade_event(
            p="0.000001",  # Very small price
            q="999999999.999999",  # Very large quantity
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert result.price == Decimal("0.000001")
        assert result.quantity == Decimal("999999999.999999")

    def test_transform_ws_trade_event_with_zero_values(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with zero price/quantity raises error."""
        raw_trade = create_raw_trade_event(
            p="0.0",
            q="0.0",
            event_time=test_timestamp_ms,
        )

        # The mapper should raise TransformationError for zero values since
        # Trade model validates price > 0 and quantity > 0
        with pytest.raises(
            TransformationError, match="Failed to transform BackpackRawPublicTradeEvent"
        ):
            mapper.transform_ws_trade_event_to_internal(raw_trade)

    def test_transform_ws_trade_event_with_very_long_id(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with very long trade ID."""
        long_id = "a" * 64  # Maximum allowed length
        raw_trade = create_raw_trade_event(
            t=long_id,
            event_time=test_timestamp_ms,
        )

        result = mapper.transform_ws_trade_event_to_internal(raw_trade)

        assert result.id == long_id

    def test_transform_ws_trade_event_transformation_error(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that WebSocket trade event transformation errors are properly wrapped."""
        raw_trade = create_raw_trade_event(event_time=test_timestamp_ms)

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal")

            with pytest.raises(
                TransformationError,
                match="Failed to transform BackpackRawPublicTradeEvent",
            ):
                mapper.transform_ws_trade_event_to_internal(raw_trade)

    def test_transform_ws_trade_event_with_different_symbols(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test WebSocket trade event transformation with different trading symbols."""
        symbols = ["BTC-USDC", "ETH-USDC", "SOL-USDC", "DOGE-USDC"]

        for symbol in symbols:
            raw_trade = create_raw_trade_event(
                s=symbol,
                t=f"trade_{symbol}",
                event_time=test_timestamp_ms,
            )

            result = mapper.transform_ws_trade_event_to_internal(raw_trade)
            assert result.symbol == symbol

    def test_transform_ws_trade_event_missing_required_fields(
        self,
        mapper: BackpackMarketDataMapper,
        test_timestamp_ms: int,
    ) -> None:
        """Test that WebSocket trade event with missing required fields raises error."""
        raw_trade = create_raw_trade_event(event_time=test_timestamp_ms)

        # Mock parse_decimal_value to return None for price
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.return_value = None

            with pytest.raises(TransformationError):
                mapper.transform_ws_trade_event_to_internal(raw_trade)

    def test_transform_ws_trade_event_real_time_scenarios(
        self,
        mapper: BackpackMarketDataMapper,
    ) -> None:
        """Test WebSocket trade event transformation in real-time scenarios."""
        # Simulate rapid succession of trades with different timestamps
        base_time = 1705314600000
        trade_events: list[BackpackRawPublicTradeEvent] = []

        for i in range(5):
            trade_events.append(
                create_raw_trade_event(
                    t=f"trade_{i}",
                    p=f"{100 + i * 0.01:.2f}",  # Slightly increasing prices
                    q=f"{10 + i}.0",  # Increasing quantities
                    m=(i % 2 == 0),  # Alternate maker status
                    event_time=base_time + i * 1000,  # 1 second apart
                ),
            )

        results: list[Trade] = []
        for trade_event in trade_events:
            result = mapper.transform_ws_trade_event_to_internal(trade_event)
            results.append(result)

        # Verify all trades were processed correctly
        assert len(results) == 5

        for i, result in enumerate(results):
            assert result.id == f"trade_{i}"
            assert result.price == Decimal(f"{100 + i * 0.01:.2f}")
            assert result.quantity == Decimal(f"{10 + i}.0")
            expected_side = OrderSide.BUY if i % 2 == 0 else OrderSide.SELL
            assert result.side == expected_side
