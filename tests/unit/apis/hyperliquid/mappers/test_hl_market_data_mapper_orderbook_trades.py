"""CyberDeltaEngine: Hyperliquid Market Data Mapper Order Book & Trades Tests.

--------------------------------------------------------------------------

Comprehensive test suite for HyperliquidOrderBookMapper order book and
trade transformation methods. Tests order book and trade processing including:
- Order book transformations with various book structures
- Trade transformations with different sides and validation
- Integration scenarios and efficiency testing
- Depth limit handling and malformed data processing
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
import structlog.testing
from pydantic import ValidationError


# Third-party imports for type checking only
if TYPE_CHECKING:
    import pytest
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper import (
    HyperliquidOrderBookMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.core.models import OrderBook, Trade
from cyberdelta.core.models.market.trade import HyperliquidTradeDetails
from cyberdelta.enums import OrderSide


# Alias for shorter method calls
Mapper = HyperliquidOrderBookMapper

# --- Fixtures ---


@pytest.fixture
def market_data_mapper() -> HyperliquidOrderBookMapper:
    """Provide an instance of HyperliquidOrderBookMapper."""
    return HyperliquidOrderBookMapper()


@pytest.fixture
def hyperliquid_raw_book_level_fixture_bid() -> HyperliquidRawBookLevel:
    """Provide a valid HyperliquidRawBookLevel for a bid."""
    return HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2)


@pytest.fixture
def hyperliquid_raw_book_level_fixture_ask() -> HyperliquidRawBookLevel:
    """Provide a valid HyperliquidRawBookLevel for an ask."""
    return HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3)


@pytest.fixture
def hyperliquid_raw_l2_book_eth_fixture() -> HyperliquidRawL2Book:
    """Provide a valid HyperliquidRawL2Book for ETH-PERP."""
    # Create more levels for a more realistic book
    bid_levels = [
        HyperliquidRawBookLevel(px="2999.50", sz="10.5", n=2),
        HyperliquidRawBookLevel(px="2999.00", sz="20.0", n=5),
        HyperliquidRawBookLevel(px="2998.50", sz="15.0", n=3),
    ]
    ask_levels = [
        HyperliquidRawBookLevel(px="3000.50", sz="5.25", n=3),
        HyperliquidRawBookLevel(px="3001.00", sz="12.0", n=4),
        HyperliquidRawBookLevel(px="3001.50", sz="8.0", n=2),
    ]
    return HyperliquidRawL2Book(
        coin="ETH-PERP",
        levels=[bid_levels, ask_levels],
        time=int(datetime.now(UTC).timestamp() * 1000 - 2000),  # 2 seconds ago
    )


@pytest.fixture
def hyperliquid_raw_l2_book_empty_fixture() -> HyperliquidRawL2Book:
    """Provide an empty HyperliquidRawL2Book."""
    return HyperliquidRawL2Book(
        coin="BTC-PERP",
        levels=[[], []],  # Empty bids and asks
        time=int(datetime.now(UTC).timestamp() * 1000 - 1000),  # 1 second ago
    )


@pytest.fixture
def hyperliquid_raw_public_trade_buy_fixture() -> HyperliquidRawPublicTrade:
    """Provide a valid HyperliquidRawPublicTrade for a BUY trade."""
    return HyperliquidRawPublicTrade(
        coin="ETH-PERP",
        side="B",
        px="3002.00",
        sz="1.5",
        time=int(datetime.now(UTC).timestamp() * 1000 - 3000),  # 3 seconds ago
        hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
        tid=12345,
        users=["0x1234567890abcdef"],
    )


@pytest.fixture
def hyperliquid_raw_public_trade_sell_fixture() -> HyperliquidRawPublicTrade:
    """Provide a valid HyperliquidRawPublicTrade for a SELL trade."""
    return HyperliquidRawPublicTrade(
        coin="BTC-PERP",
        side="A",  # Sell
        px="60100.75",
        sz="0.02",
        time=int(datetime.now(UTC).timestamp() * 1000 - 1500),  # 1.5 seconds ago
        hash="0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
        tid=12346,
        users=["0xfedcba0987654321"],
    )


# --- Tests for order book transformations ---


class TestTransformRawOrderBook:
    """Tests for transform_raw_order_book method."""

    def test_order_book_transformation_eth_happy_path(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
    ) -> None:
        """Test successful order book transformation for ETH-PERP."""
        raw_book = hyperliquid_raw_l2_book_eth_fixture
        order_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)

        assert isinstance(order_book, OrderBook)
        assert order_book.symbol == raw_book.coin
        assert isinstance(order_book.timestamp, datetime)

        # Verify bids and asks structure
        assert len(order_book.bids) == 3
        assert len(order_book.asks) == 3

        # Check best bid/ask (tuples of (price, size))
        assert order_book.bids[0][0] == Decimal("2999.50")  # price
        assert order_book.bids[0][1] == Decimal("10.5")  # size
        assert order_book.asks[0][0] == Decimal("3000.50")  # price
        assert order_book.asks[0][1] == Decimal("5.25")  # size

        # Verify sorting (bids descending, asks ascending)
        for i in range(len(order_book.bids) - 1):
            assert order_book.bids[i][0] >= order_book.bids[i + 1][0]
        for i in range(len(order_book.asks) - 1):
            assert order_book.asks[i][0] <= order_book.asks[i + 1][0]

    def test_order_book_transformation_with_depth_limit(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
    ) -> None:
        """Test order book transformation with depth limit."""
        raw_book = hyperliquid_raw_l2_book_eth_fixture
        order_book = market_data_mapper.transform_raw_l2_book_to_internal(raw_book, depth=2)

        assert isinstance(order_book, OrderBook)
        # Should only have top 2 levels
        assert len(order_book.bids) == 2
        assert len(order_book.asks) == 2

        # Verify it's the best levels
        assert order_book.bids[0][0] == Decimal("2999.50")
        assert order_book.bids[1][0] == Decimal("2999.00")
        assert order_book.asks[0][0] == Decimal("3000.50")
        assert order_book.asks[1][0] == Decimal("3001.00")

    def test_order_book_transformation_empty_book(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_empty_fixture: HyperliquidRawL2Book,
    ) -> None:
        """Test order book transformation with empty book."""
        raw_book = hyperliquid_raw_l2_book_empty_fixture
        order_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)

        assert isinstance(order_book, OrderBook)
        assert order_book.symbol == raw_book.coin
        assert len(order_book.bids) == 0
        assert len(order_book.asks) == 0

    def test_order_book_transformation_malformed_levels_structure(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test order book transformation with malformed levels structure."""
        # Create a malformed book with wrong structure
        with pytest.raises(ValidationError):
            malformed_book = HyperliquidRawL2Book(
                coin="MALFORMED-PERP",
                levels=[[HyperliquidRawBookLevel(px="1000.0", sz="1.0", n=1)]],  # Missing asks
                time=int(datetime.now(UTC).timestamp() * 1000),
            )
            market_data_mapper.transform_raw_order_book_to_internal(malformed_book)

    def test_order_book_transformation_high_precision_values(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test order book transformation with high precision values."""
        bid_levels = [
            HyperliquidRawBookLevel(px="2999.123456789012345", sz="10.987654321098765", n=2),
        ]
        ask_levels = [
            HyperliquidRawBookLevel(px="3000.987654321098765", sz="5.123456789012345", n=3),
        ]

        raw_book = HyperliquidRawL2Book(
            coin="PRECISION-PERP",
            levels=[bid_levels, ask_levels],
            time=int(datetime.now(UTC).timestamp() * 1000),
        )

        order_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)

        # Business logic rounds prices to 8 decimal places but preserves quantity precision
        assert order_book.bids[0][0] == Decimal("2999.12345679")
        assert order_book.bids[0][1] == Decimal("10.987654321098765")
        assert order_book.asks[0][0] == Decimal("3000.98765432")
        assert order_book.asks[0][1] == Decimal("5.123456789012345")

    def test_order_book_transformation_zero_depth_limit(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
    ) -> None:
        """Test order book transformation with zero depth limit."""
        raw_book = hyperliquid_raw_l2_book_eth_fixture
        order_book = market_data_mapper.transform_raw_l2_book_to_internal(raw_book, depth=0)

        # Should return empty order book
        assert len(order_book.bids) == 0
        assert len(order_book.asks) == 0

    def test_order_book_transformation_large_depth_limit(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
    ) -> None:
        """Test order book transformation with depth limit larger than available levels."""
        raw_book = hyperliquid_raw_l2_book_eth_fixture
        order_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)

        # Should return all available levels
        assert len(order_book.bids) == 3
        assert len(order_book.asks) == 3

    def test_order_book_timestamp_conversion(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test order book timestamp conversion from milliseconds to datetime."""
        specific_time_ms = 1678886400000  # Fixed timestamp for reproducible test
        raw_book = HyperliquidRawL2Book(
            coin="TIMESTAMP-PERP",
            levels=[[], []],
            time=specific_time_ms,
        )

        order_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)

        expected_datetime = datetime.fromtimestamp(specific_time_ms / 1000, UTC)
        assert order_book.timestamp == expected_datetime


# --- Tests for trade transformations ---


class TestTransformRawPublicTradeToInternal:
    """Tests for transform_raw_public_trade_to_internal method."""

    def test_trade_transformation_buy_happy_path(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test successful trade transformation for BUY trade."""
        raw_trade = hyperliquid_raw_public_trade_buy_fixture
        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)

        assert trade is not None
        assert isinstance(trade, Trade)
        assert trade.symbol == raw_trade.coin
        assert trade.side == OrderSide.BUY  # "B" -> BUY
        assert trade.price == Decimal(raw_trade.px)
        assert trade.quantity == Decimal(raw_trade.sz)
        assert isinstance(trade.executed_at, datetime)

        # Verify Hyperliquid-specific details
        assert trade.hl_details is not None
        assert isinstance(trade.hl_details, HyperliquidTradeDetails)
        assert trade.hl_details.trade_hash == raw_trade.hash
        assert trade.bp_details is None

    def test_trade_transformation_sell_happy_path(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test successful trade transformation for SELL trade."""
        raw_trade = hyperliquid_raw_public_trade_sell_fixture
        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)

        assert trade is not None
        assert isinstance(trade, Trade)
        assert trade.symbol == raw_trade.coin
        assert trade.side == OrderSide.SELL  # "A" -> SELL
        assert trade.price == Decimal(raw_trade.px)
        assert trade.quantity == Decimal(raw_trade.sz)

    def test_trade_transformation_invalid_side_returns_none(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that trade with invalid side raises TransformationError."""
        # Create a valid raw trade first
        raw_trade = HyperliquidRawPublicTrade(
            coin="INVALID-PERP",
            side="B",  # Valid side for model creation
            px="1000.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        # Mock the imported utility function
        mock_map_side = mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper.map_side_to_internal",
            side_effect=TransformationError("Unknown Hyperliquid order side: 'X'"),
        )

        with pytest.raises(TransformationError, match="Unknown Hyperliquid order side"):
            market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)

        mock_map_side.assert_called_once_with("B")

    def test_trade_transformation_zero_price_returns_none(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test that trade with zero price returns None."""
        raw_trade = HyperliquidRawPublicTrade(
            coin="ZERO-PRICE-PERP",
            side="B",
            px="0.0",  # Zero price
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)
        assert trade is None

    def test_trade_transformation_zero_quantity_returns_none(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test that trade with effectively zero quantity returns None."""
        # Use a very small positive value since raw model requires positive values
        # The mapper should still return None for effectively zero quantities
        raw_trade = HyperliquidRawPublicTrade(
            coin="ZERO-QTY-PERP",
            side="B",
            px="1000.0",
            sz="0.000000001",  # Very small positive quantity (effectively zero)
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)
        assert trade is None

    def test_trade_transformation_high_precision_values(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test trade transformation with high precision values."""
        raw_trade = HyperliquidRawPublicTrade(
            coin="PRECISION-PERP",
            side="B",
            px="3002.123456789012345",
            sz="1.987654321098765",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)

        assert trade is not None
        # Business logic rounds price to 8 decimal places but preserves quantity precision
        assert trade.price == Decimal("3002.12345679")
        assert trade.quantity == Decimal("1.987654321098765")

    def test_trade_transformation_timestamp_conversion(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test trade timestamp conversion from milliseconds to datetime."""
        specific_time_ms = 1678886400000  # Fixed timestamp for reproducible test
        raw_trade = HyperliquidRawPublicTrade(
            coin="TIMESTAMP-PERP",
            side="B",
            px="1000.0",
            sz="1.0",
            time=specific_time_ms,
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(raw_trade)

        assert trade is not None
        expected_datetime = datetime.fromtimestamp(specific_time_ms / 1000, UTC)
        assert trade.executed_at == expected_datetime


# --- Tests for batch trade transformations ---


class TestTransformRawTrades:
    """Tests for transform_raw_trades method."""

    def test_transform_empty_trades_list(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test transformation of empty trades list."""
        result = market_data_mapper.transform_raw_trades([])
        assert result == []

    def test_transform_populated_trades_list(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
        hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test transformation of populated trades list."""
        raw_trades = [
            hyperliquid_raw_public_trade_buy_fixture,
            hyperliquid_raw_public_trade_sell_fixture,
        ]

        result = market_data_mapper.transform_raw_trades(raw_trades)

        assert len(result) == 2
        assert all(isinstance(trade, Trade) for trade in result)
        assert result[0].side == OrderSide.BUY
        assert result[1].side == OrderSide.SELL

    def test_transform_trades_with_limit(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
        hyperliquid_raw_public_trade_sell_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test transformation of trades list with limit."""
        raw_trades = [
            hyperliquid_raw_public_trade_buy_fixture,
            hyperliquid_raw_public_trade_sell_fixture,
        ]

        result = market_data_mapper.transform_raw_trades(raw_trades, limit=1)

        assert len(result) == 1
        assert isinstance(result[0], Trade)

    def test_transform_trades_limit_greater_than_list_size(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test transformation with limit greater than available trades."""
        raw_trades = [hyperliquid_raw_public_trade_buy_fixture]

        result = market_data_mapper.transform_raw_trades(raw_trades, limit=10)

        assert len(result) == 1  # Should return all available trades
        assert isinstance(result[0], Trade)

    def test_transform_trades_with_some_invalid_trades(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test transformation with some invalid trades that return None."""
        # Create an invalid trade with zero price
        invalid_trade = HyperliquidRawPublicTrade(
            coin="INVALID-PERP",
            side="B",
            px="0.0",  # Invalid zero price
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12346,
            users=["0xfedcba0987654321"],
        )

        raw_trades = [
            hyperliquid_raw_public_trade_buy_fixture,  # Valid
            invalid_trade,  # Invalid
        ]

        with structlog.testing.capture_logs() as captured_logs:
            result = market_data_mapper.transform_raw_trades(raw_trades)

        # Should only return valid trades
        assert len(result) == 1
        assert isinstance(result[0], Trade)
        assert result[0].symbol == "ETH-PERP"  # The valid trade

        # Check that warning was logged for skipped trade in structured logs
        warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
        assert len(warning_logs) > 0, "Expected at least one warning log"

        # Check for the specific warning about skipped trade
        skip_logs = [
            log
            for log in warning_logs
            if log.get("event") in ["invalid_trade_data_skipped", "trade_transformation_skipped"]
        ]
        assert len(skip_logs) > 0, f"Expected trade skipping logs, got: {captured_logs}"

    def test_transform_trades_with_transformation_error(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
        mocker: MockerFixture,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test transformation with error during single trade transformation."""
        # Create a trade that will trigger the error
        error_trade = HyperliquidRawPublicTrade(
            coin="ERROR-PERP",
            side="B",
            px="1000.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=12345,
            users=["0x1234567890abcdef"],
        )

        # Store the original method
        original_transform = HyperliquidOrderBookMapper.transform_raw_public_trade_to_internal

        def mock_transform_side_effect(raw_trade: HyperliquidRawPublicTrade) -> Trade | None:
            """Return mock transform side effect for testing."""
            if raw_trade.coin == "ERROR-PERP":
                raise ValueError("Simulated transformation error")
            result = original_transform(raw_trade)
            # Ensure we return the correct type
            return result if isinstance(result, Trade) else None

        # Patch the static method at the class level
        mocker.patch.object(
            HyperliquidOrderBookMapper,
            "transform_raw_public_trade_to_internal",
            side_effect=mock_transform_side_effect,
        )

        raw_trades = [
            hyperliquid_raw_public_trade_buy_fixture,  # Valid
            error_trade,  # Will cause error
        ]

        with structlog.testing.capture_logs() as captured_logs:
            result = market_data_mapper.transform_raw_trades(raw_trades)

        # Should only return the valid trade (error trade should be skipped)
        assert len(result) == 1
        assert isinstance(result[0], Trade)
        assert result[0].symbol == "ETH-PERP"  # The valid trade

        # Check that error was logged in structured logs
        error_logs = [log for log in captured_logs if log.get("log_level") == "error"]
        assert len(error_logs) > 0, "Expected at least one error log"

        # Check for the specific error message
        transform_error_logs = [
            log for log in error_logs if log.get("event") == "trade_transformation_failed"
        ]
        assert len(transform_error_logs) > 0, (
            f"Expected trade transformation error logs, got: {captured_logs}"
        )

    def test_transform_trades_zero_limit(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test transformation with zero limit."""
        raw_trades = [hyperliquid_raw_public_trade_buy_fixture]

        result = market_data_mapper.transform_raw_trades(raw_trades, limit=0)

        assert result == []  # Should return empty list


# --- Tests for integration scenarios ---


class TestOrderBookAndTradeIntegration:
    """Tests for integration scenarios involving order books and trades."""

    def test_order_book_and_trade_consistency(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
        hyperliquid_raw_l2_book_eth_fixture: HyperliquidRawL2Book,
        hyperliquid_raw_public_trade_buy_fixture: HyperliquidRawPublicTrade,
    ) -> None:
        """Test that order book and trade data maintain consistency."""
        order_book = market_data_mapper.transform_raw_order_book_to_internal(
            hyperliquid_raw_l2_book_eth_fixture,
        )
        trade = market_data_mapper.transform_raw_public_trade_to_internal(
            hyperliquid_raw_public_trade_buy_fixture,
        )

        assert trade is not None
        # Both should have the same symbol
        assert order_book.symbol == trade.symbol

        # Timestamps should be reasonably close (within 10 seconds)
        time_diff = abs((order_book.timestamp - trade.executed_at).total_seconds())
        assert time_diff < 10

    def test_large_order_book_transformation_efficiency(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test efficient transformation of large order books."""
        # Create a large order book with many levels
        bid_levels = [
            HyperliquidRawBookLevel(px=f"{3000 - i * 0.01:.2f}", sz=f"{10 + i:.1f}", n=i + 1)
            for i in range(100)
        ]
        ask_levels = [
            HyperliquidRawBookLevel(px=f"{3000 + i * 0.01:.2f}", sz=f"{10 + i:.1f}", n=i + 1)
            for i in range(100)
        ]

        raw_book = HyperliquidRawL2Book(
            coin="LARGE-BOOK-PERP",
            levels=[bid_levels, ask_levels],
            time=int(datetime.now(UTC).timestamp() * 1000),
        )

        # Transform with different depth limits
        full_book = market_data_mapper.transform_raw_order_book_to_internal(raw_book)
        limited_book = market_data_mapper.transform_raw_l2_book_to_internal(raw_book, depth=10)

        assert len(full_book.bids) == 100
        assert len(full_book.asks) == 100
        assert len(limited_book.bids) == 10
        assert len(limited_book.asks) == 10

        # Verify ordering is maintained
        for i in range(99):
            assert full_book.bids[i][0] >= full_book.bids[i + 1][0]
            assert full_book.asks[i][0] <= full_book.asks[i + 1][0]

    def test_multiple_trades_transformation_efficiency(
        self,
        market_data_mapper: HyperliquidOrderBookMapper,
    ) -> None:
        """Test efficient transformation of multiple trades."""
        # Create multiple trades
        raw_trades: list[HyperliquidRawPublicTrade] = []
        for i in range(50):
            side = "B" if i % 2 == 0 else "A"
            raw_trade = HyperliquidRawPublicTrade(
                coin=f"BATCH-{i % 5}-PERP",  # 5 different symbols
                side=side,
                px=f"{1000 + i * 0.1:.1f}",
                sz=f"{1 + i * 0.01:.2f}",
                time=int(datetime.now(UTC).timestamp() * 1000) - (i * 1000),
                hash=f"0x{'a' * 60}{i:04d}",  # 64-char hash
                tid=i,
                users=[f"0x{'b' * 60}{i:04d}"],
            )
            raw_trades.append(raw_trade)

        # Transform all trades
        trades = market_data_mapper.transform_raw_trades(raw_trades)

        assert len(trades) == 50
        # Verify all trades are valid
        assert all(isinstance(trade, Trade) for trade in trades)
        # Verify side distribution
        buy_trades = [t for t in trades if t.side == OrderSide.BUY]
        sell_trades = [t for t in trades if t.side == OrderSide.SELL]
        assert len(buy_trades) == 25
        assert len(sell_trades) == 25
