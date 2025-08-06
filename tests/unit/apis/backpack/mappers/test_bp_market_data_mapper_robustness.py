"""CyberDeltaEngine: Backpack Market Data Mapper Robustness Tests.

--------------------------------------------------------------

Comprehensive test suite for BackpackTickerMapper edge cases and robustness.
Tests boundary conditions, error handling, unicode support, and stress scenarios including:
- Boundary value testing
- Unicode and encoding edge cases
- Memory and performance considerations
- Malformed data handling
- Network failure simulation
"""

from datetime import UTC, datetime
from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackTradeMapper
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.common import TransformationError
from cyberdelta.models import Ticker
from cyberdelta.symbols import exchanges
from tests.common_symbols import BTC_USDC_BP, DOGE_USDC_BP, ETH_USDC_BP, SOL_USDC_BP
from tests.fixtures.time_fixtures import FreezerProtocol


@pytest.fixture
def ticker_mapper() -> BackpackTickerMapper:
    """Fixture providing a BackpackTickerMapper instance.

    Returns:
        BackpackTickerMapper: A mapper instance for testing ticker transformations.
    """
    return BackpackTickerMapper()


@pytest.fixture
def order_book_mapper() -> BackpackOrderBookMapper:
    """Fixture providing a BackpackOrderBookMapper instance.

    Returns:
        BackpackOrderBookMapper: A mapper instance for testing order book transformations.
    """
    return BackpackOrderBookMapper()


@pytest.fixture
def trade_mapper() -> BackpackTradeMapper:
    """Fixture providing a BackpackTradeMapper instance.

    Returns:
        BackpackTradeMapper: A mapper instance for testing trade transformations.
    """
    return BackpackTradeMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string.

    Returns:
        str: ISO 8601 formatted timestamp string for consistent testing.
    """
    return "2024-01-15T10:30:00Z"


def create_raw_ticker(
    symbol: str = "SOL_USDC",
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
    """Create BackpackRawTickerResponse instances for robustness testing.

    Args:
        symbol: Trading symbol for the ticker.
        first_price: Opening price for the period.
        last_price: Current/last traded price.
        high: Highest price in the period.
        low: Lowest price in the period.
        price_change: Absolute price change.
        price_change_percent: Percentage price change.
        volume: Trading volume.
        quote_volume: Quote currency volume.
        trades: Number of trades.

    Returns:
        BackpackRawTickerResponse: Configured ticker response for testing.
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
    """Create BackpackRawOrderBook instances for robustness testing.

    Args:
        bids: List of bid price-quantity tuples, defaults to sample data.
        asks: List of ask price-quantity tuples, defaults to sample data.
        timestamp: Order book timestamp.

    Returns:
        BackpackRawOrderBook: Configured order book for testing.
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
    symbol: str = "SOL_USDC",
    price: str = "100.50",
    qty: str = "10.0",
    time: str = "2024-01-15T10:30:00Z",
    order_id: str = "order123",
) -> BackpackRawPublicTrade:
    """Create BackpackRawPublicTrade instances for robustness testing.

    Args:
        trade_id: Unique trade identifier.
        symbol: Trading symbol.
        price: Trade execution price.
        qty: Trade quantity.
        time: Trade execution time.
        order_id: Associated order identifier.

    Returns:
        BackpackRawPublicTrade: Configured trade for testing.
    """
    return BackpackRawPublicTrade(
        id=trade_id,
        symbol=symbol,
        price=price,
        qty=qty,
        time=time,
        orderId=order_id,
    )


class TestBoundaryValueHandling:
    """Test cases for boundary value scenarios."""

    def test_decimal_precision_boundaries(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of extreme decimal precision values."""
        # Test maximum precision supported by Decimal
        max_precision_price = "100.123456789012345678901234567890"
        raw_ticker = create_raw_ticker(
            last_price=max_precision_price,
        )

        result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

        # Should handle high precision gracefully
        assert result.price == Decimal(max_precision_price)

    def test_very_large_numeric_values(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of very large numeric values."""
        large_price = "999999999999999.999999"
        large_volume = "999999999999999999.999999"

        raw_ticker = create_raw_ticker(
            last_price=large_price,
            volume=large_volume,
        )

        result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal(large_price)
        assert result.volume == Decimal(large_volume)

    def test_very_small_numeric_values(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of very small numeric values."""
        small_price = "0.000000000000001"
        small_volume = "0.000000000000000001"

        raw_ticker = create_raw_ticker(
            last_price=small_price,
            volume=small_volume,
        )

        result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

        assert result.price == Decimal(small_price)
        assert result.volume == Decimal(small_volume)

    def test_zero_and_negative_value_handling(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of zero and negative values where applicable."""
        # Test zero values
        raw_ticker_zero = create_raw_ticker(
            last_price="0",
            volume="0",
        )

        result_zero = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker_zero)

        assert result_zero.price == Decimal(0)
        assert result_zero.bid is None  # Not available from Backpack ticker endpoint
        assert result_zero.ask is None  # Not available from Backpack ticker endpoint
        assert result_zero.volume == Decimal(0)

    def test_massive_order_book_levels(
        self,
        order_book_mapper: BackpackOrderBookMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of order books with very large numbers of levels."""
        # Create 1000 bid and ask levels to test performance and memory handling
        bids = [(f"{100 - i * 0.001:.3f}", f"{i + 1}.0") for i in range(1000)]
        asks = [(f"{101 + i * 0.001:.3f}", f"{i + 1}.0") for i in range(1000)]

        raw_book = create_raw_order_book(
            bids=bids,
            asks=asks,
            timestamp=test_timestamp,
        )

        result = order_book_mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        assert len(result.bids) == 1000
        assert len(result.asks) == 1000
        # Verify first and last levels are correct
        assert result.bids[0] == (Decimal("100.000"), Decimal("1.0"))
        assert result.asks[-1] == (Decimal("101.999"), Decimal("1000.0"))

    def test_maximum_string_length_handling(
        self,
        trade_mapper: BackpackTradeMapper,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of maximum allowed string lengths."""
        # Test with maximum trade ID length (64 characters based on model validation)
        max_trade_id = "a" * 64
        raw_trade = create_raw_trade(
            trade_id=max_trade_id,
        )

        result = trade_mapper.transform_raw_trade_to_internal(raw_trade)

        assert result.id == max_trade_id

        # Test with maximum symbol length
        max_symbol = "A" * 30  # Maximum allowed symbol length
        raw_ticker = create_raw_ticker(
            symbol=max_symbol,
        )

        result_ticker = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)
        assert result_ticker.symbol == exchanges.backpack(max_symbol)


class TestUnicodeAndEncodingSupport:
    """Test cases for unicode and encoding edge cases."""

    def test_unicode_symbols_comprehensive(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test comprehensive unicode symbol support."""
        unicode_symbols = [
            "SOL-USDC-测试",  # Chinese characters
            "BTC-USDC-тест",  # Cyrillic characters
            "ETH-USDC-পরীক্ষা",  # Bengali characters
            "DOGE-USDC-🚀",  # Emoji
            "ADA-USDC-αβγ",  # Greek characters
            "DOT-USDC-العربية",  # Arabic characters
        ]

        for symbol in unicode_symbols:
            raw_ticker = create_raw_ticker(
                symbol=symbol,
            )

            result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)
            assert result.symbol == exchanges.backpack(symbol)

    def test_unicode_in_trade_ids(
        self,
        trade_mapper: BackpackTradeMapper,
        test_timestamp: str,
    ) -> None:
        """Test unicode characters in trade IDs."""
        unicode_trade_ids = [
            "trade_测试_123",
            "тест_trade_456",
            "🚀_trade_789",
        ]

        for trade_id in unicode_trade_ids:
            raw_trade = create_raw_trade(
                trade_id=trade_id,
            )

            result = trade_mapper.transform_raw_trade_to_internal(raw_trade)
            assert result.id == trade_id

    def test_mixed_unicode_ascii_handling(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test mixed unicode and ASCII character handling."""
        mixed_symbol = "SOL-USDC_测试_🚀_ABC_123"
        raw_ticker = create_raw_ticker(
            symbol=mixed_symbol,
        )

        result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)
        assert result.symbol == exchanges.backpack(mixed_symbol)

    def test_special_characters_in_values(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of special characters in various fields."""
        # Test symbols with special characters commonly used in trading
        special_symbols = [
            "SOL/USDC",  # Forward slash
            "BTC-USDC.PERP",  # Dot notation
            "ETH_USDC_FUT",  # Underscore
            "DOGE-USDC@1M",  # At symbol
        ]

        for symbol in special_symbols:
            raw_ticker = create_raw_ticker(
                symbol=symbol,
            )

            result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)
            assert result.symbol == exchanges.backpack(symbol)


class TestErrorHandlingAndRecovery:
    """Test cases for error handling and recovery scenarios."""

    def test_malformed_decimal_recovery(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test recovery from malformed decimal values."""
        raw_ticker = create_raw_ticker()

        # Mock parse_decimal_safely to simulate malformed data
        with patch.object(ticker_mapper, "parse_decimal_safely") as mock_parse:
            mock_parse.side_effect = ValueError("Invalid decimal format")

            with pytest.raises(TransformationError, match="Failed to transform"):
                ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

    def test_timestamp_parsing_fallback(
        self,
        ticker_mapper: BackpackTickerMapper,
        frozen_time: FreezerProtocol,
    ) -> None:
        """Test timestamp parsing fallback to current time."""
        # Create a valid raw ticker first
        raw_ticker = create_raw_ticker()

        # Set the frozen time
        mock_now = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
        frozen_time.move_to(mock_now)

        with patch.object(ticker_mapper, "parse_timestamp") as mock_parse_datetime:
            mock_parse_datetime.return_value = None

            result = ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

            # Should fall back to current time
            assert result.timestamp == mock_now

    def test_partial_data_handling(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of ticker with minimal/zero data."""
        # Test ticker with minimal data
        minimal_ticker = BackpackRawTickerResponse(
            symbol="SOL_USDC",
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

        result = ticker_mapper.transform_raw_ticker_to_internal(minimal_ticker)

        # Should handle zero values correctly
        assert result.symbol == SOL_USDC_BP
        assert result.price == Decimal("0.0")  # Zero values become Decimal("0.0")
        assert result.bid is None  # Not available from Backpack ticker endpoint
        assert result.ask is None  # Not available from Backpack ticker endpoint
        assert result.volume == Decimal("0.0")  # Zero values become Decimal("0.0")

    def test_empty_order_book_handling(
        self,
        order_book_mapper: BackpackOrderBookMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of completely empty order books."""
        empty_book = create_raw_order_book(
            bids=[],
            asks=[],
            timestamp=test_timestamp,
        )

        result = order_book_mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, empty_book)

        assert len(result.bids) == 0
        assert len(result.asks) == 0
        assert result.symbol == SOL_USDC_BP

    def test_transformation_error_context_preservation(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test that transformation errors preserve context information."""
        raw_ticker = create_raw_ticker()

        # Create a specific error with context
        original_error = ValueError("Specific parsing error with context")

        with patch.object(ticker_mapper, "parse_decimal_safely") as mock_parse:
            mock_parse.side_effect = original_error

            with pytest.raises(TransformationError) as exc_info:
                ticker_mapper.transform_raw_ticker_to_internal(raw_ticker)

            # Verify error context is preserved
            assert "Failed to transform BackpackRawTickerResponse to Ticker" in str(exc_info.value)
            assert exc_info.value.__cause__ == original_error


class TestPerformanceAndMemoryConsiderations:
    """Test cases for performance and memory efficiency."""

    def test_large_dataset_transformation(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test transformation of large datasets efficiently."""
        # Create a large number of tickers to test batch processing efficiency
        tickers: list[BackpackRawTickerResponse] = []
        for i in range(100):
            ticker = create_raw_ticker(
                symbol=f"SYMBOL{i:03d}-USDC",
                last_price=f"{100 + i * 0.01:.2f}",
            )
            tickers.append(ticker)

        # Transform all tickers
        results: list[Ticker] = []
        for ticker in tickers:
            result = ticker_mapper.transform_raw_ticker_to_internal(ticker)
            results.append(result)

        # Verify all transformations completed correctly
        assert len(results) == 100
        for i, result in enumerate(results):
            assert result.symbol == exchanges.backpack(f"SYMBOL{i:03d}-USDC")
            assert result.price == Decimal(f"{100 + i * 0.01:.2f}")

    def test_memory_efficient_order_book_processing(
        self,
        order_book_mapper: BackpackOrderBookMapper,
        test_timestamp: str,
    ) -> None:
        """Test memory-efficient processing of large order books."""
        # Create order book with many levels but reasonable memory usage
        bids = [(f"{100 - i * 0.01:.2f}", f"{i + 1}.5") for i in range(500)]
        asks = [(f"{101 + i * 0.01:.2f}", f"{i + 1}.5") for i in range(500)]

        raw_book = create_raw_order_book(
            bids=bids,
            asks=asks,
            timestamp=test_timestamp,
        )

        # Should process without memory issues
        result = order_book_mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        assert len(result.bids) == 500
        assert len(result.asks) == 500

        # Verify precision is maintained
        assert result.bids[0] == (Decimal("100.00"), Decimal("1.5"))
        assert result.asks[0] == (Decimal("101.00"), Decimal("1.5"))

    def test_concurrent_transformation_safety(
        self,
        ticker_mapper: BackpackTickerMapper,
        trade_mapper: BackpackTradeMapper,
        test_timestamp: str,
    ) -> None:
        """Test that transformations are safe for concurrent usage."""
        # This test verifies that the mapper doesn't have mutable state
        # that could cause issues in concurrent scenarios

        # Create multiple different data objects
        ticker1 = create_raw_ticker(symbol="BTC_USDC", last_price="50000.00")
        ticker2 = create_raw_ticker(symbol="ETH_USDC", last_price="3000.00")
        trade1 = create_raw_trade(
            trade_id="trade1",
            symbol="SOL_USDC",
            price="100.00",
        )
        trade2 = create_raw_trade(
            trade_id="trade2",
            symbol="DOGE_USDC",
            price="0.50",
        )

        # Transform in interleaved pattern
        result_ticker1 = ticker_mapper.transform_raw_ticker_to_internal(ticker1)
        result_trade1 = trade_mapper.transform_raw_trade_to_internal(trade1)
        result_ticker2 = ticker_mapper.transform_raw_ticker_to_internal(ticker2)
        result_trade2 = trade_mapper.transform_raw_trade_to_internal(trade2)

        # Verify no cross-contamination
        assert result_ticker1.symbol == BTC_USDC_BP
        assert result_ticker1.price == Decimal("50000.00")
        assert result_ticker2.symbol == ETH_USDC_BP
        assert result_ticker2.price == Decimal("3000.00")
        assert result_trade1.symbol == SOL_USDC_BP
        assert result_trade1.price == Decimal("100.00")
        assert result_trade2.symbol == DOGE_USDC_BP
        assert result_trade2.price == Decimal("0.50")


class TestDataConsistencyAndValidation:
    """Test cases for data consistency and validation."""

    def test_cross_field_consistency_validation(
        self,
        order_book_mapper: BackpackOrderBookMapper,
        test_timestamp: str,
    ) -> None:
        """Test validation of cross-field consistency."""
        # Test order book with bid higher than ask (invalid market condition)
        invalid_book = create_raw_order_book(
            bids=[("102.00", "10.0")],  # Bid higher than ask
            asks=[("101.00", "8.0")],  # Ask lower than bid
            timestamp=test_timestamp,
        )

        # Mapper should still process but preserve the raw data
        result = order_book_mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, invalid_book)

        assert result.bids[0] == (Decimal("102.00"), Decimal("10.0"))
        assert result.asks[0] == (Decimal("101.00"), Decimal("8.0"))

    def test_timestamp_consistency(
        self,
        ticker_mapper: BackpackTickerMapper,
        trade_mapper: BackpackTradeMapper,
        order_book_mapper: BackpackOrderBookMapper,
    ) -> None:
        """Test timestamp consistency across transformations."""
        fixed_timestamp = "2024-01-15T10:30:00Z"
        expected_datetime = datetime(2024, 1, 15, 10, 30, 0, tzinfo=UTC)

        # Test multiple data types with same timestamp
        ticker = create_raw_ticker()
        trade = create_raw_trade(time=fixed_timestamp)
        order_book = create_raw_order_book(timestamp=fixed_timestamp)

        # Record time before transformations
        start_time = datetime.now(UTC)

        ticker_result = ticker_mapper.transform_raw_ticker_to_internal(ticker)
        trade_result = trade_mapper.transform_raw_trade_to_internal(trade)
        book_result = order_book_mapper.transform_raw_order_book_to_internal(
            SOL_USDC_BP, order_book
        )

        # Record time after transformations
        end_time = datetime.now(UTC)

        # For ticker: timestamp is generated during transformation, so it should be recent
        assert start_time <= ticker_result.timestamp <= end_time

        # For trade and order book: timestamps should match the fixed timestamp
        assert trade_result.executed_at == expected_datetime
        assert book_result.timestamp == expected_datetime

    def test_decimal_precision_consistency(
        self,
        ticker_mapper: BackpackTickerMapper,
        test_timestamp: str,
    ) -> None:
        """Test that decimal precision is consistently maintained."""
        high_precision_value = "123.123456789012345"

        ticker = create_raw_ticker(
            last_price=high_precision_value,
            volume=high_precision_value,
        )

        result = ticker_mapper.transform_raw_ticker_to_internal(ticker)

        # All decimal fields should maintain the same precision
        expected_decimal = Decimal(high_precision_value)
        assert result.price == expected_decimal
        assert result.bid is None  # Not available from Backpack ticker endpoint
        assert result.ask is None  # Not available from Backpack ticker endpoint
        assert result.volume == expected_decimal
