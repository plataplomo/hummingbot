"""CyberDeltaEngine: Hyperliquid Market Data Mapper Robustness Tests.

-----------------------------------------------------------------

Comprehensive test suite for HyperliquidMarketDataMapper robustness, edge cases,
and error handling. Tests various scenarios including:
- Error handling and validation failure scenarios
- Boundary value conditions and edge cases
- Unicode and encoding support
- Performance and memory considerations
- Malformed data handling and graceful degradation
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest
import structlog.testing
from _pytest.logging import LogCaptureFixture
from pydantic import ValidationError


# Third-party imports for type checking only
if TYPE_CHECKING:
    from pytest_mock import MockerFixture

# Project-specific imports
from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
)
from cyberdelta.core.models import Trade
from cyberdelta.core.models.enums import OrderSide


# Alias for shorter method calls
Mapper = HyperliquidMarketDataMapper

# --- Fixtures ---


@pytest.fixture
def market_data_mapper() -> HyperliquidMarketDataMapper:
    """Provide an instance of HyperliquidMarketDataMapper."""
    return HyperliquidMarketDataMapper()


# --- Tests for error handling and validation failures ---


def create_asset_ctx(
    name: str,
    funding: str,
    mark_px: str,
    prev_day_px: str,
    day_ntl_vlm: str,
    impact_px: str | None = None,
    open_interest: str | None = None,
    premium: str | None = None,
    oracle_px: str | None = None,
    mid_px: str | None = None,
    impact_pxs: list[str] | None = None,
    day_base_vlm: str | None = None,
) -> HyperliquidRawAssetCtx:
    """Helper to create HyperliquidRawAssetCtx with defaults for required fields."""
    return HyperliquidRawAssetCtx(
        name=name,
        funding=funding,
        markPx=mark_px,
        prevDayPx=prev_day_px,
        dayNtlVlm=day_ntl_vlm,
        impactPx=impact_px,
        openInterest=open_interest or "1000000.00",
        premium=premium or "0.0001",
        oraclePx=oracle_px or mark_px,  # Default to mark price
        midPx=mid_px or mark_px,  # Default to mark price
        impactPxs=impact_pxs or [str(float(mark_px) - 5), str(float(mark_px) + 5)],
        dayBaseVlm=day_base_vlm or str(float(day_ntl_vlm) / float(mark_px)),
    )


class TestValidationErrorHandling:
    """Test cases for validation errors and error handling."""

    def test_invalid_asset_ctx_missing_required_fields(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that asset context with missing required fields raises validation error."""
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            # Test missing required field 'name'
            invalid_data = {
                "funding": "0.00001",
                "markPx": "1000.0",
                "prevDayPx": "1000.0",
                "dayNtlVlm": "10000000.0",
                # name and impactPx missing
            }
            HyperliquidRawAssetCtx.model_validate(invalid_data)

    def test_invalid_asset_ctx_non_numeric_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that asset context with non-numeric values raises validation error."""
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            # Test invalid numeric value in funding field
            invalid_data = {
                "name": "INVALID-PERP",
                "funding": "not_a_number",  # Invalid numeric value
                "markPx": "1000.0",
                "prevDayPx": "1000.0",
                "dayNtlVlm": "10000000.0",
                "impactPx": "1000.0",
            }
            HyperliquidRawAssetCtx.model_validate(invalid_data)

    def test_invalid_order_book_malformed_structure(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that malformed order book structure raises validation error."""
        with pytest.raises(ValidationError):
            # Missing asks array in levels
            invalid_book = HyperliquidRawL2Book(
                coin="INVALID-PERP",
                levels=[[HyperliquidRawBookLevel(px="1000.0", sz="1.0", n=1)]],
                time=int(datetime.now(UTC).timestamp() * 1000),
            )
            market_data_mapper.transform_raw_order_book_to_internal(invalid_book)

    def test_invalid_trade_missing_hash(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that trade with missing hash raises validation error."""
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            # Test by trying to validate invalid data
            invalid_data = {
                "coin": "INVALID-PERP",
                "side": "B",
                "px": "1000.0",
                "sz": "1.0",
                "time": int(datetime.now(UTC).timestamp() * 1000),
                # hash missing
            }
            HyperliquidRawPublicTrade.model_validate(invalid_data)

    def test_transformation_error_propagation(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test that transformation errors are properly propagated."""
        # Mock parse_decimal_value to raise an error
        mocker.patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper.parse_decimal_value",
            side_effect=ValueError("Simulated parsing error"),
        )

        raw_asset_ctx = create_asset_ctx(
            name="ERROR-PERP",
            funding="0.00001",
            mark_px="1000.0",
            prev_day_px="1000.0",
            day_ntl_vlm="10000000.0",
            impact_px="1000.0",
        )

        with pytest.raises(TransformationError, match="Failed to transform"):
            market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_asset_ctx)


# --- Tests for boundary value conditions ---


class TestBoundaryValueConditions:
    """Test cases for boundary values and edge conditions."""

    def test_extremely_large_numeric_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of extremely large numeric values."""
        large_value_asset_ctx = create_asset_ctx(
            name="LARGE-VALUES-PERP",
            funding="0.999999999999999999",  # Very large funding rate
            mark_px="999999999999.999999999999999999",  # Very large price
            prev_day_px="999999999999.999999999999999999",
            day_ntl_vlm="999999999999999999999.999999999999",  # Very large volume
            impact_px="999999999999.999999999999999999",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(large_value_asset_ctx)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            large_value_asset_ctx,
        )

        # Should handle large values without error
        assert ticker.symbol == "LARGE-VALUES-PERP"
        assert funding_rate is not None
        assert funding_rate.symbol == "LARGE-VALUES-PERP"

    def test_extremely_small_numeric_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of extremely small numeric values."""
        small_value_asset_ctx = create_asset_ctx(
            name="SMALL-VALUES-PERP",
            funding="0.00000001",  # Small funding rate that doesn't round to zero
            mark_px="0.00000001",  # Small price that doesn't round to zero
            prev_day_px="0.00000001",
            day_ntl_vlm="0.00000001",  # Small volume that doesn't round to zero
            impact_px="0.00000001",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(small_value_asset_ctx)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            small_value_asset_ctx,
        )

        # Values that survive 8-decimal rounding should process successfully
        assert ticker.symbol == "SMALL-VALUES-PERP"
        assert funding_rate is not None  # Should succeed with non-zero mark price

    def test_zero_values_edge_cases(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of zero values in various contexts."""
        # Create asset context directly to avoid division by zero in helper
        zero_values_asset_ctx = HyperliquidRawAssetCtx(
            name="ZERO-VALUES-PERP",
            funding="0",
            markPx="0",  # Zero price (might be invalid in some contexts)
            prevDayPx="0",
            dayNtlVlm="0",
            impactPx=None,
            openInterest="0",
            premium="0",
            oraclePx="0",
            midPx="0",
            impactPxs=["0", "0"],
            dayBaseVlm="0",
        )

        # Ticker transformation should handle zero price
        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(zero_values_asset_ctx)
        assert ticker.price == Decimal(0)

        # Zero price trade should be filtered out
        zero_price_trade = HyperliquidRawPublicTrade(
            coin="ZERO-PRICE-PERP",
            side="B",
            px="0.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=1,
            users=["0xuser1"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(zero_price_trade)
        assert trade is None  # Should be filtered out

    def test_negative_values_handling(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of negative values where appropriate."""
        negative_funding_asset_ctx = create_asset_ctx(
            name="NEGATIVE-FUNDING-PERP",
            funding="-0.00012345",  # Negative funding is valid
            mark_px="1000.0",
            prev_day_px="1000.0",
            day_ntl_vlm="10000000.0",
            impact_px="1000.0",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(negative_funding_asset_ctx)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            negative_funding_asset_ctx,
        )

        assert ticker.symbol == "NEGATIVE-FUNDING-PERP"
        assert funding_rate is not None
        # Verify negative funding is preserved
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == Decimal("-0.00012345")

    def test_maximum_string_lengths(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of maximum allowed string lengths."""
        max_length_symbol = "A" * 55 + "-PERP"  # 64 characters total (within limit)

        max_length_asset_ctx = create_asset_ctx(
            name=max_length_symbol,
            funding="0.00001",
            mark_px="1000.0",
            prev_day_px="1000.0",
            day_ntl_vlm="10000000.0",
            impact_px="1000.0",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(max_length_asset_ctx)
        assert ticker.symbol == max_length_symbol


# --- Tests for Unicode and encoding support ---


class TestUnicodeAndEncodingSupport:
    """Test cases for Unicode and encoding support."""

    def test_unicode_symbol_names(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of Unicode characters in symbol names."""
        unicode_symbols = [
            "ETH🚀PERP",  # Emoji
            "ÐOGE-PERP",  # Special characters
            "比特币-PERP",  # Chinese characters
            "РУБЛЬ-PERP",  # Cyrillic characters
        ]

        for symbol in unicode_symbols:
            unicode_asset_ctx = create_asset_ctx(
                name=symbol,
                funding="0.00001",
                mark_px="1000.0",
                prev_day_px="1000.0",
                day_ntl_vlm="10000000.0",
                impact_px="1000.0",
            )

            ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(unicode_asset_ctx)
            assert ticker.symbol == symbol

    def test_unicode_in_trade_hashes(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of Unicode characters in trade hashes."""
        # While unlikely in real hashes, test robustness
        unicode_hash = "0x1234567890abcdef1234567890abcdef1234567890abcdef123456789012🚀65"

        unicode_trade = HyperliquidRawPublicTrade(
            coin="UNICODE-HASH-PERP",
            side="B",
            px="1000.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash=unicode_hash,
            tid=1,
            users=["0xuser1"],
        )

        trade = market_data_mapper.transform_raw_public_trade_to_internal(unicode_trade)
        assert trade is not None
        assert trade.hl_details is not None
        assert trade.hl_details.trade_hash == unicode_hash


# --- Tests for performance and memory considerations ---


class TestPerformanceAndMemory:
    """Test cases for performance and memory considerations."""

    def test_large_order_book_processing(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test processing of large order books efficiently."""
        # Create order book with many levels
        large_bid_levels = [
            HyperliquidRawBookLevel(px=f"{1000 - i * 0.01:.8f}", sz=f"{10 + i:.6f}", n=i + 1)
            for i in range(1000)
        ]
        large_ask_levels = [
            HyperliquidRawBookLevel(px=f"{1000 + i * 0.01:.8f}", sz=f"{10 + i:.6f}", n=i + 1)
            for i in range(1000)
        ]

        large_book = HyperliquidRawL2Book(
            coin="LARGE-BOOK-PERP",
            levels=[large_bid_levels, large_ask_levels],
            time=int(datetime.now(UTC).timestamp() * 1000),
        )

        # Transform with different depth limits
        full_book = market_data_mapper.transform_raw_order_book_to_internal(large_book)
        limited_book = market_data_mapper.transform_raw_order_book_to_internal(large_book, depth=50)

        assert len(full_book.bids) == 1000
        assert len(full_book.asks) == 1000
        assert len(limited_book.bids) == 50
        assert len(limited_book.asks) == 50

    def test_batch_trade_processing_efficiency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test efficient processing of large trade batches."""
        # Create large batch of trades
        large_trade_batch: list[HyperliquidRawPublicTrade] = []
        for i in range(1000):
            trade = HyperliquidRawPublicTrade(
                coin=f"BATCH-{i % 10}-PERP",  # 10 different symbols
                side="B" if i % 2 == 0 else "A",
                px=f"{1000 + i * 0.01:.6f}",
                sz=f"{1 + i * 0.001:.6f}",
                time=int(datetime.now(UTC).timestamp() * 1000) - (i * 100),
                hash=f"0x{'a' * 58}{i:06d}",  # Unique 64-char hashes
                tid=i,
                users=[f"0xuser{i}"],
            )
            large_trade_batch.append(trade)

        # Process all trades
        trades = market_data_mapper.transform_raw_trades(large_trade_batch)

        assert len(trades) == 1000
        # Verify all trades are valid
        assert all(isinstance(trade, Trade) for trade in trades)

    def test_memory_usage_with_high_precision_decimals(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test memory efficiency with high precision decimal values."""
        high_precision_decimals = [
            "123456789.123456789012345678",  # 30 chars - within limit
            "0.000000000000000000000001",  # 26 chars - within limit
            "999999999999.999999999999",  # 24 chars - within limit
        ]

        for i, precision_value in enumerate(high_precision_decimals):
            precision_asset_ctx = create_asset_ctx(
                name=f"PRECISION-{i}-PERP",  # Keep name short
                funding="0.00001",
                mark_px=precision_value,
                prev_day_px=precision_value,
                day_ntl_vlm=precision_value,
                impact_px=precision_value,
            )

            ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(precision_asset_ctx)
            # Should handle without memory issues
            # Business logic rounds to 8 decimal places
            if i == 0:  # "123456789.123456789012345678"
                assert ticker.price == Decimal("123456789.12345679")
            elif i == 1:  # "0.000000000000000000000001"
                assert ticker.price == Decimal(0)
            else:  # "999999999999.999999999999"
                # Business logic rounds very large numbers differently
                assert ticker.price == Decimal(1000000000000)


# --- Tests for error recovery scenarios ---


class TestErrorRecoveryScenarios:
    """Test cases for error recovery and graceful degradation."""

    def test_partial_trade_batch_processing_with_errors(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        caplog: LogCaptureFixture,
    ) -> None:
        """Test graceful handling when some trades in batch fail processing."""
        mixed_trades = [
            # Valid trade
            HyperliquidRawPublicTrade(
                coin="VALID-PERP",
                side="B",
                px="1000.0",
                sz="1.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
                tid=1,
                users=["0xuser1"],
            ),
            # Invalid trade (zero price)
            HyperliquidRawPublicTrade(
                coin="INVALID-ZERO-PRICE-PERP",
                side="B",
                px="0.0",  # Will be filtered out
                sz="1.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcde1",
                tid=2,
                users=["0xuser2"],
            ),
            # Another valid trade
            HyperliquidRawPublicTrade(
                coin="ANOTHER-VALID-PERP",
                side="A",
                px="2000.0",
                sz="2.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcde2",
                tid=3,
                users=["0xuser3"],
            ),
        ]

        with structlog.testing.capture_logs() as captured_logs:
            trades = market_data_mapper.transform_raw_trades(mixed_trades)

        # Should return only valid trades
        assert len(trades) == 2
        assert trades[0].symbol == "VALID-PERP"
        assert trades[1].symbol == "ANOTHER-VALID-PERP"

        # Should log warning for skipped trade in structured logs
        warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
        assert len(warning_logs) > 0, "Expected at least one warning log"

        # Check for the specific warning about skipped trade
        skip_logs = [log for log in warning_logs if "Skipping trade transformation" in str(log)]
        assert len(skip_logs) > 0, f"Expected trade skipping logs, got: {captured_logs}"

    def test_empty_data_handling(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test handling of empty data structures."""
        # Empty order book
        empty_book = HyperliquidRawL2Book(
            coin="EMPTY-BOOK-PERP",
            levels=[[], []],  # Empty bids and asks
            time=int(datetime.now(UTC).timestamp() * 1000),
        )

        order_book = market_data_mapper.transform_raw_order_book_to_internal(empty_book)
        assert len(order_book.bids) == 0
        assert len(order_book.asks) == 0

        # Empty trades list
        empty_trades = market_data_mapper.transform_raw_trades([])
        assert len(empty_trades) == 0

    def test_malformed_timestamp_handling(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test handling of malformed timestamps with graceful fallback."""
        # Mock parse_datetime_utc to return None for malformed timestamps
        mocker.patch(
            "cyberdelta.utils.parsing.parse_datetime_utc",
            return_value=None,
        )

        malformed_book = HyperliquidRawL2Book(
            coin="MALFORMED-TIMESTAMP-PERP",
            levels=[[], []],
            time=999999999999999999,  # Malformed timestamp
        )

        # Should handle gracefully by using current time
        order_book = market_data_mapper.transform_raw_order_book_to_internal(malformed_book)
        assert isinstance(order_book.timestamp, datetime)

    def test_resilience_to_unknown_side_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        mocker: MockerFixture,
    ) -> None:
        """Test resilience to unknown side values in trades."""
        # Create a valid trade first
        valid_trade = HyperliquidRawPublicTrade(
            coin="UNKNOWN-SIDE-PERP",
            side="B",  # Valid side initially
            px="1000.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            hash="0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
            tid=1,
            users=["0xuser1"],
        )

        # Mock the _map_side_to_internal method to raise an error for unknown sides
        def mock_map_side_side_effect(hl_side: str) -> OrderSide:
            """Return mock map side side effect for testing."""
            if hl_side == "B":  # For our test trade
                raise ValueError("Unknown side")  # Simulate unknown side error
            return OrderSide.BUY  # Default for other cases

        mocker.patch.object(
            HyperliquidMarketDataMapper,
            "_map_side_to_internal",
            side_effect=mock_map_side_side_effect,
        )

        # Transform should raise TransformationError when side mapping fails
        with pytest.raises(
            TransformationError,
            match="Failed to transform HyperliquidRawPublicTrade",
        ):
            market_data_mapper.transform_raw_public_trade_to_internal(valid_trade)

    def test_data_consistency_across_transformations(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that data remains consistent across multiple transformations."""
        base_asset_ctx = create_asset_ctx(
            name="CONSISTENCY-PERP",
            funding="0.00012345",
            mark_px="1234.567890",
            prev_day_px="1230.000000",
            day_ntl_vlm="987654321.123456",
            impact_px="1234.500000",
        )

        # Transform multiple times
        ticker1 = market_data_mapper.transform_raw_asset_ctx_to_ticker(base_asset_ctx)
        ticker2 = market_data_mapper.transform_raw_asset_ctx_to_ticker(base_asset_ctx)
        funding1 = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(base_asset_ctx)
        funding2 = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(base_asset_ctx)

        # Results should be consistent
        assert ticker1.symbol == ticker2.symbol
        assert ticker1.price == ticker2.price
        assert ticker1.volume == ticker2.volume

        assert funding1 is not None
        assert funding2 is not None
        assert funding1.symbol == funding2.symbol
        assert funding1.funding_rate == funding2.funding_rate
        assert funding1.mark_price == funding2.mark_price
