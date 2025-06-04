"""
CyberDeltaEngine: Hyperliquid Market Data Mapper Core Tests
-----------------------------------------------------------

Comprehensive test suite for HyperliquidMarketDataMapper core transformation methods.
Tests fundamental transformation logic including:
- Asset context to ticker transformations
- Asset context to funding rate transformations
- Core business logic validation
- Timestamp generation and precision handling
- Exchange-specific field mapping
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

# Third-party imports for type checking only
# Project-specific imports
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
)
from cyberdelta.core.models import Ticker
from cyberdelta.core.models.market.funding_rate import (
    FundingRate,
    HyperliquidFundingDetails,
)

# Alias for shorter method calls
Mapper = HyperliquidMarketDataMapper

# --- Fixtures ---


@pytest.fixture
def market_data_mapper() -> HyperliquidMarketDataMapper:
    """Provide an instance of HyperliquidMarketDataMapper."""
    return HyperliquidMarketDataMapper()


@pytest.fixture
def hyperliquid_raw_asset_ctx_eth_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for ETH-PERP."""
    return HyperliquidRawAssetCtx(
        name="ETH-PERP",
        funding="0.00001234",
        markPx="3010.75",
        prevDayPx="2950.00",
        dayNtlVlm="50000000.00",
        impactPx="3009.50",
    )


@pytest.fixture
def hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture() -> HyperliquidRawAssetCtx:
    """Provides a valid HyperliquidRawAssetCtx for BTC-PERP with no impactPx."""
    return HyperliquidRawAssetCtx(
        name="BTC-PERP",
        funding="-0.00000567",
        markPx="60200.50",
        prevDayPx="61000.00",
        dayNtlVlm="120000000.00",
        impactPx=None,
    )


# --- Tests for ticker transformations ---


class TestTransformRawAssetCtxToTicker:
    """Tests for transform_raw_asset_ctx_to_ticker method."""

    def test_ticker_transformation_eth_happy_path(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test successful ticker transformation for ETH-PERP with all fields."""
        raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        assert isinstance(ticker, Ticker)
        assert ticker.symbol == raw_ctx.name
        assert isinstance(ticker.timestamp, datetime)
        assert (datetime.now(UTC) - ticker.timestamp) < timedelta(seconds=5)

        # Core price and volume data
        assert ticker.price == Decimal(raw_ctx.mark_px)
        assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)

        # Note: Ticker model doesn't have exchange, hl_details, bp_details attributes
        # These would be handled by the mapper internally or in higher-level aggregation models

    def test_ticker_transformation_btc_no_impact_price(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test ticker transformation for BTC-PERP with no impact price."""
        raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        assert isinstance(ticker, Ticker)
        assert ticker.symbol == raw_ctx.name
        assert ticker.price == Decimal(raw_ctx.mark_px)
        assert ticker.volume == Decimal(raw_ctx.day_ntl_vlm)

    def test_ticker_transformation_high_precision_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test ticker transformation with high precision decimal values."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="PRECISION-PERP",
            funding="0.000012345678901234",
            markPx="3010.123456789012345",
            prevDayPx="2950.987654321098765",
            dayNtlVlm="50000000.111111111111111",
            impactPx="3009.999999999999999",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        # Verify precision is maintained
        assert ticker.price == Decimal("3010.123456789012345")
        assert ticker.volume == Decimal("50000000.111111111111111")
        assert ticker.symbol == "PRECISION-PERP"

    def test_ticker_transformation_zero_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test ticker transformation with zero values."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="ZERO-PERP",
            funding="0.0",
            markPx="0.0",
            prevDayPx="0.0",
            dayNtlVlm="0.0",
            impactPx=None,
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        assert ticker.price == Decimal("0.0")
        assert ticker.volume == Decimal("0.0")
        assert ticker.symbol == "ZERO-PERP"

    def test_ticker_timestamp_generation_consistency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test that ticker timestamps are generated consistently and recently."""
        # Transform multiple times and verify timestamps are recent and consistent
        tickers: list[Ticker] = []
        for _ in range(5):
            ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(
                hyperliquid_raw_asset_ctx_eth_fixture,
            )
            tickers.append(ticker)

        current_time = datetime.now(UTC)
        for ticker in tickers:
            assert isinstance(ticker.timestamp, datetime)
            # Each timestamp should be very recent
            time_diff = current_time - ticker.timestamp
            assert time_diff.total_seconds() < 5

        # All tickers should have timestamps within a reasonable range of each other
        earliest = min(t.timestamp for t in tickers)
        latest = max(t.timestamp for t in tickers)
        assert (latest - earliest).total_seconds() < 2

    def test_ticker_transformation_with_negative_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test ticker transformation with negative funding rate."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="NEGATIVE-FUND-PERP",
            funding="-0.00005678",
            markPx="1500.75",
            prevDayPx="1520.00",
            dayNtlVlm="25000000.50",
            impactPx="1500.25",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        # Ticker transformation should not be affected by negative funding
        assert ticker.symbol == "NEGATIVE-FUND-PERP"
        assert ticker.price == Decimal("1500.75")
        assert ticker.volume == Decimal("25000000.50")

    def test_ticker_transformation_with_large_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test ticker transformation with very large values."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="LARGE-VALUES-PERP",
            funding="0.001",  # Large funding rate
            markPx="999999.99",  # Large price
            prevDayPx="999888.88",
            dayNtlVlm="999999999999.99",  # Very large volume
            impactPx="999999.50",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        assert ticker.price == Decimal("999999.99")
        assert ticker.volume == Decimal("999999999999.99")
        assert ticker.symbol == "LARGE-VALUES-PERP"


# --- Tests for funding rate transformations ---


class TestTransformRawAssetCtxToFundingRate:
    """Tests for transform_raw_asset_ctx_to_funding_rate method."""

    def test_funding_rate_transformation_eth_positive_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test funding rate transformation for ETH with positive funding."""
        raw_ctx = hyperliquid_raw_asset_ctx_eth_fixture
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        assert funding_rate is not None
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == raw_ctx.name
        assert isinstance(funding_rate.timestamp, datetime)
        assert (datetime.now(UTC) - funding_rate.timestamp) < timedelta(seconds=10)

        # Core funding rate calculations
        expected_hourly_rate = Decimal(raw_ctx.funding)
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
        # 8-hour funding rate (standard Hyperliquid period)
        assert funding_rate.funding_rate == expected_hourly_rate * Decimal("8")

        # Additional funding rate fields
        assert funding_rate.predicted_rate is None  # Set to None by mapper
        assert funding_rate.next_funding_time is not None
        assert funding_rate.mark_price == Decimal(raw_ctx.mark_px)
        assert funding_rate.index_price is None  # Set to None by mapper

        # Exchange-specific details
        assert isinstance(funding_rate.hl_details, HyperliquidFundingDetails)
        # Handle None case for impact_px
        if raw_ctx.impact_px is not None:
            assert funding_rate.hl_details.hl_impact_px == Decimal(raw_ctx.impact_px)
        else:
            assert funding_rate.hl_details.hl_impact_px is None
        assert funding_rate.bp_details is None

    def test_funding_rate_transformation_btc_negative_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test funding rate transformation for BTC with negative funding and no impact price."""
        raw_ctx = hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        assert funding_rate is not None
        assert isinstance(funding_rate, FundingRate)
        assert funding_rate.symbol == raw_ctx.name

        # Verify negative funding rate handling
        expected_hourly_rate = Decimal(raw_ctx.funding)
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
        assert funding_rate.funding_rate == expected_hourly_rate * Decimal("8")

        # Mark price and impact price handling
        assert funding_rate.mark_price == Decimal(raw_ctx.mark_px)
        assert funding_rate.hl_details.hl_impact_px is None

    def test_funding_rate_next_funding_time_calculation(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test that next funding time is calculated correctly."""
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            hyperliquid_raw_asset_ctx_eth_fixture,
        )

        assert funding_rate is not None
        assert funding_rate.next_funding_time is not None

        # Check that next funding time is roughly the start of the next hour UTC
        now_utc = datetime.now(UTC)
        expected_next_funding_time_approx = now_utc.replace(
            minute=0,
            second=0,
            microsecond=0,
        ) + timedelta(hours=1)

        time_diff = abs(
            (funding_rate.next_funding_time - expected_next_funding_time_approx).total_seconds(),
        )
        assert time_diff < 120, (
            f"Next funding time {funding_rate.next_funding_time} not close to "
            f"{expected_next_funding_time_approx}"
        )

    def test_funding_rate_transformation_high_precision_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test funding rate transformation with high precision funding values."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="HIGH-PRECISION-PERP",
            funding="0.000123456789012345",
            markPx="2000.123456789012345",
            prevDayPx="2010.987654321098765",
            dayNtlVlm="30000000.555555555555555",
            impactPx="2000.111111111111111",
        )

        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        assert funding_rate is not None
        # Verify high precision is maintained
        expected_hourly_rate = Decimal("0.000123456789012345")
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
        assert funding_rate.funding_rate == expected_hourly_rate * Decimal("8")
        assert funding_rate.mark_price == Decimal("2000.123456789012345")
        assert funding_rate.hl_details.hl_impact_px == Decimal("2000.111111111111111")

    def test_funding_rate_transformation_zero_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test funding rate transformation with zero funding rate."""
        raw_ctx = HyperliquidRawAssetCtx(
            name="ZERO-FUNDING-PERP",
            funding="0.0",
            markPx="1000.0",
            prevDayPx="1000.0",
            dayNtlVlm="10000000.0",
            impactPx="1000.0",
        )

        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        assert funding_rate is not None
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == Decimal("0.0")
        assert funding_rate.funding_rate == Decimal("0.0")

    def test_funding_rate_transformation_extreme_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test funding rate transformation with extreme funding values."""
        test_cases = [
            ("0.001", "Very high positive funding"),
            ("-0.001", "Very high negative funding"),
            ("0.000000001", "Very small positive funding"),
            ("-0.000000001", "Very small negative funding"),
        ]

        for funding_value, description in test_cases:
            raw_ctx = HyperliquidRawAssetCtx(
                name=f"EXTREME-{description.replace(' ', '-').upper()}-PERP",
                funding=funding_value,
                markPx="1500.0",
                prevDayPx="1500.0",
                dayNtlVlm="20000000.0",
                impactPx="1500.0",
            )

            funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

            assert funding_rate is not None
            expected_hourly_rate = Decimal(funding_value)
            assert funding_rate.hl_details is not None
            assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
            assert funding_rate.funding_rate == expected_hourly_rate * Decimal("8")


# --- Tests for core business logic validation ---


class TestCoreBusinessLogicValidation:
    """Tests for core business logic validation in market data transformations."""

    def test_exchange_assignment_consistency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test that transformations are consistent (exchange info handled by mapper internally)."""
        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(
            hyperliquid_raw_asset_ctx_eth_fixture,
        )
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            hyperliquid_raw_asset_ctx_eth_fixture,
        )

        # Both should be valid transformations
        assert ticker is not None
        assert funding_rate is not None
        # Note: Exchange assignment would be handled at a higher level or in mapper internals

    def test_symbol_name_consistency_across_transformations(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that symbol names are consistent across different transformations."""
        symbol_test_cases = [
            "ETH-PERP",
            "BTC-PERP",
            "SOL-PERP",
            "AVAX-PERP",
            "LONG-SYMBOL-NAME-PERP",
        ]

        for symbol in symbol_test_cases:
            raw_ctx = HyperliquidRawAssetCtx(
                name=symbol,
                funding="0.00001",
                markPx="1000.0",
                prevDayPx="1000.0",
                dayNtlVlm="10000000.0",
                impactPx="1000.0",
            )

            ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)
            funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

            # Symbol names should be identical across transformations
            assert ticker.symbol == symbol
            assert funding_rate is not None
            assert funding_rate.symbol == symbol

    def test_timestamp_generation_proximity(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
        hyperliquid_raw_asset_ctx_eth_fixture: HyperliquidRawAssetCtx,
    ) -> None:
        """Test that timestamps generated for different transformations are close to each other."""
        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(
            hyperliquid_raw_asset_ctx_eth_fixture,
        )
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(
            hyperliquid_raw_asset_ctx_eth_fixture,
        )

        assert funding_rate is not None

        # Timestamps should be very close (within a few seconds)
        time_diff = abs((ticker.timestamp - funding_rate.timestamp).total_seconds())
        assert time_diff < 5, (
            f"Timestamps too far apart: ticker={ticker.timestamp}, "
            f"funding_rate={funding_rate.timestamp}"
        )

    def test_decimal_precision_consistency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that decimal precision is handled consistently across transformations."""
        high_precision_value = "1234.123456789012345"
        raw_ctx = HyperliquidRawAssetCtx(
            name="PRECISION-TEST-PERP",
            funding="0.000123456789012345",
            markPx=high_precision_value,
            prevDayPx=high_precision_value,
            dayNtlVlm="999999999.123456789012345",
            impactPx=high_precision_value,
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        # Both should maintain the same precision for mark price
        expected_mark_price = Decimal(high_precision_value)
        assert ticker.price == expected_mark_price
        assert funding_rate is not None
        assert funding_rate.mark_price == expected_mark_price

    def test_optional_field_handling_consistency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that optional fields are handled consistently across transformations."""
        # Test with None impact price
        raw_ctx_no_impact = HyperliquidRawAssetCtx(
            name="NO-IMPACT-PERP",
            funding="0.00001",
            markPx="2000.0",
            prevDayPx="2000.0",
            dayNtlVlm="10000000.0",
            impactPx=None,
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx_no_impact)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx_no_impact)

        # Ticker transformation should work regardless of impact price
        assert ticker.symbol == "NO-IMPACT-PERP"
        assert ticker.price == Decimal("2000.0")

        # Funding rate should handle None impact price correctly
        assert funding_rate is not None
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_impact_px is None
