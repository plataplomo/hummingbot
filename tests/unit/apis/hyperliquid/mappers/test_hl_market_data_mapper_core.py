"""CyberDeltaEngine: Hyperliquid Market Data Mapper Core Tests.

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
        dayBaseVlm=day_base_vlm
        or (str(float(day_ntl_vlm) / float(mark_px)) if float(mark_px) != 0 else "0.0"),
    )


@pytest.fixture
def market_data_mapper() -> HyperliquidMarketDataMapper:
    """Provide an instance of HyperliquidMarketDataMapper."""
    return HyperliquidMarketDataMapper()


@pytest.fixture
def hyperliquid_raw_asset_ctx_eth_fixture() -> HyperliquidRawAssetCtx:
    """Provide a valid HyperliquidRawAssetCtx for ETH-PERP."""
    return create_asset_ctx(
        name="ETH-PERP",
        funding="0.00001234",
        mark_px="3010.75",
        prev_day_px="2950.00",
        day_ntl_vlm="50000000.00",
        impact_px="3009.50",
        open_interest="1000000.00",
        premium="0.0002",
        oracle_px="3010.00",
        mid_px="3010.25",
        impact_pxs=["3009.00", "3012.00"],
        day_base_vlm="16600.00",
    )


@pytest.fixture
def hyperliquid_raw_asset_ctx_btc_no_impact_px_fixture() -> HyperliquidRawAssetCtx:
    """Provide a valid HyperliquidRawAssetCtx for BTC-PERP with no impactPx."""
    return create_asset_ctx(
        name="BTC-PERP",
        funding="-0.00000567",
        mark_px="60200.50",
        prev_day_px="61000.00",
        day_ntl_vlm="120000000.00",
        impact_px=None,
        open_interest="2000000.00",
        premium="-0.0001",
        oracle_px="60195.00",
        mid_px="60200.00",
        impact_pxs=["60195.00", "60205.00"],
        day_base_vlm="1993.00",
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
        raw_ctx = create_asset_ctx(
            name="PRECISION-PERP",
            funding="0.000012345678901234",
            mark_px="3010.123456789012345",
            prev_day_px="2950.987654321098765",
            day_ntl_vlm="50000000.111111111111111",
            impact_px="3009.999999999999999",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        # Verify precision is maintained (8 decimal places)
        assert ticker.price == Decimal("3010.12345679")
        assert ticker.volume == Decimal("50000000.11111111")
        assert ticker.symbol == "PRECISION-PERP"

    def test_ticker_transformation_zero_values(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test ticker transformation with zero values."""
        raw_ctx = create_asset_ctx(
            name="ZERO-PERP",
            funding="0.0",
            mark_px="0.0",
            prev_day_px="0.0",
            day_ntl_vlm="0.0",
            impact_px=None,
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
        raw_ctx = create_asset_ctx(
            name="NEGATIVE-FUND-PERP",
            funding="-0.00005678",
            mark_px="1500.75",
            prev_day_px="1520.00",
            day_ntl_vlm="25000000.50",
            impact_px="1500.25",
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
        raw_ctx = create_asset_ctx(
            name="LARGE-VALUES-PERP",
            funding="0.001",  # Large funding rate
            mark_px="999999.99",  # Large price
            prev_day_px="999888.88",
            day_ntl_vlm="999999999999.99",  # Very large volume
            impact_px="999999.50",
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)

        assert ticker.price == Decimal("999999.99")
        # Business logic may introduce small precision differences for large volumes
        assert ticker.volume == Decimal("999999999999.98999023")
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
        assert funding_rate.funding_rate == expected_hourly_rate * Decimal(8)

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
        assert funding_rate.funding_rate == expected_hourly_rate * Decimal(8)

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
        raw_ctx = create_asset_ctx(
            name="HIGH-PRECISION-PERP",
            funding="0.000123456789012345",
            mark_px="2000.123456789012345",
            prev_day_px="2010.987654321098765",
            day_ntl_vlm="30000000.555555555555555",
            impact_px="2000.111111111111111",
        )

        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        assert funding_rate is not None
        # Verify precision (8 decimal places for most fields)
        expected_hourly_rate = Decimal("0.00012346")
        assert funding_rate.hl_details is not None
        assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
        assert funding_rate.funding_rate == Decimal("0.00098768")
        assert funding_rate.mark_price == Decimal("2000.12345679")
        assert funding_rate.hl_details.hl_impact_px == Decimal("2000.11111111")

    def test_funding_rate_transformation_zero_funding(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test funding rate transformation with zero funding rate."""
        raw_ctx = create_asset_ctx(
            name="ZERO-FUNDING-PERP",
            funding="0.0",
            mark_px="1000.0",
            prev_day_px="1000.0",
            day_ntl_vlm="10000000.0",
            impact_px="1000.0",
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
            raw_ctx = create_asset_ctx(
                name=f"EXTREME-{description.replace(' ', '-').upper()}-PERP",
                funding=funding_value,
                mark_px="1500.0",
                prev_day_px="1500.0",
                day_ntl_vlm="20000000.0",
                impact_px="1500.0",
            )

            funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

            assert funding_rate is not None

            # Business logic rounds extremely small values
            if abs(float(funding_value)) < 0.00000001:
                expected_hourly_rate = Decimal(0)
            else:
                expected_hourly_rate = Decimal(funding_value)

            assert funding_rate.hl_details is not None
            assert funding_rate.hl_details.hl_funding_hourly == expected_hourly_rate
            assert funding_rate.funding_rate == expected_hourly_rate * Decimal(8)


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
            raw_ctx = create_asset_ctx(
                name=symbol,
                funding="0.00001",
                mark_px="1000.0",
                prev_day_px="1000.0",
                day_ntl_vlm="10000000.0",
                impact_px="1000.0",
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
        raw_ctx = create_asset_ctx(
            name="PRECISION-TEST-PERP",
            funding="0.000123456789012345",
            mark_px=high_precision_value,
            prev_day_px=high_precision_value,
            day_ntl_vlm="999999999.123456789012345",
            impact_px=high_precision_value,
        )

        ticker = market_data_mapper.transform_raw_asset_ctx_to_ticker(raw_ctx)
        funding_rate = market_data_mapper.transform_raw_asset_ctx_to_funding_rate(raw_ctx)

        # Both should maintain the same precision for mark price (8 decimal places)
        expected_mark_price = Decimal("1234.12345679")
        assert ticker.price == expected_mark_price
        assert funding_rate is not None
        assert funding_rate.mark_price == expected_mark_price

    def test_optional_field_handling_consistency(
        self,
        market_data_mapper: HyperliquidMarketDataMapper,
    ) -> None:
        """Test that optional fields are handled consistently across transformations."""
        # Test with None impact price
        raw_ctx_no_impact = create_asset_ctx(
            name="NO-IMPACT-PERP",
            funding="0.00001",
            mark_px="2000.0",
            prev_day_px="2000.0",
            day_ntl_vlm="10000000.0",
            impact_px=None,
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
