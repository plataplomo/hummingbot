"""Tests for the multi-tier funding rate provider."""

from datetime import UTC, datetime, timedelta
from typing import cast
from unittest.mock import AsyncMock, MagicMock

import pytest

from cyberdelta.exceptions import FundingRateSourceError
from cyberdelta.validation.funding_data import (
    FundingData,
    FundingRateValidationMetrics,
    IntegratedFundingData,
    SourceReliability,
    SourceType,
)
from cyberdelta.validation.multi_tier_funding_provider import (
    MultiTierFundingProvider,
)


pytestmark = pytest.mark.timing


class TestMultiTierFundingProvider:
    """Tests for the MultiTierFundingProvider class."""

    @pytest.fixture(autouse=True)
    def setup_method(self) -> None:
        """Set up test environment for each test method."""
        # Create a basic config
        self.config = {
            "primary_source_weight": 0.6,
            "secondary_source_weight": 0.3,
            "tertiary_source_weight": 0.1,
            "max_acceptable_rmse": 0.001,
            "max_acceptable_bias": 0.0005,
            "max_acceptable_age": 300.0,
            "default_accuracy_score": 0.5,
            "historical_accuracy_weight": 0.4,
            "source_count_weight": 0.2,
            "dispersion_weight": 0.3,
            "freshness_weight": 0.1,
        }

        # Create a mock validator
        self.mock_validator = MagicMock()
        mock_metrics = FundingRateValidationMetrics(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rmse=0.0005,
            mae=0.0003,
            bias=0.0001,
            sample_count=10,
            period_days=7,
        )
        self.mock_validator.get_metrics.return_value = mock_metrics

        # Create the provider
        self.provider = MultiTierFundingProvider(self.config, self.mock_validator)

        # Create mock source functions
        self.primary_source = AsyncMock()
        self.primary_source.return_value = {"rate": 0.0015, "timestamp": datetime.now(UTC)}

        self.secondary_source = AsyncMock()
        self.secondary_source.return_value = {
            "rate": 0.0014,
            "timestamp": datetime.now(UTC),
        }

        self.tertiary_source = AsyncMock()
        self.tertiary_source.return_value = {
            "rate": 0.0016,
            "timestamp": datetime.now(UTC),
        }

        self.fallback_source = AsyncMock()
        self.fallback_source.return_value = {
            "rate": 0.0013,
            "timestamp": datetime.now(UTC),
        }

    def test_register_source(self) -> None:
        """Test registering data sources."""
        # Register all source types
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            "hyperliquid",
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )
        self.provider.register_source(
            "hyperliquid",
            self.tertiary_source,
            SourceType.TERTIARY,
            SourceReliability.LOW,
        )
        self.provider.register_source(
            "hyperliquid",
            self.fallback_source,
            SourceType.FALLBACK,
            SourceReliability.LOW,
        )

        # Verify sources were registered
        assert self.provider.primary_sources["hyperliquid"] == self.primary_source
        assert self.provider.secondary_sources["hyperliquid"] == self.secondary_source
        assert self.provider.tertiary_sources["hyperliquid"] == self.tertiary_source
        assert self.provider.fallback_sources["hyperliquid"] == self.fallback_source

    @pytest.mark.asyncio
    async def test_get_funding_rate_all_sources(self) -> None:
        """Test getting funding rate with all sources available."""
        # Register all source types
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            "hyperliquid",
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )
        self.provider.register_source(
            "hyperliquid",
            self.tertiary_source,
            SourceType.TERTIARY,
            SourceReliability.LOW,
        )

        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        self.tertiary_source.assert_called_once_with("BTC-PERP")

        # Verify result
        assert rate == cast(float, pytest.approx(0.00147865, abs=1e-7))
        assert confidence == cast(float, pytest.approx(0.584117, abs=1e-6))

    @pytest.mark.asyncio
    async def test_get_funding_rate_primary_only(self) -> None:
        """Test getting funding rate with only primary source."""
        # Register only primary source
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )

        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify only primary source was called
        self.primary_source.assert_called_once_with("BTC-PERP")

        # Verify result
        assert rate == cast(float, pytest.approx(0.0015, abs=1e-5))
        assert confidence == cast(float, pytest.approx(0.466666, abs=1e-6))

    @pytest.mark.asyncio
    async def test_get_funding_rate_primary_fails(self) -> None:
        """Test getting funding rate when primary source fails."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            "hyperliquid",
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )

        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify primary and secondary sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")

        # Verify result uses secondary
        assert rate == cast(float, pytest.approx(0.0014, abs=1e-5))
        assert confidence < 0.7

    @pytest.mark.asyncio
    async def test_get_funding_rate_all_fail(self) -> None:
        """Test getting funding rate when all regular sources fail."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.secondary_source.side_effect = Exception("Secondary source failed")
        self.tertiary_source.side_effect = Exception("Tertiary source failed")
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            "hyperliquid",
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )
        self.provider.register_source(
            "hyperliquid",
            self.tertiary_source,
            SourceType.TERTIARY,
            SourceReliability.LOW,
        )
        self.provider.register_source(
            "hyperliquid",
            self.fallback_source,
            SourceType.FALLBACK,
            SourceReliability.LOW,
        )

        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify all sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        self.tertiary_source.assert_called_once_with("BTC-PERP")
        self.fallback_source.assert_called_once_with("BTC-PERP")

        # Verify result uses fallback
        assert rate == cast(float, pytest.approx(0.0013, abs=1e-5))
        assert confidence == cast(float, pytest.approx(0.1999999, abs=1e-6))

    @pytest.mark.asyncio
    async def test_get_funding_rate_all_fail_no_fallback(self) -> None:
        """Test getting funding rate when all sources fail and no fallback."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.secondary_source.side_effect = Exception("Secondary source failed")
        self.tertiary_source.side_effect = Exception("Tertiary source failed")
        self.provider.register_source(
            "hyperliquid",
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            "hyperliquid",
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )
        self.provider.register_source(
            "hyperliquid",
            self.tertiary_source,
            SourceType.TERTIARY,
            SourceReliability.LOW,
        )

        # Expect exception
        with pytest.raises(
            FundingRateSourceError,
            match="No funding rate data available for hyperliquid:BTC-PERP",
        ):
            await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify all sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        self.tertiary_source.assert_called_once_with("BTC-PERP")

    def test_clear_cache(self) -> None:
        """Test clearing the funding rate cache."""
        # Add some data to cache
        self.provider.funding_cache["hyperliquid", "BTC-PERP"] = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0015,
            timestamp=datetime.now(UTC),
            dispersion=0.0001,
            sources_count=3,
            primary_available=True,
            secondary_available=True,
            tertiary_available=True,
            confidence_score=0.8,
        )

        # Verify cache has data
        assert len(self.provider.funding_cache) == 1

        # Clear cache
        self.provider.clear_cache()

        # Verify cache is empty
        assert len(self.provider.funding_cache) == 0

    def test_clear_stale_cache_entries(self) -> None:
        """Test clearing stale entries from funding rate cache."""
        # Add some data to cache
        now = datetime.now(UTC)
        fresh_entry = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0015,
            timestamp=now,
            dispersion=0.0001,
            sources_count=3,
            primary_available=True,
            secondary_available=True,
            tertiary_available=True,
            confidence_score=0.8,
        )

        stale_entry = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="ETH-PERP",
            rate=0.0025,
            timestamp=now - timedelta(seconds=600),  # 10 minutes old
            dispersion=0.0002,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.7,
        )

        self.provider.funding_cache["hyperliquid", "BTC-PERP"] = fresh_entry
        self.provider.funding_cache["hyperliquid", "ETH-PERP"] = stale_entry

        # Verify cache has data
        assert len(self.provider.funding_cache) == 2

        # Clear stale entries
        cleared = self.provider.clear_stale_cache_entries(300.0)  # 5 minutes max age

        # Verify stale entry was cleared
        assert cleared == 1
        assert len(self.provider.funding_cache) == 1
        assert ("hyperliquid", "BTC-PERP") in self.provider.funding_cache
        assert ("hyperliquid", "ETH-PERP") not in self.provider.funding_cache

    @pytest.mark.asyncio
    async def test_integrate_funding_data_through_public_interface(self) -> None:
        """Test integrating funding data from multiple sources through public interface."""
        # Create test data
        now = datetime.now(UTC)
        primary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0015,
            timestamp=now,
            source_type=SourceType.PRIMARY,
            source_reliability=SourceReliability.HIGH,
        )

        secondary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0014,
            timestamp=now - timedelta(seconds=60),
            source_type=SourceType.SECONDARY,
            source_reliability=SourceReliability.MEDIUM,
        )

        tertiary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0016,
            timestamp=now - timedelta(seconds=120),
            source_type=SourceType.TERTIARY,
            source_reliability=SourceReliability.LOW,
        )

        # Mock the sources to return our test data
        self.primary_source.return_value = primary_data
        self.secondary_source.return_value = secondary_data
        self.tertiary_source.return_value = tertiary_data

        # Call the public method
        rate, confidence_score = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Verify result
        # The integrated rate should be the reliability-weighted average:
        # = (0.0009 + 0.000336 + 0.00008) / (0.6 + 0.24 + 0.05)
        # = 0.001316 / 0.89 = 0.0014786516...

        assert rate == pytest.approx(0.00147865, abs=1e-7)
        assert confidence_score > 0.7  # Should have high confidence with all sources

        # Verify sources were called
        self.primary_source.assert_called_once_with("hyperliquid", "BTC-PERP")
        self.secondary_source.assert_called_once_with("hyperliquid", "BTC-PERP")
        self.tertiary_source.assert_called_once_with("hyperliquid", "BTC-PERP")

        # Verify the result is cached
        assert ("hyperliquid", "BTC-PERP") in self.provider.funding_cache
        cached_data = self.provider.funding_cache["hyperliquid", "BTC-PERP"]
        assert cached_data.rate == pytest.approx(0.00147865, abs=1e-7)

    @pytest.mark.asyncio
    async def test_get_funding_rate_no_data(self) -> None:
        """Test getting funding rate when no data is available."""
        exchange_name = "hyperliquid"
        symbol_name = "BTC-PERP"
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.secondary_source.side_effect = Exception("Secondary source failed")
        self.tertiary_source.side_effect = Exception("Tertiary source failed")
        self.provider.register_source(
            exchange_name,
            self.primary_source,
            SourceType.PRIMARY,
            SourceReliability.HIGH,
        )
        self.provider.register_source(
            exchange_name,
            self.secondary_source,
            SourceType.SECONDARY,
            SourceReliability.MEDIUM,
        )
        self.provider.register_source(
            exchange_name,
            self.tertiary_source,
            SourceType.TERTIARY,
            SourceReliability.LOW,
        )

        # Expect exception
        with pytest.raises(
            FundingRateSourceError,
            match=f"No funding rate data available for {exchange_name}:{symbol_name}",
        ):
            await self.provider.get_funding_rate(exchange_name, symbol_name)

    # Test cases for _get_specific_source_funding_rate method
