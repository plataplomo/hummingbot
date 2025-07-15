"""Unit tests for the multi-tier funding provider module.

Tests multi-tier funding rate provider functionality with confidence scoring.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

# Async functions in tests must be async to match provider interface

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import Mock

import pytest

from cyberdelta.exceptions import (
    AllSourcesFailedError,
    NoFundingDataError,
)
from cyberdelta.validation.funding_data import (
    IntegratedFundingData,
    SourceReliability,
    SourceType,
)
from cyberdelta.validation.multi_tier_funding_provider import (
    FundingRateValidatorProtocol,
    MultiTierFundingProvider,
)


pytestmark = pytest.mark.timing


@pytest.fixture
def base_config() -> dict[str, Any]:
    """Create base configuration for testing."""
    return {
        "primary_source_weight": 0.6,
        "secondary_source_weight": 0.3,
        "tertiary_source_weight": 0.1,
        "max_acceptable_rmse": 0.001,
        "max_acceptable_bias": 0.0005,
        "max_acceptable_age": 300.0,
        "default_accuracy_score": 0.5,
        "cache_ttl_seconds": 300.0,
        "funding_data": {
            "weights": {
                "historical": 0.4,
                "source_count": 0.2,
                "dispersion": 0.3,
                "freshness": 0.1,
            },
            "thresholds": {
                "min_confidence_score": 0.6,
                "max_staleness_hours": 1.0,
                "max_dispersion_std_dev": 0.0005,
                "min_source_count": 2,
            },
        },
    }


@pytest.fixture
def mock_validator() -> Mock:
    """Create mock funding rate validator."""
    validator = Mock(spec=FundingRateValidatorProtocol)
    validator.calculate_metrics.return_value = {
        "rmse": 0.0005,
        "bias": 0.0002,
    }
    return validator


@pytest.fixture
def provider(base_config: dict[str, Any], mock_validator: Mock) -> MultiTierFundingProvider:
    """Create MultiTierFundingProvider instance for testing."""
    return MultiTierFundingProvider(base_config, mock_validator)


class TestMultiTierFundingProviderInit:
    """Test suite for MultiTierFundingProvider initialization."""

    # ==================== SUCCESS CASES ====================

    def test_provider_init_success_basic_config(self, base_config: dict[str, Any]) -> None:
        """Test successful initialization with basic configuration."""
        # Act
        provider = MultiTierFundingProvider(base_config)

        # Assert
        assert provider.config == base_config
        assert provider.funding_rate_validator is None
        assert provider.primary_source_weight == 0.6
        assert provider.secondary_source_weight == 0.3
        assert provider.tertiary_source_weight == 0.1
        assert provider.max_acceptable_rmse == 0.001
        assert provider.max_acceptable_bias == 0.0005
        assert provider.max_acceptable_age == 300.0
        assert provider.default_accuracy_score == 0.5
        assert provider.cache_ttl_seconds == 300.0
        assert provider.funding_cache == {}

    def test_provider_init_success_with_validator(
        self, base_config: dict[str, Any], mock_validator: Mock
    ) -> None:
        """Test successful initialization with validator."""
        # Act
        provider = MultiTierFundingProvider(base_config, mock_validator)

        # Assert
        assert provider.funding_rate_validator == mock_validator
        assert provider.historical_accuracy_weight == 0.4
        assert provider.source_count_weight == 0.2
        assert provider.dispersion_weight == 0.3
        assert provider.freshness_weight == 0.1

    def test_provider_init_success_with_custom_weights(self, base_config: dict[str, Any]) -> None:
        """Test initialization with custom weight configuration."""
        # Arrange
        base_config["funding_data"]["weights"] = {
            "historical": 0.5,
            "source_count": 0.25,
            "dispersion": 0.2,
            "freshness": 0.05,
        }

        # Act
        provider = MultiTierFundingProvider(base_config)

        # Assert
        assert provider.historical_accuracy_weight == 0.5
        assert provider.source_count_weight == 0.25
        assert provider.dispersion_weight == 0.2
        assert provider.freshness_weight == 0.05

    # ==================== EDGE CASES ====================

    def test_provider_init_edge_empty_config(self) -> None:
        """Test initialization with empty configuration."""
        # Act
        provider = MultiTierFundingProvider({})

        # Assert - All default configuration values
        assert provider.primary_source_weight == 0.6  # Default
        assert provider.secondary_source_weight == 0.3  # Default
        assert provider.tertiary_source_weight == 0.1  # Default
        assert provider.max_acceptable_rmse == 0.001  # Default
        assert provider.max_acceptable_bias == 0.0005  # Default
        assert provider.max_acceptable_age == 300.0  # Default
        assert provider.default_accuracy_score == 0.5  # Default
        assert provider.cache_ttl_seconds == 300.0  # Default
        assert provider.historical_accuracy_weight == 0.4  # Default
        assert provider.source_count_weight == 0.2  # Default
        assert provider.dispersion_weight == 0.3  # Default
        assert provider.freshness_weight == 0.1  # Default

    def test_provider_init_edge_invalid_weights_config(self, base_config: dict[str, Any]) -> None:
        """Test initialization with invalid weights configuration."""
        # Arrange
        base_config["funding_data"]["weights"] = "invalid_config"

        # Act
        provider = MultiTierFundingProvider(base_config)

        # Assert - Should use defaults
        assert provider.historical_accuracy_weight == 0.4
        assert provider.source_count_weight == 0.2
        assert provider.dispersion_weight == 0.3
        assert provider.freshness_weight == 0.1

    def test_provider_init_edge_invalid_thresholds_config(
        self, base_config: dict[str, Any]
    ) -> None:
        """Test initialization with invalid thresholds configuration."""
        # Arrange
        base_config["funding_data"]["thresholds"] = ["invalid", "config"]

        # Act
        provider = MultiTierFundingProvider(base_config)

        # Assert - Should use defaults
        assert provider.min_confidence_score == 0.6
        assert provider.max_staleness_seconds == 3600.0  # 1 hour default
        assert provider.max_dispersion_std_dev == 0.0005
        assert provider.min_source_count == 2


class TestRegisterSource:
    """Test suite for register_source method."""

    # ==================== SUCCESS CASES ====================

    def test_register_source_success_primary(self, provider: MultiTierFundingProvider) -> None:
        """Test successful registration of primary source."""

        # Arrange
        async def mock_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        # Act
        provider.register_source(
            "hyperliquid", mock_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Assert
        assert "hyperliquid" in provider.primary_sources
        assert provider.primary_sources["hyperliquid"] == mock_source

    def test_register_source_success_secondary(self, provider: MultiTierFundingProvider) -> None:
        """Test successful registration of secondary source."""

        # Arrange
        async def mock_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002, "timestamp": datetime.now(UTC)}

        # Act
        provider.register_source(
            "backpack", mock_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Assert
        assert "backpack" in provider.secondary_sources
        assert provider.secondary_sources["backpack"] == mock_source

    def test_register_source_success_tertiary(self, provider: MultiTierFundingProvider) -> None:
        """Test successful registration of tertiary source."""

        # Arrange
        async def mock_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0003, "timestamp": datetime.now(UTC)}

        # Act
        provider.register_source("binance", mock_source, SourceType.TERTIARY, SourceReliability.LOW)

        # Assert
        assert "binance" in provider.tertiary_sources
        assert provider.tertiary_sources["binance"] == mock_source

    def test_register_source_success_fallback(self, provider: MultiTierFundingProvider) -> None:
        """Test successful registration of fallback source."""

        # Arrange
        async def mock_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0004, "timestamp": datetime.now(UTC)}

        # Act
        provider.register_source(
            "coinbase", mock_source, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Assert
        assert "coinbase" in provider.fallback_sources
        assert provider.fallback_sources["coinbase"] == mock_source

    def test_register_source_success_multiple_sources(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test registration of multiple sources."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001}

        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002}

        # Act
        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "backpack", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Assert
        assert len(provider.primary_sources) == 1
        assert len(provider.secondary_sources) == 1
        assert "hyperliquid" in provider.primary_sources
        assert "backpack" in provider.secondary_sources

    # ==================== EDGE CASES ====================

    def test_register_source_edge_replace_existing(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test replacing an existing source."""

        # Arrange
        async def original_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001}

        async def replacement_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002}

        # Register original
        provider.register_source(
            "hyperliquid", original_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Replace with new source
        provider.register_source(
            "hyperliquid", replacement_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Assert
        assert provider.primary_sources["hyperliquid"] == replacement_source
        assert provider.primary_sources["hyperliquid"] != original_source


class TestGetFundingRate:
    """Test suite for get_funding_rate method."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_funding_rate_success_fresh_cache(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful retrieval from fresh cache."""
        # Arrange
        cache_data = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0001,
            timestamp=datetime.now(UTC),
            dispersion=0.00001,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.85,
            source_data={},
        )
        provider.funding_cache["hyperliquid", "BTC-PERP"] = cache_data

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert rate == 0.0001
        assert confidence == 0.85

    @pytest.mark.asyncio
    async def test_get_funding_rate_success_with_integration(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful retrieval with data integration."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.00015, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "backpack", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(rate, float)
        assert isinstance(confidence, float)
        assert 0 <= confidence <= 1
        assert rate > 0

    @pytest.mark.asyncio
    async def test_get_funding_rate_success_stale_cache_adjustment(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test retrieval with stale cache and confidence adjustment."""
        # Arrange
        old_timestamp = datetime.now(UTC) - timedelta(seconds=500)  # Stale data
        cache_data = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0001,
            timestamp=old_timestamp,
            dispersion=0.00001,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.85,
            source_data={},
        )
        provider.funding_cache["hyperliquid", "BTC-PERP"] = cache_data

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert rate == 0.0001
        assert confidence < 0.85  # Should be reduced due to staleness

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_funding_rate_edge_fallback_used(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test fallback source usage when primary sources fail."""

        # Arrange
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary source failed")

        async def fallback_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0005, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "hyperliquid", fallback_source, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert rate == 0.0005
        assert confidence < 0.5  # Fallback should have low confidence

    @pytest.mark.asyncio
    async def test_get_funding_rate_edge_partial_source_failure(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test handling when some sources fail."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        async def failing_secondary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise KeyError("Secondary source failed")

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "backpack", failing_secondary, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(rate, float)
        assert isinstance(confidence, float)
        assert rate == 0.0001  # Should match the primary source rate
        assert 0.0 <= confidence <= 1.0
        # Should succeed with just primary source despite secondary failure

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_get_funding_rate_failure_no_sources(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test failure when no sources are available."""
        # Act & Assert
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("nonexistent", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_funding_rate_failure_all_sources_fail(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test failure when all sources fail."""

        # Arrange
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def failing_fallback(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise RuntimeError("Fallback failed")

        provider.register_source(
            "hyperliquid", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "hyperliquid", failing_fallback, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act & Assert
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("hyperliquid", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_funding_rate_failure_no_fallback_source(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test failure when no fallback source is registered."""

        # Arrange
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        provider.register_source(
            "hyperliquid", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        # No fallback source registered

        # Act & Assert
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("hyperliquid", "BTC-PERP")


class TestGetPrimaryFundingRate:
    """Test suite for primary funding rate functionality through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_success(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful primary funding rate retrieval through public API."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API instead of private method
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Test the public API result
        assert rate is not None
        assert rate == 0.0001
        assert confidence is not None
        assert 0.0 <= confidence <= 1.0

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_success_with_naive_timestamp(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test primary funding rate with naive timestamp conversion."""
        # Arrange
        # Create intentionally naive timestamp for testing timezone conversion
        naive_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": naive_timestamp}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API instead of private method
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Test that we get valid rate and confidence (timestamp handling is internal)
        assert rate is not None
        assert rate == 0.0001
        assert confidence is not None
        assert 0.0 <= confidence <= 1.0

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_success_with_timestamp_conversion(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test primary funding rate with timestamp conversion from milliseconds."""
        # Arrange
        timestamp_ms = int(datetime.now(UTC).timestamp() * 1000)

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": timestamp_ms}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API instead of private method
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Test timestamp handling is done correctly (internal behavior)
        assert rate is not None
        assert rate == 0.0001
        assert confidence is not None
        assert 0.0 <= confidence <= 1.0

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_edge_no_source_registered(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test funding rate behavior when no source is registered through public API."""
        # Act - Use public API which handles missing sources internally
        with pytest.raises((NoFundingDataError, AllSourcesFailedError)):
            await provider.get_funding_rate("nonexistent", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_edge_invalid_timestamp(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test primary funding rate with invalid timestamp."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": "invalid_timestamp"}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API instead of private method
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Verify that invalid timestamp is handled gracefully
        assert rate is not None
        assert rate == 0.0001
        assert confidence is not None
        assert 0.0 <= confidence <= 1.0

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_failure_source_exception(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test primary funding rate when source raises exception."""

        # Arrange
        async def failing_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Source failed")

        provider.register_source(
            "hyperliquid", failing_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API which handles source failures internally
        with pytest.raises((AllSourcesFailedError, NoFundingDataError)):
            await provider.get_funding_rate("hyperliquid", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_primary_funding_rate_failure_missing_data(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test primary funding rate with missing required data."""

        # Arrange
        async def incomplete_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {}  # Missing rate and timestamp

        provider.register_source(
            "hyperliquid", incomplete_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act - Use public API to test handling of incomplete data
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Verify default handling works correctly
        assert rate is not None
        assert rate == 0.0  # Default value
        assert confidence is not None
        assert 0.0 <= confidence <= 1.0


class TestGetSecondaryFundingRate:
    """Test suite for secondary funding rate functionality through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_secondary_funding_rate_success(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful secondary funding rate retrieval through get_funding_rate."""

        # Arrange - Set up secondary source that should be used when primary fails
        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.00015, "timestamp": datetime.now(UTC)}

        # Register only secondary source for backpack (no primary)
        provider.register_source(
            "backpack", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Act - Use public method which internally calls _get_secondary_funding_rate
        rate, confidence = await provider.get_funding_rate("backpack", "BTC-PERP")

        # Assert - Verify secondary source was used (rate matches our mock)
        assert rate == 0.00015
        assert confidence > 0  # Should have some confidence from secondary source

        # Verify the data is cached properly (secondary source behavior)
        cached_rate, cached_confidence = await provider.get_funding_rate("backpack", "BTC-PERP")
        assert cached_rate == rate
        assert cached_confidence == confidence

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_secondary_funding_rate_edge_no_source_registered(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test secondary funding rate when no source is registered through get_funding_rate."""
        # Act - Try to get funding rate for exchange with no sources
        # This should raise an exception when no sources are available
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("nonexistent", "BTC-PERP")

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_get_secondary_funding_rate_failure_source_exception(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test secondary funding rate when source raises exception through get_funding_rate."""

        # Arrange
        async def failing_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise TypeError("Source failed")

        # Register only the failing secondary source
        provider.register_source(
            "backpack", failing_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )

        # Act - When secondary source fails and no other sources, should raise exception
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("backpack", "BTC-PERP")


class TestGetTertiaryFundingRate:
    """Test suite for tertiary funding rate functionality through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_tertiary_funding_rate_success(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful tertiary funding rate retrieval through get_funding_rate."""

        # Arrange - Set up tertiary source that should be used when primary/secondary fail
        async def tertiary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002, "timestamp": datetime.now(UTC)}

        # Register only tertiary source for binance (no primary/secondary)
        provider.register_source(
            "binance", tertiary_source, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act - Use public method which internally calls _get_tertiary_funding_rate
        rate, confidence = await provider.get_funding_rate("binance", "BTC-PERP")

        # Assert - Verify tertiary source was used (rate matches our mock)
        assert rate == 0.0002
        assert confidence > 0  # Should have some confidence from tertiary source

        # Verify the data is cached properly (tertiary source behavior)
        cached_rate, cached_confidence = await provider.get_funding_rate("binance", "BTC-PERP")
        assert cached_rate == rate
        assert cached_confidence == confidence

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_tertiary_funding_rate_edge_no_source_registered(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test tertiary funding rate when no source is registered through get_funding_rate."""
        # Act - Try to get funding rate for exchange with no sources
        # This should raise an exception when no sources are available
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("nonexistent", "BTC-PERP")

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_get_tertiary_funding_rate_failure_source_exception(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test tertiary funding rate when source raises exception through get_funding_rate."""

        # Arrange
        async def failing_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise AttributeError("Source failed")

        # Register only the failing tertiary source
        provider.register_source(
            "binance", failing_source, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act - When tertiary source fails and no other sources, should raise exception
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("binance", "BTC-PERP")


class TestGetFallbackFundingRate:
    """Test suite for fallback funding rate functionality through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_get_fallback_funding_rate_success(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful fallback funding rate retrieval through get_funding_rate."""

        # Arrange - Set up fallback source that should be used when primary fails
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def fallback_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0005, "timestamp": datetime.now(UTC)}

        # Register failing primary and working fallback
        provider.register_source(
            "coinbase", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "coinbase", fallback_source, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act - Use public method which should fall back to fallback source
        rate, confidence = await provider.get_funding_rate("coinbase", "BTC-PERP")

        # Assert - Verify fallback source was used
        assert isinstance(rate, float)
        assert isinstance(confidence, float)
        assert rate == 0.0005
        assert 0 <= confidence <= 1

    @pytest.mark.asyncio
    async def test_get_fallback_funding_rate_success_with_age_adjustment(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test fallback funding rate with age-based confidence adjustment."""
        # Arrange - Set up fallback with old data
        old_timestamp = datetime.now(UTC) - timedelta(seconds=600)  # Old data

        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def fallback_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0005, "timestamp": old_timestamp}

        # Register failing primary and fallback with old data
        provider.register_source(
            "coinbase", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "coinbase", fallback_source, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act - Use public method which should use fallback source
        rate, confidence = await provider.get_funding_rate("coinbase", "BTC-PERP")

        # Assert - Verify age adjustment is applied
        assert rate == 0.0005
        assert confidence < 0.2  # Should be reduced due to age

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_get_fallback_funding_rate_edge_naive_timestamp(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test fallback funding rate with naive timestamp through get_funding_rate."""
        # Arrange - Set up fallback with naive timestamp
        # Create intentionally naive timestamp for testing timezone conversion
        naive_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)

        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def fallback_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0005, "timestamp": naive_timestamp}

        # Register failing primary and fallback with naive timestamp
        provider.register_source(
            "coinbase", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "coinbase", fallback_source, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act - Use public method which should use fallback source
        rate, confidence = await provider.get_funding_rate("coinbase", "BTC-PERP")

        # Assert - Verify timestamp handling works
        assert isinstance(rate, float)
        assert isinstance(confidence, float)

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_get_fallback_funding_rate_failure_no_source(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test fallback funding rate when no fallback source is registered."""
        # Act & Assert - When no sources are registered at all, should raise NoFundingDataError
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("nonexistent", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_get_fallback_funding_rate_failure_source_exception(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test fallback funding rate when source raises exception through get_funding_rate."""

        # Arrange - Set up primary and fallback that both fail
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def failing_fallback(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Fallback failed")

        # Register both failing sources
        provider.register_source(
            "coinbase", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "coinbase", failing_fallback, SourceType.FALLBACK, SourceReliability.LOW
        )

        # Act & Assert - Should raise exception when all sources fail
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("coinbase", "BTC-PERP")


class TestIntegrateFundingData:
    """Test suite for funding data integration through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_integrate_funding_data_success_all_sources(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful integration with all source types through get_funding_rate."""

        # Arrange - Create sources that return different rates
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.00015, "timestamp": datetime.now(UTC)}

        async def tertiary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002, "timestamp": datetime.now(UTC)}

        # Register all sources for the same exchange
        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "hyperliquid", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )
        provider.register_source(
            "hyperliquid", tertiary_source, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(rate, float)
        assert rate > 0
        assert isinstance(confidence, float)
        assert 0 <= confidence <= 1

        # Verify cached result has integrated data
        cache_key = ("hyperliquid", "BTC-PERP")
        assert cache_key in provider.funding_cache
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 3
        assert cached_data.primary_available is True
        assert cached_data.secondary_available is True
        assert cached_data.tertiary_available is True

    @pytest.mark.asyncio
    async def test_integrate_funding_data_success_primary_only(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful integration with primary source only through get_funding_rate."""

        # Arrange - Create only primary source
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert rate == 0.0001
        assert isinstance(confidence, float)

        # Verify cached result has correct integration
        cache_key = ("hyperliquid", "BTC-PERP")
        assert cache_key in provider.funding_cache
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 1
        assert cached_data.primary_available is True
        assert cached_data.secondary_available is False
        assert cached_data.tertiary_available is False
        assert cached_data.dispersion == 0.0  # No dispersion with single source

    @pytest.mark.asyncio
    async def test_integrate_funding_data_success_with_naive_timestamps(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test integration with naive timestamps through get_funding_rate."""
        # Arrange
        # Create intentionally naive timestamp for testing timezone conversion
        naive_timestamp = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC).replace(tzinfo=None)

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": naive_timestamp}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        rate, _confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(rate, float)
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.timestamp.tzinfo == UTC

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_integrate_funding_data_edge_mixed_availability(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test integration with mixed source availability through get_funding_rate."""

        # Arrange - Create only secondary and tertiary sources (no primary)
        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.00015, "timestamp": datetime.now(UTC)}

        async def tertiary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002, "timestamp": datetime.now(UTC)}

        # Register only secondary and tertiary sources
        provider.register_source(
            "hyperliquid", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )
        provider.register_source(
            "hyperliquid", tertiary_source, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act
        rate, _confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(rate, float)
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 2
        assert cached_data.primary_available is False
        assert cached_data.secondary_available is True
        assert cached_data.tertiary_available is True

    # ==================== FAILURE CASES ====================

    @pytest.mark.asyncio
    async def test_integrate_funding_data_failure_no_valid_sources(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test integration failure when no valid sources are available through get_funding_rate."""

        # Arrange - Create all failing sources
        async def failing_primary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Primary failed")

        async def failing_secondary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Secondary failed")

        async def failing_tertiary(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            raise ValueError("Tertiary failed")

        # Register all failing sources
        provider.register_source(
            "hyperliquid", failing_primary, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "hyperliquid", failing_secondary, SourceType.SECONDARY, SourceReliability.MEDIUM
        )
        provider.register_source(
            "hyperliquid", failing_tertiary, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act & Assert
        with pytest.raises(NoFundingDataError):
            await provider.get_funding_rate("hyperliquid", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_integrate_funding_data_success_minimal_data(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test integration succeeds with minimal valid data through get_funding_rate."""

        # Arrange - Create only one working source
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        rate, _confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Should succeed with at least one valid source
        assert rate == 0.0001
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 1


class TestCalculateConfidenceFactors:
    """Test suite for confidence factors calculation through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_calculate_confidence_factors_success(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test successful confidence factors calculation through get_funding_rate."""

        # Arrange
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        _rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert
        assert isinstance(confidence, float)
        assert 0 <= confidence <= 1
        # Verify that confidence factors were calculated and applied
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.confidence_score == confidence

    @pytest.mark.asyncio
    async def test_calculate_confidence_factors_success_fresh_data(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test confidence factors with fresh data through get_funding_rate."""

        # Arrange fresh data from all sources
        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        async def secondary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.00015, "timestamp": datetime.now(UTC)}

        async def tertiary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0002, "timestamp": datetime.now(UTC)}

        # Register all sources
        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )
        provider.register_source(
            "hyperliquid", secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM
        )
        provider.register_source(
            "hyperliquid", tertiary_source, SourceType.TERTIARY, SourceReliability.LOW
        )

        # Act
        _rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Fresh data with all sources should give high confidence
        assert isinstance(confidence, float)
        assert confidence > 0.5  # Should be relatively high with all sources and fresh data
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 3  # All sources used

    # ==================== EDGE CASES ====================

    @pytest.mark.asyncio
    async def test_calculate_confidence_factors_edge_old_data(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test confidence factors with old data through get_funding_rate."""
        # Arrange old data from single source
        old_timestamp = datetime.now(UTC) - timedelta(seconds=1000)  # Very old

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": old_timestamp}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        _rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Old data with single source should give lower confidence
        assert isinstance(confidence, float)
        assert confidence < 0.7  # Should be reduced for old data and single source
        cache_key = ("hyperliquid", "BTC-PERP")
        cached_data = provider.funding_cache[cache_key]
        assert cached_data.sources_count == 1  # Single source used


class TestHistoricalAccuracyIntegration:
    """Test suite for historical accuracy integration through public API."""

    # ==================== SUCCESS CASES ====================

    @pytest.mark.asyncio
    async def test_historical_accuracy_with_validator(
        self, provider: MultiTierFundingProvider, mock_validator: Mock
    ) -> None:
        """Test that validator is used in confidence calculation through get_funding_rate."""
        # Arrange
        mock_validator.calculate_metrics.return_value = {
            "rmse": 0.0005,
            "bias": 0.0002,
        }

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        _rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Validator should have been called during confidence calculation
        assert isinstance(confidence, float)
        assert 0 <= confidence <= 1
        mock_validator.calculate_metrics.assert_called_once_with("hyperliquid", "BTC-PERP")

    @pytest.mark.asyncio
    async def test_historical_accuracy_no_validator(self, base_config: dict[str, Any]) -> None:
        """Test confidence calculation without validator through get_funding_rate."""
        # Arrange
        provider = MultiTierFundingProvider(base_config, None)

        async def primary_source(_symbol: str) -> dict[str, Any]:
            await asyncio.sleep(0)
            return {"rate": 0.0001, "timestamp": datetime.now(UTC)}

        provider.register_source(
            "hyperliquid", primary_source, SourceType.PRIMARY, SourceReliability.HIGH
        )

        # Act
        _rate, confidence = await provider.get_funding_rate("hyperliquid", "BTC-PERP")

        # Assert - Should still work without validator
        assert isinstance(confidence, float)
        assert 0 <= confidence <= 1


class TestCacheManagement:
    """Test suite for cache management methods."""

    # ==================== SUCCESS CASES ====================

    def test_clear_cache_success(self, provider: MultiTierFundingProvider) -> None:
        """Test successful cache clearing."""
        # Arrange
        cache_data = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0001,
            timestamp=datetime.now(UTC),
            dispersion=0.00001,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.85,
            source_data={},
        )
        provider.funding_cache["hyperliquid", "BTC-PERP"] = cache_data

        # Act
        provider.clear_cache()

        # Assert
        assert len(provider.funding_cache) == 0

    def test_clear_stale_cache_entries_success(self, provider: MultiTierFundingProvider) -> None:
        """Test successful clearing of stale cache entries."""
        # Arrange
        fresh_data = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0001,
            timestamp=datetime.now(UTC),
            dispersion=0.00001,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.85,
            source_data={},
        )
        stale_data = IntegratedFundingData(
            exchange="backpack",
            symbol="ETH-PERP",
            rate=0.0002,
            timestamp=datetime.now(UTC) - timedelta(seconds=600),  # Stale
            dispersion=0.00002,
            sources_count=1,
            primary_available=True,
            secondary_available=False,
            tertiary_available=False,
            confidence_score=0.75,
            source_data={},
        )
        provider.funding_cache["hyperliquid", "BTC-PERP"] = fresh_data
        provider.funding_cache["backpack", "ETH-PERP"] = stale_data

        # Act
        cleared_count = provider.clear_stale_cache_entries(max_age_seconds=300)

        # Assert
        assert cleared_count == 1
        assert len(provider.funding_cache) == 1
        assert ("hyperliquid", "BTC-PERP") in provider.funding_cache

    # ==================== EDGE CASES ====================

    def test_clear_stale_cache_entries_edge_no_stale_entries(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test clearing stale entries when none are stale."""
        # Arrange
        fresh_data = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0001,
            timestamp=datetime.now(UTC),
            dispersion=0.00001,
            sources_count=2,
            primary_available=True,
            secondary_available=True,
            tertiary_available=False,
            confidence_score=0.85,
            source_data={},
        )
        provider.funding_cache["hyperliquid", "BTC-PERP"] = fresh_data

        # Act
        cleared_count = provider.clear_stale_cache_entries()

        # Assert
        assert cleared_count == 0
        assert len(provider.funding_cache) == 1

    def test_clear_stale_cache_entries_edge_empty_cache(
        self, provider: MultiTierFundingProvider
    ) -> None:
        """Test clearing stale entries from empty cache."""
        # Act
        cleared_count = provider.clear_stale_cache_entries()

        # Assert
        assert cleared_count == 0
        assert len(provider.funding_cache) == 0


# Configuration validation is tested through the init tests and public API behavior


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("source_type", "reliability", "expected_attribute"),
    [
        (SourceType.PRIMARY, SourceReliability.HIGH, "primary_sources"),
        (SourceType.SECONDARY, SourceReliability.MEDIUM, "secondary_sources"),
        (SourceType.TERTIARY, SourceReliability.LOW, "tertiary_sources"),
        (SourceType.FALLBACK, SourceReliability.LOW, "fallback_sources"),
    ],
)
def test_register_source_parametrized(
    provider: MultiTierFundingProvider,
    source_type: SourceType,
    reliability: SourceReliability,
    expected_attribute: str,
) -> None:
    """Test source registration with various types and reliabilities."""

    # Arrange
    async def mock_source(_symbol: str) -> dict[str, Any]:
        await asyncio.sleep(0)
        return {"rate": 0.0001}

    # Act
    provider.register_source("test_exchange", mock_source, source_type, reliability)

    # Assert
    source_dict = getattr(provider, expected_attribute)
    assert "test_exchange" in source_dict
    assert source_dict["test_exchange"] == mock_source


# Parametrized config validation tests removed - private implementation details


# Parametrized confidence factors test removed - private implementation details
