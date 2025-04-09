"""
Tests for the multi-tier funding rate provider.
"""

import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timedelta

import pytest

from cyberdelta.validation.funding_data import (
    SourceType, SourceReliability, FundingData, IntegratedFundingData,
    FundingRateValidationMetrics
)
from cyberdelta.validation.multi_tier_funding_provider import (
    MultiTierFundingProvider, FundingRateSourceError
)


class TestMultiTierFundingProvider(unittest.TestCase):
    """Tests for the MultiTierFundingProvider class."""
    
    def setUp(self):
        """Set up test environment."""
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
            "freshness_weight": 0.1
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
            period_days=7
        )
        self.mock_validator.get_metrics.return_value = mock_metrics
        
        # Create the provider
        self.provider = MultiTierFundingProvider(self.config, self.mock_validator)
        
        # Create mock source functions
        self.primary_source = AsyncMock()
        self.primary_source.return_value = {
            "rate": 0.0015,
            "timestamp": datetime.now()
        }
        
        self.secondary_source = AsyncMock()
        self.secondary_source.return_value = {
            "rate": 0.0014,
            "timestamp": datetime.now()
        }
        
        self.tertiary_source = AsyncMock()
        self.tertiary_source.return_value = {
            "rate": 0.0016,
            "timestamp": datetime.now()
        }
        
        self.fallback_source = AsyncMock()
        self.fallback_source.return_value = {
            "rate": 0.0013,
            "timestamp": datetime.now()
        }
    
    def test_register_source(self):
        """Test registering data sources."""
        # Register all source types
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        self.provider.register_source(
            "hyperliquid", self.secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM)
        self.provider.register_source(
            "hyperliquid", self.tertiary_source, SourceType.TERTIARY, SourceReliability.LOW)
        self.provider.register_source(
            "hyperliquid", self.fallback_source, SourceType.FALLBACK, SourceReliability.LOW)
        
        # Verify sources were registered
        self.assertEqual(self.provider.primary_sources["hyperliquid"], self.primary_source)
        self.assertEqual(self.provider.secondary_sources["hyperliquid"], self.secondary_source)
        self.assertEqual(self.provider.tertiary_sources["hyperliquid"], self.tertiary_source)
        self.assertEqual(self.provider.fallback_sources["hyperliquid"], self.fallback_source)
    
    @pytest.mark.asyncio
    async def test_get_funding_rate_all_sources(self):
        """Test getting funding rate with all sources available."""
        # Register all source types
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        self.provider.register_source(
            "hyperliquid", self.secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM)
        self.provider.register_source(
            "hyperliquid", self.tertiary_source, SourceType.TERTIARY, SourceReliability.LOW)
        
        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")
        
        # Verify sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        self.tertiary_source.assert_called_once_with("BTC-PERP")
        
        # Verify result
        self.assertAlmostEqual(rate, 0.00149, places=5)  # Weighted average
        self.assertGreater(confidence, 0.7)  # Should be high with all sources
    
    @pytest.mark.asyncio
    async def test_get_funding_rate_primary_only(self):
        """Test getting funding rate with only primary source."""
        # Register only primary source
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        
        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")
        
        # Verify only primary source was called
        self.primary_source.assert_called_once_with("BTC-PERP")
        
        # Verify result
        self.assertAlmostEqual(rate, 0.0015, places=5)
        self.assertGreater(confidence, 0.5)  # Should be moderate with only primary
    
    @pytest.mark.asyncio
    async def test_get_funding_rate_primary_fails(self):
        """Test getting funding rate when primary source fails."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        self.provider.register_source(
            "hyperliquid", self.secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM)
        
        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")
        
        # Verify primary and secondary sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        
        # Verify result uses secondary
        self.assertAlmostEqual(rate, 0.0014, places=5)
        self.assertLess(confidence, 0.7)  # Should be lower without primary
    
    @pytest.mark.asyncio
    async def test_get_funding_rate_all_fail(self):
        """Test getting funding rate when all regular sources fail."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.secondary_source.side_effect = Exception("Secondary source failed")
        self.tertiary_source.side_effect = Exception("Tertiary source failed")
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        self.provider.register_source(
            "hyperliquid", self.secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM)
        self.provider.register_source(
            "hyperliquid", self.tertiary_source, SourceType.TERTIARY, SourceReliability.LOW)
        self.provider.register_source(
            "hyperliquid", self.fallback_source, SourceType.FALLBACK, SourceReliability.LOW)
        
        # Get funding rate
        rate, confidence = await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")
        
        # Verify all sources were called
        self.primary_source.assert_called_once_with("BTC-PERP")
        self.secondary_source.assert_called_once_with("BTC-PERP")
        self.tertiary_source.assert_called_once_with("BTC-PERP")
        self.fallback_source.assert_called_once_with("BTC-PERP")
        
        # Verify result uses fallback
        self.assertAlmostEqual(rate, 0.0013, places=5)
        self.assertAlmostEqual(confidence, 0.3, places=1)  # Low confidence for fallback
    
    @pytest.mark.asyncio
    async def test_get_funding_rate_all_fail_no_fallback(self):
        """Test getting funding rate when all sources fail and no fallback."""
        # Register sources
        self.primary_source.side_effect = Exception("Primary source failed")
        self.secondary_source.side_effect = Exception("Secondary source failed")
        self.tertiary_source.side_effect = Exception("Tertiary source failed")
        self.provider.register_source(
            "hyperliquid", self.primary_source, SourceType.PRIMARY, SourceReliability.HIGH)
        self.provider.register_source(
            "hyperliquid", self.secondary_source, SourceType.SECONDARY, SourceReliability.MEDIUM)
        self.provider.register_source(
            "hyperliquid", self.tertiary_source, SourceType.TERTIARY, SourceReliability.LOW)
        
        # Expect exception
        with self.assertRaises(FundingRateSourceError):
            await self.provider.get_funding_rate("hyperliquid", "BTC-PERP")
    
    def test_clear_cache(self):
        """Test clearing the funding rate cache."""
        # Add some data to cache
        self.provider.funding_cache[("hyperliquid", "BTC-PERP")] = IntegratedFundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0015,
            timestamp=datetime.now(),
            dispersion=0.0001,
            sources_count=3,
            primary_available=True,
            secondary_available=True,
            tertiary_available=True,
            confidence_score=0.8
        )
        
        # Verify cache has data
        self.assertEqual(len(self.provider.funding_cache), 1)
        
        # Clear cache
        self.provider.clear_cache()
        
        # Verify cache is empty
        self.assertEqual(len(self.provider.funding_cache), 0)
    
    def test_clear_stale_cache_entries(self):
        """Test clearing stale entries from funding rate cache."""
        # Add some data to cache
        now = datetime.now()
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
            confidence_score=0.8
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
            confidence_score=0.7
        )
        
        self.provider.funding_cache[("hyperliquid", "BTC-PERP")] = fresh_entry
        self.provider.funding_cache[("hyperliquid", "ETH-PERP")] = stale_entry
        
        # Verify cache has data
        self.assertEqual(len(self.provider.funding_cache), 2)
        
        # Clear stale entries
        cleared = self.provider.clear_stale_cache_entries(300.0)  # 5 minutes max age
        
        # Verify stale entry was cleared
        self.assertEqual(cleared, 1)
        self.assertEqual(len(self.provider.funding_cache), 1)
        self.assertIn(("hyperliquid", "BTC-PERP"), self.provider.funding_cache)
        self.assertNotIn(("hyperliquid", "ETH-PERP"), self.provider.funding_cache)
    
    def test_integrate_funding_data(self):
        """Test integrating funding data from multiple sources."""
        # Create test data
        now = datetime.now()
        primary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0015,
            timestamp=now,
            source_type=SourceType.PRIMARY,
            source_reliability=SourceReliability.HIGH
        )
        
        secondary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0014,
            timestamp=now - timedelta(seconds=60),
            source_type=SourceType.SECONDARY,
            source_reliability=SourceReliability.MEDIUM
        )
        
        tertiary_data = FundingData(
            exchange="hyperliquid",
            symbol="BTC-PERP",
            rate=0.0016,
            timestamp=now - timedelta(seconds=120),
            source_type=SourceType.TERTIARY,
            source_reliability=SourceReliability.LOW
        )
        
        # Integrate data
        integrated = self.provider._integrate_funding_data(
            "hyperliquid", "BTC-PERP", primary_data, secondary_data, tertiary_data)
        
        # Verify result
        self.assertEqual(integrated.exchange, "hyperliquid")
        self.assertEqual(integrated.symbol, "BTC-PERP")
        self.assertAlmostEqual(integrated.rate, 0.00149, places=5)  # Weighted average
        self.assertEqual(integrated.timestamp, now)  # Most recent timestamp
        self.assertGreater(integrated.dispersion, 0.0)
        self.assertEqual(integrated.sources_count, 3)
        self.assertTrue(integrated.primary_available)
        self.assertTrue(integrated.secondary_available)
        self.assertTrue(integrated.tertiary_available)
        
        # Check source data
        self.assertEqual(len(integrated.source_data), 3)
        self.assertEqual(integrated.source_data[SourceType.PRIMARY], primary_data)
        self.assertEqual(integrated.source_data[SourceType.SECONDARY], secondary_data)
        self.assertEqual(integrated.source_data[SourceType.TERTIARY], tertiary_data) 