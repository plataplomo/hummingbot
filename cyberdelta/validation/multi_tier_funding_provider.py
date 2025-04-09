"""
Multi-tier funding rate provider implementation.

This module contains the implementation of the multi-tier funding rate
provider, which integrates data from multiple sources and provides
confidence-scored funding rate data.
"""

import logging
import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any, Callable

from .funding_data import (
    SourceType, SourceReliability, FundingData, IntegratedFundingData,
    FundingRateValidationMetrics, ConfidenceFactors, FundingRatePrediction
)

logger = logging.getLogger(__name__)


class FundingRateSourceError(Exception):
    """Exception raised when a funding rate source fails."""
    pass


class MultiTierFundingProvider:
    """
    Funding rate provider that integrates data from multiple sources.
    
    This class implements the multi-tier signal verification mechanism,
    providing funding rate data with confidence scoring based on validation
    metrics, source reliability, and data freshness.
    """
    
    def __init__(self, config: Dict[str, Any], funding_rate_validator=None):
        """
        Initialize the multi-tier funding provider.
        
        Args:
            config: Configuration parameters
            funding_rate_validator: Optional validator for accuracy metrics
        """
        self.config = config
        self.funding_rate_validator = funding_rate_validator
        
        # Cached funding rate data
        self.funding_cache: Dict[Tuple[str, str], IntegratedFundingData] = {}
        
        # Source configuration
        self.primary_source_weight = config.get("primary_source_weight", 0.6)
        self.secondary_source_weight = config.get("secondary_source_weight", 0.3)
        self.tertiary_source_weight = config.get("tertiary_source_weight", 0.1)
        
        # Confidence scoring parameters
        self.max_acceptable_rmse = config.get("max_acceptable_rmse", 0.001)
        self.max_acceptable_bias = config.get("max_acceptable_bias", 0.0005)
        self.max_acceptable_age = config.get("max_acceptable_age", 300.0)  # 5 minutes
        self.default_accuracy_score = config.get("default_accuracy_score", 0.5)
        
        # Weight configuration for confidence scoring
        self.historical_accuracy_weight = config.get("historical_accuracy_weight", 0.4)
        self.source_count_weight = config.get("source_count_weight", 0.2)
        self.dispersion_weight = config.get("dispersion_weight", 0.3)
        self.freshness_weight = config.get("freshness_weight", 0.1)
        
        # Register data sources
        self.primary_sources: Dict[str, Callable] = {}
        self.secondary_sources: Dict[str, Callable] = {}
        self.tertiary_sources: Dict[str, Callable] = {}
        self.fallback_sources: Dict[str, Callable] = {}
        
        logger.info("Initialized multi-tier funding rate provider")
    
    def register_source(self, exchange: str, source_func: Callable, 
                        source_type: SourceType, reliability: SourceReliability) -> None:
        """
        Register a funding rate data source.
        
        Args:
            exchange: Exchange identifier
            source_func: Function that returns funding rate data
            source_type: Type of source (primary, secondary, tertiary, fallback)
            reliability: Reliability category of the source
        """
        source_info = {
            "func": source_func,
            "reliability": reliability
        }
        
        if source_type == SourceType.PRIMARY:
            self.primary_sources[exchange] = source_func
        elif source_type == SourceType.SECONDARY:
            self.secondary_sources[exchange] = source_func
        elif source_type == SourceType.TERTIARY:
            self.tertiary_sources[exchange] = source_func
        elif source_type == SourceType.FALLBACK:
            self.fallback_sources[exchange] = source_func
        
        logger.info(f"Registered {source_type.value} source for {exchange} with {reliability.value} reliability")
    
    async def get_funding_rate(self, exchange: str, symbol: str) -> Tuple[float, float]:
        """
        Get funding rate with confidence score using multi-tier approach.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Tuple of (funding_rate, confidence_score)
            
        Raises:
            FundingRateSourceError: If no funding rate data is available
        """
        # Try to get from cache if not stale
        cache_key = (exchange, symbol)
        if cache_key in self.funding_cache:
            cached_data = self.funding_cache[cache_key]
            if not cached_data.is_stale(self.max_acceptable_age):
                logger.debug(f"Using cached funding rate for {exchange}:{symbol}")
                return cached_data.rate, cached_data.confidence_score
        
        # Try primary source
        try:
            # Get data from different sources
            primary_data = await self._get_primary_funding_rate(exchange, symbol)
            secondary_data = await self._get_secondary_funding_rate(exchange, symbol)
            tertiary_data = await self._get_tertiary_funding_rate(exchange, symbol)
            
            # Integrate data from multiple sources
            integrated_data = self._integrate_funding_data(
                exchange, symbol, primary_data, secondary_data, tertiary_data)
            
            # Calculate confidence score
            confidence_factors = self._calculate_confidence_factors(
                integrated_data, exchange, symbol)
            
            confidence_score = confidence_factors.get_weighted_score(
                historical_weight=self.historical_accuracy_weight,
                source_count_weight=self.source_count_weight,
                dispersion_weight=self.dispersion_weight,
                freshness_weight=self.freshness_weight
            )
            
            # Create complete integrated data
            integrated_data.confidence_score = confidence_score
            
            # Cache the result
            self.funding_cache[cache_key] = integrated_data
            
            logger.debug(f"Got funding rate {integrated_data.rate:.6f} for {exchange}:{symbol} with confidence {confidence_score:.2f}")
            return integrated_data.rate, confidence_score
        
        except Exception as e:
            logger.warning(f"Error getting funding rate: {e}")
            # Fall back to alternative sources
            try:
                fallback_rate, fallback_confidence = await self._get_fallback_funding_rate(exchange, symbol)
                logger.debug(f"Using fallback funding rate {fallback_rate:.6f} for {exchange}:{symbol}")
                return fallback_rate, fallback_confidence
            except Exception as fallback_error:
                logger.error(f"All funding rate sources failed for {exchange}:{symbol}: {fallback_error}")
                raise FundingRateSourceError(f"No funding rate data available for {exchange}:{symbol}")
    
    async def _get_primary_funding_rate(self, exchange: str, symbol: str) -> Optional[FundingData]:
        """
        Get funding rate from primary source.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            FundingData from primary source or None if not available
        """
        if exchange not in self.primary_sources:
            logger.debug(f"No primary source registered for {exchange}")
            return None
        
        try:
            source_func = self.primary_sources[exchange]
            raw_data = await source_func(symbol)
            
            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=raw_data.get("timestamp", datetime.now()),
                source_type=SourceType.PRIMARY,
                source_reliability=SourceReliability.HIGH,
                raw_data=raw_data
            )
            
            return funding_data
        
        except Exception as e:
            logger.warning(f"Primary source for {exchange}:{symbol} failed: {e}")
            return None
    
    async def _get_secondary_funding_rate(self, exchange: str, symbol: str) -> Optional[FundingData]:
        """
        Get funding rate from secondary source.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            FundingData from secondary source or None if not available
        """
        if exchange not in self.secondary_sources:
            logger.debug(f"No secondary source registered for {exchange}")
            return None
        
        try:
            source_func = self.secondary_sources[exchange]
            raw_data = await source_func(symbol)
            
            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=raw_data.get("timestamp", datetime.now()),
                source_type=SourceType.SECONDARY,
                source_reliability=SourceReliability.MEDIUM,
                raw_data=raw_data
            )
            
            return funding_data
        
        except Exception as e:
            logger.warning(f"Secondary source for {exchange}:{symbol} failed: {e}")
            return None
    
    async def _get_tertiary_funding_rate(self, exchange: str, symbol: str) -> Optional[FundingData]:
        """
        Get funding rate from tertiary source.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            FundingData from tertiary source or None if not available
        """
        if exchange not in self.tertiary_sources:
            logger.debug(f"No tertiary source registered for {exchange}")
            return None
        
        try:
            source_func = self.tertiary_sources[exchange]
            raw_data = await source_func(symbol)
            
            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=raw_data.get("timestamp", datetime.now()),
                source_type=SourceType.TERTIARY,
                source_reliability=SourceReliability.LOW,
                raw_data=raw_data
            )
            
            return funding_data
        
        except Exception as e:
            logger.warning(f"Tertiary source for {exchange}:{symbol} failed: {e}")
            return None
    
    async def _get_fallback_funding_rate(self, exchange: str, symbol: str) -> Tuple[float, float]:
        """
        Get funding rate from fallback source.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Tuple of (funding_rate, confidence_score)
            
        Raises:
            FundingRateSourceError: If no fallback source is available
        """
        if exchange not in self.fallback_sources:
            logger.warning(f"No fallback source registered for {exchange}")
            
            # Check if we have cached data, even if stale
            cache_key = (exchange, symbol)
            if cache_key in self.funding_cache:
                cached_data = self.funding_cache[cache_key]
                age = cached_data.get_age()
                
                # Apply age penalty to confidence
                age_factor = max(0.1, 1.0 - (age / (self.max_acceptable_age * 3)))
                adjusted_confidence = cached_data.confidence_score * age_factor
                
                logger.info(f"Using stale cached funding rate for {exchange}:{symbol} with age {age:.0f}s, confidence reduced to {adjusted_confidence:.2f}")
                return cached_data.rate, adjusted_confidence
            
            raise FundingRateSourceError(f"No fallback source for {exchange}")
        
        try:
            source_func = self.fallback_sources[exchange]
            raw_data = await source_func(symbol)
            
            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=raw_data.get("timestamp", datetime.now()),
                source_type=SourceType.FALLBACK,
                source_reliability=SourceReliability.LOW,
                raw_data=raw_data
            )
            
            # Apply confidence penalty for fallback source
            fallback_confidence = 0.3  # Fixed low confidence for fallback source
            
            return funding_data.rate, fallback_confidence
        
        except Exception as e:
            logger.error(f"Fallback source for {exchange}:{symbol} failed: {e}")
            raise FundingRateSourceError(f"All sources failed for {exchange}:{symbol}")
    
    def _integrate_funding_data(
        self, 
        exchange: str,
        symbol: str,
        primary: Optional[FundingData], 
        secondary: Optional[FundingData], 
        tertiary: Optional[FundingData]
    ) -> IntegratedFundingData:
        """
        Integrate funding data from multiple sources.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            primary: Primary source funding data
            secondary: Secondary source funding data
            tertiary: Tertiary source funding data
            
        Returns:
            Integrated funding data with consensus rate
            
        Raises:
            FundingRateSourceError: If no funding data is available
        """
        rates = []
        weights = []
        sources = {}
        
        # Add available rates with appropriate weights
        if primary is not None:
            rates.append(primary.rate)
            weights.append(self.primary_source_weight)
            sources[SourceType.PRIMARY] = primary
        
        if secondary is not None:
            rates.append(secondary.rate)
            weights.append(self.secondary_source_weight)
            sources[SourceType.SECONDARY] = secondary
        
        if tertiary is not None:
            rates.append(tertiary.rate)
            weights.append(self.tertiary_source_weight)
            sources[SourceType.TERTIARY] = tertiary
        
        # If no rates available, raise exception
        if not rates:
            raise FundingRateSourceError(f"No funding data available for {exchange}:{symbol}")
        
        # Calculate weighted average
        weighted_sum = sum(r * w for r, w in zip(rates, weights))
        total_weight = sum(weights)
        
        consensus_rate = weighted_sum / total_weight
        
        # Calculate dispersion (how much sources disagree)
        if len(rates) > 1:
            dispersion = sum(abs(r - consensus_rate) for r in rates) / len(rates)
        else:
            dispersion = 0.0
        
        # Use most recent timestamp
        timestamps = []
        if primary is not None:
            timestamps.append(primary.timestamp)
        if secondary is not None:
            timestamps.append(secondary.timestamp)
        if tertiary is not None:
            timestamps.append(tertiary.timestamp)
        
        latest_timestamp = max(timestamps) if timestamps else datetime.now()
        
        # Create integrated data
        integrated_data = IntegratedFundingData(
            exchange=exchange,
            symbol=symbol,
            rate=consensus_rate,
            timestamp=latest_timestamp,
            dispersion=dispersion,
            sources_count=len(rates),
            primary_available=primary is not None,
            secondary_available=secondary is not None,
            tertiary_available=tertiary is not None,
            confidence_score=0.0,  # Will be set later
            source_data=sources
        )
        
        return integrated_data
    
    def _calculate_confidence_factors(
        self, 
        integrated_data: IntegratedFundingData, 
        exchange: str, 
        symbol: str
    ) -> ConfidenceFactors:
        """
        Calculate confidence factors for funding rate signal.
        
        Args:
            integrated_data: Integrated funding data
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Confidence factors
        """
        # Create confidence factors
        factors = ConfidenceFactors()
        
        # 1. Historical accuracy factor
        factors.historical_accuracy = self._check_historical_accuracy(exchange, symbol)
        
        # 2. Source count factor
        factors.source_count_factor = min(1.0, integrated_data.sources_count / 3)
        
        # 3. Dispersion factor (higher dispersion = lower confidence)
        factors.dispersion_factor = max(0.0, 1.0 - (integrated_data.dispersion * 100))
        
        # 4. Freshness factor
        data_age = (datetime.now() - integrated_data.timestamp).total_seconds()
        factors.freshness_factor = max(0.0, 1.0 - (data_age / self.max_acceptable_age))
        
        return factors
    
    def _check_historical_accuracy(self, exchange: str, symbol: str) -> float:
        """
        Check historical accuracy of funding rate predictions.
        
        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            
        Returns:
            Historical accuracy score (0.0-1.0)
        """
        # Skip if no validator available
        if not self.funding_rate_validator:
            return self.default_accuracy_score
        
        try:
            # Get accuracy metrics
            metrics = self.funding_rate_validator.get_metrics(exchange, symbol, days=7)
            
            if not metrics or metrics.sample_count < 5:
                # Not enough historical data
                return self.default_accuracy_score
            
            # Normalize metrics to 0.0-1.0 scale
            normalized_rmse = min(1.0, metrics.rmse / self.max_acceptable_rmse)
            normalized_bias = min(1.0, abs(metrics.bias) / self.max_acceptable_bias)
            
            # Calculate accuracy score (higher is better)
            accuracy_score = 1.0 - (normalized_rmse * 0.7 + normalized_bias * 0.3)
            
            return max(0.0, accuracy_score)
        
        except Exception as e:
            logger.warning(f"Error checking historical accuracy for {exchange}:{symbol}: {e}")
            return self.default_accuracy_score
    
    def clear_cache(self) -> None:
        """Clear the funding rate cache."""
        self.funding_cache.clear()
        logger.debug("Cleared funding rate cache")
    
    def clear_stale_cache_entries(self, max_age_seconds: Optional[float] = None) -> int:
        """
        Clear stale entries from funding rate cache.
        
        Args:
            max_age_seconds: Maximum acceptable age in seconds (uses
                             configured value if not specified)
                             
        Returns:
            Number of entries cleared
        """
        if max_age_seconds is None:
            max_age_seconds = self.max_acceptable_age
        
        keys_to_clear = []
        for cache_key, integrated_data in self.funding_cache.items():
            if integrated_data.is_stale(max_age_seconds):
                keys_to_clear.append(cache_key)
        
        for key in keys_to_clear:
            del self.funding_cache[key]
        
        logger.debug(f"Cleared {len(keys_to_clear)} stale entries from funding rate cache")
        return len(keys_to_clear) 