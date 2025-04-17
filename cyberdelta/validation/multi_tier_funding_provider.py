"""
Multi-tier funding rate provider implementation.

This module contains the implementation of the multi-tier funding rate
provider, which integrates data from multiple sources and provides
confidence-scored funding rate data.
"""

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from .funding_data import (
    ConfidenceFactors,
    FundingData,
    IntegratedFundingData,
    SourceReliability,
    SourceType,
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

    def __init__(self, config: dict[str, Any], funding_rate_validator: Any | None = None) -> None:
        """
        Initialize the multi-tier funding provider.

        Args:
            config: Configuration parameters
            funding_rate_validator: Optional validator for accuracy metrics
        """
        self.config = config
        self.funding_rate_validator = funding_rate_validator

        # Cached funding rate data
        self.funding_cache: dict[tuple[str, str], IntegratedFundingData] = {}

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
        self.primary_sources: dict[str, Callable[..., Any]] = {}
        self.secondary_sources: dict[str, Callable[..., Any]] = {}
        self.tertiary_sources: dict[str, Callable[..., Any]] = {}
        self.fallback_sources: dict[str, Callable[..., Any]] = {}

        logger.info("Initialized multi-tier funding rate provider")

    def register_source(
        self,
        exchange: str,
        source_func: Callable[..., Any],
        source_type: SourceType,
        reliability: SourceReliability,
    ) -> None:
        """
        Register a funding rate data source.

        Args:
            exchange: Exchange identifier
            source_func: Function that returns funding rate data
            source_type: Type of source (primary, secondary, tertiary, fallback)
            reliability: Reliability category of the source
        """
        # source_info = {"func": source_func, "reliability": reliability} # F841 Unused variable

        if source_type == SourceType.PRIMARY:
            self.primary_sources[exchange] = source_func
        elif source_type == SourceType.SECONDARY:
            self.secondary_sources[exchange] = source_func
        elif source_type == SourceType.TERTIARY:
            self.tertiary_sources[exchange] = source_func
        elif source_type == SourceType.FALLBACK:
            self.fallback_sources[exchange] = source_func

        logger.info(
            f"Registered {source_type.value} source for {exchange} "
            f"with {reliability.value} reliability"
        )

    async def get_funding_rate(self, exchange: str, symbol: str) -> tuple[float, float]:
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
            age = (datetime.now(UTC) - cached_data.timestamp).total_seconds()
            if age <= self.max_acceptable_age:
                logger.debug(f"Using fresh cached funding rate for {exchange}:{symbol}")
                return cached_data.rate, cached_data.confidence_score
            else:
                # Adjust confidence for stale data
                decay_factor = max(0, 1 - (age / (self.max_acceptable_age * 2)))
                adjusted_confidence = Decimal(str(cached_data.confidence_score)) * Decimal(
                    str(decay_factor)
                )
                logger.info(
                    f"Using stale cached funding rate for {exchange}:{symbol} "
                    f"with age {age:.0f}s, confidence reduced to {adjusted_confidence:.2f}"
                )
                return cached_data.rate, float(adjusted_confidence)

        # Try primary source
        try:
            # Get data from different sources
            primary_data = await self._get_primary_funding_rate(exchange, symbol)
            secondary_data = await self._get_secondary_funding_rate(exchange, symbol)
            tertiary_data = await self._get_tertiary_funding_rate(exchange, symbol)

            # Integrate data from multiple sources
            integrated_data = self._integrate_funding_data(
                exchange, symbol, primary_data, secondary_data, tertiary_data
            )

            # Calculate confidence score
            confidence_factors = self._calculate_confidence_factors(
                integrated_data, exchange, symbol
            )

            confidence_score = confidence_factors.get_weighted_score(
                historical_weight=self.historical_accuracy_weight,
                source_count_weight=self.source_count_weight,
                dispersion_weight=self.dispersion_weight,
                freshness_weight=self.freshness_weight,
            )

            # Create complete integrated data
            integrated_data.confidence_score = confidence_score

            # Cache the result
            self.funding_cache[cache_key] = integrated_data

            logger.debug(
                f"Got funding rate {integrated_data.rate:.6f} for {exchange}:{symbol} "
                f"with confidence {confidence_score:.2f}"
            )
            return integrated_data.rate, confidence_score

        except Exception as e:
            logger.warning(f"Error getting funding rate: {e}")
            # Fall back to alternative sources
            try:
                (
                    fallback_rate,
                    fallback_confidence,
                ) = await self._get_fallback_funding_rate(exchange, symbol)
                logger.debug(
                    f"Using fallback funding rate {fallback_rate:.6f} for {exchange}:{symbol}"
                )
                return fallback_rate, fallback_confidence
            except Exception as fallback_error:
                logger.error(
                    f"All funding rate sources failed for {exchange}:{symbol}: {fallback_error}"
                )
                # Re-raise with proper chaining
                raise FundingRateSourceError(
                    f"No funding rate data available for {exchange}:{symbol}"
                ) from fallback_error

    async def _get_primary_funding_rate(self, exchange: str, symbol: str) -> FundingData | None:
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

            # Ensure timestamp is timezone-aware (assume UTC if naive)
            ts = raw_data.get("timestamp", datetime.now(UTC))
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            elif not isinstance(ts, datetime):
                # Handle non-datetime case, e.g., if it's an int timestamp
                try:
                    ts = datetime.fromtimestamp(int(ts) / 1000, tz=UTC)  # Assume ms
                except (ValueError, TypeError):
                    ts = datetime.now(UTC)  # Fallback if conversion fails

            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=ts,  # Use aware timestamp
                source_type=SourceType.PRIMARY,
                source_reliability=SourceReliability.HIGH,
                raw_data=raw_data,
            )

            return funding_data

        except Exception as e:
            logger.warning(f"Primary source for {exchange}:{symbol} failed: {e}")
            return None

    async def _get_secondary_funding_rate(self, exchange: str, symbol: str) -> FundingData | None:
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

            # Ensure timestamp is timezone-aware (assume UTC if naive)
            ts = raw_data.get("timestamp", datetime.now(UTC))
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            elif not isinstance(ts, datetime):
                try:
                    ts = datetime.fromtimestamp(int(ts) / 1000, tz=UTC)  # Assume ms
                except (ValueError, TypeError):
                    ts = datetime.now(UTC)

            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=ts,  # Use aware timestamp
                source_type=SourceType.SECONDARY,
                source_reliability=SourceReliability.MEDIUM,
                raw_data=raw_data,
            )

            return funding_data

        except Exception as e:
            logger.warning(f"Secondary source for {exchange}:{symbol} failed: {e}")
            return None

    async def _get_tertiary_funding_rate(self, exchange: str, symbol: str) -> FundingData | None:
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

            # Ensure timestamp is timezone-aware (assume UTC if naive)
            ts = raw_data.get("timestamp", datetime.now(UTC))
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            elif not isinstance(ts, datetime):
                try:
                    ts = datetime.fromtimestamp(int(ts) / 1000, tz=UTC)  # Assume ms
                except (ValueError, TypeError):
                    ts = datetime.now(UTC)

            # Create funding data
            funding_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=ts,  # Use aware timestamp
                source_type=SourceType.TERTIARY,
                source_reliability=SourceReliability.LOW,
                raw_data=raw_data,
            )

            return funding_data

        except Exception as e:
            logger.warning(f"Tertiary source for {exchange}:{symbol} failed: {e}")
            return None

    async def _get_fallback_funding_rate(self, exchange: str, symbol: str) -> tuple[float, float]:
        """
        Get funding rate from fallback source.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol

        Returns:
            Tuple of (funding_rate, confidence_score)

        Raises:
            FundingRateSourceError: If fallback source also fails
        """
        if exchange not in self.fallback_sources:
            raise FundingRateSourceError(f"No fallback source registered for {exchange}")

        try:
            source_func = self.fallback_sources[exchange]
            raw_data = await source_func(symbol)

            # Ensure timestamp is timezone-aware (assume UTC if naive)
            ts = raw_data.get("timestamp", datetime.now(UTC))
            if isinstance(ts, datetime) and ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            elif not isinstance(ts, datetime):
                try:
                    ts = datetime.fromtimestamp(int(ts) / 1000, tz=UTC)  # Assume ms
                except (ValueError, TypeError):
                    ts = datetime.now(UTC)

            # Create funding data
            fallback_data = FundingData(
                exchange=exchange,
                symbol=symbol,
                rate=raw_data.get("rate", 0.0),
                timestamp=ts,  # Use aware timestamp
                source_type=SourceType.FALLBACK,
                source_reliability=SourceReliability.LOW,
                raw_data=raw_data,
            )

            # Use lower confidence for fallback data and adjust for age
            base_confidence = Decimal("0.2")
            # Ensure fallback_data.timestamp is aware before subtraction
            fallback_ts_aware = fallback_data.timestamp
            if fallback_ts_aware.tzinfo is None:
                fallback_ts_aware = fallback_ts_aware.replace(
                    tzinfo=UTC
                )  # Should not happen due to above logic, but belt-and-suspenders
            age = (datetime.now(UTC) - fallback_ts_aware).total_seconds()
            decay_factor = max(0, 1 - (age / (self.max_acceptable_age * 4)))
            adjusted_confidence = base_confidence * Decimal(str(decay_factor))
            adjusted_rate = float(fallback_data.rate)

            logger.debug(
                f"Using fallback funding rate {adjusted_rate:.6f} for {exchange}:{symbol} "
                f"with age {age:.0f}s, confidence reduced to {adjusted_confidence:.2f}"
            )

            return adjusted_rate, float(adjusted_confidence)

        except Exception as e:
            logger.error(f"Fallback source for {exchange}:{symbol} failed: {e}")
            raise FundingRateSourceError(f"All sources failed for {exchange}:{symbol}") from e

    def _integrate_funding_data(
        self,
        exchange: str,
        symbol: str,
        primary: FundingData | None,
        secondary: FundingData | None,
        tertiary: FundingData | None,
    ) -> IntegratedFundingData:
        """
        Integrate funding data from different sources.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol
            primary: Primary funding data
            secondary: Secondary funding data
            tertiary: Tertiary funding data

        Returns:
            IntegratedFundingData object
        """
        # Collect available sources
        available_sources: list[FundingData] = []
        if primary:
            available_sources.append(primary)
        if secondary:
            available_sources.append(secondary)
        if tertiary:
            available_sources.append(tertiary)

        if not available_sources:
            raise FundingRateSourceError(
                f"No funding rate data available for {exchange}:{symbol} from primary/secondary/tertiary sources"
            )

        # Get rates, timestamps, and raw data
        rates: list[Decimal] = [Decimal(str(s.rate)) for s in available_sources]
        timestamps: list[datetime] = [s.timestamp for s in available_sources]

        if not rates:
            raise FundingRateSourceError(
                f"No valid funding rate data found for {exchange}:{symbol} from primary/secondary/tertiary sources"
            )

        # Ensure weights sum to 1 (or normalize)
        total_weight = Decimal(str(sum(rates, Decimal("0"))))
        if total_weight > Decimal(0):
            normalized_weights: list[Decimal] = [r / total_weight for r in rates]
        else:
            normalized_weights = [Decimal("1.0") / Decimal(len(rates))] * len(rates)

        # Calculate weighted average
        weighted_sum = Decimal(
            str(sum((r * w for r, w in zip(rates, normalized_weights, strict=True)), Decimal("0")))
        )
        integrated_rate: Decimal = weighted_sum

        # Calculate weighted average timestamp
        epoch = datetime(1970, 1, 1, tzinfo=UTC)
        timestamps_seconds: list[Decimal] = []
        for ts in timestamps:
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            seconds = Decimal(str((ts - epoch).total_seconds()))
            timestamps_seconds.append(seconds)
        min_len = min(len(timestamps_seconds), len(normalized_weights))
        weighted_timestamp_seconds = Decimal(
            str(
                sum(
                    (timestamps_seconds[i] * normalized_weights[i] for i in range(min_len)),
                    Decimal("0"),
                )
            )
        )
        integrated_timestamp = epoch + timedelta(seconds=float(weighted_timestamp_seconds))

        # Calculate dispersion (standard deviation of rates)
        if len(rates) > 1:
            mean_rate = Decimal(str(sum(rates, Decimal("0")))) / Decimal(len(rates))
            variance = Decimal(
                str(sum(((r - mean_rate) ** 2 for r in rates), Decimal("0")))
            ) / Decimal(len(rates))
            rate_dispersion: float = float(variance.sqrt())
        else:
            rate_dispersion = 0.0

        # Compose IntegratedFundingData, converting Decimals to float if required
        integrated_data = IntegratedFundingData(
            exchange=exchange,
            symbol=symbol,
            rate=float(integrated_rate),
            timestamp=integrated_timestamp,
            confidence_score=0.0,  # To be calculated later
            dispersion=rate_dispersion,
            sources_count=len(available_sources),
            primary_available=any(s.source_type == SourceType.PRIMARY for s in available_sources),
            secondary_available=any(
                s.source_type == SourceType.SECONDARY for s in available_sources
            ),
            tertiary_available=any(s.source_type == SourceType.TERTIARY for s in available_sources),
            source_data={s.source_type: s for s in available_sources},
        )
        return integrated_data

    def _calculate_confidence_factors(
        self, integrated_data: IntegratedFundingData, exchange: str, symbol: str
    ) -> ConfidenceFactors:
        """
        Calculate confidence factors based on integrated data.

        Args:
            integrated_data: The integrated funding data
            exchange: Exchange identifier
            symbol: Trading symbol

        Returns:
            ConfidenceFactors object
        """
        # Historical accuracy score
        historical_accuracy = self._check_historical_accuracy(exchange, symbol)

        # Source count score
        max_sources = 3  # Assuming max 3 tiers (primary, secondary, tertiary)
        source_count_score = integrated_data.sources_count / max_sources

        # Dispersion score (lower dispersion = higher confidence)
        max_dispersion = Decimal("0.001")  # Example: Max acceptable std dev of 0.1%
        dispersion_score = max(0, 1 - (integrated_data.dispersion / float(max_dispersion)))

        # Freshness score (more recent = higher confidence)
        age_seconds = (datetime.now(UTC) - integrated_data.timestamp).total_seconds()
        freshness_score = max(0, 1 - (age_seconds / self.max_acceptable_age))

        return ConfidenceFactors(
            historical_accuracy=historical_accuracy,
            source_count_factor=source_count_score,
            dispersion_factor=dispersion_score,
            freshness_factor=freshness_score,
        )

    def _check_historical_accuracy(self, exchange: str, symbol: str) -> float:
        """
        Check historical accuracy of funding rate predictions.

        Args:
            exchange: Exchange identifier
            symbol: Trading symbol

        Returns:
            Accuracy score (0-1)
        """
        if self.funding_rate_validator is None:
            return float(self.default_accuracy_score)

        try:
            # Calculate accuracy metrics using the validator
            metrics = self.funding_rate_validator.calculate_metrics(exchange, symbol)

            # If no metrics are available, return default score
            if metrics["rmse"] is None or metrics["bias"] is None:
                return float(self.default_accuracy_score)

            # Normalize RMSE and bias to a score between 0 and 1
            rmse_score = max(0, 1 - (float(metrics["rmse"]) / self.max_acceptable_rmse))
            bias_score = max(0, 1 - (abs(float(metrics["bias"])) / self.max_acceptable_bias))

            # Combine scores (e.g., weighted average)
            accuracy_score = (rmse_score * 0.7) + (bias_score * 0.3)

            return float(accuracy_score)

        except Exception as e:
            logger.warning(f"Error calculating historical accuracy for {exchange}:{symbol}: {e}")
            return float(self.default_accuracy_score)

    def clear_cache(self) -> None:
        """Clear the funding rate cache."""
        self.funding_cache.clear()
        logger.debug("Cleared funding rate cache")

    def clear_stale_cache_entries(self, max_age_seconds: float | None = None) -> int:
        """
        Clear stale entries from funding rate cache.

        Args:
            max_age_seconds: Max age in seconds for cache entries to be considered fresh.
                             If None, use self.max_acceptable_age.

        Returns:
            Number of stale entries cleared
        """
        max_age = max_age_seconds or self.max_acceptable_age
        now = datetime.now(UTC)
        stale_keys = [
            key
            for key, data in self.funding_cache.items()
            if (now - data.timestamp).total_seconds() > max_age
        ]

        for key in stale_keys:
            del self.funding_cache[key]

        if stale_keys:
            logger.debug(f"Cleared {len(stale_keys)} stale cache entries")

        return len(stale_keys)
