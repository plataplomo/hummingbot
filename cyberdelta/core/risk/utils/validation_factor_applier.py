"""Validation factor applier utility for risk adjustments."""

from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.checks.models.check_result import CheckResult
from cyberdelta.core.risk.exceptions.sizing_exceptions import ValidationFactorError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


def _create_validation_factor_list() -> list["ValidationFactor"]:
    """Create typed ValidationFactor list for dataclass fields.

    Returns:
        Empty list to be used as default factory for ValidationFactor lists.
    """
    return []


def _create_str_list() -> list[str]:
    """Create typed string list for dataclass fields.

    Returns:
        Empty list to be used as default factory for string lists.
    """
    return []


# Constants
HIGH_VOLATILITY_THRESHOLD = 0.5  # Threshold for high market volatility
MODERATE_VOLATILITY_THRESHOLD = 0.3  # Threshold for moderate market volatility
HIGH_LIQUIDITY_THRESHOLD = 1000000  # Volume threshold for high liquidity
GOOD_LIQUIDITY_THRESHOLD = 100000  # Volume threshold for good liquidity


class FactorType(Enum):
    """Types of validation factors."""

    SPREAD_QUALITY = "spread_quality"
    EXCHANGE_QUALITY = "exchange_quality"
    SYMBOL_RISK = "symbol_risk"
    MARKET_CONDITIONS = "market_conditions"
    HISTORICAL_PERFORMANCE = "historical_performance"
    LIQUIDITY = "liquidity"
    TIMING = "timing"
    CORRELATION = "correlation"


class FactorAdjustmentMethod(Enum):
    """Methods for applying factor adjustments."""

    MULTIPLICATIVE = "multiplicative"
    ADDITIVE = "additive"
    EXPONENTIAL = "exponential"
    SIGMOID = "sigmoid"


@dataclass
class ValidationFactor:
    """Individual validation factor."""

    factor_type: FactorType
    base_value: Decimal
    adjusted_value: Decimal
    weight: Decimal = Decimal("1.0")
    adjustment_method: FactorAdjustmentMethod = FactorAdjustmentMethod.MULTIPLICATIVE

    # Metadata
    reason: str | None = None
    confidence: Decimal = Decimal("1.0")
    source: str | None = None

    @property
    def adjustment_ratio(self) -> Decimal:
        """Calculate adjustment ratio."""
        if self.base_value == 0:
            return Decimal("1.0")
        return self.adjusted_value / self.base_value

    @property
    def impact(self) -> Decimal:
        """Calculate weighted impact."""
        return (self.adjusted_value - self.base_value) * self.weight

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of the validation factor with all attributes
            converted to JSON-serializable types.
        """
        return {
            "factor_type": self.factor_type.value,
            "base_value": float(self.base_value),
            "adjusted_value": float(self.adjusted_value),
            "weight": float(self.weight),
            "adjustment_method": self.adjustment_method.value,
            "adjustment_ratio": float(self.adjustment_ratio),
            "impact": float(self.impact),
            "reason": self.reason,
            "confidence": float(self.confidence),
            "source": self.source,
        }


@dataclass
class ValidationFactorResult:
    """Result of validation factor application."""

    # Overall results
    base_factor: Decimal
    final_factor: Decimal
    total_adjustment: Decimal

    # Individual factors
    factors: list[ValidationFactor] = field(default_factory=_create_validation_factor_list)

    # Factor breakdown
    spread_factor: Decimal = Decimal("1.0")
    exchange_factor: Decimal = Decimal("1.0")
    symbol_factor: Decimal = Decimal("1.0")
    market_factor: Decimal = Decimal("1.0")
    performance_factor: Decimal = Decimal("1.0")
    liquidity_factor: Decimal = Decimal("1.0")
    timing_factor: Decimal = Decimal("1.0")
    correlation_factor: Decimal = Decimal("1.0")

    # Metadata
    calculation_timestamp: datetime | None = None
    warnings: list[str] = field(default_factory=_create_str_list)
    applied_checks: list[str] = field(default_factory=_create_str_list)

    @property
    def adjustment_percentage(self) -> Decimal:
        """Calculate adjustment percentage."""
        return (self.final_factor - self.base_factor) * 100

    @property
    def is_positive_adjustment(self) -> bool:
        """Check if adjustment is positive."""
        return self.final_factor > self.base_factor

    @property
    def is_negative_adjustment(self) -> bool:
        """Check if adjustment is negative."""
        return self.final_factor < self.base_factor

    def get_significant_factors(
        self, threshold: Decimal = Decimal("0.05")
    ) -> list[ValidationFactor]:
        """Get factors with significant impact.

        Returns:
            List of validation factors whose absolute impact exceeds the threshold.
        """
        return [f for f in self.factors if abs(f.impact) > threshold]

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary containing all validation result data including base factor,
            final factor, adjustments, factor breakdown, timestamps, and warnings.
        """
        return {
            "base_factor": float(self.base_factor),
            "final_factor": float(self.final_factor),
            "total_adjustment": float(self.total_adjustment),
            "adjustment_percentage": float(self.adjustment_percentage),
            "factors": [f.to_dict() for f in self.factors],
            "factor_breakdown": {
                "spread": float(self.spread_factor),
                "exchange": float(self.exchange_factor),
                "symbol": float(self.symbol_factor),
                "market": float(self.market_factor),
                "performance": float(self.performance_factor),
                "liquidity": float(self.liquidity_factor),
                "timing": float(self.timing_factor),
                "correlation": float(self.correlation_factor),
            },
            "calculation_timestamp": (
                self.calculation_timestamp.isoformat() if self.calculation_timestamp else None
            ),
            "warnings": self.warnings,
            "applied_checks": self.applied_checks,
            "significant_factors": [f.to_dict() for f in self.get_significant_factors()],
        }


class ValidationFactorApplier:
    """Applies validation factors to risk calculations."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the validation factor applier."""
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)

        # Base configuration
        self.base_validation_factor = self.config.get("base_validation_factor", Decimal("0.8"))
        self.max_factor = self.config.get("max_factor", Decimal("1.2"))
        self.min_factor = self.config.get("min_factor", Decimal("0.1"))

        # Factor weights
        self.factor_weights = {
            FactorType.SPREAD_QUALITY: self.config.get("spread_quality_weight", Decimal("0.25")),
            FactorType.EXCHANGE_QUALITY: self.config.get(
                "exchange_quality_weight", Decimal("0.20")
            ),
            FactorType.SYMBOL_RISK: self.config.get("symbol_risk_weight", Decimal("0.15")),
            FactorType.MARKET_CONDITIONS: self.config.get(
                "market_conditions_weight", Decimal("0.15")
            ),
            FactorType.HISTORICAL_PERFORMANCE: self.config.get(
                "historical_performance_weight", Decimal("0.10")
            ),
            FactorType.LIQUIDITY: self.config.get("liquidity_weight", Decimal("0.10")),
            FactorType.TIMING: self.config.get("timing_weight", Decimal("0.03")),
            FactorType.CORRELATION: self.config.get("correlation_weight", Decimal("0.02")),
        }

        # Factor thresholds
        self.spread_thresholds = {
            "excellent": Decimal("0.01"),  # > 1%
            "good": Decimal("0.005"),  # > 0.5%
            "fair": Decimal("0.002"),  # > 0.2%
            "poor": Decimal("0.001"),  # > 0.1%
        }

        # Exchange quality scores
        self.exchange_scores = {
            "binance": Decimal("1.0"),
            "coinbase": Decimal("0.95"),
            "kraken": Decimal("0.95"),
            "okx": Decimal("0.90"),
            "bybit": Decimal("0.90"),
            "hyperliquid": Decimal("0.85"),
            "backpack": Decimal("0.80"),
            "ftx": Decimal("0.70"),  # Lower due to past issues
        }

        # Symbol risk scores (1.0 = low risk, 0.5 = high risk)
        self.symbol_risk_scores = {
            "BTC": Decimal("1.0"),
            "ETH": Decimal("0.95"),
            "BNB": Decimal("0.90"),
            "SOL": Decimal("0.85"),
            "ADA": Decimal("0.80"),
            "DOT": Decimal("0.80"),
            "MATIC": Decimal("0.75"),
            "AVAX": Decimal("0.75"),
            "LINK": Decimal("0.80"),
            "UNI": Decimal("0.75"),
        }

        # Market hours adjustment (UTC)
        self.optimal_hours = list(range(12, 20))  # 12:00 - 20:00 UTC
        self.suboptimal_hours = list(range(6))  # 00:00 - 06:00 UTC

    def apply_validation_factors(
        self,
        opportunity: ArbitrageOpportunity,
        check_results: list[CheckResult] | None = None,
        market_data: dict[str, Any] | None = None,
    ) -> ValidationFactorResult:
        """Apply validation factors to an opportunity.

        Args:
            opportunity: The arbitrage opportunity
            check_results: Optional check results to consider
            market_data: Optional market data for additional context

        Returns:
            ValidationFactorResult with detailed adjustments
        """
        factors: list[ValidationFactor] = []

        # Start with base factor
        base_factor = self.base_validation_factor

        # Apply spread quality factor
        spread_factor = self._calculate_spread_factor(opportunity)
        factors.append(spread_factor)

        # Apply exchange quality factor
        exchange_factor = self._calculate_exchange_factor(opportunity)
        factors.append(exchange_factor)

        # Apply symbol risk factor
        symbol_factor = self._calculate_symbol_factor(opportunity)
        factors.append(symbol_factor)

        # Apply market conditions factor
        market_factor = self._calculate_market_factor(opportunity, market_data)
        factors.append(market_factor)

        # Apply historical performance factor
        performance_factor = self._calculate_performance_factor(opportunity, check_results)
        factors.append(performance_factor)

        # Apply liquidity factor
        liquidity_factor = self._calculate_liquidity_factor(opportunity, market_data)
        factors.append(liquidity_factor)

        # Apply timing factor
        timing_factor = self._calculate_timing_factor()
        factors.append(timing_factor)

        # Apply correlation factor
        correlation_factor = self._calculate_correlation_factor(opportunity, market_data)
        factors.append(correlation_factor)

        # Calculate final factor
        final_factor = self._combine_factors(base_factor, factors)

        # Apply bounds
        final_factor = max(self.min_factor, min(self.max_factor, final_factor))

        # Create result
        result = ValidationFactorResult(
            base_factor=base_factor,
            final_factor=final_factor,
            total_adjustment=final_factor - base_factor,
            factors=factors,
            spread_factor=spread_factor.adjusted_value,
            exchange_factor=exchange_factor.adjusted_value,
            symbol_factor=symbol_factor.adjusted_value,
            market_factor=market_factor.adjusted_value,
            performance_factor=performance_factor.adjusted_value,
            liquidity_factor=liquidity_factor.adjusted_value,
            timing_factor=timing_factor.adjusted_value,
            correlation_factor=correlation_factor.adjusted_value,
            calculation_timestamp=datetime.now(tz=UTC),
            applied_checks=[str(r.status.value) for r in check_results] if check_results else [],
        )

        # Add warnings
        if final_factor < Decimal("0.5"):
            result.warnings.append(f"Low validation factor: {final_factor:.2f}")

        if abs(final_factor - base_factor) > Decimal("0.3"):
            result.warnings.append(
                f"Large adjustment from base: {result.adjustment_percentage:.1f}%"
            )

        self.logger.debug(
            "Applied validation factors",
            base_factor=float(base_factor),
            final_factor=float(final_factor),
        )
        return result

    def _calculate_spread_factor(self, opportunity: ArbitrageOpportunity) -> ValidationFactor:
        """Calculate spread quality factor.

        Returns:
            ValidationFactor representing the quality of the arbitrage spread,
            with adjustments based on spread percentage thresholds.
        """
        base_value = Decimal("1.0")

        # Get spread percentage
        spread = getattr(opportunity, "spread_percentage", None)
        if not spread:
            return ValidationFactor(
                factor_type=FactorType.SPREAD_QUALITY,
                base_value=base_value,
                adjusted_value=base_value,
                weight=self.factor_weights[FactorType.SPREAD_QUALITY],
                reason="No spread data available",
            )

        try:
            spread_decimal = Decimal(str(spread))

            # Determine spread quality
            if spread_decimal >= self.spread_thresholds["excellent"]:
                adjusted_value = Decimal("1.2")
                reason = "Excellent spread quality"
            elif spread_decimal >= self.spread_thresholds["good"]:
                adjusted_value = Decimal("1.1")
                reason = "Good spread quality"
            elif spread_decimal >= self.spread_thresholds["fair"]:
                adjusted_value = Decimal("1.0")
                reason = "Fair spread quality"
            elif spread_decimal >= self.spread_thresholds["poor"]:
                adjusted_value = Decimal("0.9")
                reason = "Poor spread quality"
            else:
                adjusted_value = Decimal("0.8")
                reason = "Very poor spread quality"

            return ValidationFactor(
                factor_type=FactorType.SPREAD_QUALITY,
                base_value=base_value,
                adjusted_value=adjusted_value,
                weight=self.factor_weights[FactorType.SPREAD_QUALITY],
                reason=reason,
                confidence=Decimal("0.9"),
            )

        except (ValueError, TypeError):
            return ValidationFactor(
                factor_type=FactorType.SPREAD_QUALITY,
                base_value=base_value,
                adjusted_value=base_value,
                weight=self.factor_weights[FactorType.SPREAD_QUALITY],
                reason="Invalid spread data",
            )

    def _calculate_exchange_factor(self, opportunity: ArbitrageOpportunity) -> ValidationFactor:
        """Calculate exchange quality factor.

        Returns:
            ValidationFactor based on the quality scores of the exchanges involved
            in the arbitrage opportunity.
        """
        base_value = Decimal("1.0")

        # Get exchanges
        long_exchange = getattr(opportunity, "long_exchange", "").lower()
        short_exchange = getattr(opportunity, "short_exchange", "").lower()

        # Get exchange scores
        long_score = self.exchange_scores.get(long_exchange, Decimal("0.7"))
        short_score = self.exchange_scores.get(short_exchange, Decimal("0.7"))

        # Average score
        avg_score = (long_score + short_score) / 2

        # Adjust for same exchange (higher confidence)
        if long_exchange == short_exchange:
            avg_score *= Decimal("1.05")

        return ValidationFactor(
            factor_type=FactorType.EXCHANGE_QUALITY,
            base_value=base_value,
            adjusted_value=avg_score,
            weight=self.factor_weights[FactorType.EXCHANGE_QUALITY],
            reason=f"Exchange quality: {long_exchange}/{short_exchange}",
            confidence=Decimal("0.85"),
        )

    def _calculate_symbol_factor(self, opportunity: ArbitrageOpportunity) -> ValidationFactor:
        """Calculate symbol risk factor.

        Returns:
            ValidationFactor representing the risk assessment of the trading symbol,
            with higher scores for lower-risk assets.
        """
        base_value = Decimal("1.0")

        # Get symbol - opportunity.symbol is a Symbol object
        symbol_obj = getattr(opportunity, "symbol", None)
        if not symbol_obj:
            return ValidationFactor(
                factor_type=FactorType.SYMBOL_RISK,
                base_value=base_value,
                adjusted_value=base_value * Decimal("0.5"),
                reason="No symbol provided"
            )
        
        # Extract string value for lookups
        symbol_str = symbol_obj.value.upper()
        base_symbol = symbol_str.replace("USDT", "").replace("USD", "")

        # Get risk score
        risk_score = self.symbol_risk_scores.get(base_symbol, Decimal("0.6"))

        # Adjust for stablecoin pairs (lower risk)
        if "USD" in symbol_str or "USDT" in symbol_str:
            risk_score *= Decimal("1.1")

        return ValidationFactor(
            factor_type=FactorType.SYMBOL_RISK,
            base_value=base_value,
            adjusted_value=risk_score,
            weight=self.factor_weights[FactorType.SYMBOL_RISK],
            reason=f"Symbol risk assessment: {base_symbol}",
            confidence=Decimal("0.8"),
        )

    def _calculate_market_factor(
        self,
        opportunity: ArbitrageOpportunity,
        market_data: dict[str, Any] | None,
    ) -> ValidationFactor:
        """Calculate market conditions factor.

        Returns:
            ValidationFactor adjusted for current market volatility and trends,
            with lower values during high volatility periods.
        """
        base_value = Decimal("1.0")
        adjusted_value = base_value

        if market_data:
            # Check market volatility
            market_volatility = market_data.get("market_volatility", 0)
            if market_volatility > HIGH_VOLATILITY_THRESHOLD:
                adjusted_value *= Decimal("0.8")
                reason = "High market volatility"
            elif market_volatility > MODERATE_VOLATILITY_THRESHOLD:
                adjusted_value *= Decimal("0.9")
                reason = "Moderate market volatility"
            else:
                reason = "Normal market conditions"

            # Check market trend
            market_trend = market_data.get("market_trend", "neutral")
            if market_trend == "strong_bearish":
                adjusted_value *= Decimal("0.9")
            elif market_trend == "strong_bullish":
                adjusted_value *= Decimal("1.05")
        else:
            reason = "No market data available"

        return ValidationFactor(
            factor_type=FactorType.MARKET_CONDITIONS,
            base_value=base_value,
            adjusted_value=adjusted_value,
            weight=self.factor_weights[FactorType.MARKET_CONDITIONS],
            reason=reason,
            confidence=Decimal("0.7"),
        )

    def _calculate_performance_factor(
        self,
        opportunity: ArbitrageOpportunity,
        check_results: list[CheckResult] | None,
    ) -> ValidationFactor:
        """Calculate historical performance factor.

        Returns:
            ValidationFactor based on the success rate of previous risk checks,
            rewarding high check pass rates.
        """
        base_value = Decimal("1.0")
        adjusted_value = base_value

        if check_results:
            # Count successful checks
            successful_checks = sum(1 for r in check_results if r.passed)
            total_checks = len(check_results)

            if total_checks > 0:
                success_rate = Decimal(str(successful_checks)) / Decimal(str(total_checks))

                # Adjust based on success rate
                if success_rate >= Decimal("0.9"):
                    adjusted_value = Decimal("1.1")
                    reason = "Excellent check performance"
                elif success_rate >= Decimal("0.7"):
                    adjusted_value = Decimal("1.0")
                    reason = "Good check performance"
                else:
                    adjusted_value = Decimal("0.9")
                    reason = "Poor check performance"
            else:
                reason = "No check results"
        else:
            reason = "No historical performance data"

        return ValidationFactor(
            factor_type=FactorType.HISTORICAL_PERFORMANCE,
            base_value=base_value,
            adjusted_value=adjusted_value,
            weight=self.factor_weights[FactorType.HISTORICAL_PERFORMANCE],
            reason=reason,
            confidence=Decimal("0.6"),
        )

    def _calculate_liquidity_factor(
        self,
        opportunity: ArbitrageOpportunity,
        market_data: dict[str, Any] | None,
    ) -> ValidationFactor:
        """Calculate liquidity factor.

        Returns:
            ValidationFactor based on trading volume, with higher adjustments
            for more liquid markets.
        """
        base_value = Decimal("1.0")
        adjusted_value = base_value

        # Check for liquidity data
        volume = getattr(opportunity, "volume", None)
        if volume:
            try:
                volume_decimal = Decimal(str(volume))

                # High volume = better liquidity
                if volume_decimal > HIGH_LIQUIDITY_THRESHOLD:
                    adjusted_value = Decimal("1.1")
                    reason = "High liquidity"
                elif volume_decimal > GOOD_LIQUIDITY_THRESHOLD:
                    adjusted_value = Decimal("1.0")
                    reason = "Good liquidity"
                else:
                    adjusted_value = Decimal("0.9")
                    reason = "Low liquidity"
            except (ValueError, TypeError):
                reason = "Invalid volume data"
        else:
            reason = "No liquidity data"

        return ValidationFactor(
            factor_type=FactorType.LIQUIDITY,
            base_value=base_value,
            adjusted_value=adjusted_value,
            weight=self.factor_weights[FactorType.LIQUIDITY],
            reason=reason,
            confidence=Decimal("0.5"),
        )

    def _calculate_timing_factor(self) -> ValidationFactor:
        """Calculate timing factor based on current time.

        Returns:
            ValidationFactor adjusted for optimal trading hours, with slight
            increases during peak hours and decreases during off-peak times.
        """
        base_value = Decimal("1.0")

        current_hour = datetime.now(tz=UTC).hour

        if current_hour in self.optimal_hours:
            adjusted_value = Decimal("1.05")
            reason = "Optimal trading hours"
        elif current_hour in self.suboptimal_hours:
            adjusted_value = Decimal("0.95")
            reason = "Suboptimal trading hours"
        else:
            adjusted_value = Decimal("1.0")
            reason = "Normal trading hours"

        return ValidationFactor(
            factor_type=FactorType.TIMING,
            base_value=base_value,
            adjusted_value=adjusted_value,
            weight=self.factor_weights[FactorType.TIMING],
            reason=reason,
            confidence=Decimal("0.9"),
        )

    def _calculate_correlation_factor(
        self,
        opportunity: ArbitrageOpportunity,
        market_data: dict[str, Any] | None,
    ) -> ValidationFactor:
        """Calculate correlation factor.

        Returns:
            ValidationFactor based on asset correlation with major pairs,
            favoring less correlated assets for diversification.
        """
        base_value = Decimal("1.0")
        adjusted_value = base_value

        # Simple correlation check based on symbol
        symbol_obj = getattr(opportunity, "symbol", None)
        if not symbol_obj:
            return ValidationFactor(
                factor_type=FactorType.CORRELATION,
                base_value=base_value,
                adjusted_value=base_value * Decimal("0.5"),
                reason="No symbol provided"
            )
        
        symbol_str = symbol_obj.value.upper()

        # Check if correlated with major pairs
        if "BTC" in symbol_str or "ETH" in symbol_str:
            adjusted_value = Decimal("0.98")
            reason = "Correlated with major pairs"
        else:
            adjusted_value = Decimal("1.02")
            reason = "Less correlated asset"

        return ValidationFactor(
            factor_type=FactorType.CORRELATION,
            base_value=base_value,
            adjusted_value=adjusted_value,
            weight=self.factor_weights[FactorType.CORRELATION],
            reason=reason,
            confidence=Decimal("0.4"),
        )

    def _combine_factors(self, base_factor: Decimal, factors: list[ValidationFactor]) -> Decimal:
        """Combine multiple factors into final factor.

        Returns:
            Final validation factor calculated as weighted average of all individual
            factors multiplied by the base factor.
        """
        # Weighted average approach
        total_weight = sum(f.weight for f in factors)

        if total_weight == 0:
            return base_factor

        # Calculate weighted sum
        weighted_sum = sum(f.adjusted_value * f.weight for f in factors)

        # Normalize by total weight
        return base_factor * (Decimal(str(weighted_sum)) / Decimal(str(total_weight)))

    def set_factor_weight(self, factor_type: FactorType, weight: Decimal) -> None:
        """Set weight for a specific factor type.

        Raises:
            ValidationFactorError: If weight is not between 0 and 1.
        """
        if weight < 0 or weight > 1:
            raise ValidationFactorError(ValidationFactorError.WEIGHT_OUT_OF_RANGE)

        self.factor_weights[factor_type] = weight
        self.logger.info(
            "Set factor weight",
            factor_type=factor_type.value,
            weight=float(weight),
        )

    def set_exchange_score(self, exchange: str, score: Decimal) -> None:
        """Set quality score for an exchange.

        Raises:
            ValidationFactorError: If score is not between 0 and 1.
        """
        if score < 0 or score > 1:
            raise ValidationFactorError(ValidationFactorError.SCORE_OUT_OF_RANGE)

        self.exchange_scores[exchange.lower()] = score
        self.logger.info("Set exchange score", exchange=exchange, score=float(score))

    def set_symbol_risk_score(self, symbol: str, score: Decimal) -> None:
        """Set risk score for a symbol.

        Raises:
            ValidationFactorError: If score is not between 0 and 1.
        """
        if score < 0 or score > 1:
            raise ValidationFactorError(ValidationFactorError.SCORE_OUT_OF_RANGE)

        self.symbol_risk_scores[symbol.upper()] = score
        self.logger.info("Set symbol risk score", symbol=symbol, score=float(score))

    def get_applier_stats(self) -> dict[str, Any]:
        """Get applier statistics.

        Returns:
            Dictionary containing current configuration including base factor,
            bounds, weights, thresholds, and exchange/symbol counts.
        """
        return {
            "base_validation_factor": float(self.base_validation_factor),
            "max_factor": float(self.max_factor),
            "min_factor": float(self.min_factor),
            "factor_weights": {k.value: float(v) for k, v in self.factor_weights.items()},
            "spread_thresholds": {k: float(v) for k, v in self.spread_thresholds.items()},
            "exchange_count": len(self.exchange_scores),
            "symbol_count": len(self.symbol_risk_scores),
            "optimal_hours": self.optimal_hours,
            "suboptimal_hours": self.suboptimal_hours,
        }
