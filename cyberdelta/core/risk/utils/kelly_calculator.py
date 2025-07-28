"""Kelly criterion calculator utility."""

import math
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.sizing_exceptions import KellyCalculationError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Probability validation constants
PROBABILITY_TOLERANCE = 0.001  # Maximum allowed deviation from 1.0 for probability sums


class KellyMethod(Enum):
    """Kelly calculation methods."""

    CONTINUOUS = "continuous"
    BINARY = "binary"
    MULTI_OUTCOME = "multi_outcome"


@dataclass
class KellyInput:
    """Input parameters for Kelly calculation."""

    # Basic parameters
    expected_return: Decimal
    volatility: Decimal
    risk_free_rate: Decimal = Decimal("0.02")  # 2% annual

    # Binary outcome parameters (alternative to continuous)
    win_probability: Decimal | None = None
    win_amount: Decimal | None = None
    loss_amount: Decimal | None = None

    # Multi-outcome parameters
    outcomes: list[tuple[Decimal, Decimal]] | None = None  # (probability, return) pairs

    # Adjustments
    sharpe_ratio: Decimal | None = None
    max_drawdown: Decimal | None = None
    transaction_costs: Decimal = Decimal("0.001")  # 0.1%

    def validate(self) -> None:
        """Validate input parameters.
        
        Raises:
            KellyCalculationError: If any input parameter is invalid
        """
        if self.expected_return < 0:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_EXPECTED_RETURN,
                metadata={"expected_return": float(self.expected_return)},
            )

        if self.volatility <= 0:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_VOLATILITY,
                metadata={"volatility": float(self.volatility)},
            )

        if self.risk_free_rate < 0:
            raise KellyCalculationError(
                KellyCalculationError.INVALID_RISK_FREE_RATE,
                metadata={"risk_free_rate": float(self.risk_free_rate)},
            )

        if self.win_probability is not None and not (0 <= self.win_probability <= 1):
            raise KellyCalculationError(
                KellyCalculationError.INVALID_WIN_PROBABILITY,
                metadata={"win_probability": float(self.win_probability)},
            )

        if self.outcomes is not None:
            total_probability = sum(prob for prob, _ in self.outcomes)
            if abs(float(total_probability) - 1) > PROBABILITY_TOLERANCE:
                raise KellyCalculationError(
                    KellyCalculationError.INVALID_OUTCOME_PROBABILITIES,
                    metadata={
                        "total_probability": float(total_probability),
                        "expected_sum": 1.0,
                        "tolerance": PROBABILITY_TOLERANCE,
                    },
                )


@dataclass
class KellyResult:
    """Result of Kelly calculation."""

    # Primary results
    kelly_fraction: Decimal
    adjusted_kelly: Decimal
    recommended_fraction: Decimal

    # Calculation details
    method: KellyMethod
    expected_return: Decimal
    volatility: Decimal
    sharpe_ratio: Decimal

    # Risk metrics
    expected_growth_rate: Decimal
    max_drawdown_probability: Decimal | None = None
    time_to_double: Decimal | None = None

    # Adjustments applied
    sharpe_adjustment: Decimal = Decimal("1.0")
    drawdown_adjustment: Decimal = Decimal("1.0")
    transaction_cost_adjustment: Decimal = Decimal("1.0")

    # Metadata
    calculation_details: dict[str, Any] | None = None
    warnings: list[str] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary.
        
        Returns:
            Dictionary representation of Kelly calculation result
        """
        return {
            "kelly_fraction": float(self.kelly_fraction),
            "adjusted_kelly": float(self.adjusted_kelly),
            "recommended_fraction": float(self.recommended_fraction),
            "method": self.method.value,
            "expected_return": float(self.expected_return),
            "volatility": float(self.volatility),
            "sharpe_ratio": float(self.sharpe_ratio),
            "expected_growth_rate": float(self.expected_growth_rate),
            "max_drawdown_probability": (
                float(self.max_drawdown_probability) if self.max_drawdown_probability else None
            ),
            "time_to_double": float(self.time_to_double) if self.time_to_double else None,
            "sharpe_adjustment": float(self.sharpe_adjustment),
            "drawdown_adjustment": float(self.drawdown_adjustment),
            "transaction_cost_adjustment": float(self.transaction_cost_adjustment),
            "calculation_details": self.calculation_details,
            "warnings": self.warnings,
        }


class KellyCalculator:
    """Advanced Kelly criterion calculator."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the Kelly calculator."""
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)

        # Kelly parameters
        # 25% of Kelly default multiplier
        self.default_multiplier = self.config.get("default_multiplier", Decimal("0.25"))
        # 25% maximum fraction
        self.max_kelly_fraction = self.config.get("max_kelly_fraction", Decimal("0.25"))
        # 0.1% minimum fraction
        self.min_kelly_fraction = self.config.get("min_kelly_fraction", Decimal("0.001"))

        # Risk adjustments
        self.enable_sharpe_adjustment = self.config.get("enable_sharpe_adjustment", True)
        self.enable_drawdown_adjustment = self.config.get("enable_drawdown_adjustment", True)
        self.enable_transaction_cost_adjustment = self.config.get(
            "enable_transaction_cost_adjustment", True
        )

        # Thresholds
        self.min_sharpe_ratio = self.config.get("min_sharpe_ratio", Decimal("0.5"))
        # 20%
        self.max_drawdown_threshold = self.config.get("max_drawdown_threshold", Decimal("0.2"))
        # 0.1%
        self.min_expected_return = self.config.get("min_expected_return", Decimal("0.001"))

    def calculate_kelly(self, kelly_input: KellyInput) -> KellyResult:
        """Calculate Kelly fraction with comprehensive analysis.

        Args:
            kelly_input: Input parameters for Kelly calculation

        Returns:
            KellyResult with detailed calculation results
        """
        kelly_input.validate()

        # Determine calculation method
        if kelly_input.win_probability is not None and kelly_input.win_amount is not None:
            method = KellyMethod.BINARY
        elif kelly_input.outcomes is not None:
            method = KellyMethod.MULTI_OUTCOME
        else:
            method = KellyMethod.CONTINUOUS

        # Calculate base Kelly fraction
        if method == KellyMethod.CONTINUOUS:
            kelly_fraction = self._calculate_continuous_kelly(kelly_input)
        elif method == KellyMethod.BINARY:
            kelly_fraction = self._calculate_binary_kelly(kelly_input)
        else:
            kelly_fraction = self._calculate_multi_outcome_kelly(kelly_input)

        # Calculate Sharpe ratio
        excess_return = kelly_input.expected_return - (kelly_input.risk_free_rate / 365)
        sharpe_ratio = (
            excess_return / kelly_input.volatility if kelly_input.volatility > 0 else Decimal(0)
        )

        # Apply adjustments
        adjustments = self._calculate_adjustments(kelly_input, sharpe_ratio)
        adjusted_kelly = kelly_fraction * adjustments["total_adjustment"]

        # Apply bounds and multiplier
        bounded_kelly = self._apply_bounds(adjusted_kelly)
        recommended_fraction = bounded_kelly * self.default_multiplier

        # Calculate additional metrics
        expected_growth_rate = self._calculate_expected_growth_rate(
            recommended_fraction,
            kelly_input.expected_return,
            kelly_input.volatility,
        )

        max_drawdown_probability = self._calculate_max_drawdown_probability(
            recommended_fraction,
            kelly_input.volatility,
        )

        time_to_double = self._calculate_time_to_double(expected_growth_rate)

        # Create result
        result = KellyResult(
            kelly_fraction=kelly_fraction,
            adjusted_kelly=adjusted_kelly,
            recommended_fraction=recommended_fraction,
            method=method,
            expected_return=kelly_input.expected_return,
            volatility=kelly_input.volatility,
            sharpe_ratio=sharpe_ratio,
            expected_growth_rate=expected_growth_rate,
            max_drawdown_probability=max_drawdown_probability,
            time_to_double=time_to_double,
            sharpe_adjustment=adjustments["sharpe_adjustment"],
            drawdown_adjustment=adjustments["drawdown_adjustment"],
            transaction_cost_adjustment=adjustments["transaction_cost_adjustment"],
            calculation_details=adjustments["details"],
            warnings=adjustments["warnings"],
        )

        self.logger.debug(
            "Kelly calculation completed",
            recommended_fraction=float(result.recommended_fraction),
        )
        return result

    def _calculate_continuous_kelly(self, kelly_input: KellyInput) -> Decimal:
        """Calculate Kelly fraction using continuous formula.
        
        Args:
            kelly_input: Input parameters for Kelly calculation
            
        Returns:
            Kelly fraction calculated using continuous formula
        """
        # Kelly formula: f = (μ - r) / σ²
        excess_return = kelly_input.expected_return - (kelly_input.risk_free_rate / 365)

        if excess_return <= 0:
            return Decimal(0)

        return excess_return / (kelly_input.volatility * kelly_input.volatility)

    def _calculate_binary_kelly(self, kelly_input: KellyInput) -> Decimal:
        """Calculate Kelly fraction using binary outcome formula.
        
        Args:
            kelly_input: Input parameters for Kelly calculation
            
        Returns:
            Kelly fraction calculated using binary outcome formula
            
        Raises:
            KellyCalculationError: If required binary parameters are missing
        """
        # Kelly formula: f = (bp - q) / b
        # where b = win_amount/loss_amount, p = win_probability, q = 1-p

        if (
            kelly_input.win_probability is None
            or kelly_input.win_amount is None
            or kelly_input.loss_amount is None
        ):
            raise KellyCalculationError(KellyCalculationError.BINARY_KELLY_CALCULATION_REQUIREMENTS)

        b = kelly_input.win_amount / kelly_input.loss_amount
        p = kelly_input.win_probability
        q = 1 - p

        kelly_fraction = (b * p - q) / b
        return max(Decimal(0), kelly_fraction)

    def _calculate_multi_outcome_kelly(self, kelly_input: KellyInput) -> Decimal:
        """Calculate Kelly fraction for multiple outcomes.
        
        Args:
            kelly_input: Input parameters with multiple outcome probabilities
            
        Returns:
            Kelly fraction calculated for multiple outcomes
            
        Raises:
            KellyCalculationError: If outcomes are not provided
        """
        if kelly_input.outcomes is None:
            raise KellyCalculationError(
                KellyCalculationError.MULTI_OUTCOME_KELLY_CALCULATION_REQUIREMENTS
            )

        # Calculate expected return and variance
        expected_return = sum(prob * return_val for prob, return_val in kelly_input.outcomes)
        variance = sum(
            prob * (return_val - expected_return) ** 2 for prob, return_val in kelly_input.outcomes
        )

        if variance <= 0:
            return Decimal(0)

        # Use continuous formula with calculated parameters
        kelly_fraction = expected_return / variance
        return max(Decimal(0), Decimal(str(kelly_fraction)))

    def _calculate_adjustments(
        self, kelly_input: KellyInput, sharpe_ratio: Decimal
    ) -> dict[str, Any]:
        """Calculate all adjustments to Kelly fraction.
        
        Args:
            kelly_input: Input parameters for Kelly calculation
            sharpe_ratio: Calculated Sharpe ratio
            
        Returns:
            Dictionary containing adjustment factors and details
        """
        adjustments: dict[str, Any] = {
            "sharpe_adjustment": Decimal("1.0"),
            "drawdown_adjustment": Decimal("1.0"),
            "transaction_cost_adjustment": Decimal("1.0"),
            "details": {},
            "warnings": [],
        }

        # Sharpe ratio adjustment
        if self.enable_sharpe_adjustment:
            if sharpe_ratio < self.min_sharpe_ratio:
                adjustments["sharpe_adjustment"] = sharpe_ratio / self.min_sharpe_ratio
                adjustments["warnings"].append(
                    f"Low Sharpe ratio {sharpe_ratio:.2f}, applied "
                    f"{adjustments['sharpe_adjustment']:.2f} adjustment"
                )

            adjustments["details"]["sharpe_ratio"] = float(sharpe_ratio)
            adjustments["details"]["min_sharpe_ratio"] = float(self.min_sharpe_ratio)

        # Drawdown adjustment
        if self.enable_drawdown_adjustment and kelly_input.max_drawdown is not None:
            if kelly_input.max_drawdown > self.max_drawdown_threshold:
                adjustments["drawdown_adjustment"] = (
                    self.max_drawdown_threshold / kelly_input.max_drawdown
                )
                adjustments["warnings"].append(
                    f"High drawdown {kelly_input.max_drawdown:.2%}, applied "
                    f"{adjustments['drawdown_adjustment']:.2f} adjustment"
                )

            adjustments["details"]["max_drawdown"] = float(kelly_input.max_drawdown)
            adjustments["details"]["max_drawdown_threshold"] = float(self.max_drawdown_threshold)

        # Transaction cost adjustment
        if self.enable_transaction_cost_adjustment:
            # Reduce Kelly fraction by transaction costs
            cost_adjustment = max(Decimal("0.5"), 1 - kelly_input.transaction_costs * 2)
            adjustments["transaction_cost_adjustment"] = cost_adjustment
            adjustments["details"]["transaction_costs"] = float(kelly_input.transaction_costs)

        # Calculate total adjustment
        total_adjustment = (
            adjustments["sharpe_adjustment"]
            * adjustments["drawdown_adjustment"]
            * adjustments["transaction_cost_adjustment"]
        )
        adjustments["total_adjustment"] = total_adjustment

        return adjustments

    def _apply_bounds(self, kelly_fraction: Decimal) -> Decimal:
        """Apply bounds to Kelly fraction.
        
        Args:
            kelly_fraction: Unbounded Kelly fraction
            
        Returns:
            Bounded Kelly fraction within configured limits
        """
        bounded = kelly_fraction
        if isinstance(self.min_kelly_fraction, Decimal):
            bounded = max(bounded, self.min_kelly_fraction)
        if isinstance(self.max_kelly_fraction, Decimal):
            bounded = min(bounded, self.max_kelly_fraction)
        return bounded

    def _calculate_expected_growth_rate(
        self, fraction: Decimal, expected_return: Decimal, volatility: Decimal
    ) -> Decimal:
        """Calculate expected growth rate using Kelly fraction.
        
        Args:
            fraction: Kelly fraction to use
            expected_return: Expected return rate
            volatility: Return volatility
            
        Returns:
            Expected growth rate based on Kelly fraction
        """
        # Growth rate = f * μ - (f² * σ²) / 2
        return fraction * expected_return - (fraction * fraction * volatility * volatility) / 2

    def _calculate_max_drawdown_probability(
        self, fraction: Decimal, volatility: Decimal
    ) -> Decimal:
        """Calculate probability of maximum drawdown.
        
        Args:
            fraction: Kelly fraction being used
            volatility: Return volatility
            
        Returns:
            Estimated probability of experiencing maximum drawdown
        """
        # Simplified calculation based on fraction and volatility
        # Higher fraction and volatility = higher drawdown probability
        drawdown_prob = fraction * volatility * 2
        return min(Decimal("1.0"), drawdown_prob)

    def _calculate_time_to_double(self, growth_rate: Decimal) -> Decimal | None:
        """Calculate time to double capital.
        
        Args:
            growth_rate: Expected growth rate
            
        Returns:
            Time periods to double capital, or None if growth rate is non-positive
        """
        if growth_rate <= 0:
            return None

        # Time to double = ln(2) / growth_rate
        try:
            return Decimal(str(math.log(2))) / growth_rate
        except (ValueError, ZeroDivisionError):
            return None

    def calculate_kelly_for_opportunity(self, opportunity: ArbitrageOpportunity) -> KellyResult:
        """Calculate Kelly fraction for an arbitrage opportunity.

        Args:
            opportunity: Arbitrage opportunity

        Returns:
            KellyResult with calculation
        """
        # Extract parameters from opportunity
        expected_return = self._extract_expected_return(opportunity)
        volatility = self._extract_volatility(opportunity)

        # Create Kelly input
        kelly_input = KellyInput(
            expected_return=expected_return,
            volatility=volatility,
        )

        return self.calculate_kelly(kelly_input)

    def _extract_expected_return(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Extract expected return from opportunity.
        
        Args:
            opportunity: Arbitrage opportunity to extract return from
            
        Returns:
            Expected return as Decimal, defaults to 0.1% if not found
        """
        # Try spread percentage first
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                return Decimal(str(spread_percentage))
            except (ValueError, TypeError):
                pass

        # Try direct expected return
        expected_return = getattr(opportunity, "expected_return", None)
        if expected_return:
            try:
                return Decimal(str(expected_return))
            except (ValueError, TypeError):
                pass

        # Calculate from prices
        long_price = getattr(opportunity, "long_price", None)
        short_price = getattr(opportunity, "short_price", None)

        if long_price and short_price:
            try:
                long_decimal = Decimal(str(long_price))
                short_decimal = Decimal(str(short_price))

                if long_decimal > 0 and short_decimal > 0:
                    price_diff = abs(long_decimal - short_decimal)
                    avg_price = (long_decimal + short_decimal) / 2

                    if avg_price > 0:
                        return price_diff / avg_price
            except (ValueError, TypeError):
                pass

        # Default fallback
        return Decimal("0.001")  # 0.1%

    def _extract_volatility(self, opportunity: ArbitrageOpportunity) -> Decimal:
        """Extract volatility from opportunity.
        
        Args:
            opportunity: Arbitrage opportunity to extract volatility from
            
        Returns:
            Volatility as Decimal, defaults to 1% if not found
        """
        # Try direct volatility
        volatility = getattr(opportunity, "volatility", None)
        if volatility:
            try:
                return Decimal(str(volatility))
            except (ValueError, TypeError):
                pass

        # Use spread as volatility proxy
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                spread_decimal = Decimal(str(spread_percentage))
                # Volatility is typically higher than spread
                return spread_decimal * 3
            except (ValueError, TypeError):
                pass

        # Default volatility
        return Decimal("0.01")  # 1%

    def set_kelly_bounds(self, min_fraction: Decimal, max_fraction: Decimal) -> None:
        """Set Kelly fraction bounds.
        
        Args:
            min_fraction: Minimum allowed Kelly fraction
            max_fraction: Maximum allowed Kelly fraction
            
        Raises:
            KellyCalculationError: If min_fraction >= max_fraction
        """
        if min_fraction >= max_fraction:
            raise KellyCalculationError(
                KellyCalculationError.MIN_FRACTION_MUST_BE_LESS_THAN_MAX_FRACTION
            )

        self.min_kelly_fraction = min_fraction
        self.max_kelly_fraction = max_fraction
        self.logger.info(
            "Set Kelly bounds",
            min_fraction=float(min_fraction),
            max_fraction=float(max_fraction),
        )

    def set_default_multiplier(self, multiplier: Decimal) -> None:
        """Set default Kelly multiplier.
        
        Args:
            multiplier: Kelly multiplier between 0 and 1
            
        Raises:
            KellyCalculationError: If multiplier is not between 0 and 1
        """
        if multiplier <= 0 or multiplier > 1:
            raise KellyCalculationError(KellyCalculationError.MULTIPLIER_MUST_BE_BETWEEN_0_AND_1)

        self.default_multiplier = multiplier
        self.logger.info("Set Kelly multiplier", multiplier=float(multiplier))

    def enable_adjustment(self, adjustment_type: str, enabled: bool) -> None:
        """Enable or disable an adjustment type.
        
        Args:
            adjustment_type: Type of adjustment ('sharpe', 'drawdown', 'transaction_cost')
            enabled: Whether to enable the adjustment
            
        Raises:
            KellyCalculationError: If adjustment_type is unknown
        """
        if adjustment_type == "sharpe":
            self.enable_sharpe_adjustment = enabled
        elif adjustment_type == "drawdown":
            self.enable_drawdown_adjustment = enabled
        elif adjustment_type == "transaction_cost":
            self.enable_transaction_cost_adjustment = enabled
        else:
            raise KellyCalculationError(
                KellyCalculationError.UNKNOWN_ADJUSTMENT_TYPE,
                metadata={"adjustment_type": adjustment_type},
            )

        self.logger.info(
            "Set adjustment",
            adjustment_type=adjustment_type,
            enabled=enabled,
        )

    def get_calculator_stats(self) -> dict[str, Any]:
        """Get calculator statistics.
        
        Returns:
            Dictionary containing calculator configuration and settings
        """
        return {
            "default_multiplier": float(self.default_multiplier),
            "max_kelly_fraction": float(self.max_kelly_fraction),
            "min_kelly_fraction": float(self.min_kelly_fraction),
            "enable_sharpe_adjustment": self.enable_sharpe_adjustment,
            "enable_drawdown_adjustment": self.enable_drawdown_adjustment,
            "enable_transaction_cost_adjustment": self.enable_transaction_cost_adjustment,
            "min_sharpe_ratio": float(self.min_sharpe_ratio),
            "max_drawdown_threshold": float(self.max_drawdown_threshold),
            "min_expected_return": float(self.min_expected_return),
        }
