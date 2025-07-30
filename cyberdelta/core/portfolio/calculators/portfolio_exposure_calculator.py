"""Calculator for portfolio-level aggregate exposure and risk metrics."""

from __future__ import annotations

import operator
from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import InvalidCalculationInputError


# Constants
REASONABLE_VALUE_MIN = -1e15
REASONABLE_VALUE_MAX = 1e15

if TYPE_CHECKING:
    from cyberdelta.core.portfolio.calculators.position_exposure_calculator import PositionExposure

logger = get_logger(__name__)


def _risk_limit_breach_list_factory() -> list[RiskLimitBreach]:
    """Factory function that preserves list[RiskLimitBreach] type information.

    Returns:
        Empty list of RiskLimitBreach objects for use as default factory.
    """
    return []


@dataclass
class RiskLimitBreach:
    """Risk limit breach with validation."""

    limit_type: str = Field(min_length=1, description="Type of limit breached")
    limit_value: float = Field(description="The configured limit value")
    current_value: float = Field(description="The current value that breached the limit")
    severity: str = Field(
        pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$", description="Breach severity level"
    )

    @field_validator("limit_value", "current_value", mode="before")
    @classmethod
    def validate_values(cls, v: float | str | Decimal) -> float:
        """Validate limit values are finite.

        Returns:
            Validated float value within reasonable bounds.

        Raises:
            InvalidCalculationInputError: If value is not finite or not within reasonable bounds.
        """
        value: float = float(v)
        if not (REASONABLE_VALUE_MIN < value < REASONABLE_VALUE_MAX):
            raise InvalidCalculationInputError(
                parameter="limit_value", value=value, expected="finite and reasonable value"
            )
        return value

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of the risk limit breach with all attributes.
        """
        return {
            "limit_type": self.limit_type,
            "limit_value": self.limit_value,
            "current_value": self.current_value,
            "severity": self.severity,
        }


@dataclass
class PortfolioExposure:
    """Aggregate portfolio exposure metrics with validation."""

    # Portfolio totals
    total_positions: int = Field(ge=0, description="Total number of positions")
    total_market_value: Decimal = Field(ge=0, description="Total market value")
    total_notional_value: Decimal = Field(ge=0, description="Total notional value")
    total_collateral: Decimal = Field(ge=0, description="Total collateral")

    # Exposure metrics
    gross_exposure: Decimal = Field(ge=0, description="Sum of absolute exposures")
    net_exposure: Decimal = Field(description="Sum of signed exposures")
    long_exposure: Decimal = Field(ge=0, description="Total long exposure")
    short_exposure: Decimal = Field(ge=0, description="Total short exposure (absolute value)")

    # Risk metrics
    total_margin_requirement: Decimal = Field(ge=0, description="Total margin requirement")
    average_leverage: Decimal = Field(ge=0, description="Average portfolio leverage")
    max_leverage: Decimal = Field(ge=0, description="Maximum position leverage")
    portfolio_var_95: Decimal = Field(ge=0, description="Portfolio 95% VaR")
    portfolio_var_99: Decimal = Field(ge=0, description="Portfolio 99% VaR")
    total_stress_loss: Decimal = Field(ge=0, description="Total stress scenario loss")

    # Concentration metrics
    largest_position_weight: Decimal = Field(
        ge=0, le=1, description="Largest position weight (0-100%)"
    )
    top_5_concentration: Decimal = Field(ge=0, le=1, description="Top 5 positions weight (0-100%)")
    herfindahl_index: Decimal = Field(
        ge=0, le=1, description="Herfindahl concentration index (0-1)"
    )

    # Risk scores
    overall_risk_score: Decimal = Field(ge=0, le=100, description="Overall risk score (0-100)")
    concentration_risk_score: Decimal = Field(
        ge=0, le=100, description="Concentration risk score (0-100)"
    )
    leverage_risk_score: Decimal = Field(ge=0, le=100, description="Leverage risk score (0-100)")

    # Breakdown by exchange
    exposure_by_exchange: dict[str, Decimal] = Field(
        default_factory=dict, description="Exposure by exchange"
    )
    margin_by_exchange: dict[str, Decimal] = Field(
        default_factory=dict, description="Margin by exchange"
    )

    # Breakdown by asset
    exposure_by_asset: dict[str, Decimal] = Field(
        default_factory=dict, description="Exposure by asset"
    )
    position_count_by_asset: dict[str, int] = Field(
        default_factory=dict, description="Position count by asset"
    )

    # Currency exposure
    currency_exposures: dict[str, Decimal] = Field(
        default_factory=dict, description="Currency exposures"
    )

    # Risk limits
    breached_limits: list[RiskLimitBreach] = Field(
        default_factory=_risk_limit_breach_list_factory, description="Breached risk limits"
    )
    warnings: list[str] = Field(default_factory=list, description="Risk warnings")

    @field_validator(
        "total_market_value",
        "total_notional_value",
        "total_collateral",
        "gross_exposure",
        "net_exposure",
        "long_exposure",
        "short_exposure",
        "total_margin_requirement",
        "average_leverage",
        "max_leverage",
        "portfolio_var_95",
        "portfolio_var_99",
        "total_stress_loss",
        mode="before",
    )
    @classmethod
    def validate_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure decimal values are finite.

        Returns:
            Validated finite Decimal value.

        Raises:
            InvalidCalculationInputError: If decimal value is not finite.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="decimal_value", value=value, expected="finite decimal"
            )
        return value

    @field_validator(
        "exposure_by_exchange",
        "margin_by_exchange",
        "exposure_by_asset",
        "currency_exposures",
        mode="after",
    )
    @classmethod
    def validate_decimal_dicts(
        cls, v: dict[str, Decimal | str | float | int]
    ) -> dict[str, Decimal]:
        """Validate dictionaries with decimal values.

        Returns:
            Dictionary with string keys and validated finite Decimal values.

        Raises:
            InvalidCalculationInputError: If any value is not finite or negative.
        """
        result: dict[str, Decimal] = {}
        for key, value in v.items():
            val: Decimal = value if isinstance(value, Decimal) else Decimal(str(value))
            if not val.is_finite():
                raise InvalidCalculationInputError(
                    parameter=f"value_for_{key}", value=val, expected="finite decimal"
                )
            if val < 0:
                raise InvalidCalculationInputError(
                    parameter=f"value_for_{key}", value=val, expected="non-negative decimal"
                )
            result[key] = val
        return result

    @field_validator("position_count_by_asset", mode="after")
    @classmethod
    def validate_count_dict(cls, v: dict[str, int]) -> dict[str, int]:
        """Validate position count dictionary.

        Returns:
            Validated dictionary with non-negative integer counts.

        Raises:
            InvalidCalculationInputError: If any count is negative.
        """
        for key, count in v.items():
            if count < 0:
                raise InvalidCalculationInputError(
                    parameter=f"position_count_for_{key}",
                    value=count,
                    expected="non-negative integer",
                )
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation of portfolio exposure with all metrics
            and breakdowns converted to JSON-serializable types.
        """
        return {
            "total_positions": self.total_positions,
            "total_market_value": str(self.total_market_value),
            "total_notional_value": str(self.total_notional_value),
            "total_collateral": str(self.total_collateral),
            "gross_exposure": str(self.gross_exposure),
            "net_exposure": str(self.net_exposure),
            "long_exposure": str(self.long_exposure),
            "short_exposure": str(self.short_exposure),
            "total_margin_requirement": str(self.total_margin_requirement),
            "average_leverage": str(self.average_leverage),
            "max_leverage": str(self.max_leverage),
            "portfolio_var_95": str(self.portfolio_var_95),
            "portfolio_var_99": str(self.portfolio_var_99),
            "total_stress_loss": str(self.total_stress_loss),
            "largest_position_weight": str(self.largest_position_weight),
            "top_5_concentration": str(self.top_5_concentration),
            "herfindahl_index": str(self.herfindahl_index),
            "overall_risk_score": str(self.overall_risk_score),
            "concentration_risk_score": str(self.concentration_risk_score),
            "leverage_risk_score": str(self.leverage_risk_score),
            "exposure_by_exchange": {k: str(v) for k, v in self.exposure_by_exchange.items()},
            "margin_by_exchange": {k: str(v) for k, v in self.margin_by_exchange.items()},
            "exposure_by_asset": {k: str(v) for k, v in self.exposure_by_asset.items()},
            "position_count_by_asset": self.position_count_by_asset,
            "currency_exposures": {k: str(v) for k, v in self.currency_exposures.items()},
            "breached_limits": [breach.to_dict() for breach in self.breached_limits],
            "warnings": self.warnings,
        }


@dataclass
class RiskLimits:
    """Portfolio risk limits configuration."""

    max_gross_exposure: Decimal | None = None
    max_net_exposure: Decimal | None = None
    max_leverage: Decimal = Decimal(10)
    max_var_95: Decimal | None = None
    max_concentration_single: Decimal = Decimal("0.3")  # 30% max single position
    max_concentration_top5: Decimal = Decimal("0.7")  # 70% max top 5
    min_positions: int = 3  # Minimum positions for diversification
    max_exchange_concentration: Decimal = Decimal("0.8")  # 80% max per exchange


class PortfolioExposureCalculator:
    """Calculates aggregate portfolio exposure and risk metrics."""

    def __init__(
        self,
        risk_limits: RiskLimits | None = None,
        correlation_matrix: dict[tuple[str, str], Decimal] | None = None,
        diversification_benefit: float = 0.8,  # Assume 20% diversification benefit
    ) -> None:
        """Initialize portfolio exposure calculator.

        Args:
            risk_limits: Risk limit configuration
            correlation_matrix: Optional correlation matrix for assets
            diversification_benefit: Diversification factor for VaR calculation
        """
        self.risk_limits = risk_limits or RiskLimits()
        self.correlation_matrix = correlation_matrix or {}
        self.diversification_benefit = Decimal(str(diversification_benefit))

        logger.info(
            "portfolio_exposure_calculator_initialized",
            has_risk_limits=risk_limits is not None,
            diversification_benefit=float(self.diversification_benefit),
        )

    async def calculate_portfolio_exposure(
        self,
        position_exposures: list[PositionExposure],
        total_account_value: Decimal,
    ) -> PortfolioExposure:
        """Calculate aggregate portfolio exposure metrics.

        Args:
            position_exposures: List of individual position exposures
            total_account_value: Total account value across all exchanges

        Returns:
            Aggregate portfolio exposure metrics
        """
        if not position_exposures:
            return self._create_empty_portfolio_exposure()

        # Initialize aggregators
        total_market_value = Decimal(0)
        total_notional = Decimal(0)
        total_collateral = Decimal(0)
        gross_exposure = Decimal(0)
        net_exposure = Decimal(0)
        long_exposure = Decimal(0)
        short_exposure = Decimal(0)
        total_margin_req = Decimal(0)
        total_var_95 = Decimal(0)
        total_var_99 = Decimal(0)
        total_stress_loss = Decimal(0)

        # Breakdown collections
        exposure_by_exchange: dict[str, Decimal] = defaultdict(Decimal)
        margin_by_exchange: dict[str, Decimal] = defaultdict(Decimal)
        exposure_by_asset: dict[str, Decimal] = defaultdict(Decimal)
        position_count_by_asset: dict[str, int] = defaultdict(int)
        currency_exposures: dict[str, Decimal] = defaultdict(Decimal)

        # Position sizes for concentration
        position_sizes: list[tuple[str, Decimal]] = []
        leverages: list[Decimal] = []

        # Process each position
        for pos_exp in position_exposures:
            # Aggregate totals
            total_market_value += pos_exp.market_value
            total_notional += pos_exp.notional_value
            total_collateral += pos_exp.market_value  # Assuming market value = collateral
            gross_exposure += pos_exp.gross_exposure
            net_exposure += pos_exp.net_exposure

            if pos_exp.side == "LONG":
                long_exposure += pos_exp.gross_exposure
            else:
                short_exposure += pos_exp.gross_exposure

            total_margin_req += pos_exp.margin_requirement
            total_var_95 += pos_exp.var_95 or Decimal(0)
            total_var_99 += pos_exp.var_99 or Decimal(0)
            total_stress_loss += pos_exp.stress_loss or Decimal(0)

            # Breakdowns
            exposure_by_exchange[pos_exp.exchange_id] += pos_exp.gross_exposure
            margin_by_exchange[pos_exp.exchange_id] += pos_exp.margin_requirement

            # Parse asset from symbol
            asset = self._parse_base_asset(pos_exp.symbol)
            exposure_by_asset[asset] += pos_exp.gross_exposure
            position_count_by_asset[asset] += 1

            # Currency exposures
            if pos_exp.base_currency and pos_exp.base_exposure:
                currency_exposures[pos_exp.base_currency] += pos_exp.base_exposure
            if pos_exp.quote_currency and pos_exp.quote_exposure:
                currency_exposures[pos_exp.quote_currency] += pos_exp.quote_exposure

            # Track for concentration
            position_sizes.append((pos_exp.position_id, pos_exp.gross_exposure))
            leverages.append(pos_exp.leverage)

        # Apply diversification benefit to VaR
        portfolio_var_95 = total_var_95 * self.diversification_benefit
        portfolio_var_99 = total_var_99 * self.diversification_benefit

        # Calculate leverage metrics
        avg_leverage = sum(leverages) / len(leverages) if leverages else Decimal(0)
        max_leverage = max(leverages) if leverages else Decimal(0)

        # Calculate concentration metrics
        concentration_metrics = self._calculate_concentration_metrics(
            position_sizes=position_sizes,
            total_exposure=gross_exposure,
        )

        # Calculate risk scores
        risk_scores = self._calculate_risk_scores(
            leverage=Decimal(str(avg_leverage)),
            max_leverage=max_leverage,
            concentration=concentration_metrics["herfindahl_index"],
            top5_concentration=concentration_metrics["top_5_concentration"],
            var_ratio=portfolio_var_95 / total_account_value
            if total_account_value > 0
            else Decimal(0),
        )

        # Check risk limits
        breached_limits, warnings = self._check_risk_limits(
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            max_leverage=max_leverage,
            portfolio_var_95=portfolio_var_95,
            largest_position_weight=concentration_metrics["largest_position_weight"],
            top_5_concentration=concentration_metrics["top_5_concentration"],
            position_count=len(position_exposures),
            exposure_by_exchange=exposure_by_exchange,
            total_exposure=gross_exposure,
        )

        portfolio_exposure = PortfolioExposure(
            total_positions=len(position_exposures),
            total_market_value=total_market_value,
            total_notional_value=total_notional,
            total_collateral=total_collateral,
            gross_exposure=gross_exposure,
            net_exposure=net_exposure,
            long_exposure=long_exposure,
            short_exposure=short_exposure,
            total_margin_requirement=total_margin_req,
            average_leverage=Decimal(str(avg_leverage)),
            max_leverage=max_leverage,
            portfolio_var_95=portfolio_var_95,
            portfolio_var_99=portfolio_var_99,
            total_stress_loss=total_stress_loss,
            largest_position_weight=concentration_metrics["largest_position_weight"],
            top_5_concentration=concentration_metrics["top_5_concentration"],
            herfindahl_index=concentration_metrics["herfindahl_index"],
            overall_risk_score=risk_scores["overall"],
            concentration_risk_score=risk_scores["concentration"],
            leverage_risk_score=risk_scores["leverage"],
            exposure_by_exchange=dict(exposure_by_exchange),
            margin_by_exchange=dict(margin_by_exchange),
            exposure_by_asset=dict(exposure_by_asset),
            position_count_by_asset=dict(position_count_by_asset),
            currency_exposures=dict(currency_exposures),
            breached_limits=breached_limits,
            warnings=warnings,
        )

        logger.info(
            "portfolio_exposure_calculated",
            total_positions=portfolio_exposure.total_positions,
            gross_exposure=float(portfolio_exposure.gross_exposure),
            net_exposure=float(portfolio_exposure.net_exposure),
            average_leverage=float(portfolio_exposure.average_leverage),
            overall_risk_score=float(portfolio_exposure.overall_risk_score),
            breached_limits_count=len(breached_limits),
        )

        return portfolio_exposure

    def _parse_base_asset(self, symbol: str) -> str:
        """Extract base asset from symbol.

        Returns:
            Base asset symbol after removing common suffixes and quote currencies.
        """
        # Remove common suffixes
        for suffix in ["-PERP", "-SWAP", "-FUTURES", "/USD", "/USDT", "/USDC"]:
            if suffix in symbol:
                return symbol.split(suffix, maxsplit=1)[0]

        # Handle concatenated pairs
        for quote in ["USDT", "USDC", "USD", "BTC", "ETH"]:
            if symbol.endswith(quote) and len(symbol) > len(quote):
                return symbol[: -len(quote)]

        return symbol

    def _calculate_concentration_metrics(
        self,
        position_sizes: list[tuple[str, Decimal]],
        total_exposure: Decimal,
    ) -> dict[str, Decimal]:
        """Calculate concentration metrics.

        Returns:
            Dictionary containing largest position weight, top 5 concentration,
            and Herfindahl concentration index.
        """
        if not position_sizes or total_exposure == 0:
            return {
                "largest_position_weight": Decimal(0),
                "top_5_concentration": Decimal(0),
                "herfindahl_index": Decimal(0),
            }

        # Sort by size
        sorted_positions = sorted(position_sizes, key=operator.itemgetter(1), reverse=True)

        # Largest position
        largest_weight = sorted_positions[0][1] / total_exposure

        # Top 5 concentration
        top_5_exposure = sum(pos[1] for pos in sorted_positions[:5])
        top_5_concentration = top_5_exposure / total_exposure

        # Herfindahl index (sum of squared weights)
        herfindahl = sum((pos[1] / total_exposure) ** 2 for pos in position_sizes)

        return {
            "largest_position_weight": largest_weight,
            "top_5_concentration": top_5_concentration,
            "herfindahl_index": Decimal(str(herfindahl)),
        }

    def _calculate_risk_scores(
        self,
        leverage: Decimal,
        max_leverage: Decimal,
        concentration: Decimal,
        top5_concentration: Decimal,
        var_ratio: Decimal,
    ) -> dict[str, Decimal]:
        """Calculate risk scores (0-100, higher is riskier).

        Returns:
            Dictionary containing leverage, concentration, VaR, and overall risk scores.
        """
        # Leverage score
        leverage_score = min(max_leverage / Decimal(10) * 100, Decimal(100))

        # Concentration score
        # Herfindahl > 0.2 is concentrated, > 0.4 is highly concentrated
        conc_score = min(concentration * 250, Decimal(100))

        # Adjust for top 5 concentration
        if top5_concentration > Decimal("0.8"):
            conc_score = min(conc_score + 20, Decimal(100))

        # VaR score (VaR > 10% of portfolio is high risk)
        var_score = min(var_ratio * 1000, Decimal(100))

        # Overall score (weighted average)
        overall_score = (
            leverage_score * Decimal("0.4")
            + conc_score * Decimal("0.4")
            + var_score * Decimal("0.2")
        )

        return {
            "leverage": leverage_score,
            "concentration": conc_score,
            "var": var_score,
            "overall": overall_score,
        }

    def _check_exposure_limits(
        self, gross_exposure: Decimal, net_exposure: Decimal
    ) -> list[RiskLimitBreach]:
        """Check exposure limits.

        Returns:
            List of risk limit breaches for gross and net exposure violations.
        """
        breached_limits: list[RiskLimitBreach] = []

        if (
            self.risk_limits.max_gross_exposure
            and gross_exposure > self.risk_limits.max_gross_exposure
        ):
            breached_limits.append(
                RiskLimitBreach(
                    limit_type="gross_exposure",
                    limit_value=float(self.risk_limits.max_gross_exposure),
                    current_value=float(gross_exposure),
                    severity="HIGH",
                )
            )

        if (
            self.risk_limits.max_net_exposure
            and abs(net_exposure) > self.risk_limits.max_net_exposure
        ):
            breached_limits.append(
                RiskLimitBreach(
                    limit_type="net_exposure",
                    limit_value=float(self.risk_limits.max_net_exposure),
                    current_value=float(abs(net_exposure)),
                    severity="MEDIUM",
                )
            )

        return breached_limits

    def _check_leverage_and_var_limits(
        self, max_leverage: Decimal, portfolio_var_95: Decimal
    ) -> list[RiskLimitBreach]:
        """Check leverage and VaR limits.

        Returns:
            List of risk limit breaches for leverage and VaR violations.
        """
        breached_limits: list[RiskLimitBreach] = []

        if max_leverage > self.risk_limits.max_leverage:
            breached_limits.append(
                RiskLimitBreach(
                    limit_type="leverage",
                    limit_value=float(self.risk_limits.max_leverage),
                    current_value=float(max_leverage),
                    severity="CRITICAL",
                )
            )

        if self.risk_limits.max_var_95 and portfolio_var_95 > self.risk_limits.max_var_95:
            breached_limits.append(
                RiskLimitBreach(
                    limit_type="var_95",
                    limit_value=float(self.risk_limits.max_var_95),
                    current_value=float(portfolio_var_95),
                    severity="HIGH",
                )
            )

        return breached_limits

    def _check_concentration_limits(
        self,
        largest_position_weight: Decimal,
        top_5_concentration: Decimal,
        position_count: int,
        exposure_by_exchange: dict[str, Decimal],
        total_exposure: Decimal,
    ) -> tuple[list[RiskLimitBreach], list[str]]:
        """Check concentration limits.

        Returns:
            Tuple of (breached limits list, warnings list) for concentration violations.
        """
        breached_limits: list[RiskLimitBreach] = []
        warnings: list[str] = []

        if largest_position_weight > self.risk_limits.max_concentration_single:
            breached_limits.append(
                RiskLimitBreach(
                    limit_type="single_position_concentration",
                    limit_value=float(self.risk_limits.max_concentration_single * 100),
                    current_value=float(largest_position_weight * 100),
                    severity="HIGH",
                )
            )

        if top_5_concentration > self.risk_limits.max_concentration_top5:
            warnings.append(
                f"Top 5 positions represent {float(top_5_concentration * 100):.1f}% of portfolio"
            )

        if position_count < self.risk_limits.min_positions:
            warnings.append(
                f"Portfolio has only {position_count} positions, "
                f"minimum {self.risk_limits.min_positions} recommended"
            )

        if total_exposure > 0:
            for exchange, exposure in exposure_by_exchange.items():
                exchange_weight = exposure / total_exposure
                if exchange_weight > self.risk_limits.max_exchange_concentration:
                    warnings.append(
                        f"Exchange {exchange} represents "
                        f"{float(exchange_weight * 100):.1f}% of exposure"
                    )

        return breached_limits, warnings

    def _check_risk_limits(
        self,
        gross_exposure: Decimal,
        net_exposure: Decimal,
        max_leverage: Decimal,
        portfolio_var_95: Decimal,
        largest_position_weight: Decimal,
        top_5_concentration: Decimal,
        position_count: int,
        exposure_by_exchange: dict[str, Decimal],
        total_exposure: Decimal,
    ) -> tuple[list[RiskLimitBreach], list[str]]:
        """Check risk limits and generate warnings.

        Returns:
            Tuple of (all breached limits, all warnings) from all risk checks.
        """
        breached_limits: list[RiskLimitBreach] = []
        warnings: list[str] = []

        # Check exposure limits
        breached_limits.extend(self._check_exposure_limits(gross_exposure, net_exposure))

        # Check leverage and VaR limits
        breached_limits.extend(self._check_leverage_and_var_limits(max_leverage, portfolio_var_95))

        # Check concentration limits
        concentration_breaches, concentration_warnings = self._check_concentration_limits(
            largest_position_weight,
            top_5_concentration,
            position_count,
            exposure_by_exchange,
            total_exposure,
        )
        breached_limits.extend(concentration_breaches)
        warnings.extend(concentration_warnings)

        return breached_limits, warnings

    def _create_empty_portfolio_exposure(self) -> PortfolioExposure:
        """Create empty portfolio exposure object.

        Returns:
            PortfolioExposure instance with all metrics set to zero for empty portfolio.
        """
        return PortfolioExposure(
            total_positions=0,
            total_market_value=Decimal(0),
            total_notional_value=Decimal(0),
            total_collateral=Decimal(0),
            gross_exposure=Decimal(0),
            net_exposure=Decimal(0),
            long_exposure=Decimal(0),
            short_exposure=Decimal(0),
            total_margin_requirement=Decimal(0),
            average_leverage=Decimal(0),
            max_leverage=Decimal(0),
            portfolio_var_95=Decimal(0),
            portfolio_var_99=Decimal(0),
            total_stress_loss=Decimal(0),
            largest_position_weight=Decimal(0),
            top_5_concentration=Decimal(0),
            herfindahl_index=Decimal(0),
            overall_risk_score=Decimal(0),
            concentration_risk_score=Decimal(0),
            leverage_risk_score=Decimal(0),
        )
