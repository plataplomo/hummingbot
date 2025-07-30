"""Portfolio constraint checker."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.constraints.exceptions.constraint_exceptions import (
    PortfolioConstraintError,
)
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    BaseConstraintValidator,
    ConstraintContext,
)
from cyberdelta.core.risk.constraints.models.constraint_models import (
    ConstraintSeverity,
    ConstraintType,
    ConstraintViolation,
    PortfolioConstraint,
)
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


class PortfolioConstraintChecker(BaseConstraintValidator):
    """Validates portfolio-level constraints."""

    def _config_to_decimal(self, key: str, default: Decimal) -> Decimal:
        """Convert config value to Decimal safely.

        Returns:
            Decimal value from configuration or default if conversion fails.
        """
        value = self.get_config_value(key, default)
        if isinstance(value, (str, int, float, Decimal)):
            return Decimal(str(value))
        return default

    def _config_to_int(self, key: str, default: int) -> int:
        """Convert config value to int safely.

        Returns:
            Integer value from configuration or default if conversion fails.
        """
        value = self.get_config_value(key, default)
        if isinstance(value, int):
            return value
        if isinstance(value, (str, float)):
            try:
                return int(value)
            except (ValueError, TypeError):
                pass
        return default

    def _config_to_decimal_optional(
        self, key: str, default: Decimal | None = None
    ) -> Decimal | None:
        """Convert config value to optional Decimal safely.

        Returns:
            Decimal value from configuration, None, or default if conversion fails.
        """
        value = self.get_config_value(key, default)
        if value is None:
            return None
        if isinstance(value, (str, int, float, Decimal)):
            return Decimal(str(value))
        return default

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the portfolio constraint checker."""
        super().__init__(config)
        self.logger = get_logger(self.__class__.__name__)

        # Create portfolio constraint from config
        self.portfolio_constraint = PortfolioConstraint(
            max_total_allocation=self._config_to_decimal("max_total_allocation", Decimal("0.8")),
            max_total_positions=self._config_to_int("max_total_positions", 50),
            max_allocation_per_symbol=self._config_to_decimal(
                "max_allocation_per_symbol", Decimal("0.2")
            ),
            max_allocation_per_exchange=self._config_to_decimal(
                "max_allocation_per_exchange", Decimal("0.4")
            ),
            max_portfolio_risk=self._config_to_decimal_optional("max_portfolio_risk", None),
            max_correlation_exposure=self._config_to_decimal_optional(
                "max_correlation_exposure", None
            ),
            min_number_of_symbols=self._config_to_int("min_number_of_symbols", 1),
            min_number_of_exchanges=self._config_to_int("min_number_of_exchanges", 1),
        )

    @property
    def name(self) -> str:
        """Name of the constraint."""
        return "portfolio_constraint"

    @property
    def constraint_type(self) -> str:
        """Type of constraint."""
        return "portfolio"

    async def _validate_constraint(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate portfolio constraints.

        Returns:
            List of constraint violations found during validation.
        """
        violations: list[ConstraintViolation] = []

        # Validate total allocation
        total_violations = await self._validate_total_allocation(opportunity, context)
        violations.extend(total_violations)

        # Validate position count
        position_violations = await self._validate_position_count(opportunity, context)
        violations.extend(position_violations)

        # Validate symbol concentration
        symbol_violations = await self._validate_symbol_concentration(opportunity, context)
        violations.extend(symbol_violations)

        # Validate exchange concentration
        exchange_violations = await self._validate_exchange_concentration(opportunity, context)
        violations.extend(exchange_violations)

        # Validate diversification
        diversification_violations = await self._validate_diversification(opportunity, context)
        violations.extend(diversification_violations)

        # Validate portfolio risk
        risk_violations = await self._validate_portfolio_risk(opportunity, context)
        violations.extend(risk_violations)

        return violations

    async def _validate_total_allocation(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate total portfolio allocation.

        Returns:
            List of allocation constraint violations.
        """
        violations: list[ConstraintViolation] = []

        current_total_allocation = context.get_total_allocation()
        new_allocation = opportunity.allocation_percentage

        portfolio_violations = self.portfolio_constraint.validate_total_allocation(
            current_total_allocation,
            new_allocation,
        )
        violations.extend(portfolio_violations)

        return violations

    async def _validate_position_count(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate total position count.

        Returns:
            List of position count constraint violations.
        """
        violations: list[ConstraintViolation] = []

        current_positions = len(context.current_positions)
        new_position_count = current_positions + 1

        if new_position_count > self.portfolio_constraint.max_total_positions:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Total positions {new_position_count} exceeds maximum "
                        f"{self.portfolio_constraint.max_total_positions}"
                    ),
                    details={"constraint": "max_total_positions"},
                    current_value=Decimal(str(new_position_count)),
                    limit_value=Decimal(str(self.portfolio_constraint.max_total_positions)),
                )
            )

        return violations

    async def _validate_symbol_concentration(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate symbol concentration.

        Returns:
            List of symbol concentration constraint violations.
        """
        violations: list[ConstraintViolation] = []

        symbol = opportunity.symbol
        current_symbol_allocation = context.get_current_allocation(symbol)
        new_allocation = opportunity.allocation_percentage

        symbol_violations = self.portfolio_constraint.validate_symbol_concentration(
            symbol,
            current_symbol_allocation,
            new_allocation,
        )
        violations.extend(symbol_violations)

        return violations

    async def _validate_exchange_concentration(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate exchange concentration.

        Returns:
            List of exchange concentration constraint violations.
        """
        violations: list[ConstraintViolation] = []

        # Validate long exchange concentration
        long_exchange = opportunity.long_exchange
        current_long_allocation = context.get_current_exchange_allocation(long_exchange)
        new_long_allocation = opportunity.allocation_percentage / 2  # Half goes to long

        long_violations = self.portfolio_constraint.validate_exchange_concentration(
            long_exchange,
            current_long_allocation,
            new_long_allocation,
        )
        violations.extend(long_violations)

        # Validate short exchange concentration
        short_exchange = opportunity.short_exchange
        current_short_allocation = context.get_current_exchange_allocation(short_exchange)
        new_short_allocation = opportunity.allocation_percentage / 2  # Half goes to short

        short_violations = self.portfolio_constraint.validate_exchange_concentration(
            short_exchange,
            current_short_allocation,
            new_short_allocation,
        )
        violations.extend(short_violations)

        return violations

    async def _validate_diversification(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate diversification requirements.

        Returns:
            List of diversification constraint violations.
        """
        violations: list[ConstraintViolation] = []

        # Get unique symbols and exchanges
        current_symbols = {pos.symbol for pos in context.current_positions}
        current_exchanges: set[str] = set()
        for pos in context.current_positions:
            current_exchanges.add(pos.long_exchange)
            current_exchanges.add(pos.short_exchange)

        # Add new opportunity
        new_symbols = current_symbols | {opportunity.symbol}
        new_exchanges: set[str] = current_exchanges | {
            opportunity.long_exchange,
            opportunity.short_exchange,
        }

        # Check minimum symbol diversification
        if len(new_symbols) < self.portfolio_constraint.min_number_of_symbols:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.WARNING,
                    message=(
                        f"Portfolio has {len(new_symbols)} symbols, minimum required: "
                        f"{self.portfolio_constraint.min_number_of_symbols}"
                    ),
                    details={"constraint": "min_number_of_symbols"},
                    current_value=Decimal(str(len(new_symbols))),
                    limit_value=Decimal(str(self.portfolio_constraint.min_number_of_symbols)),
                )
            )

        # Check minimum exchange diversification
        if len(new_exchanges) < self.portfolio_constraint.min_number_of_exchanges:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.WARNING,
                    message=(
                        f"Portfolio has {len(new_exchanges)} exchanges, minimum required: "
                        f"{self.portfolio_constraint.min_number_of_exchanges}"
                    ),
                    details={"constraint": "min_number_of_exchanges"},
                    current_value=Decimal(str(len(new_exchanges))),
                    limit_value=Decimal(str(self.portfolio_constraint.min_number_of_exchanges)),
                )
            )

        return violations

    async def _validate_portfolio_risk(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate portfolio risk constraints.

        Returns:
            List of risk-related constraint violations.
        """
        violations: list[ConstraintViolation] = []

        # Validate maximum portfolio risk if configured
        if self.portfolio_constraint.max_portfolio_risk is not None:
            # Calculate current portfolio risk (simplified)
            current_portfolio_risk = self._calculate_portfolio_risk(context)

            # Estimate additional risk from new opportunity
            opportunity_risk = self._calculate_opportunity_risk(opportunity)

            total_risk = current_portfolio_risk + opportunity_risk

            if total_risk > self.portfolio_constraint.max_portfolio_risk:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.PORTFOLIO,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Portfolio risk {total_risk:.4f} exceeds maximum "
                            f"{self.portfolio_constraint.max_portfolio_risk:.4f}"
                        ),
                        details={"constraint": "max_portfolio_risk"},
                        current_value=total_risk,
                        limit_value=self.portfolio_constraint.max_portfolio_risk,
                    )
                )

        # Validate correlation exposure if configured
        if self.portfolio_constraint.max_correlation_exposure is not None:
            correlation_violations = await self._validate_correlation_exposure(opportunity, context)
            violations.extend(correlation_violations)

        return violations

    def _calculate_portfolio_risk(self, context: ConstraintContext) -> Decimal:
        """Calculate current portfolio risk.

        Returns:
            Total portfolio risk as a Decimal value.
        """
        total_risk = Decimal(0)

        for position in context.current_positions:
            # Get volatility for position
            volatility = getattr(position.opportunity, "volatility", None)
            if volatility:
                try:
                    vol_decimal = Decimal(str(volatility))
                    position_risk = position.total_size_usd * vol_decimal
                    total_risk += position_risk
                except (ValueError, TypeError):
                    pass

        return total_risk

    def _calculate_opportunity_risk(self, opportunity: SizedOpportunity) -> Decimal:
        """Calculate risk for a single opportunity.

        Returns:
            Risk value for the opportunity as a Decimal.
        """
        # Get volatility for opportunity
        volatility = getattr(opportunity.opportunity, "volatility", None)
        if volatility:
            try:
                vol_decimal = Decimal(str(volatility))
                return opportunity.total_size_usd * vol_decimal
            except (ValueError, TypeError):
                pass

        # Fallback to allocation-based risk
        return opportunity.allocation_percentage * Decimal("0.1")  # 10% default volatility

    async def _validate_correlation_exposure(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate correlation exposure.

        Returns:
            List of correlation exposure constraint violations.
        """
        violations: list[ConstraintViolation] = []

        # This is a simplified correlation check
        # In practice, you'd want to use actual correlation matrices

        # Check if adding this opportunity increases concentration in similar assets
        symbol = opportunity.symbol
        similar_positions = [
            pos
            for pos in context.current_positions
            if pos.symbol.startswith(symbol[:3])  # Simplified similarity check
        ]

        if similar_positions:
            total_similar_allocation = sum(pos.allocation_percentage for pos in similar_positions)
            new_total_allocation = total_similar_allocation + opportunity.allocation_percentage

            if (
                self.portfolio_constraint.max_correlation_exposure is not None
                and new_total_allocation > self.portfolio_constraint.max_correlation_exposure
            ):
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.PORTFOLIO,
                        severity=ConstraintSeverity.WARNING,
                        message=(
                            f"Correlation exposure {new_total_allocation:.2%} exceeds maximum "
                            f"{self.portfolio_constraint.max_correlation_exposure:.2%}"
                        ),
                        details={"constraint": "max_correlation_exposure"},
                        current_value=new_total_allocation,
                        limit_value=self.portfolio_constraint.max_correlation_exposure,
                        symbol=symbol,
                    )
                )

        return violations

    def set_allocation_limits(
        self, max_total: Decimal, max_per_symbol: Decimal, max_per_exchange: Decimal
    ) -> None:
        """Set allocation limits.

        Raises:
            PortfolioConstraintError: If any limit is not positive.
        """
        if max_total <= 0 or max_per_symbol <= 0 or max_per_exchange <= 0:
            raise PortfolioConstraintError(
                PortfolioConstraintError.ALLOCATION_LIMITS_MUST_BE_POSITIVE
            )

        self.portfolio_constraint.max_total_allocation = max_total
        self.portfolio_constraint.max_allocation_per_symbol = max_per_symbol
        self.portfolio_constraint.max_allocation_per_exchange = max_per_exchange

        self.logger.info(
            "Set allocation limits",
            max_total=float(max_total),
            max_per_symbol=float(max_per_symbol),
            max_per_exchange=float(max_per_exchange),
        )

    def set_position_limits(self, max_total_positions: int) -> None:
        """Set position count limits.

        Raises:
            PortfolioConstraintError: If limit is not positive.
        """
        if max_total_positions <= 0:
            raise PortfolioConstraintError(
                PortfolioConstraintError.POSITION_LIMITS_MUST_BE_POSITIVE
            )

        self.portfolio_constraint.max_total_positions = max_total_positions
        self.logger.info("Set position limit", max_positions=max_total_positions)

    def set_diversification_requirements(self, min_symbols: int, min_exchanges: int) -> None:
        """Set diversification requirements.

        Raises:
            PortfolioConstraintError: If requirements are not positive.
        """
        if min_symbols <= 0 or min_exchanges <= 0:
            raise PortfolioConstraintError(
                PortfolioConstraintError.DIVERSIFICATION_REQUIREMENTS_MUST_BE_POSITIVE
            )

        self.portfolio_constraint.min_number_of_symbols = min_symbols
        self.portfolio_constraint.min_number_of_exchanges = min_exchanges

        self.logger.info(
            "Set diversification requirements", min_symbols=min_symbols, min_exchanges=min_exchanges
        )

    def set_risk_limits(
        self,
        max_portfolio_risk: Decimal | None = None,
        max_correlation_exposure: Decimal | None = None,
    ) -> None:
        """Set risk limits."""
        if max_portfolio_risk is not None:
            self.portfolio_constraint.max_portfolio_risk = max_portfolio_risk

        if max_correlation_exposure is not None:
            self.portfolio_constraint.max_correlation_exposure = max_correlation_exposure

        self.logger.info(
            "Set risk limits",
            max_portfolio_risk=float(max_portfolio_risk) if max_portfolio_risk else None,
            max_correlation_exposure=(
                float(max_correlation_exposure) if max_correlation_exposure else None
            ),
        )

    def get_constraint_stats(self) -> dict[str, Any]:
        """Get constraint statistics.

        Returns:
            Dictionary containing current constraint configuration values.
        """
        return {
            "max_total_allocation": float(self.portfolio_constraint.max_total_allocation),
            "max_total_positions": self.portfolio_constraint.max_total_positions,
            "max_allocation_per_symbol": float(self.portfolio_constraint.max_allocation_per_symbol),
            "max_allocation_per_exchange": float(
                self.portfolio_constraint.max_allocation_per_exchange
            ),
            "max_portfolio_risk": (
                float(self.portfolio_constraint.max_portfolio_risk)
                if self.portfolio_constraint.max_portfolio_risk
                else None
            ),
            "max_correlation_exposure": (
                float(self.portfolio_constraint.max_correlation_exposure)
                if self.portfolio_constraint.max_correlation_exposure
                else None
            ),
            "min_number_of_symbols": self.portfolio_constraint.min_number_of_symbols,
            "min_number_of_exchanges": self.portfolio_constraint.min_number_of_exchanges,
        }
