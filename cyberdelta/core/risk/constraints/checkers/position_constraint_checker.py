"""Position constraint checker."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.constraints.exceptions.constraint_exceptions import (
    PositionConstraintError,
)
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    BaseConstraintValidator,
    ConstraintContext,
)
from cyberdelta.core.risk.constraints.models.constraint_models import (
    ConstraintSeverity,
    ConstraintType,
    ConstraintViolation,
    PositionConstraint,
)
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


class PositionConstraintChecker(BaseConstraintValidator):
    """Validates position-level constraints."""

    def _config_to_decimal(self, key: str, default: Decimal) -> Decimal:
        """Convert config value to Decimal safely."""
        value = self.get_config_value(key, default)
        if isinstance(value, (str, int, float, Decimal)):
            return Decimal(str(value))
        return default

    def _config_to_int(self, key: str, default: int) -> int:
        """Convert config value to int safely."""
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
        """Convert config value to optional Decimal safely."""
        value = self.get_config_value(key, default)
        if value is None:
            return None
        if isinstance(value, (str, int, float, Decimal)):
            return Decimal(str(value))
        return default

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the position constraint checker."""
        super().__init__(config)
        self.logger = get_logger(self.__class__.__name__)

        # Create position constraint from config
        self.position_constraint = PositionConstraint(
            min_position_size=self._config_to_decimal("min_position_size", Decimal("1.0")),
            max_position_size=self._config_to_decimal("max_position_size", Decimal("100000.0")),
            min_allocation_percentage=self._config_to_decimal(
                "min_allocation_percentage", Decimal("0.001")
            ),
            max_allocation_percentage=self._config_to_decimal(
                "max_allocation_percentage", Decimal("0.1")
            ),
            max_leverage=self._config_to_decimal("max_leverage", Decimal("3.0")),
            max_positions_per_symbol=self._config_to_int("max_positions_per_symbol", 1),
            max_positions_per_exchange=self._config_to_int("max_positions_per_exchange", 10),
            max_risk_per_position=self._config_to_decimal_optional("max_risk_per_position", None),
            max_volatility_per_position=self._config_to_decimal_optional(
                "max_volatility_per_position", None
            ),
        )

    @property
    def name(self) -> str:
        """Name of the constraint."""
        return "position_constraint"

    @property
    def constraint_type(self) -> str:
        """Type of constraint."""
        return "position"

    async def _validate_constraint(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate position constraints."""
        violations: list[ConstraintViolation] = []

        # Validate position size
        position_size = opportunity.total_size_usd
        size_violations = self.position_constraint.validate_size(position_size)
        violations.extend(size_violations)

        # Validate allocation percentage
        allocation_percentage = opportunity.allocation_percentage
        allocation_violations = self.position_constraint.validate_allocation(allocation_percentage)
        violations.extend(allocation_violations)

        # Validate leverage
        leverage_violations = await self._validate_leverage(opportunity, context)
        violations.extend(leverage_violations)

        # Validate position limits
        limits_violations = await self._validate_position_limits(opportunity, context)
        violations.extend(limits_violations)

        # Validate risk constraints
        risk_violations = await self._validate_risk_constraints(opportunity, context)
        violations.extend(risk_violations)

        return violations

    async def _validate_leverage(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate leverage constraints."""
        violations: list[ConstraintViolation] = []

        # Calculate implied leverage
        position_size = opportunity.total_size_usd
        if context.available_capital > 0:
            implied_leverage = position_size / context.available_capital

            if implied_leverage > self.position_constraint.max_leverage:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.POSITION,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Position leverage {implied_leverage:.2f}x exceeds maximum "
                            f"{self.position_constraint.max_leverage:.2f}x"
                        ),
                        details={"constraint": "max_leverage"},
                        current_value=implied_leverage,
                        limit_value=self.position_constraint.max_leverage,
                        symbol=opportunity.symbol,
                    )
                )

        return violations

    async def _validate_position_limits(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate position count limits."""
        violations: list[ConstraintViolation] = []

        # Count existing positions for this symbol
        symbol_positions = [
            pos for pos in context.current_positions if pos.symbol == opportunity.symbol
        ]

        if len(symbol_positions) >= self.position_constraint.max_positions_per_symbol:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Maximum positions per symbol "
                        f"({self.position_constraint.max_positions_per_symbol}) exceeded for "
                        f"{opportunity.symbol}"
                    ),
                    details={"constraint": "max_positions_per_symbol"},
                    current_value=Decimal(str(len(symbol_positions) + 1)),
                    limit_value=Decimal(str(self.position_constraint.max_positions_per_symbol)),
                    symbol=opportunity.symbol,
                )
            )

        # Count existing positions for exchanges
        long_exchange_positions = [
            pos
            for pos in context.current_positions
            if pos.long_exchange == opportunity.long_exchange
        ]

        short_exchange_positions = [
            pos
            for pos in context.current_positions
            if pos.short_exchange == opportunity.short_exchange
        ]

        if len(long_exchange_positions) >= self.position_constraint.max_positions_per_exchange:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Maximum positions per exchange "
                        f"({self.position_constraint.max_positions_per_exchange}) exceeded for "
                        f"{opportunity.long_exchange}"
                    ),
                    details={"constraint": "max_positions_per_exchange"},
                    current_value=Decimal(str(len(long_exchange_positions) + 1)),
                    limit_value=Decimal(str(self.position_constraint.max_positions_per_exchange)),
                    exchange=opportunity.long_exchange,
                )
            )

        if len(short_exchange_positions) >= self.position_constraint.max_positions_per_exchange:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Maximum positions per exchange "
                        f"({self.position_constraint.max_positions_per_exchange}) exceeded for "
                        f"{opportunity.short_exchange}"
                    ),
                    details={"constraint": "max_positions_per_exchange"},
                    current_value=Decimal(str(len(short_exchange_positions) + 1)),
                    limit_value=Decimal(str(self.position_constraint.max_positions_per_exchange)),
                    exchange=opportunity.short_exchange,
                )
            )

        return violations

    async def _validate_risk_constraints(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate risk-based constraints."""
        violations: list[ConstraintViolation] = []

        # Validate position risk if configured
        if self.position_constraint.max_risk_per_position is not None:
            # Calculate position risk (simplified as position size * volatility)
            volatility = getattr(opportunity.opportunity, "volatility", None)
            if volatility:
                try:
                    vol_decimal = Decimal(str(volatility))
                    position_risk = opportunity.total_size_usd * vol_decimal

                    if position_risk > self.position_constraint.max_risk_per_position:
                        violations.append(
                            ConstraintViolation(
                                constraint_type=ConstraintType.POSITION,
                                severity=ConstraintSeverity.ERROR,
                                message=(
                                    f"Position risk ${position_risk:.2f} exceeds maximum "
                                    f"${self.position_constraint.max_risk_per_position:.2f}"
                                ),
                                details={"constraint": "max_risk_per_position"},
                                current_value=position_risk,
                                limit_value=self.position_constraint.max_risk_per_position,
                                symbol=opportunity.symbol,
                            )
                        )
                except (ValueError, TypeError):
                    pass

        # Validate position volatility if configured
        if self.position_constraint.max_volatility_per_position is not None:
            volatility = getattr(opportunity.opportunity, "volatility", None)
            if volatility:
                try:
                    vol_decimal = Decimal(str(volatility))

                    if vol_decimal > self.position_constraint.max_volatility_per_position:
                        violations.append(
                            ConstraintViolation(
                                constraint_type=ConstraintType.POSITION,
                                severity=ConstraintSeverity.WARNING,
                                message=(
                                    f"Position volatility {vol_decimal:.4f} exceeds maximum "
                                    f"{self.position_constraint.max_volatility_per_position:.4f}"
                                ),
                                details={"constraint": "max_volatility_per_position"},
                                current_value=vol_decimal,
                                limit_value=self.position_constraint.max_volatility_per_position,
                                symbol=opportunity.symbol,
                            )
                        )
                except (ValueError, TypeError):
                    pass

        return violations

    def set_position_size_limits(self, min_size: Decimal, max_size: Decimal) -> None:
        """Set position size limits."""
        if min_size >= max_size:
            raise PositionConstraintError(
                PositionConstraintError.MIN_SIZE_MUST_BE_LESS_THAN_MAX_SIZE
            )

        self.position_constraint.min_position_size = min_size
        self.position_constraint.max_position_size = max_size
        self.logger.info(
            "Set position size limits", min_size_usd=float(min_size), max_size_usd=float(max_size)
        )

    def set_allocation_limits(self, min_allocation: Decimal, max_allocation: Decimal) -> None:
        """Set allocation percentage limits."""
        if min_allocation >= max_allocation:
            raise PositionConstraintError(
                PositionConstraintError.MIN_ALLOCATION_MUST_BE_LESS_THAN_MAX_ALLOCATION
            )

        self.position_constraint.min_allocation_percentage = min_allocation
        self.position_constraint.max_allocation_percentage = max_allocation
        self.logger.info(
            "Set allocation limits",
            min_allocation=float(min_allocation),
            max_allocation=float(max_allocation),
        )

    def set_leverage_limit(self, max_leverage: Decimal) -> None:
        """Set maximum leverage."""
        if max_leverage <= 0:
            raise PositionConstraintError(PositionConstraintError.MAX_LEVERAGE_MUST_BE_POSITIVE)

        self.position_constraint.max_leverage = max_leverage
        self.logger.info("Set leverage limit", max_leverage=float(max_leverage))

    def set_position_count_limits(self, max_per_symbol: int, max_per_exchange: int) -> None:
        """Set position count limits."""
        if max_per_symbol <= 0 or max_per_exchange <= 0:
            raise PositionConstraintError(
                PositionConstraintError.POSITION_COUNT_LIMITS_MUST_BE_POSITIVE
            )

        self.position_constraint.max_positions_per_symbol = max_per_symbol
        self.position_constraint.max_positions_per_exchange = max_per_exchange
        self.logger.info(
            "Set position count limits",
            max_per_symbol=max_per_symbol,
            max_per_exchange=max_per_exchange,
        )

    def set_risk_limits(
        self, max_risk: Decimal | None = None, max_volatility: Decimal | None = None
    ) -> None:
        """Set risk limits."""
        if max_risk is not None:
            self.position_constraint.max_risk_per_position = max_risk

        if max_volatility is not None:
            self.position_constraint.max_volatility_per_position = max_volatility

        self.logger.info(
            "Set risk limits",
            max_risk=float(max_risk) if max_risk is not None else None,
            max_volatility=float(max_volatility) if max_volatility is not None else None,
        )

    def get_constraint_stats(self) -> dict[str, Any]:
        """Get constraint statistics."""
        return {
            "min_position_size": float(self.position_constraint.min_position_size),
            "max_position_size": float(self.position_constraint.max_position_size),
            "min_allocation_percentage": float(self.position_constraint.min_allocation_percentage),
            "max_allocation_percentage": float(self.position_constraint.max_allocation_percentage),
            "max_leverage": float(self.position_constraint.max_leverage),
            "max_positions_per_symbol": self.position_constraint.max_positions_per_symbol,
            "max_positions_per_exchange": self.position_constraint.max_positions_per_exchange,
            "max_risk_per_position": (
                float(self.position_constraint.max_risk_per_position)
                if self.position_constraint.max_risk_per_position is not None
                else None
            ),
            "max_volatility_per_position": (
                float(self.position_constraint.max_volatility_per_position)
                if self.position_constraint.max_volatility_per_position is not None
                else None
            ),
        }
