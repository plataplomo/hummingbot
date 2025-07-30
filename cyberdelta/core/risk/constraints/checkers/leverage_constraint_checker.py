"""Leverage constraint checker."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.constraints.exceptions.constraint_exceptions import (
    LeverageConstraintError,
)
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    BaseConstraintValidator,
    ConstraintContext,
)
from cyberdelta.core.risk.constraints.models.constraint_models import (
    ConstraintSeverity,
    ConstraintType,
    ConstraintViolation,
    LeverageConstraint,
)
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


class LeverageConstraintChecker(BaseConstraintValidator):
    """Validates leverage constraints."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the leverage constraint checker."""
        super().__init__(config)
        self.logger = get_logger(self.__class__.__name__)

        # Create leverage constraint from config
        self.leverage_constraint = LeverageConstraint(
            max_total_leverage=self._config_to_decimal("max_total_leverage", Decimal("5.0")),
            max_net_leverage=self._config_to_decimal("max_net_leverage", Decimal("3.0")),
            max_leverage_per_symbol=self._config_to_dict_str_decimal("max_leverage_per_symbol", {}),
            max_leverage_per_exchange=self._config_to_dict_str_decimal(
                "max_leverage_per_exchange", {}
            ),
            max_leverage_for_volatility=self._config_to_dict_str_decimal(
                "max_leverage_for_volatility", {}
            ),
        )

        # Set default symbol-specific leverage limits
        self._set_default_symbol_limits()

        # Set default exchange-specific leverage limits
        self._set_default_exchange_limits()

        # Set default volatility-based leverage limits
        self._set_default_volatility_limits()

    def _config_to_decimal(self, key: str, default: Decimal) -> Decimal:
        """Convert config value to Decimal safely.

        Args:
            key: Configuration key to look up
            default: Default value if key not found

        Returns:
            Decimal value from config or default
        """
        value = self.get_config_value(key, default)
        if isinstance(value, (str, int, float)):
            return Decimal(str(value))
        if isinstance(value, Decimal):
            return value
        return default

    def _config_to_dict_str_decimal(
        self, key: str, default: dict[str, Decimal]
    ) -> dict[str, Decimal]:
        """Convert config value to dict[str, Decimal] safely.

        Args:
            key: Configuration key to look up
            default: Default dictionary if key not found

        Returns:
            Dictionary mapping strings to Decimal values
        """
        value = self.get_config_value(key, default)
        if isinstance(value, dict):
            result: dict[str, Decimal] = {}
            for k, v in value.items():
                if isinstance(v, (str, int, float)):
                    result[str(k)] = Decimal(str(v))
                elif isinstance(v, Decimal):
                    result[str(k)] = v
            return result
        return default

    def _set_default_symbol_limits(self) -> None:
        """Set default symbol-specific leverage limits."""
        default_symbol_limits = {
            "BTC": Decimal("5.0"),
            "ETH": Decimal("4.0"),
            "BNB": Decimal("3.0"),
            "SOL": Decimal("3.0"),
            "ADA": Decimal("2.0"),
            "DOT": Decimal("2.0"),
            "MATIC": Decimal("2.0"),
            "AVAX": Decimal("2.0"),
            "LINK": Decimal("2.0"),
            "UNI": Decimal("2.0"),
        }

        # Only set defaults if not already configured
        for symbol, limit in default_symbol_limits.items():
            if symbol not in self.leverage_constraint.max_leverage_per_symbol:
                self.leverage_constraint.max_leverage_per_symbol[symbol] = limit

    def _set_default_exchange_limits(self) -> None:
        """Set default exchange-specific leverage limits."""
        default_exchange_limits = {
            "binance": Decimal("5.0"),
            "coinbase": Decimal("3.0"),
            "kraken": Decimal("3.0"),
            "ftx": Decimal("5.0"),
            "bybit": Decimal("5.0"),
            "okx": Decimal("5.0"),
            "hyperliquid": Decimal("10.0"),
            "backpack": Decimal("3.0"),
        }

        # Only set defaults if not already configured
        for exchange, limit in default_exchange_limits.items():
            if exchange not in self.leverage_constraint.max_leverage_per_exchange:
                self.leverage_constraint.max_leverage_per_exchange[exchange] = limit

    def _set_default_volatility_limits(self) -> None:
        """Set default volatility-based leverage limits."""
        default_volatility_limits = {
            "low": Decimal("5.0"),  # < 0.2 (20% daily volatility)
            "medium": Decimal("3.0"),  # 0.2 - 0.5 (20-50% daily volatility)
            "high": Decimal("2.0"),  # 0.5 - 1.0 (50-100% daily volatility)
            "extreme": Decimal("1.0"),  # > 1.0 (>100% daily volatility)
        }

        # Only set defaults if not already configured
        for vol_range, limit in default_volatility_limits.items():
            if vol_range not in self.leverage_constraint.max_leverage_for_volatility:
                self.leverage_constraint.max_leverage_for_volatility[vol_range] = limit

    @property
    def name(self) -> str:
        """Name of the constraint."""
        return "leverage_constraint"

    @property
    def constraint_type(self) -> str:
        """Type of constraint."""
        return "leverage"

    async def _validate_constraint(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate leverage constraints.

        Args:
            opportunity: Sized trading opportunity to validate
            context: Current constraint context

        Returns:
            List of constraint violations found
        """
        violations: list[ConstraintViolation] = []

        # Calculate position leverage
        position_leverage = self._calculate_position_leverage(opportunity, context)

        # Validate total leverage
        total_violations = await self._validate_total_leverage(
            opportunity, context, position_leverage
        )
        violations.extend(total_violations)

        # Validate net leverage
        net_violations = await self._validate_net_leverage(opportunity, context, position_leverage)
        violations.extend(net_violations)

        # Validate symbol-specific leverage
        symbol_violations = await self._validate_symbol_leverage(opportunity, position_leverage)
        violations.extend(symbol_violations)

        # Validate exchange-specific leverage
        exchange_violations = await self._validate_exchange_leverage(
            opportunity, context, position_leverage
        )
        violations.extend(exchange_violations)

        # Validate volatility-based leverage
        volatility_violations = await self._validate_volatility_leverage(
            opportunity, position_leverage
        )
        violations.extend(volatility_violations)

        return violations

    def _calculate_position_leverage(
        self, opportunity: SizedOpportunity, context: ConstraintContext
    ) -> Decimal:
        """Calculate leverage for the position.

        Args:
            opportunity: Sized trading opportunity
            context: Current constraint context with available capital

        Returns:
            Calculated leverage ratio as Decimal
        """
        if context.available_capital <= 0:
            return Decimal(0)

        return opportunity.total_size_usd / context.available_capital

    async def _validate_total_leverage(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
        position_leverage: Decimal,
    ) -> list[ConstraintViolation]:
        """Validate total leverage constraints.

        Args:
            opportunity: Sized trading opportunity
            context: Current constraint context
            position_leverage: Calculated leverage for this position

        Returns:
            List of total leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Calculate total leverage including current positions
        current_total_leverage = context.current_leverage
        additional_leverage = position_leverage

        total_violations = self.leverage_constraint.validate_total_leverage(
            current_total_leverage,
            additional_leverage,
        )
        violations.extend(total_violations)

        return violations

    async def _validate_net_leverage(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
        position_leverage: Decimal,
    ) -> list[ConstraintViolation]:
        """Validate net leverage constraints.

        Args:
            opportunity: Sized trading opportunity
            context: Current constraint context
            position_leverage: Calculated leverage for this position

        Returns:
            List of net leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        # For arbitrage positions, net leverage should be lower since long/short cancel out
        # This is a simplified calculation
        current_net_leverage = context.current_leverage * Decimal("0.5")  # Assume 50% net
        # Arbitrage adds minimal net leverage
        additional_net_leverage = position_leverage * Decimal("0.1")

        total_net_leverage = current_net_leverage + additional_net_leverage

        if total_net_leverage > self.leverage_constraint.max_net_leverage:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.LEVERAGE,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Net leverage {total_net_leverage:.2f}x exceeds maximum "
                        f"{self.leverage_constraint.max_net_leverage:.2f}x"
                    ),
                    details={"constraint": "max_net_leverage"},
                    current_value=total_net_leverage,
                    limit_value=self.leverage_constraint.max_net_leverage,
                )
            )

        return violations

    async def _validate_symbol_leverage(
        self,
        opportunity: SizedOpportunity,
        position_leverage: Decimal,
    ) -> list[ConstraintViolation]:
        """Validate symbol-specific leverage constraints.

        Args:
            opportunity: Sized trading opportunity
            position_leverage: Calculated leverage for this position

        Returns:
            List of symbol-specific leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        symbol = opportunity.symbol

        # Check if symbol has specific leverage limits
        symbol_violations = self.leverage_constraint.validate_symbol_leverage(
            symbol, position_leverage
        )
        violations.extend(symbol_violations)

        return violations

    async def _validate_exchange_leverage(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
        position_leverage: Decimal,
    ) -> list[ConstraintViolation]:
        """Validate exchange-specific leverage constraints.

        Args:
            opportunity: Sized trading opportunity
            context: Current constraint context
            position_leverage: Calculated leverage for this position

        Returns:
            List of exchange-specific leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Check both exchanges
        for exchange in [opportunity.long_exchange, opportunity.short_exchange]:
            # Half the leverage goes to each exchange
            exchange_leverage = position_leverage / 2

            exchange_violations = self.leverage_constraint.validate_exchange_leverage(
                exchange,
                exchange_leverage,
            )
            violations.extend(exchange_violations)

        return violations

    async def _validate_volatility_leverage(
        self,
        opportunity: SizedOpportunity,
        position_leverage: Decimal,
    ) -> list[ConstraintViolation]:
        """Validate volatility-based leverage constraints.

        Args:
            opportunity: Sized trading opportunity
            position_leverage: Calculated leverage for this position

        Returns:
            List of volatility-based leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Get volatility for opportunity
        volatility = getattr(opportunity.opportunity, "volatility", None)
        if volatility:
            try:
                vol_decimal = Decimal(str(volatility))

                # Determine volatility range
                vol_range = self._get_volatility_range(vol_decimal)

                # Check if volatility range has leverage limits
                if vol_range in self.leverage_constraint.max_leverage_for_volatility:
                    max_leverage = self.leverage_constraint.max_leverage_for_volatility[vol_range]

                    if position_leverage > max_leverage:
                        violations.append(
                            ConstraintViolation(
                                constraint_type=ConstraintType.LEVERAGE,
                                severity=ConstraintSeverity.ERROR,
                                message=(
                                    f"Leverage {position_leverage:.2f}x exceeds maximum "
                                    f"{max_leverage:.2f}x for {vol_range} volatility "
                                    f"({vol_decimal:.2%})"
                                ),
                                details={
                                    "constraint": "max_leverage_for_volatility",
                                    "volatility_range": vol_range,
                                    "volatility": float(vol_decimal),
                                },
                                current_value=position_leverage,
                                limit_value=max_leverage,
                                symbol=opportunity.symbol,
                            )
                        )
            except (ValueError, TypeError):
                pass

        return violations

    def _get_volatility_range(self, volatility: Decimal) -> str:
        """Get volatility range for a given volatility value.

        Args:
            volatility: Volatility value as Decimal

        Returns:
            Volatility range category: 'low', 'medium', 'high', or 'extreme'
        """
        if volatility < Decimal("0.2"):
            return "low"
        if volatility < Decimal("0.5"):
            return "medium"
        if volatility < Decimal("1.0"):
            return "high"
        return "extreme"

    def set_total_leverage_limit(self, max_total: Decimal, max_net: Decimal) -> None:
        """Set total leverage limits.

        Args:
            max_total: Maximum total leverage allowed
            max_net: Maximum net leverage allowed

        Raises:
            LeverageConstraintError: If leverage limits are not positive
        """
        if max_total <= 0 or max_net <= 0:
            raise LeverageConstraintError(LeverageConstraintError.LEVERAGE_LIMITS_MUST_BE_POSITIVE)

        self.leverage_constraint.max_total_leverage = max_total
        self.leverage_constraint.max_net_leverage = max_net

        self.logger.info("Set leverage limits", max_total=float(max_total), max_net=float(max_net))

    def set_symbol_leverage_limit(self, symbol: str, max_leverage: Decimal) -> None:
        """Set leverage limit for a specific symbol.

        Args:
            symbol: Trading symbol
            max_leverage: Maximum leverage allowed for this symbol

        Raises:
            LeverageConstraintError: If leverage limit is not positive
        """
        if max_leverage <= 0:
            raise LeverageConstraintError(LeverageConstraintError.LEVERAGE_LIMITS_MUST_BE_POSITIVE)

        self.leverage_constraint.max_leverage_per_symbol[symbol] = max_leverage
        self.logger.info(
            "Set symbol leverage limit", symbol=symbol, max_leverage=float(max_leverage)
        )

    def set_exchange_leverage_limit(self, exchange: str, max_leverage: Decimal) -> None:
        """Set leverage limit for a specific exchange.

        Args:
            exchange: Exchange name
            max_leverage: Maximum leverage allowed for this exchange

        Raises:
            LeverageConstraintError: If leverage limit is not positive
        """
        if max_leverage <= 0:
            raise LeverageConstraintError(LeverageConstraintError.LEVERAGE_LIMITS_MUST_BE_POSITIVE)

        self.leverage_constraint.max_leverage_per_exchange[exchange] = max_leverage
        self.logger.info(
            "Set exchange leverage limit", exchange=exchange, max_leverage=float(max_leverage)
        )

    def set_volatility_leverage_limits(self, volatility_limits: dict[str, Decimal]) -> None:
        """Set volatility-based leverage limits.

        Args:
            volatility_limits: Dictionary mapping volatility ranges to leverage limits

        Raises:
            LeverageConstraintError: If any leverage limit is not positive
        """
        for vol_range, limit in volatility_limits.items():
            if limit <= 0:
                raise LeverageConstraintError(
                    LeverageConstraintError.LEVERAGE_LIMITS_MUST_BE_POSITIVE
                )

            self.leverage_constraint.max_leverage_for_volatility[vol_range] = limit

        self.logger.info("Set volatility leverage limits", limits=dict(volatility_limits))

    def get_leverage_limit_for_symbol(self, symbol: str) -> Decimal:
        """Get leverage limit for a specific symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Maximum leverage allowed for the symbol
        """
        return self.leverage_constraint.max_leverage_per_symbol.get(
            symbol, self.leverage_constraint.max_total_leverage
        )

    def get_leverage_limit_for_exchange(self, exchange: str) -> Decimal:
        """Get leverage limit for a specific exchange.

        Args:
            exchange: Exchange name

        Returns:
            Maximum leverage allowed for the exchange
        """
        return self.leverage_constraint.max_leverage_per_exchange.get(
            exchange, self.leverage_constraint.max_total_leverage
        )

    def get_leverage_limit_for_volatility(self, volatility: Decimal) -> Decimal:
        """Get leverage limit for a specific volatility level.

        Args:
            volatility: Volatility value as Decimal

        Returns:
            Maximum leverage allowed for the volatility level
        """
        vol_range = self._get_volatility_range(volatility)
        return self.leverage_constraint.max_leverage_for_volatility.get(
            vol_range, self.leverage_constraint.max_total_leverage
        )

    def get_constraint_stats(self) -> dict[str, Any]:
        """Get constraint statistics.

        Returns:
            Dictionary containing all leverage constraint limits and settings
        """
        return {
            "max_total_leverage": float(self.leverage_constraint.max_total_leverage),
            "max_net_leverage": float(self.leverage_constraint.max_net_leverage),
            "max_leverage_per_symbol": {
                k: float(v) for k, v in self.leverage_constraint.max_leverage_per_symbol.items()
            },
            "max_leverage_per_exchange": {
                k: float(v) for k, v in self.leverage_constraint.max_leverage_per_exchange.items()
            },
            "max_leverage_for_volatility": {
                k: float(v) for k, v in self.leverage_constraint.max_leverage_for_volatility.items()
            },
        }
