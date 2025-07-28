"""Exchange constraint checker."""

from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.constraints.exceptions.constraint_exceptions import (
    ExchangeConstraintError,
)
from cyberdelta.core.risk.constraints.interfaces.constraint_interfaces import (
    BaseConstraintValidator,
    ConstraintContext,
)
from cyberdelta.core.risk.constraints.models.constraint_models import (
    ConstraintSeverity,
    ConstraintType,
    ConstraintViolation,
    ExchangeConstraint,
)
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity


class ExchangeConstraintChecker(BaseConstraintValidator):
    """Validates exchange-specific constraints."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the exchange constraint checker."""
        super().__init__(config)
        self.logger = get_logger(self.__class__.__name__)

        # Create exchange constraint from config
        self.exchange_constraint = ExchangeConstraint(
            max_positions_per_exchange=self._config_to_int("max_positions_per_exchange", 10),
            max_allocation_per_exchange=self._config_to_decimal(
                "max_allocation_per_exchange", Decimal("0.4")
            ),
            min_order_size=self._config_to_dict_str_decimal("min_order_size", {}),
            max_order_size=self._config_to_dict_str_decimal("max_order_size", {}),
            max_leverage_per_exchange=self._config_to_dict_str_decimal(
                "max_leverage_per_exchange", {}
            ),
            allowed_exchanges=self._config_to_list_str("allowed_exchanges", []),
            blocked_exchanges=self._config_to_list_str("blocked_exchanges", []),
        )

        # Set default order sizes for common exchanges
        self._set_default_order_sizes()

    def _config_to_decimal(self, key: str, default: Decimal) -> Decimal:
        """Convert config value to Decimal safely.

        Args:
            key: Configuration key to retrieve
            default: Default value if conversion fails

        Returns:
            Decimal value or default if conversion fails
        """
        value = self.get_config_value(key, default)
        if isinstance(value, (str, int, float)):
            return Decimal(str(value))
        if isinstance(value, Decimal):
            return value
        return default

    def _config_to_int(self, key: str, default: int) -> int:
        """Convert config value to int safely.

        Args:
            key: Configuration key to retrieve
            default: Default value if conversion fails

        Returns:
            Integer value or default if conversion fails
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

    def _config_to_dict_str_decimal(
        self, key: str, default: dict[str, Decimal]
    ) -> dict[str, Decimal]:
        """Convert config value to dict[str, Decimal] safely.

        Args:
            key: Configuration key to retrieve
            default: Default value if conversion fails

        Returns:
            Dictionary with string keys and Decimal values
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

    def _config_to_list_str(self, key: str, default: list[str]) -> list[str]:
        """Convert config value to list[str] safely.

        Args:
            key: Configuration key to retrieve
            default: Default value if conversion fails

        Returns:
            List of strings or default if conversion fails
        """
        value = self.get_config_value(key, default)
        if isinstance(value, list):
            return [str(item) for item in value if item is not None]
        return default

    def _set_default_order_sizes(self) -> None:
        """Set default order sizes for common exchanges."""
        default_min_sizes = {
            "binance": Decimal("10.0"),
            "coinbase": Decimal("5.0"),
            "kraken": Decimal("5.0"),
            "ftx": Decimal("1.0"),
            "bybit": Decimal("1.0"),
            "okx": Decimal("1.0"),
            "hyperliquid": Decimal("1.0"),
            "backpack": Decimal("1.0"),
        }

        default_max_sizes = {
            "binance": Decimal("1000000.0"),
            "coinbase": Decimal("1000000.0"),
            "kraken": Decimal("500000.0"),
            "ftx": Decimal("1000000.0"),
            "bybit": Decimal("1000000.0"),
            "okx": Decimal("1000000.0"),
            "hyperliquid": Decimal("500000.0"),
            "backpack": Decimal("100000.0"),
        }

        # Only set defaults if not already configured
        for exchange, min_size in default_min_sizes.items():
            if exchange not in self.exchange_constraint.min_order_size:
                self.exchange_constraint.min_order_size[exchange] = min_size

        for exchange, max_size in default_max_sizes.items():
            if exchange not in self.exchange_constraint.max_order_size:
                self.exchange_constraint.max_order_size[exchange] = max_size

    @property
    def name(self) -> str:
        """Name of the constraint."""
        return "exchange_constraint"

    @property
    def constraint_type(self) -> str:
        """Type of constraint."""
        return "exchange"

    async def _validate_constraint(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate exchange constraints.

        Args:
            opportunity: Sized opportunity to validate
            context: Constraint validation context

        Returns:
            List of constraint violations, empty if all constraints pass
        """
        violations: list[ConstraintViolation] = []

        # Validate both long and short exchanges
        exchange_pairs = [
            ("long", opportunity.long_exchange),
            ("short", opportunity.short_exchange),
        ]
        for exchange_type, exchange in exchange_pairs:
            # Validate exchange is allowed
            exchange_violations = self.exchange_constraint.validate_exchange_allowed(exchange)
            violations.extend(exchange_violations)

            # Validate order size
            order_size = (
                opportunity.long_size_usd if exchange_type == "long" else opportunity.short_size_usd
            )
            size_violations = self.exchange_constraint.validate_order_size(exchange, order_size)
            violations.extend(size_violations)

            # Validate position count per exchange
            position_violations = await self._validate_position_count(exchange, context)
            violations.extend(position_violations)

            # Validate allocation per exchange
            allocation_violations = await self._validate_exchange_allocation(
                exchange, opportunity, context
            )
            violations.extend(allocation_violations)

            # Validate leverage per exchange
            leverage_violations = await self._validate_exchange_leverage(
                exchange, opportunity, context
            )
            violations.extend(leverage_violations)

        return violations

    async def _validate_position_count(
        self,
        exchange: str,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate position count per exchange.

        Args:
            exchange: Exchange identifier
            context: Constraint validation context

        Returns:
            List of position count constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Count current positions on this exchange
        exchange_positions = [
            pos
            for pos in context.current_positions
            if exchange in {pos.long_exchange, pos.short_exchange}
        ]

        current_count = len(exchange_positions)
        new_count = current_count + 1

        if new_count > self.exchange_constraint.max_positions_per_exchange:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.EXCHANGE,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Position count {new_count} exceeds maximum "
                        f"{self.exchange_constraint.max_positions_per_exchange} for {exchange}"
                    ),
                    details={"constraint": "max_positions_per_exchange"},
                    current_value=Decimal(str(new_count)),
                    limit_value=Decimal(str(self.exchange_constraint.max_positions_per_exchange)),
                    exchange=exchange,
                )
            )

        return violations

    async def _validate_exchange_allocation(
        self,
        exchange: str,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate allocation per exchange.

        Args:
            exchange: Exchange identifier
            opportunity: Sized opportunity to validate
            context: Constraint validation context

        Returns:
            List of allocation constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Calculate current allocation on this exchange
        current_allocation = context.get_current_exchange_allocation(exchange)

        # Calculate new allocation (half of opportunity allocation per exchange for arbitrage)
        new_allocation = opportunity.allocation_percentage / 2

        total_allocation = current_allocation + new_allocation

        if total_allocation > self.exchange_constraint.max_allocation_per_exchange:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.EXCHANGE,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Exchange allocation {total_allocation:.2%} exceeds maximum "
                        f"{self.exchange_constraint.max_allocation_per_exchange:.2%} for {exchange}"
                    ),
                    details={"constraint": "max_allocation_per_exchange"},
                    current_value=total_allocation,
                    limit_value=self.exchange_constraint.max_allocation_per_exchange,
                    exchange=exchange,
                )
            )

        return violations

    async def _validate_exchange_leverage(
        self,
        exchange: str,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate leverage per exchange.

        Args:
            exchange: Exchange identifier
            opportunity: Sized opportunity to validate
            context: Constraint validation context

        Returns:
            List of leverage constraint violations
        """
        violations: list[ConstraintViolation] = []

        # Check if exchange has specific leverage limits
        if exchange in self.exchange_constraint.max_leverage_per_exchange:
            max_leverage = self.exchange_constraint.max_leverage_per_exchange[exchange]

            # Calculate implied leverage for this exchange
            if context.available_capital > 0:
                position_size = opportunity.total_size_usd / 2  # Half per exchange
                implied_leverage = position_size / context.available_capital

                if implied_leverage > max_leverage:
                    violations.append(
                        ConstraintViolation(
                            constraint_type=ConstraintType.EXCHANGE,
                            severity=ConstraintSeverity.ERROR,
                            message=(
                                f"Exchange leverage {implied_leverage:.2f}x exceeds maximum "
                                f"{max_leverage:.2f}x for {exchange}"
                            ),
                            details={"constraint": "max_leverage_per_exchange"},
                            current_value=implied_leverage,
                            limit_value=max_leverage,
                            exchange=exchange,
                        )
                    )

        return violations

    def add_allowed_exchange(self, exchange: str) -> None:
        """Add an exchange to the allowed list."""
        if exchange not in self.exchange_constraint.allowed_exchanges:
            self.exchange_constraint.allowed_exchanges.append(exchange)
            self.logger.info("Added exchange to allowed list", exchange=exchange)

    def remove_allowed_exchange(self, exchange: str) -> None:
        """Remove an exchange from the allowed list."""
        if exchange in self.exchange_constraint.allowed_exchanges:
            self.exchange_constraint.allowed_exchanges.remove(exchange)
            self.logger.info("Removed exchange from allowed list", exchange=exchange)

    def block_exchange(self, exchange: str) -> None:
        """Block an exchange."""
        if exchange not in self.exchange_constraint.blocked_exchanges:
            self.exchange_constraint.blocked_exchanges.append(exchange)
            self.logger.info("Blocked exchange", exchange=exchange)

    def unblock_exchange(self, exchange: str) -> None:
        """Unblock an exchange."""
        if exchange in self.exchange_constraint.blocked_exchanges:
            self.exchange_constraint.blocked_exchanges.remove(exchange)
            self.logger.info("Unblocked exchange", exchange=exchange)

    def set_order_size_limits(self, exchange: str, min_size: Decimal, max_size: Decimal) -> None:
        """Set order size limits for an exchange.

        Args:
            exchange: Exchange identifier
            min_size: Minimum order size in USD
            max_size: Maximum order size in USD

        Raises:
            ExchangeConstraintError: If min_size >= max_size
        """
        if min_size >= max_size:
            raise ExchangeConstraintError(
                ExchangeConstraintError.INVALID_ORDER_SIZE_RANGE,
                metadata={"min_size": float(min_size), "max_size": float(max_size)},
            )

        self.exchange_constraint.min_order_size[exchange] = min_size
        self.exchange_constraint.max_order_size[exchange] = max_size
        self.logger.info(
            "Set order size limits",
            exchange=exchange,
            min_size_usd=float(min_size),
            max_size_usd=float(max_size),
        )

    def set_exchange_leverage_limit(self, exchange: str, max_leverage: Decimal) -> None:
        """Set leverage limit for an exchange.

        Args:
            exchange: Exchange identifier
            max_leverage: Maximum leverage allowed

        Raises:
            ExchangeConstraintError: If max_leverage <= 0
        """
        if max_leverage <= 0:
            raise ExchangeConstraintError(
                ExchangeConstraintError.INVALID_LEVERAGE_VALUE,
                metadata={"max_leverage": float(max_leverage)},
            )

        self.exchange_constraint.max_leverage_per_exchange[exchange] = max_leverage
        self.logger.info("Set leverage limit", exchange=exchange, max_leverage=float(max_leverage))

    def set_exchange_allocation_limit(self, max_allocation: Decimal) -> None:
        """Set allocation limit per exchange.

        Args:
            max_allocation: Maximum allocation percentage (0-1)

        Raises:
            ExchangeConstraintError: If max_allocation <= 0 or max_allocation > 1
        """
        if max_allocation <= 0 or max_allocation > 1:
            raise ExchangeConstraintError(
                ExchangeConstraintError.INVALID_ALLOCATION_RANGE,
                metadata={"max_allocation": float(max_allocation), "valid_range": "0-1"},
            )

        self.exchange_constraint.max_allocation_per_exchange = max_allocation
        self.logger.info("Set exchange allocation limit", max_allocation=float(max_allocation))

    def set_exchange_position_limit(self, max_positions: int) -> None:
        """Set position limit per exchange.

        Args:
            max_positions: Maximum number of positions per exchange

        Raises:
            ExchangeConstraintError: If max_positions <= 0
        """
        if max_positions <= 0:
            raise ExchangeConstraintError(
                ExchangeConstraintError.INVALID_POSITION_LIMIT,
                metadata={"max_positions": max_positions},
            )

        self.exchange_constraint.max_positions_per_exchange = max_positions
        self.logger.info("Set exchange position limit", max_positions=max_positions)

    def get_exchange_info(self, exchange: str) -> dict[str, Any]:
        """Get information about an exchange.

        Args:
            exchange: Exchange identifier

        Returns:
            Dictionary containing exchange information including limits and constraints
        """
        return {
            "exchange": exchange,
            "allowed": (
                exchange.lower()
                not in [e.lower() for e in self.exchange_constraint.blocked_exchanges]
            ),
            "blocked": (
                exchange.lower() in [e.lower() for e in self.exchange_constraint.blocked_exchanges]
            ),
            "in_allowed_list": (
                exchange.lower() in [e.lower() for e in self.exchange_constraint.allowed_exchanges]
            ),
            "min_order_size": float(
                self.exchange_constraint.min_order_size.get(exchange, Decimal(0))
            ),
            "max_order_size": float(
                self.exchange_constraint.max_order_size.get(exchange, Decimal(1000000))
            ),
            "max_leverage": float(
                self.exchange_constraint.max_leverage_per_exchange.get(exchange, Decimal(1))
            ),
        }

    def get_constraint_stats(self) -> dict[str, Any]:
        """Get constraint statistics.

        Returns:
            Dictionary containing all constraint configuration and statistics
        """
        return {
            "max_positions_per_exchange": self.exchange_constraint.max_positions_per_exchange,
            "max_allocation_per_exchange": float(
                self.exchange_constraint.max_allocation_per_exchange
            ),
            "allowed_exchanges": self.exchange_constraint.allowed_exchanges,
            "blocked_exchanges": self.exchange_constraint.blocked_exchanges,
            "min_order_sizes": {
                k: float(v) for k, v in self.exchange_constraint.min_order_size.items()
            },
            "max_order_sizes": {
                k: float(v) for k, v in self.exchange_constraint.max_order_size.items()
            },
            "max_leverage_per_exchange": {
                k: float(v) for k, v in self.exchange_constraint.max_leverage_per_exchange.items()
            },
        }
