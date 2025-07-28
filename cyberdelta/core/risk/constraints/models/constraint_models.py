"""Constraint models for position sizing."""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any


class ConstraintType(Enum):
    """Types of constraints."""

    POSITION = "position"
    PORTFOLIO = "portfolio"
    EXCHANGE = "exchange"
    LEVERAGE = "leverage"


class ConstraintSeverity(Enum):
    """Severity levels for constraint violations."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ConstraintViolation:
    """Represents a constraint violation."""

    constraint_type: ConstraintType
    severity: ConstraintSeverity
    message: str
    details: dict[str, Any]

    # Violation values
    current_value: Decimal | None = None
    limit_value: Decimal | None = None

    # Context
    symbol: str | None = None
    exchange: str | None = None

    @property
    def is_blocking(self) -> bool:
        """Check if violation is blocking.
        
        Returns:
            True if severity is ERROR or CRITICAL, False otherwise
        """
        return self.severity in {ConstraintSeverity.ERROR, ConstraintSeverity.CRITICAL}

    def to_dict(self) -> dict[str, Any]:
        """Convert violation to dictionary.
        
        Returns:
            dict[str, Any]: Dictionary representation of the constraint violation.
        """
        return {
            "constraint_type": self.constraint_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "details": self.details,
            "current_value": float(self.current_value) if self.current_value else None,
            "limit_value": float(self.limit_value) if self.limit_value else None,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "is_blocking": self.is_blocking,
        }


@dataclass
class PositionConstraint:
    """Constraints for individual positions."""

    # Size constraints
    min_position_size: Decimal
    max_position_size: Decimal

    # Allocation constraints
    min_allocation_percentage: Decimal
    max_allocation_percentage: Decimal

    # Leverage constraints
    max_leverage: Decimal

    # Position-specific constraints
    max_positions_per_symbol: int = 1
    max_positions_per_exchange: int = 10

    # Risk constraints
    max_risk_per_position: Decimal | None = None
    max_volatility_per_position: Decimal | None = None

    def validate_size(self, size: Decimal) -> list[ConstraintViolation]:
        """Validate position size.
        
        Args:
            size: The position size to validate.
            
        Returns:
            list[ConstraintViolation]: List of violations if size is outside allowed range.
        """
        violations: list[ConstraintViolation] = []

        if size < self.min_position_size:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Position size ${size:.2f} below minimum ${self.min_position_size:.2f}"
                    ),
                    details={"constraint": "min_position_size"},
                    current_value=size,
                    limit_value=self.min_position_size,
                )
            )

        if size > self.max_position_size:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Position size ${size:.2f} exceeds maximum ${self.max_position_size:.2f}"
                    ),
                    details={"constraint": "max_position_size"},
                    current_value=size,
                    limit_value=self.max_position_size,
                )
            )

        return violations

    def validate_allocation(self, allocation: Decimal) -> list[ConstraintViolation]:
        """Validate allocation percentage.
        
        Args:
            allocation: The allocation percentage to validate (as decimal, e.g., 0.15 for 15%).
            
        Returns:
            list[ConstraintViolation]: List of violations if allocation is outside allowed range.
        """
        violations: list[ConstraintViolation] = []

        if allocation < self.min_allocation_percentage:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Allocation {allocation:.2%} below minimum "
                        f"{self.min_allocation_percentage:.2%}"
                    ),
                    details={"constraint": "min_allocation_percentage"},
                    current_value=allocation,
                    limit_value=self.min_allocation_percentage,
                )
            )

        if allocation > self.max_allocation_percentage:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.POSITION,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Allocation {allocation:.2%} exceeds maximum "
                        f"{self.max_allocation_percentage:.2%}"
                    ),
                    details={"constraint": "max_allocation_percentage"},
                    current_value=allocation,
                    limit_value=self.max_allocation_percentage,
                )
            )

        return violations


@dataclass
class PortfolioConstraint:
    """Constraints for the overall portfolio."""

    # Total exposure constraints
    max_total_allocation: Decimal
    max_total_positions: int

    # Concentration constraints
    max_allocation_per_symbol: Decimal
    max_allocation_per_exchange: Decimal

    # Risk constraints
    max_portfolio_risk: Decimal | None = None
    max_correlation_exposure: Decimal | None = None

    # Diversification constraints
    min_number_of_symbols: int = 1
    min_number_of_exchanges: int = 1

    def validate_total_allocation(
        self, current_allocation: Decimal, new_allocation: Decimal
    ) -> list[ConstraintViolation]:
        """Validate total portfolio allocation.
        
        Args:
            current_allocation: Current portfolio allocation percentage.
            new_allocation: Additional allocation to be added.
            
        Returns:
            list[ConstraintViolation]: List of violations if total allocation exceeds maximum.
        """
        violations: list[ConstraintViolation] = []
        total_allocation = current_allocation + new_allocation

        if total_allocation > self.max_total_allocation:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Total allocation {total_allocation:.2%} exceeds maximum "
                        f"{self.max_total_allocation:.2%}"
                    ),
                    details={"constraint": "max_total_allocation"},
                    current_value=total_allocation,
                    limit_value=self.max_total_allocation,
                )
            )

        return violations

    def validate_symbol_concentration(
        self, symbol: str, current_allocation: Decimal, new_allocation: Decimal
    ) -> list[ConstraintViolation]:
        """Validate symbol concentration.
        
        Args:
            symbol: The symbol to validate concentration for.
            current_allocation: Current allocation to this symbol.
            new_allocation: Additional allocation to be added.
            
        Returns:
            list[ConstraintViolation]: List of violations if symbol concentration exceeds maximum.
        """
        violations: list[ConstraintViolation] = []
        total_allocation = current_allocation + new_allocation

        if total_allocation > self.max_allocation_per_symbol:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Symbol {symbol} allocation {total_allocation:.2%} exceeds maximum "
                        f"{self.max_allocation_per_symbol:.2%}"
                    ),
                    details={"constraint": "max_allocation_per_symbol"},
                    current_value=total_allocation,
                    limit_value=self.max_allocation_per_symbol,
                    symbol=symbol,
                )
            )

        return violations

    def validate_exchange_concentration(
        self, exchange: str, current_allocation: Decimal, new_allocation: Decimal
    ) -> list[ConstraintViolation]:
        """Validate exchange concentration.
        
        Args:
            exchange: The exchange to validate concentration for.
            current_allocation: Current allocation to this exchange.
            new_allocation: Additional allocation to be added.
            
        Returns:
            list[ConstraintViolation]: List of violations if exchange concentration exceeds maximum.
        """
        violations: list[ConstraintViolation] = []
        total_allocation = current_allocation + new_allocation

        if total_allocation > self.max_allocation_per_exchange:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.PORTFOLIO,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Exchange {exchange} allocation {total_allocation:.2%} exceeds maximum "
                        f"{self.max_allocation_per_exchange:.2%}"
                    ),
                    details={"constraint": "max_allocation_per_exchange"},
                    current_value=total_allocation,
                    limit_value=self.max_allocation_per_exchange,
                    exchange=exchange,
                )
            )

        return violations


@dataclass
class ExchangeConstraint:
    """Constraints for exchange-specific limitations."""

    # Exchange limits
    max_positions_per_exchange: int
    max_allocation_per_exchange: Decimal

    # Trading constraints
    min_order_size: dict[str, Decimal]  # By exchange
    max_order_size: dict[str, Decimal]  # By exchange

    # Risk constraints
    max_leverage_per_exchange: dict[str, Decimal]  # By exchange

    # Operational constraints
    allowed_exchanges: list[str]
    blocked_exchanges: list[str]

    def validate_exchange_allowed(self, exchange: str) -> list[ConstraintViolation]:
        """Validate exchange is allowed.
        
        Args:
            exchange: The exchange identifier to validate.
            
        Returns:
            list[ConstraintViolation]: List of violations if exchange is blocked or not 
                in allowed list.
        """
        violations: list[ConstraintViolation] = []

        if exchange.lower() in [e.lower() for e in self.blocked_exchanges]:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.EXCHANGE,
                    severity=ConstraintSeverity.ERROR,
                    message=f"Exchange {exchange} is blocked",
                    details={"constraint": "blocked_exchanges"},
                    exchange=exchange,
                )
            )

        if self.allowed_exchanges and exchange.lower() not in [
            e.lower() for e in self.allowed_exchanges
        ]:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.EXCHANGE,
                    severity=ConstraintSeverity.ERROR,
                    message=f"Exchange {exchange} is not in allowed list",
                    details={"constraint": "allowed_exchanges"},
                    exchange=exchange,
                )
            )

        return violations

    def validate_order_size(self, exchange: str, order_size: Decimal) -> list[ConstraintViolation]:
        """Validate order size for exchange.
        
        Args:
            exchange: The exchange to validate order size for.
            order_size: The order size to validate.
            
        Returns:
            list[ConstraintViolation]: List of violations if order size is outside exchange limits.
        """
        violations: list[ConstraintViolation] = []

        # Check minimum order size
        if exchange in self.min_order_size:
            min_size = self.min_order_size[exchange]
            if order_size < min_size:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.EXCHANGE,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Order size ${order_size:.2f} below minimum "
                            f"${min_size:.2f} for {exchange}"
                        ),
                        details={"constraint": "min_order_size"},
                        current_value=order_size,
                        limit_value=min_size,
                        exchange=exchange,
                    )
                )

        # Check maximum order size
        if exchange in self.max_order_size:
            max_size = self.max_order_size[exchange]
            if order_size > max_size:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.EXCHANGE,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Order size ${order_size:.2f} exceeds maximum "
                            f"${max_size:.2f} for {exchange}"
                        ),
                        details={"constraint": "max_order_size"},
                        current_value=order_size,
                        limit_value=max_size,
                        exchange=exchange,
                    )
                )

        return violations


@dataclass
class LeverageConstraint:
    """Constraints for leverage usage."""

    # Global leverage limits
    max_total_leverage: Decimal
    max_net_leverage: Decimal

    # Symbol-specific leverage limits
    max_leverage_per_symbol: dict[str, Decimal]

    # Exchange-specific leverage limits
    max_leverage_per_exchange: dict[str, Decimal]

    # Risk-based leverage limits
    max_leverage_for_volatility: dict[str, Decimal]  # Volatility ranges -> max leverage

    def validate_total_leverage(
        self, current_leverage: Decimal, additional_leverage: Decimal
    ) -> list[ConstraintViolation]:
        """Validate total leverage.
        
        Args:
            current_leverage: Current total leverage.
            additional_leverage: Additional leverage to be added.
            
        Returns:
            list[ConstraintViolation]: List of violations if total leverage exceeds maximum.
        """
        violations: list[ConstraintViolation] = []
        total_leverage = current_leverage + additional_leverage

        if total_leverage > self.max_total_leverage:
            violations.append(
                ConstraintViolation(
                    constraint_type=ConstraintType.LEVERAGE,
                    severity=ConstraintSeverity.ERROR,
                    message=(
                        f"Total leverage {total_leverage:.2f}x exceeds maximum "
                        f"{self.max_total_leverage:.2f}x"
                    ),
                    details={"constraint": "max_total_leverage"},
                    current_value=total_leverage,
                    limit_value=self.max_total_leverage,
                )
            )

        return violations

    def validate_symbol_leverage(self, symbol: str, leverage: Decimal) -> list[ConstraintViolation]:
        """Validate symbol-specific leverage.
        
        Args:
            symbol: The symbol to validate leverage for.
            leverage: The leverage amount to validate.
            
        Returns:
            list[ConstraintViolation]: List of violations if leverage exceeds 
                symbol-specific maximum.
        """
        violations: list[ConstraintViolation] = []

        if symbol in self.max_leverage_per_symbol:
            max_leverage = self.max_leverage_per_symbol[symbol]
            if leverage > max_leverage:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.LEVERAGE,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Leverage {leverage:.2f}x for {symbol} exceeds maximum "
                            f"{max_leverage:.2f}x"
                        ),
                        details={"constraint": "max_leverage_per_symbol"},
                        current_value=leverage,
                        limit_value=max_leverage,
                        symbol=symbol,
                    )
                )

        return violations

    def validate_exchange_leverage(
        self, exchange: str, leverage: Decimal
    ) -> list[ConstraintViolation]:
        """Validate exchange-specific leverage.
        
        Args:
            exchange: The exchange to validate leverage for.
            leverage: The leverage amount to validate.
            
        Returns:
            list[ConstraintViolation]: List of violations if leverage exceeds 
                exchange-specific maximum.
        """
        violations: list[ConstraintViolation] = []

        if exchange in self.max_leverage_per_exchange:
            max_leverage = self.max_leverage_per_exchange[exchange]
            if leverage > max_leverage:
                violations.append(
                    ConstraintViolation(
                        constraint_type=ConstraintType.LEVERAGE,
                        severity=ConstraintSeverity.ERROR,
                        message=(
                            f"Leverage {leverage:.2f}x for {exchange} exceeds maximum "
                            f"{max_leverage:.2f}x"
                        ),
                        details={"constraint": "max_leverage_per_exchange"},
                        current_value=leverage,
                        limit_value=max_leverage,
                        exchange=exchange,
                    )
                )

        return violations
