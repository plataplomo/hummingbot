"""Sizing result models."""

from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any

from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Type alias for configuration and metadata values
ConfigValue = str | int | float | bool | Decimal | list[Any] | dict[str, Any] | None


class SizingStatus(Enum):
    """Status of a sizing operation."""

    SUCCESS = "success"
    FAILED = "failed"
    INSUFFICIENT_CAPITAL = "insufficient_capital"
    CONSTRAINTS_VIOLATED = "constraints_violated"
    ERROR = "error"


@dataclass
class SizingResult:
    """Result of a sizing operation."""

    status: SizingStatus
    position_size_usd: Decimal
    allocation_percentage: Decimal
    message: str | None = None
    details: dict[str, Any] | None = None
    execution_time_ms: float | None = None

    # Sizing breakdown
    base_size: Decimal | None = None
    validation_factor: Decimal | None = None
    constraint_adjustments: dict[str, Decimal] | None = None

    # Risk metrics
    expected_return: Decimal | None = None
    volatility: Decimal | None = None
    kelly_fraction: Decimal | None = None
    risk_adjusted_size: Decimal | None = None

    @property
    def success(self) -> bool:
        """Check if sizing was successful.
        
        Returns:
            True if status is SUCCESS, False otherwise
        """
        return self.status == SizingStatus.SUCCESS

    @property
    def failed(self) -> bool:
        """Check if sizing failed.
        
        Returns:
            True if status is FAILED, False otherwise
        """
        return self.status == SizingStatus.FAILED

    @property
    def insufficient_capital(self) -> bool:
        """Check if sizing failed due to insufficient capital.
        
        Returns:
            True if status is INSUFFICIENT_CAPITAL, False otherwise
        """
        return self.status == SizingStatus.INSUFFICIENT_CAPITAL

    @property
    def constraints_violated(self) -> bool:
        """Check if sizing failed due to constraint violations.
        
        Returns:
            True if status is CONSTRAINTS_VIOLATED, False otherwise
        """
        return self.status == SizingStatus.CONSTRAINTS_VIOLATED

    @property
    def error(self) -> bool:
        """Check if sizing failed due to error.
        
        Returns:
            True if status is ERROR, False otherwise
        """
        return self.status == SizingStatus.ERROR

    @classmethod
    def success_result(
        cls,
        position_size_usd: Decimal,
        allocation_percentage: Decimal,
        message: str | None = None,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
        base_size: Decimal | None = None,
        validation_factor: Decimal | None = None,
        constraint_adjustments: dict[str, Decimal] | None = None,
        expected_return: Decimal | None = None,
        volatility: Decimal | None = None,
        kelly_fraction: Decimal | None = None,
        risk_adjusted_size: Decimal | None = None,
    ) -> "SizingResult":
        """Create a successful sizing result.
        
        Args:
            position_size_usd: The calculated position size in USD.
            allocation_percentage: The position allocation as a percentage of available capital.
            message: Optional success message.
            details: Optional additional details.
            execution_time_ms: Optional execution time in milliseconds.
            base_size: Optional base position size before adjustments.
            validation_factor: Optional validation adjustment factor.
            constraint_adjustments: Optional constraint-based adjustments.
            expected_return: Optional expected return.
            volatility: Optional volatility measure.
            kelly_fraction: Optional Kelly criterion fraction.
            risk_adjusted_size: Optional risk-adjusted position size.
            
        Returns:
            SizingResult: A successful sizing result with SUCCESS status.
        """
        return cls(
            status=SizingStatus.SUCCESS,
            position_size_usd=position_size_usd,
            allocation_percentage=allocation_percentage,
            message=message,
            details=details,
            execution_time_ms=execution_time_ms,
            base_size=base_size,
            validation_factor=validation_factor,
            constraint_adjustments=constraint_adjustments,
            expected_return=expected_return,
            volatility=volatility,
            kelly_fraction=kelly_fraction,
            risk_adjusted_size=risk_adjusted_size,
        )

    @classmethod
    def failure_result(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
    ) -> "SizingResult":
        """Create a failed sizing result.
        
        Args:
            message: Failure reason message.
            details: Optional additional failure details.
            execution_time_ms: Optional execution time in milliseconds.
            
        Returns:
            SizingResult: A failed sizing result with FAILED status and zero position size.
        """
        return cls(
            status=SizingStatus.FAILED,
            position_size_usd=Decimal(0),
            allocation_percentage=Decimal(0),
            message=message,
            details=details,
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def insufficient_capital_result(
        cls,
        required_capital: Decimal,
        available_capital: Decimal,
        message: str | None = None,
        execution_time_ms: float | None = None,
    ) -> "SizingResult":
        """Create an insufficient capital result.
        
        Args:
            required_capital: Capital required for the position.
            available_capital: Capital currently available.
            message: Optional custom message, defaults to a descriptive message.
            execution_time_ms: Optional execution time in milliseconds.
            
        Returns:
            SizingResult: A sizing result with INSUFFICIENT_CAPITAL status and zero position size.
        """
        return cls(
            status=SizingStatus.INSUFFICIENT_CAPITAL,
            position_size_usd=Decimal(0),
            allocation_percentage=Decimal(0),
            message=message
            or (
                f"Insufficient capital: required ${required_capital:.2f}, "
                f"available ${available_capital:.2f}"
            ),
            details={
                "required_capital": float(required_capital),
                "available_capital": float(available_capital),
            },
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def constraint_violation_result(
        cls,
        violated_constraints: list[str],
        message: str | None = None,
        execution_time_ms: float | None = None,
    ) -> "SizingResult":
        """Create a constraint violation result.
        
        Args:
            violated_constraints: List of constraint names that were violated.
            message: Optional custom message, defaults to listing the violations.
            execution_time_ms: Optional execution time in milliseconds.
            
        Returns:
            SizingResult: A sizing result with CONSTRAINTS_VIOLATED status and zero position size.
        """
        return cls(
            status=SizingStatus.CONSTRAINTS_VIOLATED,
            position_size_usd=Decimal(0),
            allocation_percentage=Decimal(0),
            message=message or f"Constraint violations: {', '.join(violated_constraints)}",
            details={
                "violated_constraints": violated_constraints,
            },
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def error_result(
        cls,
        message: str,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
    ) -> "SizingResult":
        """Create an error result.
        
        Args:
            message: Error message describing what went wrong.
            details: Optional additional error details.
            execution_time_ms: Optional execution time in milliseconds.
            
        Returns:
            SizingResult: A sizing result with ERROR status and zero position size.
        """
        return cls(
            status=SizingStatus.ERROR,
            position_size_usd=Decimal(0),
            allocation_percentage=Decimal(0),
            message=message,
            details=details,
            execution_time_ms=execution_time_ms,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary.
        
        Returns:
            dict[str, Any]: Dictionary representation with all sizing result fields.
        """
        return {
            "status": self.status.value,
            "position_size_usd": float(self.position_size_usd),
            "allocation_percentage": float(self.allocation_percentage),
            "message": self.message,
            "details": self.details,
            "execution_time_ms": self.execution_time_ms,
            "base_size": float(self.base_size) if self.base_size else None,
            "validation_factor": float(self.validation_factor) if self.validation_factor else None,
            "constraint_adjustments": (
                {k: float(v) for k, v in self.constraint_adjustments.items()}
                if self.constraint_adjustments
                else None
            ),
            "expected_return": float(self.expected_return) if self.expected_return else None,
            "volatility": float(self.volatility) if self.volatility else None,
            "kelly_fraction": float(self.kelly_fraction) if self.kelly_fraction else None,
            "risk_adjusted_size": (
                float(self.risk_adjusted_size) if self.risk_adjusted_size else None
            ),
        }


@dataclass
class SizingContext:
    """Context for sizing operations."""

    sizing_method: str
    available_capital: Decimal
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    # Capital constraints
    max_allocation_per_trade: Decimal | None = None
    min_allocation_per_trade: Decimal | None = None

    # Risk parameters
    max_leverage: Decimal | None = None
    risk_free_rate: Decimal | None = None

    def get_config_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get a configuration value.
        
        Args:
            key: Configuration key to look up.
            default: Default value if key not found.
            
        Returns:
            ConfigValue: The configuration value or default if not found.
        """
        if self.config is None:
            return default
        result = self.config.get(key, default)
        return result if result is not None else default

    def get_metadata_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get a metadata value.
        
        Args:
            key: Metadata key to look up.
            default: Default value if key not found.
            
        Returns:
            ConfigValue: The metadata value or default if not found.
        """
        if self.metadata is None:
            return default
        result = self.metadata.get(key, default)
        return result if result is not None else default

    def add_metadata(self, key: str, value: ConfigValue) -> None:
        """Add metadata to the context."""
        if self.metadata is None:
            self.metadata = {}
        self.metadata[key] = value

    def update_config(self, config: dict[str, Any]) -> None:
        """Update configuration."""
        if self.config is None:
            self.config = {}
        self.config.update(config)


@dataclass
class SizedOpportunity:
    """An arbitrage opportunity with calculated position size."""

    opportunity: ArbitrageOpportunity
    sizing_result: SizingResult

    # Position details
    long_size_usd: Decimal
    short_size_usd: Decimal

    # Risk metrics
    expected_profit_usd: Decimal | None = None
    expected_return_percentage: Decimal | None = None
    risk_adjusted_return: Decimal | None = None

    # Execution details
    sizing_method: str | None = None
    sizing_timestamp: float | None = None

    @property
    def total_size_usd(self) -> Decimal:
        """Total position size in USD.
        
        Returns:
            Sum of long and short position sizes
        """
        return self.long_size_usd + self.short_size_usd

    @property
    def is_valid(self) -> bool:
        """Check if the sized opportunity is valid.
        
        Returns:
            True if sizing successful and both positions have positive size
        """
        return self.sizing_result.success and self.long_size_usd > 0 and self.short_size_usd > 0

    @property
    def allocation_percentage(self) -> Decimal:
        """Allocation percentage from sizing result.
        
        Returns:
            Position allocation as percentage of available capital
        """
        return self.sizing_result.allocation_percentage

    @property
    def symbol(self) -> str:
        """Symbol from the opportunity.
        
        Returns:
            Trading symbol or 'unknown' if not available
        """
        return getattr(self.opportunity, "symbol", "unknown")

    @property
    def long_exchange(self) -> str:
        """Long exchange from the opportunity.
        
        Returns:
            Exchange name for long position or 'unknown' if not available
        """
        return getattr(self.opportunity, "long_exchange", "unknown")

    @property
    def short_exchange(self) -> str:
        """Short exchange from the opportunity.
        
        Returns:
            Exchange name for short position or 'unknown' if not available
        """
        return getattr(self.opportunity, "short_exchange", "unknown")

    def to_dict(self) -> dict[str, Any]:
        """Convert sized opportunity to dictionary.
        
        Returns:
            dict[str, Any]: Dictionary representation of the sized opportunity including all fields.
        """
        return {
            "symbol": self.symbol,
            "long_exchange": self.long_exchange,
            "short_exchange": self.short_exchange,
            "long_size_usd": float(self.long_size_usd),
            "short_size_usd": float(self.short_size_usd),
            "total_size_usd": float(self.total_size_usd),
            "allocation_percentage": float(self.allocation_percentage),
            "expected_profit_usd": (
                float(self.expected_profit_usd) if self.expected_profit_usd else None
            ),
            "expected_return_percentage": (
                float(self.expected_return_percentage) if self.expected_return_percentage else None
            ),
            "risk_adjusted_return": (
                float(self.risk_adjusted_return) if self.risk_adjusted_return else None
            ),
            "sizing_method": self.sizing_method,
            "sizing_timestamp": self.sizing_timestamp,
            "sizing_result": self.sizing_result.to_dict(),
        }

    @classmethod
    def from_sizing_result(
        cls,
        opportunity: ArbitrageOpportunity,
        sizing_result: SizingResult,
        sizing_method: str | None = None,
    ) -> "SizedOpportunity":
        """Create SizedOpportunity from sizing result.
        
        Args:
            opportunity: The arbitrage opportunity to size.
            sizing_result: The successful sizing result.
            sizing_method: Optional name of the sizing method used.
            
        Returns:
            SizedOpportunity: A new sized opportunity with position sizes split equally 
                between long and short.
            
        Raises:
            SizingError: If sizing_result is not successful.
        """
        if not sizing_result.success:
            raise SizingError(SizingError.CANNOT_CREATE_SIZED_OPPORTUNITY_FROM_FAILED_RESULT)

        # For arbitrage, typically split position equally between long and short
        position_size = sizing_result.position_size_usd
        long_size = position_size / 2
        short_size = position_size / 2

        return cls(
            opportunity=opportunity,
            sizing_result=sizing_result,
            long_size_usd=long_size,
            short_size_usd=short_size,
            sizing_method=sizing_method,
            expected_return_percentage=sizing_result.expected_return,
        )
