"""Constraint interfaces for position sizing validation."""

import decimal
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Protocol

from cyberdelta.core.risk.constraints.models.constraint_models import ConstraintViolation
from cyberdelta.core.risk.exceptions.base_exceptions import RiskError
from cyberdelta.core.risk.sizing.models.sizing_result import SizedOpportunity
from cyberdelta.core.symbols import Symbol


# Type alias for configuration and metadata values
ConfigValue = str | int | float | bool | Decimal | list[Any] | dict[str, Any] | None


class ConstraintResultStatus(Enum):
    """Status of constraint validation."""

    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"


@dataclass
class ConstraintResult:
    """Result of constraint validation."""

    status: ConstraintResultStatus
    violations: list[ConstraintViolation]
    message: str | None = None
    details: dict[str, Any] | None = None

    # Performance metrics
    execution_time_ms: float | None = None
    constraints_checked: int = 0

    @property
    def passed(self) -> bool:
        """Check if all constraints passed."""
        return self.status == ConstraintResultStatus.PASSED

    @property
    def failed(self) -> bool:
        """Check if any constraints failed."""
        return self.status == ConstraintResultStatus.FAILED

    @property
    def has_warnings(self) -> bool:
        """Check if there are warnings."""
        return self.status == ConstraintResultStatus.WARNING

    @property
    def has_blocking_violations(self) -> bool:
        """Check if there are blocking violations."""
        return any(v.is_blocking for v in self.violations)

    @property
    def blocking_violations(self) -> list[ConstraintViolation]:
        """Get blocking violations."""
        return [v for v in self.violations if v.is_blocking]

    @property
    def warning_violations(self) -> list[ConstraintViolation]:
        """Get warning violations."""
        return [v for v in self.violations if not v.is_blocking]

    @classmethod
    def passed_result(
        cls,
        message: str | None = None,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
        constraints_checked: int = 0,
    ) -> "ConstraintResult":
        """Create a passed result.

        Args:
            message: Optional result message
            details: Optional result details
            execution_time_ms: Execution time in milliseconds
            constraints_checked: Number of constraints checked

        Returns:
            ConstraintResult instance with PASSED status
        """
        return cls(
            status=ConstraintResultStatus.PASSED,
            violations=[],
            message=message or "All constraints passed",
            details=details,
            execution_time_ms=execution_time_ms,
            constraints_checked=constraints_checked,
        )

    @classmethod
    def failed_result(
        cls,
        violations: list[ConstraintViolation],
        message: str | None = None,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
        constraints_checked: int = 0,
    ) -> "ConstraintResult":
        """Create a failed result.

        Args:
            violations: List of constraint violations
            message: Optional result message
            details: Optional result details
            execution_time_ms: Execution time in milliseconds
            constraints_checked: Number of constraints checked

        Returns:
            ConstraintResult instance with FAILED status
        """
        return cls(
            status=ConstraintResultStatus.FAILED,
            violations=violations,
            message=message or f"Constraints failed: {len(violations)} violations",
            details=details,
            execution_time_ms=execution_time_ms,
            constraints_checked=constraints_checked,
        )

    @classmethod
    def warning_result(
        cls,
        violations: list[ConstraintViolation],
        message: str | None = None,
        details: dict[str, Any] | None = None,
        execution_time_ms: float | None = None,
        constraints_checked: int = 0,
    ) -> "ConstraintResult":
        """Create a warning result.

        Args:
            violations: List of constraint violations (warnings)
            message: Optional result message
            details: Optional result details
            execution_time_ms: Execution time in milliseconds
            constraints_checked: Number of constraints checked

        Returns:
            ConstraintResult instance with WARNING status
        """
        return cls(
            status=ConstraintResultStatus.WARNING,
            violations=violations,
            message=message or f"Constraints passed with warnings: {len(violations)} warnings",
            details=details,
            execution_time_ms=execution_time_ms,
            constraints_checked=constraints_checked,
        )

    def to_dict(self) -> dict[str, Any]:
        """Convert result to dictionary.

        Returns:
            Dictionary representation of the constraint result
        """
        return {
            "status": self.status.value,
            "violations": [v.to_dict() for v in self.violations],
            "message": self.message,
            "details": self.details,
            "execution_time_ms": self.execution_time_ms,
            "constraints_checked": self.constraints_checked,
            "passed": self.passed,
            "failed": self.failed,
            "has_warnings": self.has_warnings,
            "has_blocking_violations": self.has_blocking_violations,
            "blocking_violations_count": len(self.blocking_violations),
            "warning_violations_count": len(self.warning_violations),
        }


@dataclass
class ConstraintContext:
    """Context for constraint validation."""

    # Available capital information
    total_capital: Decimal
    available_capital: Decimal
    reserved_capital: Decimal

    # Current portfolio state
    current_positions: list[SizedOpportunity]
    current_allocations: dict[str, Decimal]  # By symbol
    current_exchange_allocations: dict[str, Decimal]  # By exchange

    # Market data
    current_leverage: Decimal
    current_risk_metrics: dict[str, Any] | None = None

    # Configuration
    config: dict[str, Any] | None = None
    metadata: dict[str, Any] | None = None

    def get_current_allocation(self, symbol: Symbol) -> Decimal:
        """Get current allocation for a symbol.

        Args:
            symbol: Symbol to get allocation for

        Returns:
            Current allocation amount for the symbol
        """
        return self.current_allocations.get(symbol.value, Decimal(0))

    def get_current_exchange_allocation(self, exchange: str) -> Decimal:
        """Get current allocation for an exchange.

        Args:
            exchange: Exchange to get allocation for

        Returns:
            Current allocation amount for the exchange
        """
        return self.current_exchange_allocations.get(exchange, Decimal(0)) or Decimal(0)

    def get_total_allocation(self) -> Decimal:
        """Get total current allocation.

        Returns:
            Sum of all current allocations
        """
        return sum(self.current_allocations.values(), Decimal(0))

    def get_config_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get configuration value.

        Args:
            key: Configuration key to look up
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if self.config is None:
            return default
        result = self.config.get(key, default)
        return result if result is not None else default

    def get_metadata_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get metadata value.

        Args:
            key: Metadata key to look up
            default: Default value if key not found

        Returns:
            Metadata value or default
        """
        if self.metadata is None:
            return default
        result = self.metadata.get(key, default)
        return result if result is not None else default

    def add_metadata(self, key: str, value: ConfigValue) -> None:
        """Add metadata to context."""
        if self.metadata is None:
            self.metadata = {}
        self.metadata[key] = value


class ConstraintInterface(Protocol):
    """Protocol for constraint validators."""

    @property
    def name(self) -> str:
        """Name of the constraint."""
        ...

    @property
    def constraint_type(self) -> str:
        """Type of constraint."""
        ...

    async def validate(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> ConstraintResult:
        """Validate constraints for a sized opportunity.

        Args:
            opportunity: The sized opportunity to validate
            context: Context information for validation

        Returns:
            ConstraintResult with validation results
        """
        ...

    def is_enabled(self) -> bool:
        """Check if constraint is enabled."""
        ...

    def enable(self) -> None:
        """Enable the constraint."""
        ...

    def disable(self) -> None:
        """Disable the constraint."""
        ...


class BaseConstraintValidator(ABC):
    """Base class for constraint validators."""

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the validator."""
        self.config = config or {}
        self._enabled: bool = bool(self.config.get("enabled", True))

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the constraint."""
        ...

    @property
    @abstractmethod
    def constraint_type(self) -> str:
        """Type of constraint."""
        ...

    @abstractmethod
    async def _validate_constraint(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> list[ConstraintViolation]:
        """Validate specific constraint.

        Args:
            opportunity: The sized opportunity
            context: Validation context

        Returns:
            List of constraint violations
        """
        ...

    async def validate(
        self,
        opportunity: SizedOpportunity,
        context: ConstraintContext,
    ) -> ConstraintResult:
        """Validate constraints for a sized opportunity.

        Args:
            opportunity: The sized opportunity to validate
            context: Context information for validation

        Returns:
            ConstraintResult containing validation results
        """
        if not self._enabled:
            return ConstraintResult.passed_result(
                message=f"Constraint {self.name} is disabled",
                details={"constraint": self.name, "enabled": False},
            )

        start_time = time.time()

        try:
            violations = await self._validate_constraint(opportunity, context)
            execution_time_ms = (time.time() - start_time) * 1000

            # Categorize violations
            blocking_violations = [v for v in violations if v.is_blocking]
            warning_violations = [v for v in violations if not v.is_blocking]

            if blocking_violations:
                return ConstraintResult.failed_result(
                    violations=violations,
                    message=(
                        f"{self.name} validation failed: "
                        f"{len(blocking_violations)} blocking violations"
                    ),
                    details={"constraint": self.name},
                    execution_time_ms=execution_time_ms,
                    constraints_checked=1,
                )
            if warning_violations:
                return ConstraintResult.warning_result(
                    violations=violations,
                    message=(
                        f"{self.name} validation passed with warnings: "
                        f"{len(warning_violations)} warnings"
                    ),
                    details={"constraint": self.name},
                    execution_time_ms=execution_time_ms,
                    constraints_checked=1,
                )
            return ConstraintResult.passed_result(
                message=f"{self.name} validation passed",
                details={"constraint": self.name},
                execution_time_ms=execution_time_ms,
                constraints_checked=1,
            )

        except RiskError as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return ConstraintResult.failed_result(
                violations=[],
                message=f"{self.name} validation error: {e!s}",
                details={
                    "constraint": self.name,
                    "error": str(e),
                    "error_type": "risk_error",
                    "metadata": getattr(e, "metadata", {}),
                },
                execution_time_ms=execution_time_ms,
            )
        except (ValueError, TypeError, ArithmeticError) as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return ConstraintResult.failed_result(
                violations=[],
                message=f"{self.name} validation error: {e!s}",
                details={
                    "constraint": self.name,
                    "error": str(e),
                    "error_type": "calculation_error",
                },
                execution_time_ms=execution_time_ms,
            )
        except (KeyError, AttributeError, decimal.InvalidOperation, decimal.DivisionByZero) as e:
            execution_time_ms = (time.time() - start_time) * 1000
            return ConstraintResult.failed_result(
                violations=[],
                message=f"{self.name} validation error: {e!s}",
                details={
                    "constraint": self.name,
                    "error": str(e),
                    "error_type": "validation_error",
                },
                execution_time_ms=execution_time_ms,
            )

    def is_enabled(self) -> bool:
        """Check if constraint is enabled.

        Returns:
            True if constraint is enabled, False otherwise
        """
        return self._enabled

    def enable(self) -> None:
        """Enable the constraint."""
        self._enabled = True

    def disable(self) -> None:
        """Disable the constraint."""
        self._enabled = False

    def get_config_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get configuration value.

        Args:
            key: Configuration key to look up
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        result = self.config.get(key, default)
        return result if result is not None else default

    def update_config(self, config: dict[str, Any]) -> None:
        """Update configuration."""
        self.config.update(config)
