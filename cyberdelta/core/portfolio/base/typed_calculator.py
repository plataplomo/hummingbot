"""Typed calculator base class with direct AppSettings access.

Following the risk module pattern of generic base classes with result types.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, TypeVar

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.calculation import (
    CalculationError,
    InsufficientDataError,
    InvalidCalculationInputError,
)


# Type-preserving factory functions for dataclass fields
def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information."""
    return []


def _any_dict_factory() -> dict[str, Any]:
    """Factory function that preserves dict[str, Any] type information."""
    return {}


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol

# Type variables for input and result
TInput = TypeVar("TInput", bound=object)
TResult = TypeVar("TResult", bound=object)


@dataclass(frozen=True)
class CalculationResult[TResult]:
    """Generic calculation result with metadata."""

    success: bool
    result: TResult | None
    errors: list[str] = field(default_factory=_str_list_factory)
    warnings: list[str] = field(default_factory=_str_list_factory)
    metadata: dict[str, Any] = field(default_factory=_any_dict_factory)
    execution_time_ms: float = 0.0
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))

    @classmethod
    def success_result(
        cls,
        result: TResult,
        warnings: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        execution_time_ms: float = 0.0,
    ) -> CalculationResult[TResult]:
        """Create a successful result."""
        return cls(
            success=True,
            result=result,
            warnings=warnings or [],
            metadata=metadata or {},
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def failure_result(
        cls,
        errors: list[str],
        warnings: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        execution_time_ms: float = 0.0,
    ) -> CalculationResult[TResult]:
        """Create a failure result."""
        return cls(
            success=False,
            result=None,
            errors=errors,
            warnings=warnings or [],
            metadata=metadata or {},
            execution_time_ms=execution_time_ms,
        )


class TypedCalculator[TInput, TResult](ABC):
    """Base class for typed calculators with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Generic types for input/output
    - Result type pattern
    - Performance tracking
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[Any],
        calculator_name: str,
    ) -> None:
        """Initialize the typed calculator.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
            calculator_name: Name of this calculator for logging
        """
        self.app_settings = app_settings
        self.portfolio_config = app_settings.portfolio_tracker
        self.state_container = state_container
        self.calculator_name = calculator_name
        self.logger = get_logger(f"{self.__class__.__module__}.{self.__class__.__name__}")

        # Configuration from AppSettings
        self.calculation_config = self.portfolio_config.calculation
        self.validation_config = self.portfolio_config.validation

        # Performance tracking
        self.calculation_count = 0
        self.total_execution_time = 0.0
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0

    @abstractmethod
    async def calculate(self, input_data: TInput) -> CalculationResult[TResult]:
        """Perform the calculation.

        Args:
            input_data: Input data for calculation

        Returns:
            Calculation result
        """
        ...

    @abstractmethod
    async def validate_input(self, input_data: TInput) -> tuple[bool, list[str]]:
        """Validate input data.

        Args:
            input_data: Input to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        ...

    async def calculate_with_validation(self, input_data: TInput) -> CalculationResult[TResult]:
        """Calculate with input validation and error handling.

        Args:
            input_data: Input data for calculation

        Returns:
            Calculation result with metadata
        """
        start_time = time.time()

        try:
            # Validate input
            is_valid, errors = await self.validate_input(input_data)
            if not is_valid:
                self.error_count += 1
                execution_time = (time.time() - start_time) * 1000
                return CalculationResult[TResult].failure_result(
                    errors=errors,
                    metadata={"calculator": self.calculator_name},
                    execution_time_ms=execution_time,
                )

            # Perform calculation
            result = await self.calculate(input_data)

            # Update metrics
            execution_time = (time.time() - start_time) * 1000
            self.calculation_count += 1
            self.total_execution_time += execution_time

            if not result.success:
                self.error_count += 1

            # Update execution time in result
            return CalculationResult(
                success=result.success,
                result=result.result,
                errors=result.errors,
                warnings=result.warnings,
                metadata=result.metadata,
                execution_time_ms=execution_time,
                timestamp=result.timestamp,
            )

        except (CalculationError, InsufficientDataError, InvalidCalculationInputError) as e:
            self.error_count += 1
            execution_time = (time.time() - start_time) * 1000
            self.logger.exception(
                "Calculation error",
                error=str(e),
                error_type=type(e).__name__,
                calculator=self.calculator_name,
            )
            return CalculationResult[TResult].failure_result(
                errors=[str(e)],
                metadata={
                    "calculator": self.calculator_name,
                    "error_type": type(e).__name__,
                },
                execution_time_ms=execution_time,
            )
        except Exception as e:
            self.error_count += 1
            execution_time = (time.time() - start_time) * 1000
            self.logger.exception(
                "Unexpected calculation error",
                error=str(e),
                calculator=self.calculator_name,
            )
            return CalculationResult[TResult].failure_result(
                errors=[f"Unexpected error: {e}"],
                metadata={"calculator": self.calculator_name},
                execution_time_ms=execution_time,
            )

    async def batch_calculate(
        self,
        inputs: list[TInput],
    ) -> list[CalculationResult[TResult]]:
        """Perform batch calculations.

        Args:
            inputs: List of inputs to calculate

        Returns:
            List of calculation results
        """
        results: list[CalculationResult[TResult]] = []
        for input_data in inputs:
            result = await self.calculate_with_validation(input_data)
            results.append(result)
        return results

    def get_metrics(self) -> dict[str, Any]:
        """Get performance metrics.

        Returns:
            Dictionary of metrics
        """
        avg_execution_time = (
            self.total_execution_time / self.calculation_count if self.calculation_count > 0 else 0
        )

        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / cache_total if cache_total > 0 else 0

        return {
            "calculator_name": self.calculator_name,
            "calculation_count": self.calculation_count,
            "error_count": self.error_count,
            "error_rate": self.error_count / max(self.calculation_count, 1),
            "average_execution_time_ms": avg_execution_time,
            "total_execution_time_ms": self.total_execution_time,
            "cache_hits": self.cache_hits,
            "cache_misses": self.cache_misses,
            "cache_hit_rate": cache_hit_rate,
        }

    def reset_metrics(self) -> None:
        """Reset performance metrics."""
        self.calculation_count = 0
        self.total_execution_time = 0.0
        self.error_count = 0
        self.cache_hits = 0
        self.cache_misses = 0

    def __str__(self) -> str:
        """String representation."""
        return (
            f"{self.__class__.__name__}("
            f"calculator={self.calculator_name}, "
            f"calculations={self.calculation_count})"
        )
