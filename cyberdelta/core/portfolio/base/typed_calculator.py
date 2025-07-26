"""Typed calculator base class with direct AppSettings access.

Following the risk module pattern of generic base classes with result types.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, TypeVar

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions.calculation import (
    CalculationError,
    InsufficientDataError,
    InvalidCalculationInputError,
    InvalidCalculatorNameError,
    InvalidRateRangeError,
    NegativeExecutionTimeError,
)


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol

# Type variables for input and result
TInput = TypeVar("TInput", bound=object)
TResult = TypeVar("TResult", bound=object)


@dataclass
class CalculationMetadata:
    """Calculation metadata with validation."""

    calculator: str = Field(min_length=1, description="Calculator name")
    error_type: str | None = Field(default=None, description="Error type if applicable")
    cache_hit: bool = Field(default=False, description="Whether result came from cache")
    data_source: str | None = Field(default=None, description="Data source used")

    @field_validator("calculator", mode="before")
    @classmethod
    def validate_calculator_name(cls, v: str) -> str:
        """Validate calculator name is non-empty."""
        if not v:
            raise InvalidCalculatorNameError(calculator_name=v)
        return v.strip()


@dataclass
class CalculationResult[TResult]:
    """Generic calculation result with validation."""

    success: bool = Field(description="Whether calculation succeeded")
    result: TResult | None = Field(default=None, description="Calculation result if successful")
    errors: list[str] = Field(default_factory=list, description="Error messages if failed")
    warnings: list[str] = Field(default_factory=list, description="Warning messages")
    metadata: CalculationMetadata | None = Field(default=None, description="Calculation metadata")
    execution_time_ms: float = Field(
        default=0.0, ge=0, description="Execution time in milliseconds"
    )
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC), description="Result timestamp"
    )

    @field_validator("execution_time_ms", mode="before")
    @classmethod
    def validate_execution_time(cls, v: float | str) -> float:
        """Validate execution time is non-negative."""
        value: float = v if isinstance(v, (int, float)) else float(v)
        if value < 0:
            raise NegativeExecutionTimeError(execution_time=value)
        return value

    @classmethod
    def success_result(
        cls,
        result: TResult,
        warnings: list[str] | None = None,
        metadata: CalculationMetadata | None = None,
        execution_time_ms: float = 0.0,
    ) -> CalculationResult[TResult]:
        """Create a successful result."""
        return cls(
            success=True,
            result=result,
            warnings=warnings or [],
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )

    @classmethod
    def failure_result(
        cls,
        errors: list[str],
        warnings: list[str] | None = None,
        metadata: CalculationMetadata | None = None,
        execution_time_ms: float = 0.0,
    ) -> CalculationResult[TResult]:
        """Create a failure result."""
        return cls(
            success=False,
            result=None,
            errors=errors,
            warnings=warnings or [],
            metadata=metadata,
            execution_time_ms=execution_time_ms,
        )


@dataclass
class CalculatorMetrics:
    """Calculator performance metrics with validation."""

    calculator_name: str = Field(min_length=1, description="Calculator name")
    calculation_count: int = Field(ge=0, description="Total calculations performed")
    error_count: int = Field(ge=0, description="Total errors encountered")
    error_rate: float = Field(ge=0, le=1, description="Error rate (0-1)")
    average_execution_time_ms: float = Field(
        ge=0, description="Average execution time in milliseconds"
    )
    total_execution_time_ms: float = Field(ge=0, description="Total execution time in milliseconds")
    cache_hits: int = Field(ge=0, description="Cache hits")
    cache_misses: int = Field(ge=0, description="Cache misses")
    cache_hit_rate: float = Field(ge=0, le=1, description="Cache hit rate (0-1)")

    @field_validator("error_rate", "cache_hit_rate", mode="before")
    @classmethod
    def validate_rates(cls, v: float | str) -> float:
        """Validate rates are between 0 and 1."""
        value: float = v if isinstance(v, (int, float)) else float(v)
        if not 0 <= value <= 1:
            raise InvalidRateRangeError(rate=value, rate_type="Cache hit rate")
        return value


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
        state_container: StateContainerProtocol[BaseStateModel],
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
                    metadata=CalculationMetadata(calculator=self.calculator_name),
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
                metadata=CalculationMetadata(
                    calculator=self.calculator_name,
                    error_type=type(e).__name__,
                ),
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
                metadata=CalculationMetadata(calculator=self.calculator_name),
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

    def get_metrics(self) -> CalculatorMetrics:
        """Get performance metrics.

        Returns:
            Calculator metrics using Pydantic model
        """
        avg_execution_time = (
            self.total_execution_time / self.calculation_count if self.calculation_count > 0 else 0
        )

        cache_total = self.cache_hits + self.cache_misses
        cache_hit_rate = self.cache_hits / cache_total if cache_total > 0 else 0

        return CalculatorMetrics(
            calculator_name=self.calculator_name,
            calculation_count=self.calculation_count,
            error_count=self.error_count,
            error_rate=self.error_count / max(self.calculation_count, 1),
            average_execution_time_ms=avg_execution_time,
            total_execution_time_ms=self.total_execution_time,
            cache_hits=self.cache_hits,
            cache_misses=self.cache_misses,
            cache_hit_rate=cache_hit_rate,
        )

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
