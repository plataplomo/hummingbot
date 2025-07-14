"""Base abstract class for all position sizers."""

import time
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Any

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext, SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


# Type alias for configuration values
ConfigValue = str | int | float | bool | Decimal | list[Any] | dict[str, Any] | None


class BaseSizer(ABC):
    """Abstract base class for all position sizers."""

    def _to_decimal(self, value: ConfigValue) -> Decimal:
        """Convert value to Decimal with validation."""
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            return Decimal(value)
        if isinstance(value, (int, float)):
            return Decimal(str(value))
        raise SizingError(SizingError.INVALID_TYPE_CONVERSION)

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        """Initialize the sizer.

        Args:
            config: Configuration dictionary for the sizer
        """
        self.config = config or {}
        self.logger = get_logger(self.__class__.__name__)
        self._enabled: bool = bool(self.config.get("enabled", True))

        # Common sizing parameters
        min_pos_raw = self.get_config_value("min_position_size", Decimal("1.0"))
        self.min_position_size = (
            self._to_decimal(min_pos_raw) if min_pos_raw is not None else Decimal("1.0")
        )
        max_pos_raw = self.get_config_value("max_position_size", Decimal("100000.0"))
        self.max_position_size = (
            self._to_decimal(max_pos_raw) if max_pos_raw is not None else Decimal("100000.0")
        )
        precision_raw = self.get_config_value("position_precision", 2)
        if isinstance(precision_raw, int):
            self.position_precision = precision_raw
        else:
            self.position_precision = 2

        # Risk parameters
        # 10%
        max_alloc_raw = self.get_config_value("max_allocation_per_trade", Decimal("0.1"))
        self.max_allocation_per_trade = (
            self._to_decimal(max_alloc_raw) if max_alloc_raw is not None else Decimal("0.1")
        )
        # 0.1%
        min_alloc_raw = self.get_config_value("min_allocation_per_trade", Decimal("0.001"))
        self.min_allocation_per_trade = (
            self._to_decimal(min_alloc_raw) if min_alloc_raw is not None else Decimal("0.001")
        )

        # Validation factors
        enable_val_raw = self.get_config_value("enable_validation_factors", True)
        self.enable_validation_factors = bool(enable_val_raw)
        default_val_raw = self.get_config_value("default_validation_factor", Decimal("0.8"))
        self.default_validation_factor = (
            self._to_decimal(default_val_raw) if default_val_raw is not None else Decimal("0.8")
        )

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the sizer."""
        ...

    @property
    @abstractmethod
    def sizing_method(self) -> str:
        """Sizing method identifier."""
        ...

    @abstractmethod
    async def _calculate_base_size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate base position size.

        This method should be implemented by subclasses to perform
        the specific sizing calculation.

        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing

        Returns:
            Base position size in USD
        """
        ...

    async def size(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> SizingResult:
        """Size an opportunity with error handling and validation.

        Args:
            opportunity: The arbitrage opportunity to size
            context: Context information for sizing

        Returns:
            SizingResult with position size and details
        """
        if not self._enabled:
            return SizingResult.failure_result(
                message=f"Sizer {self.name} is disabled",
                details={"sizer": self.name, "enabled": False},
            )

        start_time = time.time()

        try:
            self.logger.debug("Starting sizing", sizer_name=self.name)

            # Calculate base size
            base_size = await self._calculate_base_size(opportunity, context)

            # Apply validation factors
            validation_factor = await self._calculate_validation_factor(opportunity, context)
            adjusted_size = base_size * validation_factor

            # Apply constraints
            constrained_size = self._apply_constraints(adjusted_size, context)

            # Calculate allocation percentage
            allocation_percentage = (
                constrained_size / context.available_capital
                if context.available_capital > 0
                else Decimal(0)
            )

            # Create sizing result
            execution_time_ms = (time.time() - start_time) * 1000

            result = SizingResult.success_result(
                position_size_usd=constrained_size,
                allocation_percentage=allocation_percentage,
                message=f"Sizing completed with {self.name}",
                details={
                    "sizer": self.name,
                    "sizing_method": self.sizing_method,
                    "base_size": float(base_size),
                    "validation_factor": float(validation_factor),
                    "adjusted_size": float(adjusted_size),
                    "final_size": float(constrained_size),
                    "available_capital": float(context.available_capital),
                },
                execution_time_ms=execution_time_ms,
                base_size=base_size,
                validation_factor=validation_factor,
                risk_adjusted_size=constrained_size,
            )

            self.logger.debug(
                "Sizing completed",
                size_usd=float(constrained_size),
                allocation_percentage=float(allocation_percentage),
            )

        except SizingError as e:
            execution_time_ms = (time.time() - start_time) * 1000
            self.logger.exception("Sizing failed", sizer_name=self.name)
            return SizingResult.failure_result(
                message=str(e),
                details={
                    "sizer": self.name,
                    "error_type": type(e).__name__,
                    "execution_time_ms": execution_time_ms,
                },
            )

        except Exception as e:
            execution_time_ms = (time.time() - start_time) * 1000
            self.logger.exception("Sizing failed with unexpected error", sizer_name=self.name)
            return SizingResult.error_result(
                message=f"Unexpected error in {self.name}: {e!s}",
                details={
                    "sizer": self.name,
                    "error_type": type(e).__name__,
                    "execution_time_ms": execution_time_ms,
                },
            )
        else:
            return result

    async def _calculate_validation_factor(
        self,
        opportunity: ArbitrageOpportunity,
        context: SizingContext,
    ) -> Decimal:
        """Calculate validation factor for the opportunity.

        Args:
            opportunity: The arbitrage opportunity
            context: Sizing context

        Returns:
            Validation factor (0-1)
        """
        if not self.enable_validation_factors:
            return Decimal("1.0")

        # Start with default factor
        factor = self.default_validation_factor

        # Adjust based on spread quality
        spread_percentage = getattr(opportunity, "spread_percentage", None)
        if spread_percentage:
            try:
                spread_decimal = Decimal(str(spread_percentage))
                # Higher spread = higher confidence
                if spread_decimal > Decimal("0.01"):  # 1%
                    factor *= Decimal("1.1")  # 10% bonus
                elif spread_decimal < Decimal("0.001"):  # 0.1%
                    factor *= Decimal("0.9")  # 10% penalty
            except (ValueError, TypeError):
                pass

        # Adjust based on exchange quality
        long_exchange = getattr(opportunity, "long_exchange", None)
        short_exchange = getattr(opportunity, "short_exchange", None)

        if long_exchange and short_exchange:
            # Known high-quality exchanges get bonus
            quality_exchanges = {"binance", "coinbase", "kraken", "ftx"}
            if (
                long_exchange.lower() in quality_exchanges
                and short_exchange.lower() in quality_exchanges
            ):
                factor *= Decimal("1.05")  # 5% bonus

        # Ensure factor is within bounds
        factor = max(factor, Decimal("0.1"))
        return min(factor, Decimal("1.0"))

    def _apply_constraints(self, size: Decimal, context: SizingContext) -> Decimal:
        """Apply sizing constraints to the calculated size.

        Args:
            size: Calculated position size
            context: Sizing context

        Returns:
            Constrained position size
        """
        # Apply minimum/maximum position size constraints
        constrained_size = max(self.min_position_size, min(self.max_position_size, size))

        # Apply allocation percentage constraints
        if context.available_capital > 0:
            max_allocation_size = context.available_capital * self.max_allocation_per_trade
            min_allocation_size = context.available_capital * self.min_allocation_per_trade
            constrained_size = max(min_allocation_size, min(max_allocation_size, constrained_size))

        # Apply context-specific constraints
        if context.max_allocation_per_trade and context.available_capital > 0:
            max_context_size = context.available_capital * context.max_allocation_per_trade
            constrained_size = min(constrained_size, max_context_size)

        if context.min_allocation_per_trade and context.available_capital > 0:
            min_context_size = context.available_capital * context.min_allocation_per_trade
            constrained_size = max(constrained_size, min_context_size)

        # Round to specified precision
        return self._round_to_precision(constrained_size, self.position_precision)

    def _round_to_precision(self, value: Decimal, precision: int) -> Decimal:
        """Round value to specified decimal precision."""
        return value.quantize(Decimal(f"0.{'0' * precision}"))

    def enable(self) -> None:
        """Enable the sizer."""
        self._enabled = True
        self.logger.info("Sizer enabled", sizer_name=self.name)

    def disable(self) -> None:
        """Disable the sizer."""
        self._enabled = False
        self.logger.info("Sizer disabled", sizer_name=self.name)

    @property
    def enabled(self) -> bool:
        """Check if the sizer is enabled."""
        return self._enabled

    def get_config_value(self, key: str, default: ConfigValue = None) -> ConfigValue:
        """Get a configuration value.

        Args:
            key: Configuration key
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        result = self.config.get(key, default)
        return result if result is not None else default

    def update_config(self, config: dict[str, Any]) -> None:
        """Update the sizer configuration.

        Args:
            config: New configuration dictionary
        """
        self.config.update(config)

        # Update common parameters
        min_pos_raw = self.get_config_value("min_position_size", self.min_position_size)
        if min_pos_raw is not None:
            self.min_position_size = self._to_decimal(min_pos_raw)
        max_pos_raw = self.get_config_value("max_position_size", self.max_position_size)
        if max_pos_raw is not None:
            self.max_position_size = self._to_decimal(max_pos_raw)
        max_alloc_raw = self.get_config_value(
            "max_allocation_per_trade", self.max_allocation_per_trade
        )
        if max_alloc_raw is not None:
            self.max_allocation_per_trade = self._to_decimal(max_alloc_raw)
        min_alloc_raw = self.get_config_value(
            "min_allocation_per_trade", self.min_allocation_per_trade
        )
        if min_alloc_raw is not None:
            self.min_allocation_per_trade = self._to_decimal(min_alloc_raw)

        self.logger.info("Updated configuration", sizer_name=self.name)

    def set_position_bounds(self, min_size: Decimal, max_size: Decimal) -> None:
        """Set position size bounds.

        Args:
            min_size: Minimum position size
            max_size: Maximum position size
        """
        if min_size >= max_size:
            raise SizingError(SizingError.MIN_SIZE_MUST_BE_LESS_THAN_MAX_SIZE)

        self.min_position_size = min_size
        self.max_position_size = max_size
        self.logger.info(
            "Set position bounds", min_size_usd=float(min_size), max_size_usd=float(max_size)
        )

    def set_allocation_bounds(self, min_allocation: Decimal, max_allocation: Decimal) -> None:
        """Set allocation percentage bounds.

        Args:
            min_allocation: Minimum allocation percentage
            max_allocation: Maximum allocation percentage
        """
        if min_allocation >= max_allocation:
            raise SizingError(SizingError.MIN_ALLOCATION_MUST_BE_LESS_THAN_MAX_ALLOCATION)
        if min_allocation < 0 or max_allocation > 1:
            raise SizingError(SizingError.ALLOCATION_PERCENTAGES_MUST_BE_BETWEEN_0_AND_1)

        self.min_allocation_per_trade = min_allocation
        self.max_allocation_per_trade = max_allocation
        self.logger.info(
            "Set allocation bounds",
            min_allocation=float(min_allocation),
            max_allocation=float(max_allocation),
        )

    def __str__(self) -> str:
        """String representation of the sizer."""
        return f"{self.__class__.__name__}(name={self.name}, enabled={self._enabled})"

    def __repr__(self) -> str:
        """Detailed representation of the sizer."""
        return (
            f"{self.__class__.__name__}(name={self.name}, method={self.sizing_method}, "
            f"enabled={self._enabled})"
        )
