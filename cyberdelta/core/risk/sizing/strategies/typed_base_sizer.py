"""Base abstract class for all position sizers with direct AppSettings access."""

import time
from abc import ABC, abstractmethod
from decimal import Decimal

from cyberdelta.config import AppSettings
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.risk.exceptions.sizing_exceptions import SizingError
from cyberdelta.core.risk.sizing.models.sizing_result import SizingContext, SizingResult
from cyberdelta.validation.funding_data import ArbitrageOpportunity


class TypedBaseSizer(ABC):
    """Abstract base class for all position sizers with direct AppSettings access."""

    def __init__(self, app_settings: AppSettings) -> None:
        """Initialize the sizer with direct AppSettings access.

        Args:
            app_settings: The application settings instance
        """
        self.app_settings = app_settings
        self.sizing_settings = app_settings.risk.sizing
        self.logger = get_logger(self.__class__.__name__)

        # Cache frequently accessed values for performance
        self._min_position_size = self.sizing_settings.min_position_size
        self._max_position_size = self.sizing_settings.max_position_size
        self._max_portfolio_allocation = self.sizing_settings.max_portfolio_allocation
        self._base_validation_factor = self.sizing_settings.base_validation_factor

        # Common sizing parameters from AppSettings
        self.position_precision = 2  # Hardcoded for now as not in config
        self.enable_validation_factors = self.sizing_settings.enable_validation_factors
        self.enable_volatility_adjustment = self.sizing_settings.enable_volatility_adjustment
        self.enable_spread_adjustment = self.sizing_settings.enable_spread_adjustment

        # Validation factors
        self.default_validation_factor = self._base_validation_factor

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
        constrained_size = max(self._min_position_size, min(self._max_position_size, size))

        # Apply allocation percentage constraints
        if context.available_capital > 0:
            max_allocation_size = context.available_capital * self._max_portfolio_allocation
            constrained_size = min(constrained_size, max_allocation_size)

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
        """Round value to specified decimal precision.

        Returns:
            Decimal: Rounded value with specified precision.
        """
        return value.quantize(Decimal(f"0.{'0' * precision}"))

    def _to_decimal(self, value: str | float | Decimal) -> Decimal:
        """Convert value to Decimal with validation.

        Returns:
            Decimal: Converted decimal value.
        """
        if isinstance(value, Decimal):
            return value
        if isinstance(value, str):
            return Decimal(value)
        # float check handles both int and float since int is a subclass of numbers
        return Decimal(str(value))

    def set_position_bounds(self, min_size: Decimal, max_size: Decimal) -> None:
        """Set position size bounds.

        Args:
            min_size: Minimum position size
            max_size: Maximum position size

        Raises:
            SizingError: If min_size is not less than max_size.
        """
        if min_size >= max_size:
            raise SizingError(SizingError.MIN_SIZE_MUST_BE_LESS_THAN_MAX_SIZE)

        self._min_position_size = min_size
        self._max_position_size = max_size
        self.logger.info(
            "Set position bounds", min_size_usd=float(min_size), max_size_usd=float(max_size)
        )

    def __str__(self) -> str:
        """String representation of the sizer.

        Returns:
            str: String representation with class name and sizer name.
        """
        return f"{self.__class__.__name__}(name={self.name})"

    def __repr__(self) -> str:
        """Detailed representation of the sizer.

        Returns:
            str: Detailed representation with class name, sizer name, and sizing method.
        """
        return f"{self.__class__.__name__}(name={self.name}, method={self.sizing_method})"
