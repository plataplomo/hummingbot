"""Exposure calculator with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING

from pydantic import Field, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.base import CalculationResult, TypedCalculator
from cyberdelta.core.portfolio.base.typed_calculator import CalculationMetadata
from cyberdelta.core.portfolio.exceptions import InvalidCalculationInputError


# Constants
MAX_VOLATILITY_DECIMAL = 10  # 1000% max volatility

if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.models.base import BaseStateModel
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol


@dataclass
class ExposureMetrics:
    """Exposure calculation result with validation."""

    position_id: str = Field(min_length=1, description="Position identifier")
    symbol: str = Field(min_length=1, description="Trading symbol")
    gross_exposure: Decimal = Field(ge=0, description="Gross exposure (non-negative)")
    net_exposure: Decimal = Field(description="Net exposure (can be positive or negative)")
    leverage: Decimal = Field(ge=0, description="Position leverage (non-negative)")
    margin_requirement: Decimal = Field(ge=0, description="Required margin (non-negative)")
    liquidation_price: Decimal | None = Field(
        default=None, gt=0, description="Liquidation price if applicable"
    )
    var_95: Decimal | None = Field(default=None, description="95% Value at Risk")
    stress_loss: Decimal | None = Field(default=None, description="Loss under stress scenario")
    currency_exposures: dict[str, Decimal] = Field(
        default_factory=dict, description="Exposure by currency"
    )

    @field_validator(
        "gross_exposure", "net_exposure", "leverage", "margin_requirement", mode="before"
    )
    @classmethod
    def validate_finite_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure all decimal values are finite.
        
        Returns:
            Validated finite Decimal value.
            
        Raises:
            InvalidCalculationInputError: If decimal value is not finite.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="exposure_metrics", value=value, expected="finite decimal value"
            )
        return value

    @field_validator("currency_exposures", mode="before")
    @classmethod
    def validate_currency_exposures(
        cls, v: dict[str, Decimal | str | float | int]
    ) -> dict[str, Decimal]:
        """Validate currency exposure values.
        
        Returns:
            Dictionary with currency codes as keys and validated finite Decimal exposures.
            
        Raises:
            InvalidCalculationInputError: If currency code is empty or exposure is not finite.
        """
        validated: dict[str, Decimal] = {}
        for currency, exposure in v.items():
            if not currency:
                raise InvalidCalculationInputError(
                    parameter="currency_code", value=currency, expected="non-empty string"
                )
            exposure_val: Decimal = (
                exposure if isinstance(exposure, Decimal) else Decimal(str(exposure))
            )
            if not exposure_val.is_finite():
                raise InvalidCalculationInputError(
                    parameter=f"exposure_{currency}",
                    value=exposure_val,
                    expected="finite decimal value",
                )
            validated[currency.upper()] = exposure_val
        return validated


@dataclass
class ExposureInput:
    """Input for exposure calculation with validation."""

    position: DerivativePosition
    current_price: Decimal = Field(gt=0, description="Current market price (positive)")
    volatility: Decimal | None = Field(
        default=None, ge=0, le=10, description="Market volatility (0-1000%)"
    )
    correlation_data: dict[str, float] | None = Field(
        default=None, description="Correlation matrix data"
    )


@dataclass
class AggregateExposureMetrics:
    """Aggregate exposure metrics for a portfolio of positions."""

    total_gross_exposure: Decimal = Field(
        ge=0, description="Total absolute exposure (non-negative)"
    )
    total_net_exposure: Decimal = Field(
        description="Total signed exposure (can be positive or negative)"
    )
    position_count: int = Field(ge=0, description="Number of positions included (non-negative)")
    currency_exposures: dict[str, Decimal] = Field(
        default_factory=dict, description="Currency exposure breakdown"
    )

    @field_validator("total_gross_exposure", "total_net_exposure", mode="before")
    @classmethod
    def validate_exposure_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure exposure values are finite.
        
        Returns:
            Validated finite Decimal value.
            
        Raises:
            InvalidCalculationInputError: If exposure value is not finite.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="exposure_value", value=value, expected="finite decimal value"
            )
        return value

    @field_validator("currency_exposures", mode="before")
    @classmethod
    def validate_currency_exposures_dict(
        cls, v: dict[str, Decimal | str | float | int]
    ) -> dict[str, Decimal]:
        """Validate currency exposure dictionary.
        
        Returns:
            Dictionary with uppercase currency codes and validated finite exposures.
            
        Raises:
            InvalidCalculationInputError: If currency code is empty or exposure is not finite.
        """
        validated: dict[str, Decimal] = {}
        for currency, exposure in v.items():
            if not currency:
                raise InvalidCalculationInputError(
                    parameter="currency_code", value=currency, expected="non-empty string"
                )
            exposure_val: Decimal = (
                exposure if isinstance(exposure, Decimal) else Decimal(str(exposure))
            )
            if not exposure_val.is_finite():
                raise InvalidCalculationInputError(
                    parameter=f"exposure_{currency}",
                    value=exposure_val,
                    expected="finite decimal value",
                )
            validated[currency.upper()] = exposure_val
        return validated

    @field_validator("current_price", mode="before")
    @classmethod
    def validate_current_price(cls, v: Decimal | str | float) -> Decimal:
        """Validate current price is positive and finite.
        
        Returns:
            Validated positive finite Decimal price.
            
        Raises:
            InvalidCalculationInputError: If price is not finite or not positive.
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="current_price", value=value, expected="finite decimal value"
            )
        if value <= 0:
            raise InvalidCalculationInputError(
                parameter="current_price", value=value, expected="positive decimal value"
            )
        return value

    @field_validator("volatility", mode="before")
    @classmethod
    def validate_volatility(cls, v: Decimal | str | float | None) -> Decimal | None:
        """Validate volatility if provided.
        
        Returns:
            Validated non-negative finite Decimal volatility or None if input is None.
            
        Raises:
            InvalidCalculationInputError: If volatility is not finite, negative, or exceeds maximum.
        """
        if v is not None:
            value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
            if not value.is_finite():
                raise InvalidCalculationInputError(
                    parameter="volatility", value=value, expected="finite decimal value"
                )
            if value < 0:
                raise InvalidCalculationInputError(
                    parameter="volatility", value=value, expected="non-negative decimal value"
                )
            if value > MAX_VOLATILITY_DECIMAL:
                raise InvalidCalculationInputError(
                    parameter="volatility",
                    value=value,
                    expected=f"value <= {MAX_VOLATILITY_DECIMAL}",
                )
            return value
        return v

    @field_validator("correlation_data", mode="before")
    @classmethod
    def validate_correlation_data(cls, v: dict[str, float] | None) -> dict[str, float] | None:
        """Validate correlation data if provided.
        
        Returns:
            Validated correlation dictionary with values between -1 and 1, or None if input is None.
            
        Raises:
            InvalidCalculationInputError: If any correlation value is not between -1 and 1.
        """
        if v is not None:
            for key, corr in v.items():
                # Type validation is handled by dict[str, float] annotation
                # Additional validation for correlation range
                if not -1 <= corr <= 1:
                    raise InvalidCalculationInputError(
                        parameter=f"correlation_{key}",
                        value=corr,
                        expected="value between -1 and 1",
                    )
        return v


class ExposureCalculator(TypedCalculator[ExposureInput, ExposureMetrics]):
    """Exposure calculator with direct AppSettings access.

    Follows risk module patterns:
    - Direct AppSettings access
    - Inherits from TypedCalculator
    - Protocol-based dependencies
    - Strong typing with result types
    """

    def __init__(
        self,
        app_settings: AppSettings,
        state_container: StateContainerProtocol[BaseStateModel],
    ) -> None:
        """Initialize the exposure calculator.

        Args:
            app_settings: Application settings with portfolio configuration
            state_container: State container for data access
        """
        super().__init__(app_settings, state_container, "ExposureCalculator")

        # Configuration from AppSettings
        self.default_volatility = self.portfolio_config.calculation.default_volatility
        self.stress_scenario_move = self.portfolio_config.calculation.stress_scenario_move
        self.var_confidence_level = self.portfolio_config.calculation.var_confidence_level
        self.leverage_warning_threshold = (
            self.portfolio_config.calculation.leverage_warning_threshold
        )

        self.logger.info(
            "exposure_calculator_created",
            default_volatility=self.default_volatility,
            stress_scenario_move=self.stress_scenario_move,
            var_confidence_level=self.var_confidence_level,
        )

    async def calculate(self, input_data: ExposureInput) -> CalculationResult[ExposureMetrics]:
        """Calculate exposure metrics for a position.

        Args:
            input_data: Input containing position and market data

        Returns:
            Calculation result with exposure metrics
        """
        try:
            position = input_data.position
            current_price = input_data.current_price
            volatility = input_data.volatility or self.default_volatility

            # Calculate basic exposure metrics
            notional_value = abs(position.size) * current_price
            net_exposure = position.size * current_price  # Signed
            gross_exposure = abs(net_exposure)

            # Calculate leverage
            margin_requirement = self._calculate_margin_requirement(position, current_price)
            leverage = notional_value / margin_requirement if margin_requirement > 0 else Decimal(0)

            # Calculate liquidation price
            liquidation_price = self._calculate_liquidation_price(
                position, current_price, margin_requirement
            )

            # Calculate VaR
            var_95 = self._calculate_var(notional_value, volatility, self.var_confidence_level)

            # Calculate stress loss
            stress_loss = self._calculate_stress_loss(
                position, current_price, self.stress_scenario_move
            )

            # Calculate currency exposures
            currency_exposures = self._calculate_currency_exposures(position, current_price)

            metrics = ExposureMetrics(
                position_id=f"{position.exchange}:{position.symbol}",
                symbol=position.symbol,
                gross_exposure=gross_exposure,
                net_exposure=net_exposure,
                leverage=leverage,
                margin_requirement=margin_requirement,
                liquidation_price=liquidation_price,
                var_95=var_95,
                stress_loss=stress_loss,
                currency_exposures=currency_exposures,
            )

            # Check for warnings
            warnings: list[str] = []
            if leverage > self.leverage_warning_threshold:
                warnings.append(f"High leverage detected: {leverage}")

            return CalculationResult[ExposureMetrics].success_result(
                result=metrics,
                warnings=warnings,
                metadata=CalculationMetadata(calculator=self.calculator_name),
            )

        except (ValueError, TypeError, ArithmeticError) as e:
            return CalculationResult[ExposureMetrics].failure_result(
                errors=[f"Exposure calculation failed: {e}"],
                metadata=CalculationMetadata(
                    calculator=self.calculator_name, error_type=type(e).__name__
                ),
            )

    async def validate_input(self, input_data: ExposureInput) -> tuple[bool, list[str]]:
        """Validate input data for exposure calculation.

        Args:
            input_data: Input to validate

        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors: list[str] = []

        if input_data.current_price <= 0:
            errors.append(f"Invalid current price: {input_data.current_price}")

        if input_data.volatility is not None and input_data.volatility < 0:
            errors.append(f"Invalid volatility: {input_data.volatility}")

        return len(errors) == 0, errors

    def _calculate_margin_requirement(
        self, position: DerivativePosition, current_price: Decimal
    ) -> Decimal:
        """Calculate margin requirement for the position.
        
        Returns:
            Required margin amount based on notional value and margin rate.
        """
        # Simplified calculation - in reality this would be more complex
        notional_value = abs(position.size) * current_price
        margin_rate = Decimal("0.1")  # 10% margin requirement
        return notional_value * margin_rate

    def _calculate_liquidation_price(
        self, position: DerivativePosition, current_price: Decimal, margin_requirement: Decimal
    ) -> Decimal | None:
        """Calculate liquidation price for the position.
        
        Returns:
            Estimated liquidation price for the position, or None if position size is zero.
        """
        if position.size == 0:
            return None

        # Simplified calculation
        if position.size > 0:  # Long position
            return current_price * Decimal("0.9")  # 10% drop
        # Short position
        return current_price * Decimal("1.1")  # 10% rise

    def _calculate_var(
        self, notional_value: Decimal, volatility: Decimal, confidence_level: Decimal
    ) -> Decimal:
        """Calculate Value at Risk.
        
        Returns:
            Value at Risk amount based on notional value, volatility, and confidence level.
        """
        # Simplified VaR calculation
        # In reality, this would use proper statistical methods

        # Convert confidence level to z-score (approximate)
        z_score = Decimal("1.65") if confidence_level == Decimal("0.95") else Decimal("2.33")

        return notional_value * volatility * z_score

    def _calculate_stress_loss(
        self, position: DerivativePosition, current_price: Decimal, stress_move: Decimal
    ) -> Decimal:
        """Calculate loss in stress scenario.
        
        Returns:
            Estimated loss amount under specified stress market movement.
        """
        if position.size > 0:  # Long position
            stress_price = current_price * (1 - stress_move)
            return (current_price - stress_price) * position.size
        # Short position
        stress_price = current_price * (1 + stress_move)
        return (stress_price - current_price) * abs(position.size)

    def _calculate_currency_exposures(
        self, position: DerivativePosition, current_price: Decimal
    ) -> dict[str, Decimal]:
        """Calculate currency exposures from the position.
        
        Returns:
            Dictionary mapping currency codes to exposure amounts.
        """
        # Simplified - extract base and quote currencies from symbol
        if "/" in position.symbol:
            base, quote = position.symbol.split("/", 1)
        else:
            # Fallback
            base, quote = position.symbol, "USD"

        notional_value = position.size * current_price

        return {
            base: abs(notional_value),
            quote: Decimal(0),  # Simplified - in reality this would be more complex
        }

    async def calculate_portfolio_exposure(
        self, positions: list[DerivativePosition]
    ) -> AggregateExposureMetrics:
        """Calculate aggregate exposure for a portfolio of positions.
        
        Returns:
            Aggregate exposure metrics including total gross/net exposure and currency breakdowns.
        """
        total_gross_exposure = Decimal(0)
        total_net_exposure = Decimal(0)
        currency_exposures: dict[str, Decimal] = {}

        for position in positions:
            try:
                # Get current price (simplified - would use price service)
                current_price = position.entry_price or Decimal(1)

                input_data = ExposureInput(position=position, current_price=current_price)

                result = await self.calculate(input_data)
                if result.success and result.result:
                    metrics = result.result
                    total_gross_exposure += metrics.gross_exposure
                    total_net_exposure += metrics.net_exposure

                    # Aggregate currency exposures
                    for currency, exposure in metrics.currency_exposures.items():
                        current_exposure = currency_exposures.get(currency, Decimal(0))
                        currency_exposures[currency] = current_exposure + exposure

            except (ValueError, TypeError, ArithmeticError) as e:
                self.logger.warning(
                    "Failed to calculate exposure for position",
                    position_symbol=position.symbol,
                    error=str(e),
                )

        return AggregateExposureMetrics(
            total_gross_exposure=total_gross_exposure,
            total_net_exposure=total_net_exposure,
            currency_exposures=currency_exposures,
            position_count=len(positions),
        )
