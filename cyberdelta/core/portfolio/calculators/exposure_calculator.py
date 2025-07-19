"""Exposure calculator with direct AppSettings access following risk module patterns."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config import AppSettings
from cyberdelta.core.portfolio.base import CalculationResult, TypedCalculator


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.protocols import StateContainerProtocol


@dataclass
class ExposureMetrics:
    """Exposure calculation result."""

    position_id: str
    symbol: str
    gross_exposure: Decimal
    net_exposure: Decimal
    leverage: Decimal
    margin_requirement: Decimal
    liquidation_price: Decimal | None
    var_95: Decimal | None
    stress_loss: Decimal | None
    currency_exposures: dict[str, Decimal]


@dataclass
class ExposureInput:
    """Input for exposure calculation."""

    position: DerivativePosition
    current_price: Decimal
    volatility: Decimal | None = None
    correlation_data: dict[str, float] | None = None


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
        state_container: StateContainerProtocol[Any],
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
                metadata={
                    "calculator": self.calculator_name,
                    "volatility_used": volatility,
                },
            )

        except (ValueError, TypeError, ArithmeticError) as e:
            return CalculationResult[ExposureMetrics].failure_result(
                errors=[f"Exposure calculation failed: {e}"],
                metadata={"calculator": self.calculator_name},
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
        """Calculate margin requirement for the position."""
        # Simplified calculation - in reality this would be more complex
        notional_value = abs(position.size) * current_price
        margin_rate = Decimal("0.1")  # 10% margin requirement
        return notional_value * margin_rate

    def _calculate_liquidation_price(
        self, position: DerivativePosition, current_price: Decimal, margin_requirement: Decimal
    ) -> Decimal | None:
        """Calculate liquidation price for the position."""
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
        """Calculate Value at Risk."""
        # Simplified VaR calculation
        # In reality, this would use proper statistical methods

        # Convert confidence level to z-score (approximate)
        z_score = Decimal("1.65") if confidence_level == Decimal("0.95") else Decimal("2.33")

        return notional_value * volatility * z_score

    def _calculate_stress_loss(
        self, position: DerivativePosition, current_price: Decimal, stress_move: Decimal
    ) -> Decimal:
        """Calculate loss in stress scenario."""
        if position.size > 0:  # Long position
            stress_price = current_price * (1 - stress_move)
            return (current_price - stress_price) * position.size
        # Short position
        stress_price = current_price * (1 + stress_move)
        return (stress_price - current_price) * abs(position.size)

    def _calculate_currency_exposures(
        self, position: DerivativePosition, current_price: Decimal
    ) -> dict[str, Decimal]:
        """Calculate currency exposures from the position."""
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
    ) -> dict[str, Any]:
        """Calculate aggregate exposure for a portfolio of positions."""
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

        return {
            "total_gross_exposure": total_gross_exposure,
            "total_net_exposure": total_net_exposure,
            "currency_exposures": currency_exposures,
            "position_count": len(positions),
        }
