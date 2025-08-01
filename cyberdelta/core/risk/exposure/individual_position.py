"""Calculator for position-level exposure and risk metrics."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import RiskCalculationError
from cyberdelta.core.symbols import Symbol


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.services.currency import CurrencyConversionService

logger = get_logger(__name__)

# Constants
REASONABLE_PERCENTAGE_MIN = -1e10
REASONABLE_PERCENTAGE_MAX = 1e10
MINIMUM_SYMBOL_PARTS = 2
HIGH_LEVERAGE_THRESHOLD = 30
MEDIUM_LEVERAGE_THRESHOLD = 20
LOW_LEVERAGE_THRESHOLD = 10


@dataclass
class PositionExposure:
    """Position exposure metrics with validation."""

    # Position identification
    position_id: str = Field(min_length=1, description="Position identifier")
    exchange_id: str = Field(min_length=1, description="Exchange identifier")
    symbol: Symbol = Field(description="Trading symbol")
    side: str = Field(pattern="^(LONG|SHORT)$", description="Position side (LONG or SHORT)")

    # Size and value metrics
    size: Decimal = Field(description="Position size")
    notional_value: Decimal = Field(ge=0, description="Notional value (size * price)")
    market_value: Decimal = Field(description="Market value (margin for derivatives)")

    # Exposure metrics
    gross_exposure: Decimal = Field(ge=0, description="Absolute notional value")
    net_exposure: Decimal = Field(description="Signed notional value")

    # Risk metrics
    margin_requirement: Decimal = Field(ge=0, description="Required margin")
    leverage: Decimal = Field(ge=0, description="Position leverage")
    liquidation_price: Decimal | None = Field(
        default=None, gt=0, description="Liquidation price if applicable"
    )
    distance_to_liquidation: Decimal | None = Field(
        default=None, ge=-100, le=100, description="Distance to liquidation (%)"
    )

    # Greeks (for options, set to 0 for futures/spot)
    delta: Decimal = Field(default=Decimal(0), ge=-1, le=1, description="Delta (rate of change)")
    gamma: Decimal = Field(default=Decimal(0), ge=0, description="Gamma (convexity)")
    vega: Decimal = Field(default=Decimal(0), description="Vega (volatility sensitivity)")
    theta: Decimal = Field(default=Decimal(0), le=0, description="Theta (time decay)")

    # Additional risk metrics
    var_95: Decimal | None = Field(default=None, ge=0, description="95% Value at Risk")
    var_99: Decimal | None = Field(default=None, ge=0, description="99% Value at Risk")
    stress_loss: Decimal | None = Field(default=None, ge=0, description="Stress scenario loss")

    # Currency exposure
    base_currency: str | None = Field(
        default=None, min_length=1, max_length=10, description="Base currency"
    )
    quote_currency: str | None = Field(
        default=None, min_length=1, max_length=10, description="Quote currency"
    )
    base_exposure: Decimal | None = Field(default=None, description="Base currency exposure")
    quote_exposure: Decimal | None = Field(default=None, description="Quote currency exposure")

    @field_validator(
        "size",
        "notional_value",
        "gross_exposure",
        "net_exposure",
        "margin_requirement",
        "leverage",
        mode="before",
    )
    @classmethod
    def validate_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure decimal values are finite.

        Args:
            v: Decimal value to validate

        Returns:
            Validated finite Decimal value

        Raises:
            InvalidCalculationInputError: If value is not finite
        """
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise RiskCalculationError(
                parameter="decimal_value", value=value, expected="finite decimal"
            )
        return value

    @field_validator("side", mode="after")
    @classmethod
    def validate_side_consistency(cls, v: str, info: ValidationInfo) -> str:
        """Validate side consistency with net exposure.

        Args:
            v: Position side (LONG or SHORT)
            info: Validation context with other field data

        Returns:
            Validated side value

        Raises:
            InvalidCalculationInputError: If side is inconsistent with net exposure
        """
        if "net_exposure" in info.data:
            net = info.data["net_exposure"]
            if v == "LONG" and net < 0:
                raise RiskCalculationError(
                    parameter="side_consistency",
                    value=f"LONG with net_exposure={net}",
                    expected="LONG position with positive net exposure",
                )
            if v == "SHORT" and net > 0:
                raise RiskCalculationError(
                    parameter="side_consistency",
                    value=f"SHORT with net_exposure={net}",
                    expected="SHORT position with negative net exposure",
                )
        return v

    @field_validator("base_currency", "quote_currency", mode="before")
    @classmethod
    def validate_currency_codes(cls, v: str | None) -> str | None:
        """Validate and normalize currency codes.

        Args:
            v: Currency code to validate

        Returns:
            Normalized uppercase currency code or None

        Raises:
            InvalidCalculationInputError: If currency code is empty string
        """
        if v is not None:
            if not v.strip():
                raise RiskCalculationError(
                    parameter="currency_code", value=v, expected="non-empty string"
                )
            return v.upper().strip()
        return v

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary.

        Returns:
            Dictionary representation with all exposure metrics
        """
        return {
            "position_id": self.position_id,
            "exchange_id": self.exchange_id,
            "symbol": self.symbol,
            "side": self.side,
            "size": str(self.size),
            "notional_value": str(self.notional_value),
            "market_value": str(self.market_value),
            "gross_exposure": str(self.gross_exposure),
            "net_exposure": str(self.net_exposure),
            "margin_requirement": str(self.margin_requirement),
            "leverage": str(self.leverage),
            "liquidation_price": str(self.liquidation_price) if self.liquidation_price else None,
            "distance_to_liquidation": str(self.distance_to_liquidation)
            if self.distance_to_liquidation
            else None,
            "delta": str(self.delta),
            "gamma": str(self.gamma),
            "vega": str(self.vega),
            "theta": str(self.theta),
            "var_95": str(self.var_95) if self.var_95 else None,
            "var_99": str(self.var_99) if self.var_99 else None,
            "stress_loss": str(self.stress_loss) if self.stress_loss else None,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "base_exposure": str(self.base_exposure) if self.base_exposure else None,
            "quote_exposure": str(self.quote_exposure) if self.quote_exposure else None,
        }


@dataclass
class PortfolioImpactMetrics:
    """Position impact metrics on overall portfolio with validation."""

    position_weight: float = Field(
        ge=0, le=100, description="Position weight as % of total portfolio (0-100%)"
    )
    exposure_weight: float = Field(ge=0, description="Exposure weight as % of total portfolio")
    var_contribution: float = Field(ge=0, description="VaR contribution as % of total portfolio")
    concentration_risk: str = Field(
        pattern="^(LOW|MEDIUM|HIGH|CRITICAL|N/A)$", description="Concentration risk level"
    )

    @field_validator("position_weight", "exposure_weight", "var_contribution", mode="before")
    @classmethod
    def validate_percentages(cls, v: float | str) -> float:
        """Validate percentage values are finite.

        Args:
            v: Percentage value to validate

        Returns:
            Validated float percentage value

        Raises:
            InvalidCalculationInputError: If value is not finite or reasonable
        """
        value: float = v if isinstance(v, (int, float)) else float(v)
        if not (REASONABLE_PERCENTAGE_MIN < value < REASONABLE_PERCENTAGE_MAX):
            raise RiskCalculationError(
                parameter="percentage_value",
                value=value,
                expected="finite and reasonable percentage",
            )
        return value


class PositionExposureCalculator:
    """Calculates exposure and risk metrics for individual positions."""

    def __init__(
        self,
        currency_converter: CurrencyConversionService | None = None,
        default_volatility: float = 0.5,  # 50% annualized vol as default
        stress_scenario_move: float = 0.2,  # 20% adverse move
        margin_buffer: float = 0.1,  # 10% buffer for margin calculations
    ) -> None:
        """Initialize position exposure calculator.

        Args:
            currency_converter: Optional currency converter service
            default_volatility: Default volatility for VaR calculations
            stress_scenario_move: Percentage move for stress testing
            margin_buffer: Safety buffer for margin calculations
        """
        self.currency_converter = currency_converter
        self.default_volatility = Decimal(str(default_volatility))
        self.stress_scenario_move = Decimal(str(stress_scenario_move))
        self.margin_buffer = Decimal(str(margin_buffer))

        logger.info(
            "position_exposure_calculator_initialized",
            default_volatility=float(self.default_volatility),
            stress_scenario_move=float(self.stress_scenario_move),
        )

    async def calculate_exposure(
        self,
        position: DerivativePosition,
        current_price: Decimal,
        volatility: Decimal | None = None,
    ) -> PositionExposure:
        """Calculate exposure metrics for a position.

        Args:
            position: The position to analyze
            current_price: Current market price
            volatility: Optional volatility override

        Returns:
            Position exposure metrics

        Raises:
            ValueError: If calculation inputs are invalid
            TypeError: If input types are incorrect
            KeyError: If required fields are missing
            AttributeError: If required attributes are missing
            ArithmeticError: If arithmetic operations fail
        """
        try:
            # Use provided volatility or default
            vol = volatility or self.default_volatility

            # Calculate basic metrics
            notional_value = abs(position.size) * current_price

            # Net exposure is signed based on position side
            net_exposure = notional_value if position.side.value == "BUY" else -notional_value

            # Calculate leverage and margin
            # Since DerivativePosition doesn't have collateral_value, calculate it from notional
            default_leverage = Decimal(10)  # Assume 10x leverage for derivatives
            collateral_value = notional_value / default_leverage

            if collateral_value > 0:
                leverage = notional_value / collateral_value
                margin_requirement = collateral_value * (1 + self.margin_buffer)
            else:
                leverage = Decimal(1)
                margin_requirement = notional_value * Decimal("0.1")  # 10% default

            # Calculate liquidation metrics
            liquidation_price = self._calculate_liquidation_price(
                position=position, current_price=current_price
            )

            distance_to_liq = None
            if liquidation_price and current_price > 0:
                if position.side.value == "BUY":
                    distance_to_liq = (current_price - liquidation_price) / current_price * 100
                else:
                    distance_to_liq = (liquidation_price - current_price) / current_price * 100

            # Calculate Greeks (simplified for futures/perps)
            delta = Decimal(1) if position.side.value == "BUY" else Decimal(-1)
            gamma = Decimal(0)  # Linear instruments have no gamma
            vega = Decimal(0)  # No vega for non-options
            theta = Decimal(0)  # No time decay for perps

            # Calculate VaR using simplified normal distribution approach for 1-day period
            daily_vol = vol / Decimal(365).sqrt()  # Convert annual to daily
            var_95 = notional_value * daily_vol * Decimal("1.645")  # 95% confidence
            var_99 = notional_value * daily_vol * Decimal("2.326")  # 99% confidence

            # Calculate stress loss
            if position.side.value == "BUY":
                stress_loss = notional_value * self.stress_scenario_move
            else:
                stress_loss = notional_value * self.stress_scenario_move

            # Parse currency exposure
            base_currency, quote_currency = self._parse_symbol_currencies(position.symbol.value)

            # Calculate currency exposures
            base_exposure = position.size if base_currency else None
            quote_exposure = -net_exposure if quote_currency else None

            # Map OrderSide enum to expected string format
            side_str = "LONG" if position.side.value == "BUY" else "SHORT"

            exposure = PositionExposure(
                position_id=f"{position.exchange}_{position.symbol.value}",
                exchange_id=position.exchange,
                symbol=position.symbol,
                side=side_str,
                size=position.size,
                notional_value=notional_value,
                market_value=collateral_value,
                gross_exposure=notional_value,
                net_exposure=net_exposure,
                margin_requirement=margin_requirement,
                leverage=leverage,
                liquidation_price=liquidation_price,
                distance_to_liquidation=distance_to_liq,
                delta=delta,
                gamma=gamma,
                vega=vega,
                theta=theta,
                var_95=var_95,
                var_99=var_99,
                stress_loss=stress_loss,
                base_currency=base_currency,
                quote_currency=quote_currency,
                base_exposure=base_exposure,
                quote_exposure=quote_exposure,
            )

            logger.debug(
                "position_exposure_calculated",
                position_id=f"{position.exchange}_{position.symbol}",
                symbol=position.symbol,
                notional_value=float(notional_value),
                leverage=float(leverage),
                distance_to_liquidation=float(distance_to_liq) if distance_to_liq else None,
            )
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception(
                "position_exposure_calculation_failed",
                position_id=f"{position.exchange}_{position.symbol}",
                symbol=position.symbol,
                error_type=type(e).__name__,
            )
            raise
        else:
            return exposure

    def _calculate_liquidation_price(
        self, position: DerivativePosition, current_price: Decimal
    ) -> Decimal | None:
        """Calculate estimated liquidation price.

        Args:
            position: The derivative position
            current_price: Current market price

        Returns:
            Estimated liquidation price or None if not calculable
        """
        if position.entry_price is None or position.size == 0:
            return None

        # Calculate collateral value for liquidation calculation
        default_leverage = Decimal(10)
        notional_value = abs(position.size) * position.entry_price
        collateral_value = notional_value / default_leverage

        if collateral_value <= 0:
            return None

        # Simplified liquidation price calculation
        # Assumes liquidation when losses exceed available collateral
        entry_price = position.entry_price

        if position.side.value == "BUY":
            # Long position liquidated when price drops below liquidation threshold
            liq_price = entry_price - (collateral_value / position.size)
            return max(liq_price, Decimal(0))

        # SHORT - Short position liquidated when price rises above liquidation threshold
        return entry_price + (collateral_value / abs(position.size))

    def _parse_symbol_currencies(self, symbol: str) -> tuple[str | None, str | None]:
        """Parse base and quote currencies from symbol.

        Args:
            symbol: Trading symbol to parse

        Returns:
            Tuple of (base_currency, quote_currency), either can be None
        """
        # Common patterns:
        # BTC-PERP, ETH-PERP -> base currency only
        # BTC/USD, ETH/USDT -> base and quote
        # BTCUSDT -> need to parse

        if "-PERP" in symbol:
            base = symbol.replace("-PERP", "")
            return base, "USD"  # Assume USD settlement

        if "/" in symbol:
            parts = symbol.split("/")
            if len(parts) == MINIMUM_SYMBOL_PARTS:
                return parts[0], parts[1]

        # Try to parse concatenated symbols
        for quote in ["USDT", "USDC", "USD", "BTC", "ETH"]:
            if symbol.endswith(quote):
                base = symbol[: -len(quote)]
                return base, quote

        # Default: assume single currency
        return symbol, None

    def calculate_portfolio_impact(
        self, exposure: PositionExposure, total_portfolio_value: Decimal
    ) -> PortfolioImpactMetrics:
        """Calculate position's impact on overall portfolio.

        Args:
            exposure: Position exposure metrics
            total_portfolio_value: Total portfolio value

        Returns:
            Portfolio impact metrics
        """
        if total_portfolio_value == 0:
            return PortfolioImpactMetrics(
                position_weight=0.0,
                exposure_weight=0.0,
                var_contribution=0.0,
                concentration_risk="N/A",
            )

        position_weight = exposure.market_value / total_portfolio_value * 100
        exposure_weight = exposure.gross_exposure / total_portfolio_value * 100
        var_contribution = (exposure.var_95 or Decimal(0)) / total_portfolio_value * 100

        # Assess concentration risk
        if exposure_weight > HIGH_LEVERAGE_THRESHOLD:
            concentration_risk = "CRITICAL"
        elif exposure_weight > MEDIUM_LEVERAGE_THRESHOLD:
            concentration_risk = "HIGH"
        elif exposure_weight > LOW_LEVERAGE_THRESHOLD:
            concentration_risk = "MEDIUM"
        else:
            concentration_risk = "LOW"

        return PortfolioImpactMetrics(
            position_weight=float(position_weight),
            exposure_weight=float(exposure_weight),
            var_contribution=float(var_contribution),
            concentration_risk=concentration_risk,
        )

    async def calculate_batch(
        self,
        positions: list[DerivativePosition],
        prices: dict[str, Decimal],
        volatilities: dict[str, Decimal] | None = None,
    ) -> list[PositionExposure]:
        """Calculate exposures for multiple positions.

        Args:
            positions: List of positions
            prices: Current prices by symbol
            volatilities: Optional volatilities by symbol

        Returns:
            List of position exposures
        """
        exposures: list[PositionExposure] = []

        for position in positions:
            price = prices.get(position.symbol.value)
            if not price:
                logger.warning(
                    "position_exposure_skipped_no_price",
                    position_id=f"{position.exchange}_{position.symbol.value}",
                    symbol=position.symbol,
                )
                continue

            vol = volatilities.get(position.symbol.value) if volatilities else None

            try:
                exposure = await self.calculate_exposure(
                    position=position, current_price=price, volatility=vol
                )
                exposures.append(exposure)
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError):
                logger.exception(
                    "batch_position_exposure_failed",
                    position_id=f"{position.exchange}_{position.symbol}",
                    symbol=position.symbol,
                )
                continue

        return exposures
