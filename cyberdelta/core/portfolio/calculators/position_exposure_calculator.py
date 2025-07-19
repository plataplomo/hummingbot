"""Calculator for position-level exposure and risk metrics."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.services.currency_converter import CurrencyConverter

logger = get_logger(__name__)

# Constants
MINIMUM_SYMBOL_PARTS = 2
HIGH_LEVERAGE_THRESHOLD = 30
MEDIUM_LEVERAGE_THRESHOLD = 20
LOW_LEVERAGE_THRESHOLD = 10


@dataclass
class PositionExposure:
    """Position exposure metrics."""

    # Position identification
    position_id: str
    exchange_id: str
    symbol: str
    side: str  # "LONG" or "SHORT"

    # Size and value metrics
    size: Decimal
    notional_value: Decimal  # size * price
    market_value: Decimal  # For spot: same as notional. For derivatives: margin

    # Exposure metrics
    gross_exposure: Decimal  # Absolute notional value
    net_exposure: Decimal  # Signed notional value (positive for long, negative for short)

    # Risk metrics
    margin_requirement: Decimal
    leverage: Decimal
    liquidation_price: Decimal | None
    distance_to_liquidation: Decimal | None  # Percentage distance

    # Greeks (for options, set to 0 for futures/spot)
    delta: Decimal
    gamma: Decimal
    vega: Decimal
    theta: Decimal

    # Additional risk metrics
    var_95: Decimal | None = None  # Value at Risk (95% confidence)
    var_99: Decimal | None = None  # Value at Risk (99% confidence)
    stress_loss: Decimal | None = None  # Loss in stress scenario

    # Currency exposure
    base_currency: str | None = None
    quote_currency: str | None = None
    base_exposure: Decimal | None = None
    quote_exposure: Decimal | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
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


class PositionExposureCalculator:
    """Calculates exposure and risk metrics for individual positions."""

    def __init__(
        self,
        currency_converter: CurrencyConverter | None = None,
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
            base_currency, quote_currency = self._parse_symbol_currencies(position.symbol)

            # Calculate currency exposures
            base_exposure = position.size if base_currency else None
            quote_exposure = -net_exposure if quote_currency else None

            # Map OrderSide enum to expected string format
            side_str = "LONG" if position.side.value == "BUY" else "SHORT"

            exposure = PositionExposure(
                position_id=f"{position.exchange}_{position.symbol}",
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
        """Calculate estimated liquidation price."""
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
        """Parse base and quote currencies from symbol."""
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
    ) -> dict[str, Any]:
        """Calculate position's impact on overall portfolio.

        Args:
            exposure: Position exposure metrics
            total_portfolio_value: Total portfolio value

        Returns:
            Portfolio impact metrics
        """
        if total_portfolio_value == 0:
            return {
                "position_weight": 0,
                "exposure_weight": 0,
                "var_contribution": 0,
                "concentration_risk": "N/A",
            }

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

        return {
            "position_weight": float(position_weight),
            "exposure_weight": float(exposure_weight),
            "var_contribution": float(var_contribution),
            "concentration_risk": concentration_risk,
        }

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
            price = prices.get(position.symbol)
            if not price:
                logger.warning(
                    "position_exposure_skipped_no_price",
                    position_id=f"{position.exchange}_{position.symbol}",
                    symbol=position.symbol,
                )
                continue

            vol = volatilities.get(position.symbol) if volatilities else None

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
