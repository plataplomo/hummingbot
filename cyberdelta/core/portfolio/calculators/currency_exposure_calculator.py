"""Calculator for currency exposure and FX risk metrics."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from cyberdelta.config.structlog_config import get_logger


# Type-preserving factory functions for dataclass fields
def _str_set_factory() -> set[str]:
    """Factory function that preserves set[str] type information."""
    return set()


def _str_list_factory() -> list[str]:
    """Factory function that preserves list[str] type information."""
    return []


def _currency_exposure_dict_factory() -> dict[str, CurrencyExposure]:
    """Factory function that preserves dict[str, CurrencyExposure] type information."""
    return {}


if TYPE_CHECKING:
    from cyberdelta.core.portfolio.services.currency_converter import CurrencyConverter

logger = get_logger(__name__)


@dataclass
class CurrencyExposure:
    """Currency exposure for a single currency."""

    currency: str
    gross_exposure: Decimal  # Total absolute exposure
    net_exposure: Decimal  # Net long/short exposure
    long_exposure: Decimal  # Total long positions
    short_exposure: Decimal  # Total short positions

    # Breakdown by source
    spot_exposure: Decimal = Decimal(0)
    derivative_exposure: Decimal = Decimal(0)
    collateral_exposure: Decimal = Decimal(0)

    # Risk metrics
    value_in_base_currency: Decimal | None = None  # Value in portfolio base currency
    fx_rate: Decimal | None = None  # Current FX rate to base
    fx_volatility: Decimal | None = None  # Historical volatility
    var_95: Decimal | None = None  # FX VaR
    stress_loss: Decimal | None = None  # Loss in stress scenario

    # Position details
    position_count: int = 0
    exchanges: set[str] = field(default_factory=_str_set_factory)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "currency": self.currency,
            "gross_exposure": str(self.gross_exposure),
            "net_exposure": str(self.net_exposure),
            "long_exposure": str(self.long_exposure),
            "short_exposure": str(self.short_exposure),
            "spot_exposure": str(self.spot_exposure),
            "derivative_exposure": str(self.derivative_exposure),
            "collateral_exposure": str(self.collateral_exposure),
            "value_in_base_currency": str(self.value_in_base_currency)
            if self.value_in_base_currency
            else None,
            "fx_rate": str(self.fx_rate) if self.fx_rate else None,
            "fx_volatility": str(self.fx_volatility) if self.fx_volatility else None,
            "var_95": str(self.var_95) if self.var_95 else None,
            "stress_loss": str(self.stress_loss) if self.stress_loss else None,
            "position_count": self.position_count,
            "exchanges": list(self.exchanges),
        }


@dataclass
class PortfolioCurrencyExposure:
    """Aggregate currency exposure for the portfolio."""

    base_currency: str
    total_currencies: int

    # Aggregate metrics
    total_fx_exposure: Decimal  # Total non-base currency exposure
    total_fx_risk: Decimal  # Total FX VaR

    # Individual currency exposures
    currency_exposures: dict[str, CurrencyExposure] = field(
        default_factory=_currency_exposure_dict_factory
    )

    # Concentration metrics
    largest_currency_weight: Decimal = Decimal(0)
    top_3_currency_concentration: Decimal = Decimal(0)

    # Risk metrics
    portfolio_fx_var_95: Decimal = Decimal(0)
    portfolio_fx_stress_loss: Decimal = Decimal(0)
    correlation_benefit: Decimal = Decimal(0)  # Diversification benefit

    # Risk indicators
    concentrated_currencies: list[str] = field(default_factory=_str_list_factory)  # > 20% exposure
    volatile_currencies: list[str] = field(default_factory=_str_list_factory)  # High volatility
    warnings: list[str] = field(default_factory=_str_list_factory)

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "base_currency": self.base_currency,
            "total_currencies": self.total_currencies,
            "total_fx_exposure": str(self.total_fx_exposure),
            "total_fx_risk": str(self.total_fx_risk),
            "currency_exposures": {
                ccy: exp.to_dict() for ccy, exp in self.currency_exposures.items()
            },
            "largest_currency_weight": str(self.largest_currency_weight),
            "top_3_currency_concentration": str(self.top_3_currency_concentration),
            "portfolio_fx_var_95": str(self.portfolio_fx_var_95),
            "portfolio_fx_stress_loss": str(self.portfolio_fx_stress_loss),
            "correlation_benefit": str(self.correlation_benefit),
            "concentrated_currencies": self.concentrated_currencies,
            "volatile_currencies": self.volatile_currencies,
            "warnings": self.warnings,
        }


class CurrencyExposureCalculator:
    """Calculates currency exposure and FX risk metrics."""

    # Constants for parsing and validation
    EXPECTED_SYMBOL_PARTS = 2  # Expected parts when splitting symbol by separator
    MIN_CURRENCIES_FOR_DIVERSIFICATION = 3  # Minimum currencies for good FX diversification

    def __init__(
        self,
        base_currency: str = "USD",
        currency_converter: CurrencyConverter | None = None,
        fx_volatilities: dict[str, float] | None = None,
        fx_correlations: dict[tuple[str, str], float] | None = None,
        concentration_threshold: float = 0.2,  # 20% concentration warning
        high_volatility_threshold: float = 0.15,  # 15% annual volatility
        stress_scenario_move: float = 0.1,  # 10% adverse FX move
    ) -> None:
        """Initialize currency exposure calculator.

        Args:
            base_currency: Portfolio base currency
            currency_converter: Currency conversion service
            fx_volatilities: Historical FX volatilities by currency
            fx_correlations: FX correlation matrix
            concentration_threshold: Threshold for concentration warnings
            high_volatility_threshold: Threshold for high volatility warnings
            stress_scenario_move: FX move for stress testing
        """
        self.base_currency = base_currency
        self.currency_converter = currency_converter
        self.fx_volatilities = fx_volatilities or self._get_default_volatilities()
        self.fx_correlations = fx_correlations or {}
        self.concentration_threshold = Decimal(str(concentration_threshold))
        self.high_volatility_threshold = Decimal(str(high_volatility_threshold))
        self.stress_scenario_move = Decimal(str(stress_scenario_move))

        logger.info(
            "currency_exposure_calculator_initialized",
            base_currency=base_currency,
            has_converter=currency_converter is not None,
        )

    def _get_default_volatilities(self) -> dict[str, float]:
        """Get default FX volatilities."""
        return {
            "USD": 0.0,  # Base currency
            "EUR": 0.08,
            "GBP": 0.09,
            "JPY": 0.10,
            "CHF": 0.08,
            "AUD": 0.11,
            "CAD": 0.09,
            "BTC": 0.50,  # Crypto currencies
            "ETH": 0.60,
            "BNB": 0.55,
            "SOL": 0.70,
            "USDT": 0.02,  # Stablecoins
            "USDC": 0.02,
            "DAI": 0.03,
        }

    async def calculate_currency_exposure(
        self,
        positions: list[dict[str, Any]],
        balances: dict[str, dict[str, Decimal]],
        prices: dict[str, Decimal] | None = None,
    ) -> PortfolioCurrencyExposure:
        """Calculate portfolio currency exposure.

        Args:
            positions: List of positions with currency info
            balances: Balances by exchange and currency
            prices: Optional current prices for conversion

        Returns:
            Portfolio currency exposure metrics
        """
        # Initialize currency accumulators
        currency_data: dict[str, dict[str, Any]] = defaultdict(
            lambda: {
                "gross": Decimal(0),
                "net": Decimal(0),
                "long": Decimal(0),
                "short": Decimal(0),
                "spot": Decimal(0),
                "derivative": Decimal(0),
                "collateral": Decimal(0),
                "position_count": 0,
                "exchanges": set(),
            }
        )

        # Process positions
        for position in positions:
            await self._process_position_currency_exposure(
                position=position,
                currency_data=currency_data,
            )

        # Process balances
        for exchange, exchange_balances in balances.items():
            for currency, balance in exchange_balances.items():
                if balance > 0:
                    currency_data[currency]["gross"] += balance
                    currency_data[currency]["net"] += balance
                    currency_data[currency]["long"] += balance
                    currency_data[currency]["spot"] += balance
                    currency_data[currency]["exchanges"].add(exchange)

        # Create currency exposure objects
        currency_exposures: dict[str, CurrencyExposure] = {}
        total_fx_exposure = Decimal(0)

        for currency, data in currency_data.items():
            if currency == self.base_currency:
                continue  # Skip base currency

            # Get FX rate and volatility
            fx_rate = await self._get_fx_rate(currency, prices)
            fx_vol = Decimal(str(self.fx_volatilities.get(currency, 0.2)))

            # Calculate value in base currency
            value_in_base = data["net"] * fx_rate if fx_rate else None

            # Calculate FX VaR (simplified)
            var_95 = None
            if value_in_base and fx_vol > 0:
                # 1-day VaR = exposure * daily_vol * z_score
                daily_vol = fx_vol / Decimal(365).sqrt()
                var_95 = abs(value_in_base) * daily_vol * Decimal("1.645")

            # Calculate stress loss
            stress_loss = None
            if value_in_base:
                stress_loss = abs(value_in_base) * self.stress_scenario_move

            exposure = CurrencyExposure(
                currency=currency,
                gross_exposure=data["gross"],
                net_exposure=data["net"],
                long_exposure=data["long"],
                short_exposure=data["short"],
                spot_exposure=data["spot"],
                derivative_exposure=data["derivative"],
                collateral_exposure=data["collateral"],
                value_in_base_currency=value_in_base,
                fx_rate=fx_rate,
                fx_volatility=fx_vol,
                var_95=var_95,
                stress_loss=stress_loss,
                position_count=data["position_count"],
                exchanges=data["exchanges"],
            )

            currency_exposures[currency] = exposure
            if value_in_base:
                total_fx_exposure += abs(value_in_base)

        # Calculate portfolio-level metrics
        return await self._calculate_portfolio_fx_metrics(
            currency_exposures=currency_exposures,
            total_fx_exposure=total_fx_exposure,
        )

    async def _process_position_currency_exposure(
        self,
        position: dict[str, Any],
        currency_data: dict[str, dict[str, Any]],
    ) -> None:
        """Process currency exposure from a position."""
        # Parse currencies from position
        base_ccy = position.get("base_currency")
        quote_ccy = position.get("quote_currency")

        # If not provided, try to parse from symbol
        if not base_ccy or not quote_ccy:
            base_ccy, quote_ccy = self._parse_position_currencies(position)

        position_value = Decimal(str(position.get("notional_value", 0)))
        position_size = Decimal(str(position.get("size", 0)))
        side = position.get("side", "LONG")
        exchange = position.get("exchange_id", "unknown")

        # Base currency exposure (what we're holding)
        if base_ccy:
            if side == "LONG":
                currency_data[base_ccy]["gross"] += abs(position_size)
                currency_data[base_ccy]["net"] += position_size
                currency_data[base_ccy]["long"] += abs(position_size)
            else:
                currency_data[base_ccy]["gross"] += abs(position_size)
                currency_data[base_ccy]["net"] -= abs(position_size)
                currency_data[base_ccy]["short"] += abs(position_size)

            currency_data[base_ccy]["derivative"] += abs(position_size)
            currency_data[base_ccy]["position_count"] += 1
            currency_data[base_ccy]["exchanges"].add(exchange)

        # Quote currency exposure (what we're paying/receiving)
        if quote_ccy and quote_ccy != base_ccy:
            if side == "LONG":
                # Long position = short quote currency
                currency_data[quote_ccy]["gross"] += position_value
                currency_data[quote_ccy]["net"] -= position_value
                currency_data[quote_ccy]["short"] += position_value
            else:
                # Short position = long quote currency
                currency_data[quote_ccy]["gross"] += position_value
                currency_data[quote_ccy]["net"] += position_value
                currency_data[quote_ccy]["long"] += position_value

            currency_data[quote_ccy]["derivative"] += position_value
            currency_data[quote_ccy]["exchanges"].add(exchange)

        # Collateral currency (usually USD or USDT)
        collateral_value = Decimal(str(position.get("collateral_value", 0)))
        collateral_ccy = position.get("collateral_currency", "USD")
        if collateral_value > 0:
            currency_data[collateral_ccy]["collateral"] += collateral_value
            currency_data[collateral_ccy]["exchanges"].add(exchange)

    def _parse_position_currencies(self, position: dict[str, Any]) -> tuple[str | None, str | None]:
        """Parse currencies from position symbol."""
        symbol = position.get("symbol", "")

        # Handle perpetuals
        if "-PERP" in symbol:
            base = symbol.replace("-PERP", "")
            return base, "USD"

        # Handle pairs with /
        if "/" in symbol:
            parts = symbol.split("/")
            if len(parts) == self.EXPECTED_SYMBOL_PARTS:
                return parts[0], parts[1]

        # Handle concatenated pairs
        for quote in ["USDT", "USDC", "USD", "BUSD", "DAI"]:
            if symbol.endswith(quote):
                base = symbol[: -len(quote)]
                return base, quote

        # Default
        return symbol, None

    async def _get_fx_rate(
        self,
        currency: str,
        prices: dict[str, Decimal] | None,
    ) -> Decimal | None:
        """Get FX rate to base currency."""
        if currency == self.base_currency:
            return Decimal(1)

        # Try currency converter service
        if self.currency_converter:
            try:
                rate = await self.currency_converter.get_rate(
                    from_currency=currency,
                    to_currency=self.base_currency,
                )
            except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
                logger.warning(
                    "fx_rate_lookup_failed",
                    currency=currency,
                    base_currency=self.base_currency,
                    error=str(e),
                )
            else:
                return rate

        # Try prices dict
        if prices:
            # Direct pair
            pair = f"{currency}/{self.base_currency}"
            if pair in prices:
                return prices[pair]

            # Inverse pair
            inv_pair = f"{self.base_currency}/{currency}"
            if inv_pair in prices:
                return Decimal(1) / prices[inv_pair]

            # Try concatenated
            concat_pair = f"{currency}{self.base_currency}"
            if concat_pair in prices:
                return prices[concat_pair]

        # Default rates for common currencies
        default_rates = {
            "USDT": Decimal("1.0"),
            "USDC": Decimal("1.0"),
            "BUSD": Decimal("1.0"),
            "DAI": Decimal("1.0"),
            "EUR": Decimal("1.1"),
            "GBP": Decimal("1.25"),
            "JPY": Decimal("0.0067"),
            "BTC": Decimal(45000),
            "ETH": Decimal(3000),
        }

        return default_rates.get(currency)

    async def _calculate_portfolio_fx_metrics(
        self,
        currency_exposures: dict[str, CurrencyExposure],
        total_fx_exposure: Decimal,
    ) -> PortfolioCurrencyExposure:
        """Calculate portfolio-level FX metrics."""
        if not currency_exposures:
            return PortfolioCurrencyExposure(
                base_currency=self.base_currency,
                total_currencies=0,
                total_fx_exposure=Decimal(0),
                total_fx_risk=Decimal(0),
            )

        # Calculate concentration metrics
        exposures_by_value = [
            (ccy, exp.value_in_base_currency or Decimal(0))
            for ccy, exp in currency_exposures.items()
            if exp.value_in_base_currency
        ]
        exposures_by_value.sort(key=lambda x: abs(x[1]), reverse=True)

        largest_weight = Decimal(0)
        top_3_exposure = Decimal(0)

        if total_fx_exposure > 0 and exposures_by_value:
            largest_weight = abs(exposures_by_value[0][1]) / total_fx_exposure
            top_3_exposure = sum(abs(exp[1]) for exp in exposures_by_value[:3]) / total_fx_exposure

        # Aggregate VaR (with diversification benefit)
        total_var = sum(exp.var_95 or Decimal(0) for exp in currency_exposures.values())
        portfolio_fx_var = total_var * Decimal("0.8")  # 20% diversification benefit

        # Aggregate stress loss
        total_stress = sum(exp.stress_loss or Decimal(0) for exp in currency_exposures.values())

        # Identify concentrated currencies
        concentrated: list[str] = []
        volatile: list[str] = []
        warnings: list[str] = []

        for ccy, exposure in currency_exposures.items():
            if exposure.value_in_base_currency and total_fx_exposure > 0:
                weight = abs(exposure.value_in_base_currency) / total_fx_exposure
                if weight > self.concentration_threshold:
                    concentrated.append(ccy)
                    warnings.append(f"{ccy} represents {float(weight * 100):.1f}% of FX exposure")

            if exposure.fx_volatility and exposure.fx_volatility > self.high_volatility_threshold:
                volatile.append(ccy)

        if len(currency_exposures) < self.MIN_CURRENCIES_FOR_DIVERSIFICATION:
            warnings.append(f"Low FX diversification: only {len(currency_exposures)} currencies")

        return PortfolioCurrencyExposure(
            base_currency=self.base_currency,
            total_currencies=len(currency_exposures),
            total_fx_exposure=total_fx_exposure,
            total_fx_risk=portfolio_fx_var,
            currency_exposures=currency_exposures,
            largest_currency_weight=largest_weight,
            top_3_currency_concentration=top_3_exposure,
            portfolio_fx_var_95=portfolio_fx_var,
            portfolio_fx_stress_loss=Decimal(str(total_stress)),
            correlation_benefit=total_var - portfolio_fx_var if total_var > 0 else Decimal(0),
            concentrated_currencies=concentrated,
            volatile_currencies=volatile,
            warnings=warnings,
        )
