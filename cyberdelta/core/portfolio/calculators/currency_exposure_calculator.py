"""Calculator for currency exposure and FX risk metrics."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from pydantic.dataclasses import dataclass

from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.portfolio.exceptions import (
    CurrencyMismatchError,
    InvalidCalculationInputError,
)
from cyberdelta.enums import OrderSide


if TYPE_CHECKING:
    from cyberdelta.core.models import DerivativePosition
    from cyberdelta.core.portfolio.services.currency_converter import CurrencyConverter

logger = get_logger(__name__)


@dataclass
class CurrencyExposure:
    """Currency exposure for a single currency with validation."""

    currency: str = Field(min_length=1, max_length=10, description="Currency code")
    gross_exposure: Decimal = Field(ge=0, description="Total absolute exposure (non-negative)")
    net_exposure: Decimal = Field(description="Net long/short exposure")
    long_exposure: Decimal = Field(ge=0, description="Total long positions (non-negative)")
    short_exposure: Decimal = Field(le=0, description="Total short positions (non-positive)")

    # Breakdown by source
    spot_exposure: Decimal = Field(default=Decimal(0), description="Spot exposure")
    derivative_exposure: Decimal = Field(default=Decimal(0), description="Derivative exposure")
    collateral_exposure: Decimal = Field(default=Decimal(0), description="Collateral exposure")

    # Risk metrics
    value_in_base_currency: Decimal | None = Field(
        default=None, description="Value in portfolio base currency"
    )
    fx_rate: Decimal | None = Field(default=None, gt=0, description="Current FX rate to base")
    fx_volatility: Decimal | None = Field(
        default=None, ge=0, le=5, description="Historical volatility (0-500%)"
    )
    var_95: Decimal | None = Field(default=None, le=0, description="95% FX VaR (non-positive)")
    stress_loss: Decimal | None = Field(
        default=None, le=0, description="Loss in stress scenario (non-positive)"
    )

    # Position details
    position_count: int = Field(default=0, ge=0, description="Number of positions")
    exchanges: set[str] = Field(default_factory=set, description="Exchanges with exposure")

    @field_validator("currency", mode="before")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        """Validate and normalize currency code."""
        if not v:
            raise InvalidCalculationInputError(
                parameter="currency", value=v, expected="non-empty string"
            )
        return v.upper().strip()

    @field_validator(
        "gross_exposure",
        "net_exposure",
        "long_exposure",
        "short_exposure",
        "spot_exposure",
        "derivative_exposure",
        "collateral_exposure",
        mode="before",
    )
    @classmethod
    def validate_decimals(cls, v: Decimal | str | float) -> Decimal:
        """Ensure all decimal values are finite."""
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="exposure", value=value, expected="finite number"
            )
        return value

    @field_validator("long_exposure", "short_exposure", mode="after")
    @classmethod
    def validate_exposure_consistency(cls, v: Decimal, info: ValidationInfo) -> Decimal:
        """Validate exposure sign consistency."""
        if info.field_name == "long_exposure" and v < 0:
            raise InvalidCalculationInputError(
                parameter="long_exposure", value=v, expected="non-negative value"
            )
        if info.field_name == "short_exposure" and v > 0:
            raise InvalidCalculationInputError(
                parameter="short_exposure", value=v, expected="non-positive value"
            )
        return v

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
    """Aggregate currency exposure for the portfolio with validation."""

    base_currency: str = Field(min_length=1, max_length=10, description="Base currency code")
    total_currencies: int = Field(ge=0, description="Total number of currencies")

    # Aggregate metrics
    total_fx_exposure: Decimal = Field(
        ge=0, description="Total non-base currency exposure (non-negative)"
    )
    total_fx_risk: Decimal = Field(ge=0, description="Total FX VaR (non-negative)")

    # Individual currency exposures
    currency_exposures: dict[str, CurrencyExposure] = Field(
        default_factory=dict, description="Individual currency exposures"
    )

    # Concentration metrics
    largest_currency_weight: Decimal = Field(
        default=Decimal(0), ge=0, le=1, description="Largest currency weight (0-100%)"
    )
    top_3_currency_concentration: Decimal = Field(
        default=Decimal(0), ge=0, le=1, description="Top 3 concentration (0-100%)"
    )

    # Risk metrics
    portfolio_fx_var_95: Decimal = Field(
        default=Decimal(0), ge=0, description="Portfolio 95% FX VaR"
    )
    portfolio_fx_stress_loss: Decimal = Field(
        default=Decimal(0), ge=0, description="Portfolio FX stress loss"
    )
    correlation_benefit: Decimal = Field(
        default=Decimal(0), ge=0, description="Diversification benefit"
    )

    # Risk indicators
    concentrated_currencies: list[str] = Field(
        default_factory=list, description="Currencies with >20% exposure"
    )
    volatile_currencies: list[str] = Field(
        default_factory=list, description="High volatility currencies"
    )
    warnings: list[str] = Field(default_factory=list, description="Risk warnings")

    @field_validator("base_currency", mode="before")
    @classmethod
    def validate_base_currency(cls, v: str) -> str:
        """Validate and normalize base currency."""
        if not v:
            raise InvalidCalculationInputError(
                parameter="base_currency", value=v, expected="non-empty string"
            )
        return v.upper().strip()

    @field_validator(
        "total_fx_exposure",
        "total_fx_risk",
        "portfolio_fx_var_95",
        "portfolio_fx_stress_loss",
        "correlation_benefit",
        mode="before",
    )
    @classmethod
    def validate_risk_metrics(cls, v: Decimal | str | float) -> Decimal:
        """Ensure risk metrics are finite and non-negative."""
        value: Decimal = v if isinstance(v, Decimal) else Decimal(str(v))
        if not value.is_finite():
            raise InvalidCalculationInputError(
                parameter="risk_metric", value=value, expected="finite number"
            )
        return value

    @field_validator("currency_exposures", mode="after")
    @classmethod
    def validate_currency_exposures(
        cls, v: dict[str, CurrencyExposure]
    ) -> dict[str, CurrencyExposure]:
        """Validate currency exposures dictionary."""
        # v is already typed as dict[str, CurrencyExposure] by Pydantic
        for currency, exposure in v.items():
            # exposure is already typed as CurrencyExposure by Pydantic
            if exposure.currency != currency:
                raise CurrencyMismatchError(
                    expected_currency=currency, actual_currency=exposure.currency
                )
        return v

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


class CurrencyAccumulator(BaseModel):
    """Model for accumulating currency exposure data during calculation."""

    gross: Decimal = Field(default=Decimal(0), description="Gross exposure")
    net: Decimal = Field(default=Decimal(0), description="Net exposure")
    long: Decimal = Field(default=Decimal(0), description="Long exposure")
    short: Decimal = Field(default=Decimal(0), description="Short exposure")
    spot: Decimal = Field(default=Decimal(0), description="Spot exposure")
    derivative: Decimal = Field(default=Decimal(0), description="Derivative exposure")
    collateral: Decimal = Field(default=Decimal(0), description="Collateral exposure")
    position_count: int = Field(default=0, ge=0, description="Number of positions")
    exchanges: set[str] = Field(default_factory=set, description="Exchanges involved")


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
        positions: list[DerivativePosition],
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
        currency_data: dict[str, CurrencyAccumulator] = defaultdict(CurrencyAccumulator)

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
                    currency_data[currency].gross += balance
                    currency_data[currency].net += balance
                    currency_data[currency].long += balance
                    currency_data[currency].spot += balance
                    currency_data[currency].exchanges.add(exchange)

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
            value_in_base = data.net * fx_rate if fx_rate else None

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
                gross_exposure=data.gross,
                net_exposure=data.net,
                long_exposure=data.long,
                short_exposure=data.short,
                spot_exposure=data.spot,
                derivative_exposure=data.derivative,
                collateral_exposure=data.collateral,
                value_in_base_currency=value_in_base,
                fx_rate=fx_rate,
                fx_volatility=fx_vol,
                var_95=var_95,
                stress_loss=stress_loss,
                position_count=data.position_count,
                exchanges=data.exchanges,
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
        position: DerivativePosition,
        currency_data: dict[str, CurrencyAccumulator],
    ) -> None:
        """Process currency exposure from a position."""
        # Parse currencies from position symbol - DerivativePosition doesn't have currency fields
        base_ccy, quote_ccy = self._parse_position_currencies(position)

        position_size = position.size
        side = position.side
        exchange = position.exchange
        
        # Calculate position value from size and mark_price
        mark_price = position.mark_price or Decimal(0)
        position_value = abs(position_size) * mark_price

        # Base currency exposure (what we're holding)
        if base_ccy:
            if side == OrderSide.BUY:  # Long position
                currency_data[base_ccy].gross += abs(position_size)
                currency_data[base_ccy].net += position_size
                currency_data[base_ccy].long += abs(position_size)
            else:  # Short position
                currency_data[base_ccy].gross += abs(position_size)
                currency_data[base_ccy].net -= abs(position_size)
                currency_data[base_ccy].short += abs(position_size)

            currency_data[base_ccy].derivative += abs(position_size)
            currency_data[base_ccy].position_count += 1
            currency_data[base_ccy].exchanges.add(exchange)

        # Quote currency exposure (what we're paying/receiving)
        if quote_ccy and quote_ccy != base_ccy:
            if side == OrderSide.BUY:  # Long position = short quote currency
                currency_data[quote_ccy].gross += position_value
                currency_data[quote_ccy].net -= position_value
                currency_data[quote_ccy].short += position_value
            else:  # Short position = long quote currency
                currency_data[quote_ccy].gross += position_value
                currency_data[quote_ccy].net += position_value
                currency_data[quote_ccy].long += position_value

            currency_data[quote_ccy].derivative += position_value
            currency_data[quote_ccy].exchanges.add(exchange)

    def _parse_position_currencies(
        self, position: DerivativePosition
    ) -> tuple[str | None, str | None]:
        """Parse currencies from position symbol."""
        symbol = position.symbol

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
