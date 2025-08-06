"""Market Order Configuration Model.

This module defines the configuration for market order execution,
including slippage limits, liquidity requirements, and safety parameters.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.core.execution.orders.market_order_errors import MarketOrderParameterError
from cyberdelta.symbols import Symbol
from cyberdelta.utils.parsing import parse_decimal_value


class MarketOrderConfig(BaseModel):
    """Configuration for market order execution.

    This model defines all configurable parameters for executing market orders
    using aggressive IoC limit orders. All decimal values are validated to ensure
    financial precision.
    """

    model_config = ConfigDict(validate_assignment=True, frozen=True)

    # Slippage configuration
    default_slippage_pct: Decimal = Field(
        default=Decimal("0.001"),
        gt=Decimal(0),
        le=Decimal("0.1"),
        description="Default slippage percentage (0.001 = 0.1%)",
    )
    max_slippage_pct: Decimal = Field(
        default=Decimal("0.05"),
        gt=Decimal(0),
        le=Decimal("0.2"),
        description="Maximum allowed slippage percentage (0.05 = 5%)",
    )

    # Price deviation limits
    max_price_deviation_pct: Decimal = Field(
        default=Decimal("0.10"),
        gt=Decimal(0),
        le=Decimal("0.5"),
        description="Maximum price deviation from reference (0.10 = 10%)",
    )

    # Liquidity requirements
    min_liquidity_ratio: Decimal = Field(
        default=Decimal("2.0"),
        ge=Decimal("1.0"),
        le=Decimal("10.0"),
        description="Minimum liquidity ratio (2.0 = 2x order size required)",
    )

    # Base asset slippage overrides
    slippage_by_base_asset: dict[str, Decimal] = Field(
        default_factory=lambda: {
            "BTC": Decimal("0.005"),  # 0.5% for high liquidity
            "ETH": Decimal("0.005"),
            "SOL": Decimal("0.01"),
            "default": Decimal("0.02"),  # 2% for others
        },
        description="Slippage overrides by base asset (BTC, ETH, etc)",
    )

    # Market order enablement
    enabled: bool = Field(
        default=True,
        description="Whether market orders are enabled",
    )

    # AllMids integration
    use_all_mids_for_reference: bool = Field(
        default=False,
        description="Use AllMids endpoint for reference price when available",
    )

    # Timeout configuration
    order_timeout_seconds: int = Field(
        default=10,
        gt=0,
        le=60,
        description="Timeout for order execution in seconds",
    )

    @field_validator("default_slippage_pct", "max_slippage_pct", "max_price_deviation_pct")
    @classmethod
    def validate_percentage(cls, v: Decimal) -> Decimal:
        """Validate percentage values are finite and positive.

        Returns:
            Decimal: Validated finite positive percentage value.

        Raises:
            MarketOrderParameterError.finite_decimal_error: If percentage is not finite
            MarketOrderParameterError.positive_error: If percentage is not positive
        """
        if not v.is_finite():
            raise MarketOrderParameterError.finite_decimal_error()
        if v <= Decimal(0):
            raise MarketOrderParameterError.positive_error()
        return v

    @field_validator("slippage_by_base_asset")
    @classmethod
    def validate_slippage_map(cls, v: dict[str, Decimal]) -> dict[str, Decimal]:
        """Validate all slippage values in the base asset map.

        Returns:
            dict[str, Decimal]: Validated slippage mapping with required 'default' entry.

        Raises:
            MarketOrderParameterError.slippage_default_error: If 'default' entry is missing
            MarketOrderParameterError.slippage_invalid_error: If slippage values are invalid
        """
        if "default" not in v:
            raise MarketOrderParameterError.slippage_default_error()

        for base_asset, slippage in v.items():
            parsed = parse_decimal_value(
                slippage,
                allow_none=False,
                field_name=f"slippage_by_base_asset[{base_asset}]",
            )
            if not parsed.is_finite() or parsed <= Decimal(0):
                raise MarketOrderParameterError.slippage_invalid_error(base_asset, slippage)

        return v

    def get_slippage_for_symbol(self, symbol: Symbol) -> Decimal:
        """Get the slippage configuration for a specific Symbol.

        Args:
            symbol: Symbol object

        Returns:
            Decimal: Configured slippage for the symbol, or default if not found
        """
        # Extract base asset from symbol for configuration lookup
        try:
            symbol_key = symbol.base_asset or symbol.value
        except ValueError:
            # Components not set, use symbol value as fallback
            symbol_key = symbol.value

        return self.slippage_by_base_asset.get(symbol_key, self.slippage_by_base_asset["default"])

    def validate_slippage(self, slippage: Decimal) -> Decimal:
        """Validate and cap slippage to maximum allowed.

        Args:
            slippage: Proposed slippage value

        Returns:
            Decimal: Validated slippage capped at max_slippage_pct
        """
        if not slippage.is_finite() or slippage < Decimal(0):
            return self.default_slippage_pct

        return min(slippage, self.max_slippage_pct)
