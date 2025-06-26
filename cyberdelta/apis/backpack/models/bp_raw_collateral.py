"""Raw models for Backpack collateral endpoint responses.

Based on OpenAPI specification analysis:
- Endpoint: GET /api/v1/capital/collateral
- Instruction: collateralQuery
- Response Schema: MarginAccountSummary with Collateral array
- Subaccount Support: Optional subaccountId parameter
"""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cyberdelta.utils.parsing import validate_str_field

from .bp_common_raw_types import (
    RawBpNonEmptyStringMax64,
    RawBpStringToFiniteDecimal,
)


class BackpackRawCollateralAsset(BaseModel):
    """Individual asset collateral information.

    Maps exactly to the OpenAPI Collateral schema. Each asset's
    collateral contribution is calculated based on quantity, price,
    and exchange-specific weight factors.
    """

    symbol: RawBpNonEmptyStringMax64 = Field(
        ...,
        alias="symbol",
        description="Asset symbol (e.g., BTC, ETH, USDC)",
    )
    asset_mark_price: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="assetMarkPrice",
        description="Current mark price of the asset",
    )
    total_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="totalQuantity",
        description="Total quantity held (sum of all balance types)",
    )
    balance_notional: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="balanceNotional",
        description="Notional value of balance (quantity × price)",
    )
    collateral_weight: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="collateralWeight",
        description="Risk weight factor (0-1) applied to this asset",
    )
    collateral_value: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="collateralValue",
        description="Effective collateral value (notional × weight)",
    )
    open_order_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="openOrderQuantity",
        description="Quantity locked in open orders",
    )
    lend_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="lendQuantity",
        description="Quantity currently lent out to other users",
    )
    available_quantity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="availableQuantity",
        description="Quantity available for immediate trading/withdrawal",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )

    @field_validator("symbol", mode="before")
    @classmethod
    def validate_symbol(cls, v: object) -> str:
        """Validate symbol is non-empty string per OpenAPI spec."""
        return validate_str_field(v, field_name="symbol", max_length=64, allow_empty=False)


class BackpackRawCollateralResponse(BaseModel):
    """Raw response from /api/v1/capital/collateral endpoint.

    Maps to OpenAPI MarginAccountSummary schema with exact field names
    from the API specification. This represents the complete margin
    state including equity, liabilities, and per-asset collateral.
    """

    # Core Equity Fields (required in OpenAPI spec)
    net_equity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="netEquity",
        description="Total account equity (assets - liabilities)",
    )
    net_equity_available: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="netEquityAvailable",
        description="Available equity for new positions",
    )
    net_equity_locked: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="netEquityLocked",
        description="Equity locked in open orders/positions",
    )
    assets_value: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="assetsValue",
        description="Total value of all assets",
    )
    liabilities_value: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="liabilitiesValue",
        description="Total value of all liabilities",
    )

    # Margin Fields (required in OpenAPI spec)
    imf: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="imf",
        description="Initial Margin Fraction (account-level)",
    )
    mmf: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="mmf",
        description="Maintenance Margin Fraction (account-level)",
    )
    margin_fraction: str | None = Field(
        None,
        alias="marginFraction",
        description="Current margin utilization fraction (nullable in OpenAPI)",
    )

    # Position & Risk Fields (required in OpenAPI spec)
    borrow_liability: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="borrowLiability",
        description="Total borrowed amount liability",
    )
    pnl_unrealized: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="pnlUnrealized",
        description="Total unrealized PnL across positions",
    )
    unsettled_equity: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="unsettledEquity",
        description="Equity pending settlement",
    )
    net_exposure_futures: RawBpStringToFiniteDecimal = Field(
        ...,
        alias="netExposureFutures",
        description="Net futures/perp exposure notional",
    )

    # Collateral Details (required array in OpenAPI spec)
    collateral: list[BackpackRawCollateralAsset] = Field(
        ...,
        alias="collateral",
        description="Per-asset collateral breakdown",
    )

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        populate_by_name=True,
        validate_assignment=True,
    )


class BackpackRawCollateralQueryParams(BaseModel):
    """Query parameters for collateral endpoint.

    Based on OpenAPI spec: only subaccountId is supported as optional parameter.
    """

    subaccount_id: int | None = Field(
        default=None,
        alias="subaccountId",
        description="Optional subaccount ID (uint16 in OpenAPI spec)",
        ge=0,
        le=65535,  # uint16 max value
    )

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
