"""Fee structure configuration models.

This module contains configuration for exchange fee structures
used in both simulation and real trading.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class FeeStructureConfig(BaseModel):
    """Configuration for exchange fee structures."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # Fee rates
    maker_fee_rate: float = Field(
        ...,
        ge=0.0,
        le=0.1,
        description="Maker fee rate as decimal (e.g., 0.0002 = 0.02%)",
    )
    taker_fee_rate: float = Field(
        ...,
        ge=0.0,
        le=0.1,
        description="Taker fee rate as decimal (e.g., 0.0005 = 0.05%)",
    )

    # Fee calculation method
    fee_calculation_method: Literal["percentage", "fixed"] = Field(
        default="percentage",
        description="Method for calculating fees: 'percentage' or 'fixed'",
    )

    # Fee asset
    fee_asset: str | None = Field(
        default=None,
        description="Asset used for fee payment (e.g., 'USDC', 'BNB'). If None, uses quote currency",
    )

    # Fee limits (optional)
    minimum_fee: float | None = Field(
        default=None,
        ge=0.0,
        description="Minimum fee amount in fee asset",
    )
    maximum_fee: float | None = Field(
        default=None,
        gt=0.0,
        description="Maximum fee amount in fee asset",
    )

    # Rebates (optional)
    maker_rebate_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=0.1,
        description="Maker rebate rate as decimal (negative fee for providing liquidity)",
    )

    # VIP/Volume tiers (optional)
    volume_based_tiers: bool = Field(
        default=False,
        description="Whether exchange uses volume-based fee tiers",
    )
    vip_tier: int | None = Field(
        default=None,
        ge=0,
        description="Current VIP/volume tier for fee calculation",
    )

    # Special fee rules
    use_exchange_token_discount: bool = Field(
        default=False,
        description="Whether to apply exchange token discount (e.g., BNB on Binance)",
    )
    exchange_token_discount_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Discount rate when using exchange token (e.g., 0.25 = 25% discount)",
    )

