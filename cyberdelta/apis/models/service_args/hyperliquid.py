"""Hyperliquid-specific service argument models for internal service layer interfaces.

This module contains Hyperliquid-specific Pydantic models that encapsulate arguments for various
service methods, centralizing validation logic and improving API clarity.
"""

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cyberdelta.core.symbols.models import Symbol
from cyberdelta.exceptions.service_validation import TimeRangeError


class HyperliquidGetOrderStatusArgs(BaseModel):
    """Arguments for Hyperliquid-specific order status queries."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    wallet_address: str = Field(..., min_length=1, max_length=128)
    order_id: int = Field(..., ge=0)


class HyperliquidGetOrderHistoryArgs(BaseModel):
    """Arguments for fetching order history (Hyperliquid-specific)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    wallet_address: str = Field(..., min_length=1, max_length=128)
    start_time_ms: int = Field(..., ge=0)
    end_time_ms: int = Field(..., ge=0)

    @model_validator(mode="after")
    def check_time_range(self) -> "HyperliquidGetOrderHistoryArgs":
        """Validate time range logic.

        Returns:
            Self for method chaining.

        Raises:
            TimeRangeError: If start_time_ms is greater than or equal to end_time_ms.
        """
        if self.start_time_ms >= self.end_time_ms:
            raise TimeRangeError(
                start_field="start_time_ms",
                end_field="end_time_ms",
                start_value=self.start_time_ms,
                end_value=self.end_time_ms,
            )
        return self


class HyperliquidTransferL2UsdArgs(BaseModel):
    """Arguments for L2 USD transfer requests."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    destination_address: str = Field(..., min_length=1, max_length=128)
    amount: Decimal = Field(..., gt=Decimal(0))


class HyperliquidGetUserStateArgs(BaseModel):
    """Arguments for fetching user state information."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    wallet_address: str = Field(..., min_length=1, max_length=128)


class HyperliquidGetUserFillsArgs(BaseModel):
    """Arguments for fetching user fills (trade history)."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    wallet_address: str = Field(..., min_length=1, max_length=128)


class HyperliquidGetOpenOrdersArgs(BaseModel):
    """Arguments for fetching open orders on Hyperliquid."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol | None = Field(default=None, description="Optional symbol to filter orders")
    wallet_address: str | None = Field(default=None, min_length=1, max_length=128)


class HyperliquidUpdateLeverageArgs(BaseModel):
    """Arguments for updating leverage on a specific asset."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    asset_index: int = Field(..., ge=0)
    leverage: int = Field(..., ge=1, le=1000)
    is_cross: bool = Field(default=True)


class HyperliquidWithdrawL1Args(BaseModel):
    """Arguments for L1 withdrawal requests."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    asset: str = Field(..., min_length=1, max_length=64)
    amount: Decimal = Field(..., gt=Decimal(0))
    destination_address: str = Field(..., min_length=1, max_length=128)


class HyperliquidGetCandleSnapshotArgs(BaseModel):
    """Arguments for fetching candle snapshot data."""

    model_config = ConfigDict(extra="forbid", validate_assignment=True)

    symbol: Symbol = Field(...)
    timeframe: str = Field(..., min_length=1, max_length=32)
    start_time_ms: int = Field(..., ge=0)
    end_time_ms: int = Field(..., ge=0)

    @model_validator(mode="after")
    def check_time_range(self) -> "HyperliquidGetCandleSnapshotArgs":
        """Validate time range logic.

        Returns:
            Self for method chaining.

        Raises:
            TimeRangeError: If start_time_ms is greater than or equal to end_time_ms.
        """
        if self.start_time_ms >= self.end_time_ms:
            raise TimeRangeError(
                start_field="start_time_ms",
                end_field="end_time_ms",
                start_value=self.start_time_ms,
                end_value=self.end_time_ms,
            )
        return self
