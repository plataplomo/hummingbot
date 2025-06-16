"""Hyperliquid exchange action payload models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalNonEmptyString64HL,
    RawStrictBool,
    RawStrictEthereumAddressStrHL,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawOrderType,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)


# Model for ETH specific withdrawal action (part of the signed payload)
class HyperliquidRawEthWithdrawalActionPayload(BaseModel):
    """Represents the specific action payload for withdrawing ETH to L1.

    This forms part of the signed message for the /exchange endpoint and contains
    the amount and destination address for ETH withdrawals to Layer 1.
    """

    amount: RawFiniteDecimalStr
    destination: RawStrictEthereumAddressStrHL

    model_config = ConfigDict(extra="forbid", frozen=True)


# Model for individual order specifications within a bulk order placement
class HyperliquidRawOrderItemSpec(BaseModel):
    """Represents the detailed specification for a single order within batch operations.

    This model defines the structure for individual orders within the 'orders' list
    of a batch order placement action. It forms part of the signed message for the
    /exchange endpoint and ensures proper validation of order parameters.

    Corresponds to the 'OrderRequest' structure in Hyperliquid's documentation.
    struct OrderRequest {
        asset: u32,
        is_buy: bool,
        reduce_only: bool,
        limit_px: RustDecimal,
        sz: RustDecimal,
        order_type: OrderTypeWire,
        cloid: Option<Bytes16>,
    }
    """

    a: RawNonNegativeInt = Field(..., alias="asset_index")
    b: RawStrictBool = Field(..., alias="is_buy")
    p: RawFiniteDecimalStr = Field(..., alias="limit_px")
    s: RawFiniteDecimalStr = Field(..., alias="size")
    r: RawStrictBool = Field(..., alias="reduce_only")
    t: HyperliquidRawOrderType = Field(..., alias="order_type_details")
    c: RawOptionalNonEmptyString64HL | None = Field(default=None, alias="client_order_id")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


# Model for the overall BATCH order placement action (signed payload)
class HyperliquidRawBatchPlaceOrderActionPayload(BaseModel):
    """Represents the action payload for placing one or more orders in a batch.

    This forms part of the signed message for the /exchange endpoint and contains
    the order specifications for batch order placement operations.

    Corresponds to the 'action' field when 'type' is 'order' for batch operations.
    """

    type: Literal["order"] = "order"
    grouping: Literal["na"] = "na"
    orders: list[HyperliquidRawOrderItemSpec]

    model_config = ConfigDict(extra="forbid", frozen=True)


# --- New Models to Add ---


class HyperliquidRawL2UsdTransferActionDetails(BaseModel):
    """Represents the 'action' details for an L2 USD transfer."""

    chain: Literal["L2"]
    payload: HyperliquidRawL2UsdTransferPayload

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class HyperliquidRawCancelOrderAction(BaseModel):
    """Represents the 'action' payload for cancelling an order."""

    asset: RawNonNegativeInt
    oid: RawNonNegativeInt

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class HyperliquidRawUpdateLeverageAction(BaseModel):
    """Represents the action payload for updating leverage on a specific asset.
    
    This forms part of the signed message for the /exchange endpoint and contains
    the asset index, leverage mode (cross/isolated), and leverage value.
    """
    
    asset: RawNonNegativeInt = Field(description="Asset index from meta response")
    isCross: RawStrictBool = Field(description="True for cross margin, False for isolated")
    leverage: RawNonNegativeInt = Field(description="Leverage value (e.g., 10, 20, 50)")
    
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
