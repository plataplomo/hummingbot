"""Hyperliquid exchange action payload models."""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from cyberdelta.apis.exceptions.field_validation import (
    ConflictingMarketIdentifiersError,
    MissingMarketIdentifierError,
)
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawAssetString64HL,
    RawFiniteDecimalStr,
    RawNonNegativeInt,
    RawOptionalCloidHL,
    RawStrictBool,
    RawStrictEthereumAddressStrHL,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawOrderType,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)
from cyberdelta.apis.hyperliquid.models.signing_validators import (
    EthereumAddressNormalizer,
)


# Model for ETH specific withdrawal action (part of the signed payload)
class HyperliquidRawEthWithdrawalActionPayload(BaseModel, EthereumAddressNormalizer):
    """Represents the specific action payload for withdrawing ETH to L1.

    This forms part of the signed message for the /exchange endpoint and contains
    the amount and destination address for ETH withdrawals to Layer 1.

    Includes automatic Ethereum address normalization for the destination field.
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

    Supports both perpetual and spot markets:
    - Perpetual orders: Use 'a' (asset_index) field
    - Spot orders: Use either 'a' (asset_index) OR 'coin' (symbol) field

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

    # Market identifiers - exactly one must be provided
    a: RawNonNegativeInt | None = Field(None, alias="asset_index")
    coin: RawAssetString64HL | None = Field(None, alias="coin")

    # Common fields for both markets
    b: RawStrictBool = Field(..., alias="is_buy")
    p: RawFiniteDecimalStr = Field(..., alias="limit_px")
    s: RawFiniteDecimalStr = Field(..., alias="size")
    r: RawStrictBool = Field(..., alias="reduce_only")
    t: HyperliquidRawOrderType = Field(..., alias="order_type_details")
    c: RawOptionalCloidHL = Field(default=None, alias="client_order_id")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @model_validator(mode="after")
    def validate_market_identifier(self) -> HyperliquidRawOrderItemSpec:
        """Ensure exactly one market identifier is provided.

        Returns:
            The validated instance with exactly one market identifier set.

        Raises:
            ConflictingMarketIdentifiersError: If both asset index and coin are provided.
            MissingMarketIdentifierError: If neither asset index nor coin is provided.
        """
        has_asset_index = self.a is not None
        has_coin = self.coin is not None

        if has_asset_index and has_coin:
            raise ConflictingMarketIdentifiersError
        if not has_asset_index and not has_coin:
            raise MissingMarketIdentifierError
        return self


class HyperliquidRawL2UsdTransferActionDetails(BaseModel):
    """Represents the 'action' details for an L2 USD transfer.

    Includes automatic serialization for signing.
    """

    chain: Literal["L2"]
    payload: HyperliquidRawL2UsdTransferPayload

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class HyperliquidRawCancelItem(BaseModel):
    """Represents a single cancel item in the cancels array.

    Based on official SDK: uses short field names 'a' for asset and 'o' for oid.
    This is used in the 'cancels' array for cancel order requests.
    Includes automatic serialization for signing.

    Example:
        {"type": "cancel", "cancels": [{"a": 0, "o": 12345}]}
    """

    a: RawNonNegativeInt = Field(..., description="Asset index")
    o: RawNonNegativeInt = Field(..., description="Order ID to cancel")

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawUpdateLeverageAction(BaseModel):
    """Represents the action payload for updating leverage on a specific asset.

    This forms part of the signed message for the /exchange endpoint and contains
    the asset index, leverage mode (cross/isolated), and leverage value.
    Includes automatic serialization for signing.
    """

    asset: RawNonNegativeInt = Field(description="Asset index from meta response")
    isCross: RawStrictBool = Field(description="True for cross margin, False for isolated")
    leverage: RawNonNegativeInt = Field(description="Leverage value (e.g., 10, 20, 50)")

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)
