"""CyberDeltaEngine: Hyperliquid API Raw Models (Top-Level Request Payloads).

-------------------------------------------------------------------------

This module defines Pydantic models for the *entire* request payload structure
for various Hyperliquid Exchange API endpoints (primarily for the `/exchange` route).
These models are intended to be returned by the `HyperliquidRequestBuilder`.

These models ensure that the entire structure sent to the API is validated according
to the project's raw model policies.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field

# Action specific payloads (previously built actions)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawCancelOrderAction,
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawPlaceOrderAction,  # This is the individual order action for the list
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.utils.parsing import validate_str_field


# --- /exchange endpoint: L2 USD Transfer --- #
class HyperliquidApiL2UsdTransferRequest(BaseModel):
    """Top-level request payload for an L2 USD transfer."""

    type: Annotated[
        Literal["usdTransfer"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("usdTransfer")
    action: HyperliquidRawL2UsdTransferActionDetails

    model_config = ConfigDict(extra="forbid", frozen=True)


# --- /exchange endpoint: Withdrawals (ETH and other tokens) --- #
class HyperliquidApiEthWithdrawalRequest(BaseModel):
    """Top-level request payload for an ETH withdrawal."""

    type: Annotated[
        Literal["withdrawEth"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("withdrawEth")
    action: HyperliquidRawEthWithdrawalActionPayload

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidApiTokenWithdrawalRequest(BaseModel):
    """Top-level request payload for a generic token withdrawal (to L1)."""

    type: Annotated[
        Literal["withdraw"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("withdraw")
    action: HyperliquidRawWithdrawalToL1ActionPayload  # From hl_raw_transfer_withdrawal

    model_config = ConfigDict(extra="forbid", frozen=True)


# For the builder, it might be easier to return one of these specific types,
# or a Union if the calling code needs to handle both.
# Alternatively, the builder can decide which to return based on the asset.


# --- /exchange endpoint: Place Order --- #
class HyperliquidApiPlaceOrderRequest(BaseModel):
    """Top-level request payload for placing one or more orders."""

    type: Annotated[
        Literal["order"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("order")
    # The `HyperliquidRawBatchPlaceOrderActionPayload` has 'type', 'grouping', and 'orders'
    # It seems this top-level request *is* the HyperliquidRawBatchPlaceOrderActionPayload.
    # Let's verify the structure from hl_request_builder.py for build_place_order_payload
    # It was: return {"type": "order", "actions": [place_order_action.model_dump(by_alias=True)]}
    # The Hyperliquid spec for place order (type: "order") shows an 'actions' list of 'OrderSpec'.
    # So `HyperliquidRawPlaceOrderAction` is the model for each item in the 'actions' list.

    actions: list[HyperliquidRawPlaceOrderAction]

    model_config = ConfigDict(extra="forbid", frozen=True)


# --- /exchange endpoint: Cancel Order --- #
class HyperliquidApiCancelOrderRequest(BaseModel):
    """Top-level request payload for cancelling an order."""

    type: Annotated[
        Literal["cancel"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field("cancel")
    action: HyperliquidRawCancelOrderAction

    model_config = ConfigDict(extra="forbid", frozen=True)
