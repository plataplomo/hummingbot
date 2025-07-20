"""CyberDeltaEngine: Hyperliquid API Raw Models (Transfer & Withdrawal Action Payloads).

-----------------------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request for L2 transfers and L1 withdrawals.
"""

from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
)

from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawDefaultString,
    RawPositiveFiniteDecimalStr,
    RawStrictEthereumAddressStrHL,
)
from cyberdelta.apis.hyperliquid.models.signing_validators import (
    EthereumAddressNormalizer,
)
from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawL2UsdTransferPayload(
    BaseModel,
    EthereumAddressNormalizer,
):
    """Payload for an L2 USDC transfer action.

    Corresponds to action type "usdTransfer" with chain "L2".
    Includes automatic Ethereum address normalization for the destination field.
    """

    destination: RawStrictEthereumAddressStrHL = Field(
        ...,
        description="The recipient's 0x address.",
    )
    token: Annotated[
        Literal["USDC"],
        BeforeValidator(lambda v: validate_str_field(v, "token", max_length=16)),
    ] = Field(..., description="The token to transfer, must be USDC for L2.")
    amount: RawPositiveFiniteDecimalStr = Field(..., description="The positive amount to transfer.")

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawWithdrawalToL1ActionPayload(
    BaseModel,
    EthereumAddressNormalizer,
):
    """Payload for withdrawing funds to L1.

    Corresponds to action type "withdraw".
    Includes automatic Ethereum address normalization for the destination field.
    """

    token: RawDefaultString = Field(
        ...,
        description="The symbol of the token to withdraw",
        max_length=24,
    )
    amount: RawPositiveFiniteDecimalStr = Field(..., description="The positive amount to withdraw.")
    destination: RawStrictEthereumAddressStrHL = Field(
        ...,
        description="The recipient's 0x address on L1.",
    )
    # Note: Hyperliquid docs also mention "withdrawEth" type. If its payload differs,
    # a separate model or a union model might be needed.
    # For now, this model assumes "token" field covers ETH as well for a generic "withdraw" type.

    model_config = ConfigDict(extra="forbid", frozen=True)


class HyperliquidRawInternalUsdTransferPayload(
    BaseModel,
):
    """Top-level request payload for internal USD transfers between spot and perp accounts.

    Uses the 'usdClassTransfer' action type for moving USDC between spot and perpetual
    accounts within the same wallet. This matches the official SDK implementation.
    """

    type: Annotated[
        Literal["usdClassTransfer"],
        BeforeValidator(lambda v: validate_str_field(v, "type", max_length=32)),
    ] = Field(
        "usdClassTransfer", description="Action type for USD class transfers between spot and perp"
    )

    amount: Annotated[
        str, BeforeValidator(lambda v: validate_str_field(v, "amount", max_length=32))
    ] = Field(..., description="Amount to transfer in USDC as string")

    toPerp: bool = Field(..., description="Transfer direction: True=spot→perp, False=perp→spot")

    nonce: int = Field(
        ..., description="Nonce/timestamp for the action, typically millisecond timestamp"
    )

    model_config = ConfigDict(extra="forbid", frozen=True)


# If "withdrawEth" has a different structure, define HyperliquidRawEthWithdrawalActionPayload here.
# Example:
# class HyperliquidRawEthWithdrawalActionPayload(BaseModel):
