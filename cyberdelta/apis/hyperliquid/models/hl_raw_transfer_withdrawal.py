"""
CyberDeltaEngine: Hyperliquid API Raw Models (Transfer & Withdrawal Action Payloads)
-----------------------------------------------------------------------------------

This module defines Pydantic models for constructing parts of the raw
Hyperliquid Exchange API request for L2 transfers and L1 withdrawals.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import parse_decimal_value, validate_str_field


class HyperliquidRawL2UsdTransferPayload(BaseModel):
    """
    Payload for an L2 USDC transfer action.
    Corresponds to action type "usdTransfer" with chain "L2".
    """

    destination: str = Field(..., description="The recipient's 0x address.")
    token: Literal["USDC"] = Field(..., description="The token to transfer, must be USDC for L2.")
    amount: str = Field(..., description="The string representation of the amount to transfer.")

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("destination", mode="before")
    @classmethod
    def validate_destination_address(cls, v: str, info: ValidationInfo) -> str:
        field_name = info.field_name or "destination"
        s = validate_str_field(v, field_name=field_name, max_length=42, allow_empty=False)
        if not s.startswith("0x") or len(s) != 42:
            raise ValueError(
                f"{field_name} '{s}' is not a valid 42-character address starting with 0x."
            )
        return s

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount_str(cls, v: str, info: ValidationInfo) -> str:
        from decimal import Decimal

        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite() or d <= Decimal(0):
                raise ValueError(f"{field_name} must be a positive finite decimal string.")
        except ValueError as e:
            raise ValueError(
                f"{field_name} '{s}' is not a valid positive finite decimal string: {e}"
            ) from e
        return s


class HyperliquidRawWithdrawalToL1ActionPayload(BaseModel):
    """
    Payload for withdrawing funds to L1.
    Corresponds to action type "withdraw".
    """

    token: str = Field(..., description="The symbol of the token to withdraw (e.g., USDC, ETH).")
    amount: str = Field(..., description="The string representation of the amount to withdraw.")
    destination: str = Field(..., description="The recipient's 0x address on L1.")
    # Note: Hyperliquid docs also mention "withdrawEth" type. If its payload differs,
    # a separate model or a union model might be needed.
    # For now, this model assumes "token" field covers ETH as well for a generic "withdraw" type.

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("token", mode="before")
    @classmethod
    def validate_token_str(cls, v: str, info: ValidationInfo) -> str:
        field_name = info.field_name or "token"
        # Basic validation: non-empty, reasonable length. Could be extended if there's a known set.
        return validate_str_field(v, field_name=field_name, max_length=24, allow_empty=False)

    @field_validator("destination", mode="before")
    @classmethod
    def validate_destination_address(cls, v: str, info: ValidationInfo) -> str:
        field_name = info.field_name or "destination"
        s = validate_str_field(v, field_name=field_name, max_length=42, allow_empty=False)
        if not s.startswith("0x") or len(s) != 42:
            raise ValueError(
                f"{field_name} '{s}' is not a valid 42-character address starting with 0x."
            )
        return s

    @field_validator("amount", mode="before")
    @classmethod
    def validate_amount_str(cls, v: str, info: ValidationInfo) -> str:
        from decimal import Decimal

        field_name = info.field_name or "amount"
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            d = parse_decimal_value(s, allow_none=False, field_name=field_name)
            if d is None or not d.is_finite() or d <= Decimal(0):
                raise ValueError(f"{field_name} must be a positive finite decimal string.")
        except ValueError as e:
            raise ValueError(
                f"{field_name} '{s}' is not a valid positive finite decimal string: {e}"
            ) from e
        return s


# If "withdrawEth" has a different structure, define HyperliquidRawEthWithdrawalActionPayload here.
# Example:
# class HyperliquidRawEthWithdrawalActionPayload(BaseModel):
#     amount: str
#     destination: str
#     model_config = ConfigDict(extra="forbid", frozen=True)
