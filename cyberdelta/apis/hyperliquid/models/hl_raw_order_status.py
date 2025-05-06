from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import validate_str_field


class HyperliquidRawOrderStatusRequestPayload(BaseModel):
    """
    Request payload for the 'orderStatus' info type.
    """

    type: Literal["orderStatus"] = Field("orderStatus")
    user: str = Field(..., min_length=42, max_length=42)  # Assuming Ethereum address
    oid: int = Field(..., ge=0)

    model_config = ConfigDict(extra="forbid", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_literal(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type"
        s = validate_str_field(v, field_name=field_name, max_length=32)
        if s != "orderStatus":
            raise ValueError(f"{field_name} must be 'orderStatus', got '{s}'")
        return s

    @field_validator("user", mode="before")
    @classmethod
    def validate_user_address(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "user"
        s = validate_str_field(v, field_name=field_name, max_length=42, allow_empty=False)
        if not s.startswith("0x") or len(s) != 42:
            raise ValueError(
                f"{field_name} '{s}' is not a valid 42-character address starting with 0x."
            )
        return s

    @field_validator("oid")
    @classmethod
    def validate_oid(cls, value: int) -> int:
        if value < 0:
            raise ValueError("Order ID (oid) must be non-negative.")
        return value
