"""
Hyperliquid Raw User Fill Model
"""

import logging

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)

logger = logging.getLogger(__name__)


class HyperliquidRawFill(BaseModel):
    """Raw Pydantic model for a single fill record from the Hyperliquid userFills endpoint.

    Performs strict validation on all fields based on expected types and constraints.
    """

    tid: int = Field(..., description="Transaction ID", ge=0)
    oid: int = Field(..., description="Order ID", ge=0)
    coin: str = Field(..., description="Asset identifier", max_length=64)
    px: str = Field(..., description="Fill price", max_length=64)
    sz: str = Field(..., description="Fill size", max_length=64)
    start_position: str = Field(
        ..., alias="startPosition", description="Start position size", max_length=64
    )
    fee: str = Field(..., description="Fee paid", max_length=64)
    liquidation_mark_px: str | None = Field(
        None,
        alias="liquidationMarkPx",
        description="Liquidation mark price if applicable",
        max_length=64,
    )
    time: int = Field(..., description="Timestamp (milliseconds epoch)", ge=0)
    side: str = Field(..., description="Side ('B' for Buy, 'A' for Ask/Sell)")
    dir: str = Field(..., description="Direction description", max_length=64)
    hash: str = Field(..., description="Transaction hash", max_length=128)  # Assuming max length
    is_maker: bool = Field(..., alias="isMaker")
    cloid: str | None = Field(None, description="Client order ID if provided", max_length=128)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",
        frozen=True,
    )

    # --- Field Validators --- #

    @field_validator("tid", "oid", "time", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> object:
        """Validate field is a non-negative integer."""
        field_name = info.field_name or "integer_field"
        if isinstance(v, int):
            v_int = v
        elif isinstance(v, str) and v.isdigit():
            v_int = int(v)
        else:
            raise TypeError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v_int}")
        return v_int

    @field_validator("coin", "dir", "hash", mode="before")
    @classmethod
    def validate_required_string(cls, v: object, info: ValidationInfo) -> str:
        """Validate required, non-empty string fields with specific max_lengths."""
        field_name = info.field_name or "string_field"
        # Safely access max_length, provide default if metadata or max_length is missing
        max_len = getattr(
            getattr(cls.model_fields.get(field_name), "metadata", [None])[0], "max_length", 64
        )
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("px", "sz", "start_position", "fee", mode="before")
    @classmethod
    def validate_required_decimal_string(cls, v: object, info: ValidationInfo) -> object:
        """Validate required decimal strings are non-empty and represent finite decimals."""
        field_name = info.field_name or "decimal_field"
        v_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            dec_val = parse_decimal_value(v_str, allow_none=False, field_name=field_name)
            if dec_val is None or not dec_val.is_finite():
                raise ValueError("Must be a finite Decimal")
        except ValueError as e:
            raise ValueError(f"String '{v_str}' not parseable as finite Decimal: {e}") from e
        return v_str  # Return original valid string

    @field_validator("liquidation_mark_px", mode="before")
    @classmethod
    def validate_optional_decimal_string(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional decimal strings are non-empty if present and finite."""
        if v is None:
            return None
        field_name = info.field_name or "optional_decimal_field"
        v_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            dec_val = parse_decimal_value(v_str, allow_none=True, field_name=field_name)
            if dec_val is not None and not dec_val.is_finite():
                raise ValueError("Must be a finite Decimal if present")
        except ValueError as e:
            raise ValueError(f"String '{v_str}' not parseable as finite Decimal: {e}") from e
        return v_str  # Return original valid string or None

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        """Validate the 'side' field against allowed values ('B', 'A')."""
        return validate_enum_field(v, allowed={"B", "A"}, field_name="side")

    @field_validator("is_maker", mode="before")
    @classmethod
    def validate_is_maker(cls, v: object, info: ValidationInfo) -> bool:
        """Validate is_maker is a boolean."""
        if not isinstance(v, bool):
            raise TypeError(
                f"{info.field_name or 'is_maker'}: Must be a boolean, got {type(v).__name__}"
            )
        return v

    @field_validator("cloid", mode="before")
    @classmethod
    def validate_optional_string(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional cloid string: must be non-empty if present."""
        if v is None:
            return None
        field_name = info.field_name or "cloid"
        max_len = getattr(
            getattr(cls.model_fields.get(field_name), "metadata", [None])[0], "max_length", 128
        )
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)
