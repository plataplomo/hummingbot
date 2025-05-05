"""
CyberDeltaEngine: Backpack API Raw Models (User Fills)
-------------------------------------------------------

Strict Pydantic models for validating the *raw* structure of Backpack Exchange API responses
related to user fills (trades). Adheres to the Raw Model Policy.
"""

from datetime import UTC, datetime

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_enum_field,
    validate_str_field,
)


# --- Core Backpack Fill Model ---
class BackpackRawFill(BaseModel):
    """
    Strict boundary model for a user fill (trade) object from Backpack API.

    Validates structure, types, and basic formats (non-empty, length, numeric format).
    Rejects extra fields. Immutable.
    """

    fee: str = Field(...)
    fee_symbol: str = Field(..., alias="feeSymbol", max_length=32)
    is_maker: bool = Field(..., alias="isMaker")
    order_id: str = Field(..., alias="orderId", max_length=128)
    price: str = Field(...)
    quantity: str = Field(...)
    side: str = Field(..., max_length=3)  # 'Bid' or 'Ask'
    symbol: str = Field(..., max_length=64)
    timestamp: str = Field(...)  # ISO Format: "YYYY-MM-DDTHH:MM:SS.ffffffZ"
    trade_id: int = Field(..., alias="tradeId", ge=0)
    client_id: str | None = Field(None, alias="clientId", max_length=128)

    model_config = ConfigDict(
        populate_by_name=True,
        extra="forbid",  # Strict: No extra fields allowed
        frozen=True,  # Immutable
    )

    # --- Field Validators ---

    @field_validator("fee", "price", "quantity", mode="before")
    @classmethod
    def validate_required_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate required, non-empty, finite decimal strings (max_length=64)."""
        field_name = info.field_name or "unknown_decimal_field"
        # Reuse parsing utils for consistency
        s = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        # Ensure finite
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value '{s}' must be a finite decimal.")
        return s

    @field_validator("fee_symbol", "order_id", "symbol", mode="before")
    @classmethod
    def validate_required_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate required, non-empty strings with specific max lengths."""
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is required for validation metadata.")
        # Fetch max_length from Field definition if possible, fallback otherwise
        model_field = cls.model_fields.get(field_name)
        max_len = getattr(
            getattr(model_field, "metadata", [None])[0],
            "max_length",
            64,  # Default fallback
        )
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("client_id", mode="before")
    @classmethod
    def validate_optional_str(cls, v: object | None, info: ValidationInfo) -> str | None:
        """Validate optional, non-empty strings with specific max lengths."""
        if v is None:
            return None
        field_name = info.field_name
        if field_name is None:
            raise ValueError("Field name is required for validation metadata.")
        # Fetch max_length from Field definition
        model_field = cls.model_fields.get(field_name)
        max_len = getattr(
            getattr(model_field, "metadata", [None])[0],
            "max_length",
            128,
        )
        # Allow empty is False here because if present, it shouldn't be empty
        return validate_str_field(v, field_name=field_name, max_length=max_len, allow_empty=False)

    @field_validator("is_maker", mode="before")
    @classmethod
    def validate_bool(cls, v: object, info: ValidationInfo) -> bool:
        """Validate boolean field."""
        field_name = info.field_name or "unknown_bool_field"
        if not isinstance(v, bool):
            # Use ValueError for Pydantic compatibility
            raise ValueError(f"{field_name}: Must be a boolean, got {type(v).__name__}")
        return v

    @field_validator("side", mode="before")
    @classmethod
    def validate_side_enum(cls, v: object, info: ValidationInfo) -> str:
        """Validate side enum ('Bid' or 'Ask')."""
        field_name = info.field_name or "unknown_enum_field"
        return validate_enum_field(v, allowed={"Bid", "Ask"}, field_name=field_name)

    @field_validator("trade_id", mode="before")
    @classmethod
    def validate_non_negative_int(cls, v: object, info: ValidationInfo) -> int:
        """Validate required non-negative integer fields."""
        field_name = info.field_name or "unknown_int_field"
        if not isinstance(v, int):
            # Allow numeric strings if they represent valid integers
            if isinstance(v, str) and v.isdigit():
                v_int = int(v)
            else:
                raise TypeError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        else:
            v_int = v

        # Check non-negativity using the Field constraint (ge=0) if possible,
        # otherwise explicitly check here.
        min_val = 0
        if info.field_name:
            model_field = cls.model_fields.get(info.field_name)
            if model_field:
                min_val = getattr(
                    getattr(model_field, "metadata", [None])[0],
                    "ge",
                    0,
                )

        if v_int < min_val:
            raise ValueError(f"{field_name}: Must be >= {min_val}, got {v_int}")
        return v_int

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_iso_timestamp_str(cls, v: object, info: ValidationInfo) -> str:
        """Validate the timestamp string is ISO format and parseable to UTC datetime."""
        field_name = info.field_name or "timestamp"
        # First, basic string validation
        ts_str = validate_str_field(v, field_name=field_name, allow_empty=False, max_length=30)
        # Now, attempt parsing
        try:
            # Replace 'Z' with '+00:00' for standard fromisoformat
            if ts_str.endswith("Z"):
                ts_str_iso = ts_str[:-1] + "+00:00"
            else:
                # Assume it might already have timezone offset or be naive
                ts_str_iso = ts_str

            dt_obj = datetime.fromisoformat(ts_str_iso)
            # Ensure it represents a valid UTC time, convert if necessary
            offset = dt_obj.utcoffset()
            if offset is None:
                # Naive datetime, assume UTC and convert to aware
                dt_utc = dt_obj.replace(tzinfo=UTC)
            elif offset.total_seconds() != 0:
                # Aware datetime, but not UTC, convert
                dt_utc = dt_obj.astimezone(UTC)
            else:
                # Already UTC aware
                dt_utc = dt_obj

            _ = dt_utc
            # We only need to validate the format here, return original valid string
            return ts_str
        except ValueError as e:
            raise ValueError(
                f"{field_name}: Invalid ISO timestamp format '{ts_str}'. Error: {e}"
            ) from e
        except Exception as e:
            raise ValueError(
                f"{field_name}: Failed to parse timestamp string '{ts_str}'. Error: {e}"
            ) from e


# Add __init__.py exports if needed later
