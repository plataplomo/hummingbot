from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import parse_decimal_value, validate_enum_field, validate_str_field

# ... (Existing models like HyperliquidRawOrder, HyperliquidRawFill remain) ...


# --- Exchange Action Response Models ---


class HyperliquidRawExchangeStatusResting(BaseModel):
    """Raw model for a 'resting' order status within an exchange response."""

    oid: int = Field(..., ge=0)  # Ensure non-negative
    # Add other fields observed in resting order status if needed
    # Example: remainingSz: str | None = None
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("oid", mode="before")
    @classmethod
    def validate_oid(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "oid"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v}")
        return v


class HyperliquidRawExchangeStatusFilled(BaseModel):
    """Raw model for a 'filled' order status within an exchange response."""

    oid: int = Field(..., ge=0)  # Ensure non-negative
    total_sz: str = Field(..., alias="totalSz", max_length=64)
    avg_px: str = Field(..., alias="avgPx", max_length=64)
    # Potentially add fills list if present
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    @field_validator("oid", mode="before")
    @classmethod
    def validate_oid(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "oid"
        if not isinstance(v, int):
            raise ValueError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        if v < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v}")
        return v

    @field_validator("total_sz", "avg_px", mode="before")
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "unknown_decimal_field"
        v_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            dec_val = parse_decimal_value(v_str, allow_none=False, field_name=field_name)
            if dec_val is None:
                raise ValueError(f"{field_name}: Parsing returned None unexpectedly.")
            if not dec_val.is_finite():
                raise ValueError(f"{field_name}: Value '{v_str}' must represent a finite decimal.")
        except ValueError as e:
            raise ValueError(
                f"{field_name}: Invalid finite decimal string '{v_str}'. Reason: {e}"
            ) from e
        return v_str


class HyperliquidRawExchangeStatusObject(BaseModel):
    """Raw model for complex status objects in exchange responses."""

    resting: HyperliquidRawExchangeStatusResting | None = Field(None)
    filled: HyperliquidRawExchangeStatusFilled | None = Field(None)
    error: str | None = Field(None, max_length=1024)  # Add max_length
    # Potentially add other status types like 'modified', 'canceled' if they appear as objects
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("error", mode="before")
    @classmethod
    def validate_optional_error_string(cls, v: object | None, info: ValidationInfo) -> str | None:
        if v is None:
            return None
        field_name = info.field_name or "error"
        # Revert to allow_empty=False to match test expectation
        return validate_str_field(v, field_name=field_name, max_length=1024, allow_empty=False)


class HyperliquidRawExchangeResponseData(BaseModel):
    """Raw model for the 'data' part of an exchange action response."""

    type: str = Field(..., max_length=32)
    # Corrected return type hint in validator below reflects the actual possible validated types
    statuses: list[str | HyperliquidRawExchangeStatusObject] = Field(...)
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_string(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "type"
        return validate_str_field(v, field_name=field_name, max_length=32, allow_empty=False)

    @field_validator("statuses", mode="before")
    @classmethod
    def validate_statuses_list(
        cls,
        v: object,
        info: ValidationInfo,  # Input must be object for raw validation
    ) -> list[str | HyperliquidRawExchangeStatusObject]:
        """
        Validate the 'statuses' list which can contain simple strings or complex status objects.
        Uses isinstance checks to determine the type of each item and validates accordingly,
        avoiding the use of `cast`.
        """
        field_name = info.field_name or "statuses"
        # DEFENSIVE CHECK: Ensure input is a list.
        if not isinstance(v, list):
            raise TypeError(f"{field_name}: Must be a list, got {type(v).__name__}.")

        # Proceed with iteration now that we know v is a list
        validated_list: list[str | HyperliquidRawExchangeStatusObject] = []
        allowed_strings: set[str] = {"canceled", "modified", "success"}

        # DEFENSIVE CHECK: v is list[Any], item_raw is Any.
        # Pyright=[reportUnknownArgumentType, reportUnknownVariableType]
        for i, item_raw in enumerate(v):
            current_field = f"{field_name}[{i}]"
            # DEFENSIVE CHECK: Runtime check needed for item_raw (Any).
            # Pyright=[reportUnknownArgumentType]
            if isinstance(item_raw, str):
                try:
                    # Use helper to validate against allowed string values
                    validated_str = validate_enum_field(
                        item_raw, allowed=allowed_strings, field_name=current_field
                    )
                    validated_list.append(validated_str)
                except ValueError as e:
                    # Re-raise with context
                    raise ValueError(
                        f"{current_field}: Invalid status string '{item_raw}'. {e}"
                    ) from e
            elif isinstance(item_raw, dict):
                try:
                    # Validate dict against the complex object model
                    validated_obj = HyperliquidRawExchangeStatusObject.model_validate(item_raw)
                    validated_list.append(validated_obj)
                except ValidationError as e:
                    # Re-raise with context
                    raise ValueError(
                        f"{current_field}: Invalid status object format. Errors: {e}"
                    ) from e
            else:
                # Reject any other type
                raise TypeError(
                    f"{current_field}: Invalid type {type(item_raw).__name__}. "
                    f"Expected str or dict."
                )

        return validated_list  # Return the rebuilt list


class HyperliquidRawExchangeResponse(BaseModel):
    """Raw model for the top-level response from the /exchange endpoint."""

    status: Literal["ok"] = Field(...)
    data: HyperliquidRawExchangeResponseData | None = Field(None)
    # Sometimes 'data' might be missing or structured differently on error/simple success?
    # Making data optional and handling its absence might be safer.
    model_config = ConfigDict(extra="forbid", frozen=True)  # Set extra='forbid' and frozen=True

    @field_validator("status", mode="before")
    @classmethod
    def validate_status_ok(cls, v: object, info: ValidationInfo) -> str:
        """Validate the top-level status is exactly 'ok'."""
        field_name = info.field_name or "status"
        return validate_enum_field(v, allowed={"ok"}, field_name=field_name)
