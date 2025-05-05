from typing import Any, Literal, cast

from pydantic import BaseModel, ConfigDict, Field, ValidationError, ValidationInfo, field_validator

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
    def validate_oid(cls, v: object) -> int:
        field_name = "oid"
        if not isinstance(v, int):
            if isinstance(v, str) and v.isdigit():
                v_int = int(v)
            else:
                # Use ValueError for Pydantic compatibility
                raise ValueError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        else:
            v_int = v
        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v_int}")
        return v_int


class HyperliquidRawExchangeStatusFilled(BaseModel):
    """Raw model for a 'filled' order status within an exchange response."""

    oid: int = Field(..., ge=0)  # Ensure non-negative
    total_sz: str = Field(..., alias="totalSz", max_length=64)
    avg_px: str = Field(..., alias="avgPx", max_length=64)
    # Potentially add fills list if present
    model_config = ConfigDict(populate_by_name=True, extra="ignore", frozen=True)

    @field_validator("oid", mode="before")
    @classmethod
    def validate_oid(cls, v: object) -> int:
        # Reusing validator logic
        field_name = "oid"
        if not isinstance(v, int):
            if isinstance(v, str) and v.isdigit():
                v_int = int(v)
            else:
                # Use ValueError for Pydantic compatibility
                raise ValueError(f"{field_name}: Must be an integer, got {type(v).__name__}")
        else:
            v_int = v
        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative, got {v_int}")
        return v_int

    @field_validator("total_sz", "avg_px", mode="before")
    @classmethod
    def validate_decimal_string_format(cls, v: object, info: ValidationInfo) -> str:
        """Validate required decimal strings."""
        field_name = info.field_name or "unknown_decimal_field"  # Provide default
        v_str = validate_str_field(v, field_name=field_name, max_length=64, allow_empty=False)
        try:
            dec_val = parse_decimal_value(v_str, allow_none=False, field_name=field_name)
            if dec_val is None or not dec_val.is_finite():
                raise ValueError(f"{field_name}: Value '{v_str}' must be a finite decimal.")
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"{field_name}: String '{v_str}' not parseable as finite Decimal: {e}"
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
    def validate_optional_error_string(cls, v: object | None) -> str | None:
        """Validate optional error string."""
        if v is None:
            return None
        return validate_str_field(v, field_name="error", max_length=1024, allow_empty=False)


class HyperliquidRawExchangeResponseData(BaseModel):
    """Raw model for the 'data' part of an exchange action response."""

    type: str = Field(..., max_length=32)  # Assuming max length
    # Statuses can be simple strings or complex objects
    statuses: list[
        Literal["canceled", "modified", "success"] | HyperliquidRawExchangeStatusObject
    ] = Field(...)
    model_config = ConfigDict(extra="ignore", frozen=True)

    @field_validator("type", mode="before")
    @classmethod
    def validate_type_string(cls, v: object) -> str:
        # Basic string validation. Could use validate_enum_field if known values emerge.
        return validate_str_field(v, field_name="type", max_length=32, allow_empty=False)

    @field_validator("statuses", mode="before")
    @classmethod
    def validate_statuses_list(
        cls, v: list[Any]
    ) -> list[Literal["canceled", "modified", "success"] | HyperliquidRawExchangeStatusObject]:
        """Validate the structure and content of the statuses list."""
        validated_list: list[
            Literal["canceled", "modified", "success"] | HyperliquidRawExchangeStatusObject
        ] = []
        allowed_strings = {"canceled", "modified", "success"}

        # Iterate through the raw list provided
        for i, item_raw in enumerate(v):
            item_raw: Any
            # DEFENSIVE CHECK: Runtime check needed (Pyright reportUnknownVariableType etc). Mypy=ok
            if isinstance(item_raw, str):
                # DEFENSIVE CHECK: Assert type for checker after isinstance
                assert isinstance(item_raw, str)
                try:
                    validated_str = validate_enum_field(
                        item_raw, allowed=allowed_strings, field_name=f"statuses[{i}]"
                    )
                    # Cast validated string to the Literal type for list compatibility
                    # Justification: We just validated it belongs to the allowed set.
                    validated_literal = cast(
                        Literal["canceled", "modified", "success"], validated_str
                    )
                    validated_list.append(validated_literal)
                except ValueError as e:
                    raise ValueError(f"statuses[{i}]: Invalid status string: {e}") from e
            # DEFENSIVE CHECK: Runtime check needed (Pyright reportUnknownVariableType etc). Mypy=ok
            elif isinstance(item_raw, dict):
                # DEFENSIVE CHECK: Assert type for checker after isinstance
                assert isinstance(item_raw, dict)
                try:
                    # Validate the dict as the nested Pydantic model
                    validated_obj = HyperliquidRawExchangeStatusObject.model_validate(item_raw)
                    validated_list.append(validated_obj)
                except ValidationError as e:
                    # Wrap Pydantic error for clarity
                    raise ValueError(
                        f"statuses[{i}]: Invalid status object format: {str(e)}"
                    ) from e
            else:
                # Handle unexpected types
                # DEFENSIVE CHECK: Runtime check needed (Pyright reportUnknownArgumentType). Mypy=ok
                # Use ValueError for Pydantic compatibility
                raise ValueError(
                    f"statuses[{i}]: Invalid type {type(item_raw).__name__}. Expected str/dict."
                )  # Shortened more

        return validated_list


class HyperliquidRawExchangeResponse(BaseModel):
    """Raw model for the top-level response from the /exchange endpoint."""

    status: Literal["ok"] = Field(...)
    data: HyperliquidRawExchangeResponseData | None = Field(None)
    # Sometimes 'data' might be missing or structured differently on error/simple success?
    # Making data optional and handling its absence might be safer.
    model_config = ConfigDict(extra="forbid", frozen=True)  # Set extra='forbid' and frozen=True

    # No explicit validator needed for 'status' due to Literal type hint
    # No explicit validator needed for 'data'; relies on nested model validation
    # However, we add a validator for 'status' to ensure consistency with raw policy
    @field_validator("status", mode="before")
    @classmethod
    def validate_status_ok(cls, v: object) -> str:
        """Validate the top-level status is exactly 'ok'."""
        return validate_enum_field(v, allowed={"ok"}, field_name="status")
