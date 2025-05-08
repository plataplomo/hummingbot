"""
CyberDeltaEngine: Hyperliquid API Raw Models (Frontend Open Orders)
------------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'frontendOpenOrders' info endpoint.
Validates the raw structure only, enforcing type and format constraints.
Never use for internal business logic.
"""

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


class HyperliquidRawFrontendOpenOrder(BaseModel):
    """
    Raw boundary model for a single open order with frontend-specific fields.
    """

    coin: str = Field(..., alias="coin")
    is_position_tpsl: bool = Field(..., alias="isPositionTpsl")
    is_trigger: bool = Field(..., alias="isTrigger")
    limit_px: str = Field(..., alias="limitPx")
    oid: int = Field(..., alias="oid")
    order_type: str = Field(..., alias="orderType")  # e.g., "Limit", "Trigger"
    orig_sz: str = Field(..., alias="origSz")
    reduce_only: bool = Field(..., alias="reduceOnly")
    side: str = Field(..., alias="side")  # "A" or "B"
    sz: str = Field(..., alias="sz")  # Current remaining size
    timestamp: int = Field(..., alias="timestamp")
    trigger_condition: str = Field(..., alias="triggerCondition")  # e.g., "N/A", specific condition
    trigger_px: str = Field(..., alias="triggerPx")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    # --- Field Validators --- #

    @field_validator("coin", "order_type", "side", "trigger_condition", mode="before")
    @classmethod
    def validate_string_fields(cls, v: object, info: ValidationInfo) -> str:
        # Basic validation, specific enums could be added if needed
        field_name = info.field_name or "string_field"
        max_len = 64 if field_name != "trigger_condition" else 128  # Allow longer conditions
        s = validate_str_field(v, field_name=field_name, max_length=max_len)
        if field_name == "side" and s not in ("A", "B"):
            raise ValueError(f'{field_name}: Must be "A" or "B"')
        # Could add validation for order_type if known enum exists
        return s

    @field_validator("is_position_tpsl", "is_trigger", "reduce_only", mode="before")
    @classmethod
    def validate_boolean_fields(cls, v: object, info: ValidationInfo) -> bool:
        field_name = info.field_name or "bool_field"
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            if v.lower() == "true":
                return True
            if v.lower() == "false":
                return False
        raise ValueError(f"{field_name}: Expected boolean, got {type(v).__name__}")

    @field_validator("limit_px", "orig_sz", "sz", "trigger_px", mode="before")
    @classmethod
    def validate_decimal_string_fields(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "decimal_str_field"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        # Note: trigger_px can be "0.0" even if not active, finite check is sufficient
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal string")
        return s

    @field_validator("oid", "timestamp", mode="before")
    @classmethod
    def validate_integer_fields(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "int_field"
        # Allow string conversion for ints
        if isinstance(v, str):
            try:
                v_int = int(v)
            except ValueError:
                raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int):
            v_int = v
        else:
            raise ValueError(f"{field_name}: Expected int or int-like string")
        # Basic check: timestamps and OIDs should be non-negative
        if v_int < 0:
            raise ValueError(f"{field_name}: Must be non-negative")
        return v_int


# The overall response is a list of these objects
# No separate top-level response model needed unless the API wraps the list
