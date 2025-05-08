"""
CyberDeltaEngine: Hyperliquid API Raw Models (Portfolio History)
----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'portfolio' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import Any

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.utils.parsing import (
    parse_decimal_value,
    validate_str_field,
)


class HyperliquidRawPortfolioHistoryEntry(BaseModel):
    """Raw boundary model for a single point in account value or PnL history."""

    timestamp: int = Field(..., alias=0)  # Accessed by index in the list
    value: str = Field(..., alias=1)  # Decimal string, accessed by index

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("timestamp", mode="before")
    @classmethod
    def validate_timestamp_ms(cls, v: object, info: ValidationInfo) -> int:
        field_name = info.field_name or "timestamp"
        if isinstance(v, str):
            try:
                v_int = int(v)
            except ValueError:
                raise ValueError(f"{field_name}: Expected int or int-like string") from None
        elif isinstance(v, int):
            v_int = v
        else:
            raise ValueError(f"{field_name}: Expected int or int-like string")
        if v_int < 0:
            raise ValueError(f"{field_name}: Timestamp cannot be negative")
        return v_int

    @field_validator("value", mode="before")
    @classmethod
    def validate_value_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "value"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s


class HyperliquidRawPortfolioTimeframeData(BaseModel):
    """Raw boundary model for portfolio data within a specific timeframe."""

    account_value_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(
        ..., alias="accountValueHistory"
    )
    pnl_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(..., alias="pnlHistory")
    vlm: str = Field(..., alias="vlm")  # Volume, decimal string

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("account_value_history", "pnl_history", mode="before")
    @classmethod
    def validate_history_list(cls, v: object, info: ValidationInfo) -> list[Any]:
        field_name = info.field_name or "history_list"
        if not isinstance(v, list):
            raise ValueError(f"{field_name}: Expected list")
        # Check basic item structure (list of 2 items)
        for item_idx, item in enumerate(v):
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError(
                    f"{field_name}[{item_idx}]: Expected 2-element list, got {type(item).__name__}"
                )
        # Nested validation done by HyperliquidRawPortfolioHistoryEntry
        return v

    @field_validator("vlm", mode="before")
    @classmethod
    def validate_vlm_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "vlm"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s


class HyperliquidRawPortfolioTupleItem(BaseModel):
    """Represents one item [timeframe_str, data_obj] in the main response list."""

    timeframe: str = Field(..., alias=0)  # Accessed by index
    data: HyperliquidRawPortfolioTimeframeData = Field(..., alias=1)  # Accessed by index

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("timeframe", mode="before")
    @classmethod
    def validate_timeframe_str(cls, v: object, info: ValidationInfo) -> str:
        # e.g., "day", "week", "month", "allTime", "perpDay", ...
        return validate_str_field(v, field_name="timeframe", max_length=32)

    # Data validation happens in nested HyperliquidRawPortfolioTimeframeData


# Root model for the overall response which is a list of these tuples
class HyperliquidRawPortfolioResponse(RootModel[list[HyperliquidRawPortfolioTupleItem]]):
    """
    Raw boundary model for the portfolio history response.
    The root object is a list of [timeframe_str, data_obj] tuples.
    """

    root: list[HyperliquidRawPortfolioTupleItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_list(cls, v: object) -> list[Any]:
        if not isinstance(v, list):
            raise ValueError("Expected root object to be a list")
        # Basic check for item structure (list of 2 items)
        for item_idx, item in enumerate(v):
            if not isinstance(item, list) or len(item) != 2:
                raise ValueError(
                    f"Item {item_idx}: Expected 2-element list, got {type(item).__name__}"
                )
        # Further validation happens in HyperliquidRawPortfolioTupleItem
        return v
