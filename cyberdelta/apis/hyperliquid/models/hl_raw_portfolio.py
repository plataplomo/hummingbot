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


class HyperliquidRawPortfolioHistoryEntry(RootModel[tuple[int, str]]):
    """
    Raw boundary model for a single point in account value or PnL history.
    Represents the [timestamp, value_str] list structure.
    """

    root: tuple[int, str]

    @field_validator("root", mode="before")
    @classmethod
    def validate_history_entry_tuple(cls, v: object) -> tuple[int, str]:
        if not isinstance(v, (list, tuple)):
            raise ValueError("Expected 2-element list or tuple [timestamp, value_str]")
        if len(v) != 2:
            raise ValueError("Expected 2-element list or tuple [timestamp, value_str]")

        # Now we know v is a list or tuple of length 2
        timestamp_raw: Any = v[0]
        value_raw: Any = v[1]
        field_name_ts = "history_entry.timestamp"
        field_name_val = "history_entry.value"

        # Validate timestamp (int)
        if isinstance(timestamp_raw, str):
            try:
                timestamp = int(timestamp_raw)
            except ValueError:
                raise ValueError(f"{field_name_ts}: Expected int or int-like string") from None
        elif isinstance(timestamp_raw, int):
            timestamp = timestamp_raw
        else:
            raise ValueError(f"{field_name_ts}: Expected int or int-like string")
        if timestamp < 0:
            raise ValueError(f"{field_name_ts}: Timestamp cannot be negative")

        # Validate value (decimal string)
        value_str = validate_str_field(value_raw, field_name=field_name_val, max_length=64)
        d = parse_decimal_value(value_str, allow_none=False, field_name=field_name_val)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name_val}: Value must be a finite decimal string")

        return (timestamp, value_str)


class HyperliquidRawPortfolioTimeframeData(BaseModel):
    """Raw boundary model for portfolio data within a specific timeframe."""

    account_value_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(
        ..., alias="accountValueHistory"
    )
    pnl_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(..., alias="pnlHistory")
    vlm: str = Field(..., alias="vlm")  # Volume, decimal string

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)

    @field_validator("vlm", mode="before")
    @classmethod
    def validate_vlm_decimal_str(cls, v: object, info: ValidationInfo) -> str:
        field_name = info.field_name or "vlm"
        s = validate_str_field(v, field_name=field_name, max_length=64)
        d = parse_decimal_value(s, allow_none=False, field_name=field_name)
        if d is None or not d.is_finite():
            raise ValueError(f"{field_name}: Value must be a finite decimal")
        return s


class HyperliquidRawPortfolioTupleItem(RootModel[tuple[str, HyperliquidRawPortfolioTimeframeData]]):
    """Represents one item [timeframe_str, data_obj] in the main response list."""

    root: tuple[str, HyperliquidRawPortfolioTimeframeData]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_tuple(
        cls, v: object
    ) -> tuple[str, HyperliquidRawPortfolioTimeframeData]:
        if not isinstance(v, (list, tuple)):
            raise ValueError("Expected 2-element list or tuple [timeframe_str, data_obj]")
        if len(v) != 2:
            raise ValueError("Expected 2-element list or tuple [timeframe_str, data_obj]")

        timeframe_raw: Any = v[0]
        data_raw: Any = v[1]

        # Validate timeframe string
        timeframe = validate_str_field(timeframe_raw, field_name="timeframe", max_length=32)

        # Ensure data object is a dictionary before returning
        if not isinstance(data_raw, dict):
            raise ValueError("Expected data object (element 1) to be a dictionary")

        # Return the validated timeframe and the *raw* data dictionary.
        # Pydantic will then validate this dictionary against HyperliquidRawPortfolioTimeframeData.
        # Mypy doesn't know this, hence the ignore.
        return (timeframe, data_raw)  # type: ignore[return-value]


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
        # Basic check moved to HyperliquidRawPortfolioTupleItem validator
        # for item_idx, item in enumerate(v):
        #     if not isinstance(item, list) or len(item) != 2:
        #         raise ValueError(
        #             f"Item {item_idx}: Expected 2-element list, got {type(item).__name__}"
        #         )
        # Further validation happens in HyperliquidRawPortfolioTupleItem
        return v
