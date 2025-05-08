"""
CyberDeltaEngine: Hyperliquid API Raw Models (Portfolio History)
----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'portfolio' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from collections.abc import Sequence
from typing import cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    validate_and_parse_raw_non_negative_int,
    validate_and_return_finite_decimal_str,
)
from cyberdelta.utils.parsing import (
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
            raise ValueError(
                "History entry: Expected 2-element list or tuple [timestamp, value_str]"
            )

        # Cast to Sequence after type and basic structure check for type hinting
        v_seq = cast(Sequence[object], v)
        if len(v_seq) != 2:
            raise ValueError(
                "History entry: Expected 2-element list or tuple [timestamp, value_str]"
            )

        timestamp_raw: object = v_seq[0]
        value_raw: object = v_seq[1]

        timestamp = validate_and_parse_raw_non_negative_int(
            timestamp_raw, field_name="history_entry.timestamp"
        )
        value_str = validate_and_return_finite_decimal_str(
            value_raw, field_name="history_entry.value"
        )

        return (timestamp, value_str)


class HyperliquidRawPortfolioTimeframeData(BaseModel):
    """Raw boundary model for portfolio data within a specific timeframe."""

    account_value_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(
        ..., alias="accountValueHistory"
    )
    pnl_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(..., alias="pnlHistory")
    vlm: RawFiniteDecimalStr = Field(..., alias="vlm")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawPortfolioTupleItem(RootModel[tuple[str, HyperliquidRawPortfolioTimeframeData]]):
    """Represents one item [timeframe_str, data_obj] in the main response list."""

    root: tuple[str, HyperliquidRawPortfolioTimeframeData]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_tuple(cls, v: object) -> tuple[str, dict[str, object]]:
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                "Portfolio item: Expected 2-element list or tuple [timeframe_str, data_obj]"
            )

        v_seq = cast(Sequence[object], v)
        if len(v_seq) != 2:
            raise ValueError(
                "Portfolio item: Expected 2-element list or tuple [timeframe_str, data_obj]"
            )

        timeframe_raw: object = v_seq[0]
        data_raw: object = v_seq[1]

        timeframe = validate_str_field(
            timeframe_raw, field_name="timeframe", max_length=32, allow_empty=False
        )

        if not isinstance(data_raw, dict):
            raise ValueError("Portfolio item: Expected data object (element 1) to be a dictionary")

        return (timeframe, cast(dict[str, object], data_raw))


# Root model for the overall response which is a list of these tuples
class HyperliquidRawPortfolioResponse(RootModel[list[HyperliquidRawPortfolioTupleItem]]):
    """
    Raw boundary model for the portfolio history response.
    The root object is a list of [timeframe_str, data_obj] tuples.
    """

    root: list[HyperliquidRawPortfolioTupleItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_list(cls, v: object) -> list[object]:
        if not isinstance(v, list):
            raise ValueError("Portfolio response: Expected root object to be a list")
        return cast(list[object], v)
