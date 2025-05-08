"""
CyberDeltaEngine: Hyperliquid API Raw Models (Portfolio History)
----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'portfolio' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

from typing import cast

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    RootModel,
    ValidationInfo,
    field_validator,
)

from cyberdelta.apis.hyperliquid.models.common_raw_types import (
    RawFiniteDecimalStr,
    RawNonNegativeFiniteDecimalStr,
    RawTimeframeString,
    RawTimestampMsInt,
)


class HyperliquidRawPortfolioHistoryEntry(RootModel[tuple[RawTimestampMsInt, RawFiniteDecimalStr]]):
    """
    Raw boundary model for a single point in account value or PnL history.
    Represents the [timestamp, value_str] tuple structure, validated by Annotated types.
    """

    root: tuple[RawTimestampMsInt, RawFiniteDecimalStr]

    @field_validator("root", mode="before")
    @classmethod
    def validate_history_entry_tuple_structure(
        cls, v: object, info: ValidationInfo
    ) -> tuple[object, object] | list[object]:
        """Ensures input is a 2-element list/tuple. Pydantic handles element validation."""
        field_name = info.field_name or "history_entry_tuple"
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got {type(v).__name__}."
            )

        v_casted = cast(list[object] | tuple[object, ...], v)

        if len(v_casted) != 2:
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got length {len(v_casted)}."
            )
        if isinstance(v_casted, tuple):
            return v_casted  # v_casted is tuple[object, object] as per linter
        return v_casted  # v_casted is list[object] as per linter


class HyperliquidRawPortfolioTimeframeData(BaseModel):
    """Raw boundary model for portfolio data within a specific timeframe."""

    account_value_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(
        ..., alias="accountValueHistory"
    )
    pnl_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(..., alias="pnlHistory")
    vlm: RawNonNegativeFiniteDecimalStr = Field(..., alias="vlm")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawPortfolioTupleItem(
    RootModel[tuple[RawTimeframeString, HyperliquidRawPortfolioTimeframeData]]
):
    """Represents one item [timeframe_str, data_obj] in the main response list."""

    root: tuple[RawTimeframeString, HyperliquidRawPortfolioTimeframeData]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_tuple_item_structure(
        cls, v: object, info: ValidationInfo
    ) -> tuple[object, dict[str, object]] | list[object]:
        """Ensures input is a 2-element list/tuple. Pydantic handles element validation."""
        field_name = info.field_name or "portfolio_tuple_item"
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got {type(v).__name__}."
            )

        v_casted = cast(list[object] | tuple[object, ...], v)

        if len(v_casted) != 2:
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got length {len(v_casted)}."
            )

        element_0_value = v_casted[0]
        element_1_value = v_casted[1]

        if not isinstance(element_1_value, dict):
            actual_type_name = type(element_1_value).__name__
            raise ValueError(
                f"Field '{field_name}', element 1: Expected data object to be a dictionary, got {actual_type_name}."
            )

        element_1_dict = cast(dict[str, object], element_1_value)

        if isinstance(v_casted, tuple):
            return (element_0_value, element_1_dict)
        return [element_0_value, element_1_dict]


class HyperliquidRawPortfolioResponse(RootModel[list[HyperliquidRawPortfolioTupleItem]]):
    """
    Raw boundary model for the portfolio history response.
    The root object is a list of [timeframe_str, data_obj] tuples.
    """

    root: list[HyperliquidRawPortfolioTupleItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_list_structure(cls, v: object, info: ValidationInfo) -> list[object]:
        """Ensures the root input is a list. Pydantic will handle item validation."""
        field_name = info.field_name or "portfolio_response_list"
        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        return cast(list[object], v)
