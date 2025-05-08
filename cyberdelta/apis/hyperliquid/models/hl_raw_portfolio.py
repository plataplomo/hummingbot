"""
CyberDeltaEngine: Hyperliquid API Raw Models (Portfolio History)
----------------------------------------------------------------

Strict boundary validation models for the Hyperliquid 'portfolio' info endpoint.
Validates the raw structure only.
Never use for internal business logic.
"""

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
    def validate_history_entry_tuple_structure(cls, v: object, info: ValidationInfo) -> object:
        """Ensures input is a 2-element list/tuple. Pydantic handles element validation."""
        field_name = info.field_name or "history_entry_tuple"
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got {type(v).__name__}."
            )
        if len(v) != 2:
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got length {len(v)}."
            )
        return v


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
    def validate_portfolio_tuple_item_structure(cls, v: object, info: ValidationInfo) -> object:
        """Ensures input is a 2-element list/tuple. Pydantic handles element validation."""
        field_name = info.field_name or "portfolio_tuple_item"
        if not isinstance(v, (list, tuple)):
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got {type(v).__name__}."
            )
        if len(v) != 2:
            raise ValueError(
                f"Field '{field_name}': Expected 2-element list/tuple, got length {len(v)}."
            )
        # Element 0 (timeframe) will be validated by RawTimeframeString by Pydantic.
        # Element 1 (data_obj) needs to be a dict for HyperliquidRawPortfolioTimeframeData.
        # Pydantic will raise error if el1 is not dict when trying to parse HyperliquidRawPortfolioTimeframeData.
        # However, adding an explicit check here for el1 being a dict is a good pre-validation step.

        # Assign v[1] to a variable to potentially help linter with type inference for the error message.
        element_1_value = v[1]
        if not isinstance(element_1_value, dict):
            actual_type_name = type(element_1_value).__name__
            raise ValueError(
                f"Field '{field_name}', element 1: Expected data object to be a dictionary, got {actual_type_name}."
            )
        return v  # Return raw tuple/list for Pydantic to process elements


class HyperliquidRawPortfolioResponse(RootModel[list[HyperliquidRawPortfolioTupleItem]]):
    """
    Raw boundary model for the portfolio history response.
    The root object is a list of [timeframe_str, data_obj] tuples.
    """

    root: list[HyperliquidRawPortfolioTupleItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_list_structure(cls, v: object, info: ValidationInfo) -> object:
        """Ensures the root input is a list. Pydantic will handle item validation."""
        field_name = info.field_name or "portfolio_response_list"
        if not isinstance(v, list):
            raise ValueError(f"Field '{field_name}': Expected a list, got {type(v).__name__}.")

        return v
