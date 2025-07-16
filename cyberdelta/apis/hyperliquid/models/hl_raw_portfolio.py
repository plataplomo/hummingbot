"""CyberDeltaEngine: Hyperliquid API Raw Models (Portfolio History).

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

from cyberdelta.apis.base.validation_context_domain import DictMatchPolicy
from cyberdelta.apis.exceptions.parsing import (
    DictStructureError,
    SequenceLengthError,
    StructureTypeError,
)
from cyberdelta.apis.hyperliquid.models.hl_common_raw_types import (
    RawFiniteDecimalStr,
    RawNonNegativeFiniteDecimalStr,
    RawTimeframeString,
    RawTimestampMsInt,
)


# Portfolio data structure constants
PORTFOLIO_PAIR_COUNT = 2  # Expected count for portfolio data pairs


class HyperliquidRawPortfolioHistoryEntry(RootModel[tuple[RawTimestampMsInt, RawFiniteDecimalStr]]):
    """Raw boundary model for a single point in account value or PnL history.

    Represents the [timestamp, value_str] tuple structure, validated by Annotated types.
    """

    root: tuple[RawTimestampMsInt, RawFiniteDecimalStr]

    @field_validator("root", mode="before")
    @classmethod
    def validate_history_entry_tuple_structure(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> list[int | str]:  # Return list for Pydantic to map to tuple elements
        """Ensure input is a 2-element list/tuple or a dict {0: ts, 1: val}.

        Pydantic handles element validation against RawTimestampMsInt and RawFiniteDecimalStr.
        """
        field_name = info.field_name or "history_entry_tuple"

        if isinstance(v, dict):
            # Cast v_dict to have values that are Union[int, str] to match return type.
            # This assumes that the raw inputs for timestamp and value string are int or str.
            v_dict = cast("dict[int, int | str]", v)

            if 0 in v_dict and 1 in v_dict:
                if len(v_dict) == PORTFOLIO_PAIR_COUNT:  # Ensure only keys 0 and 1 are present
                    return [v_dict[0], v_dict[1]]
                # Handles cases like {0: val0, 1: val1, 2: val2}
                raise DictStructureError(
                    field_name=field_name,
                    expected_keys=[0, 1],
                    actual_keys=list(v_dict.keys()),
                    match_policy=DictMatchPolicy.EXACT_MATCH,
                )
            raise DictStructureError(
                field_name=field_name,
                expected_keys=[0, 1],
                actual_keys=list(v_dict.keys()),
                match_policy=DictMatchPolicy.CONTAINS_REQUIRED,
            )
        if isinstance(v, list | tuple):
            v_sequence = cast("list[object] | tuple[object, ...]", v)
            if len(v_sequence) != PORTFOLIO_PAIR_COUNT:
                raise SequenceLengthError(
                    field_name=field_name,
                    expected_length=PORTFOLIO_PAIR_COUNT,
                    actual_length=len(v_sequence),
                )
            # Ensure it's a list of [int | str] for Pydantic to process for the tuple
            # This involves casting elements from object to int | str.
            # JUSTIFICATION (RULE-NO-SILENCING-V4): Required to match explicit
            # return type list[int | str] mandated by user. Elements are originally
            # object from the input sequence; Pydantic will validate them further.
            # Runtime checks for actual types (int/str) are deferred to Pydantic.
            elem0 = cast("int | str", v_sequence[0])
            elem1 = cast("int | str", v_sequence[1])
            return [elem0, elem1]
        raise StructureTypeError(
            field_name=field_name,
            expected_structure="2-element list/tuple or dict {0: ts, 1: val}",
            actual_type=type(v).__name__,
        )


class HyperliquidRawPortfolioTimeframeData(BaseModel):
    """Raw boundary model for portfolio data within a specific timeframe."""

    account_value_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(
        ...,
        alias="accountValueHistory",
    )
    pnl_history: list[HyperliquidRawPortfolioHistoryEntry] = Field(..., alias="pnlHistory")
    vlm: RawNonNegativeFiniteDecimalStr = Field(..., alias="vlm")

    model_config = ConfigDict(populate_by_name=True, extra="forbid", frozen=True)


class HyperliquidRawPortfolioTupleItem(
    RootModel[tuple[RawTimeframeString, HyperliquidRawPortfolioTimeframeData]],
):
    """Represents one item [timeframe_str, data_obj] in the main response list."""

    root: tuple[RawTimeframeString, HyperliquidRawPortfolioTimeframeData]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_tuple_item_structure(
        cls,
        v: object,
        info: ValidationInfo,
    ) -> tuple[object, dict[str, object]] | list[object]:
        """Ensure input is a 2-element list/tuple.

        Pydantic handles element validation.
        """
        field_name = info.field_name or "portfolio_tuple_item"
        if not isinstance(v, list | tuple):
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="2-element list/tuple",
                actual_type=type(v).__name__,
            )

        v_casted = cast("list[object] | tuple[object, ...]", v)

        if len(v_casted) != PORTFOLIO_PAIR_COUNT:
            raise SequenceLengthError(
                field_name=field_name,
                expected_length=PORTFOLIO_PAIR_COUNT,
                actual_length=len(v_casted),
            )

        element_0_value = v_casted[0]
        element_1_value = v_casted[1]

        if not isinstance(element_1_value, dict):
            actual_type_name = type(element_1_value).__name__
            raise StructureTypeError(
                field_name=field_name,
                expected_structure="data object to be a dictionary",
                actual_type=actual_type_name,
                element_info="element 1",
            )

        element_1_dict = cast("dict[str, object]", element_1_value)

        if isinstance(v_casted, tuple):
            return (element_0_value, element_1_dict)
        return [element_0_value, element_1_dict]


class HyperliquidRawPortfolioResponse(RootModel[list[HyperliquidRawPortfolioTupleItem]]):
    """Raw boundary model for the portfolio history response.

    The root object is a list of [timeframe_str, data_obj] tuples.
    """

    root: list[HyperliquidRawPortfolioTupleItem]

    @field_validator("root", mode="before")
    @classmethod
    def validate_portfolio_list_structure(cls, v: object, info: ValidationInfo) -> list[object]:
        """Ensure the root input is a list.

        Pydantic will handle item validation.
        """
        field_name = info.field_name or "portfolio_response_list"
        if not isinstance(v, list):
            raise StructureTypeError(
                field_name=field_name, expected_structure="list", actual_type=type(v).__name__
            )

        return cast("list[object]", v)
