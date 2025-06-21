"""Unit Tests for Hyperliquid Raw Portfolio Models."""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_portfolio import (
    HyperliquidRawPortfolioHistoryEntry,
    HyperliquidRawPortfolioResponse,
    HyperliquidRawPortfolioTimeframeData,
    HyperliquidRawPortfolioTupleItem,
)

# --- Test Data --- #

# For HyperliquidRawPortfolioHistoryEntry, API sends a list [timestamp, value].
# The model uses Field(alias=0) and Field(alias=1).
# If validating a dict, keys should be the aliases (0, 1) due to populate_by_name=True.
# If validating a list, Pydantic can map by order to fields if simple.
VALID_HISTORY_ENTRY_DICT: dict[int | str, Any] = {0: 1741886630493, 1: "0.0"}  # Populated by alias
VALID_HISTORY_ENTRY_LIST: list[Any] = [1741886630493, "0.0"]  # API like structure

VALID_TIMEFRAME_DATA: dict[str, Any] = {
    "accountValueHistory": [
        VALID_HISTORY_ENTRY_LIST,
        [1741895270493, "10.5"],
    ],  # Using list form here
    "pnlHistory": [[1741886630493, "-2.3"], [1741895270493, "0.0"]],
    "vlm": "12345.67",
}

VALID_PORTFOLIO_TUPLE_ITEM: list[Any] = ["day", VALID_TIMEFRAME_DATA]

VALID_PORTFOLIO_RESPONSE: list[list[Any]] = [
    VALID_PORTFOLIO_TUPLE_ITEM,
    ["week", VALID_TIMEFRAME_DATA.copy()],
]


# --- Fixtures --- #


@pytest.fixture
def valid_history_entry_dict_data() -> dict[int | str, Any]:
    """Return valid history entry dict data for testing."""
    return VALID_HISTORY_ENTRY_DICT.copy()


@pytest.fixture
def valid_history_entry_list_data() -> list[Any]:
    """Return valid history entry list data for testing."""
    return VALID_HISTORY_ENTRY_LIST[:]  # Use slicing for list copy


@pytest.fixture
def valid_timeframe_data() -> dict[str, Any]:
    """Return valid timeframe data for testing."""
    data = VALID_TIMEFRAME_DATA.copy()
    # accountValueHistory and pnlHistory items are lists, copy them properly
    data["accountValueHistory"] = [item[:] for item in data["accountValueHistory"]]
    data["pnlHistory"] = [item[:] for item in data["pnlHistory"]]
    return data


@pytest.fixture
def valid_portfolio_tuple_item_data() -> list[Any]:
    """Return valid portfolio tuple item data for testing."""
    item_copy = [VALID_PORTFOLIO_TUPLE_ITEM[0]]
    timeframe_data_original = VALID_PORTFOLIO_TUPLE_ITEM[1]
    timeframe_data_copy = timeframe_data_original.copy()  # shallow copy of the dict
    # Deep copy lists within timeframe_data_copy
    timeframe_data_copy["accountValueHistory"] = [
        h[:] for h in timeframe_data_original["accountValueHistory"]
    ]
    timeframe_data_copy["pnlHistory"] = [h[:] for h in timeframe_data_original["pnlHistory"]]
    item_copy.append(timeframe_data_copy)
    return item_copy


# --- Test Cases --- #


# HyperliquidRawPortfolioHistoryEntry Tests
def test_history_entry_valid_from_list(valid_history_entry_list_data: list[Any]) -> None:
    """Test history entry valid from list."""
    item = HyperliquidRawPortfolioHistoryEntry.model_validate(valid_history_entry_list_data)
    assert item.root[0] == valid_history_entry_list_data[0]
    assert item.root[1] == "0"  # Business logic normalizes "0.0" to "0"


def test_history_entry_valid_from_dict(valid_history_entry_dict_data: dict[int | str, Any]) -> None:
    """Test history entry valid from dict."""
    item = HyperliquidRawPortfolioHistoryEntry.model_validate(valid_history_entry_dict_data)
    assert item.root[0] == valid_history_entry_dict_data[0]
    assert item.root[1] == "0"  # Business logic normalizes "0.0" to "0"


@pytest.mark.parametrize(
    "value_list",
    [
        [None, "0.0"],
        [1741886630493, None],
        ["not-an-int", "0.0"],
        [1741886630493, "not-a-decimal"],
        [1741886630493, "Infinity"],
        [-100, "0.0"],
        [123],
        [123, "1.0", "extra"],
    ],
)
def test_history_entry_invalid_list_input(value_list: list[Any]) -> None:
    """Test history entry invalid list input."""
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioHistoryEntry.model_validate(value_list)


@pytest.mark.parametrize("key_alias, value", [(0, "not-an-int"), (1, "not-a-decimal"), (0, -123)])
def test_history_entry_invalid_dict_input(key_alias: int, value: str | int) -> None:
    """Test history entry invalid dict input."""
    data: dict[int | str, Any] = {0: 1741886630493, 1: "0.0"}
    data[key_alias] = value
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioHistoryEntry.model_validate(data)


def test_history_entry_extra_field_dict_input(
    valid_history_entry_dict_data: dict[int | str, Any],
) -> None:
    """Test history entry extra field dict input."""
    data_copy = valid_history_entry_dict_data.copy()
    data_copy[2] = "extra"  # Integer key due to aliases
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioHistoryEntry.model_validate(data_copy)


# HyperliquidRawPortfolioTimeframeData Tests
def test_timeframe_data_valid(valid_timeframe_data: dict[str, Any]) -> None:
    """Test timeframe data valid."""
    data = HyperliquidRawPortfolioTimeframeData.model_validate(valid_timeframe_data)
    assert len(data.account_value_history) == len(valid_timeframe_data["accountValueHistory"])
    assert (
        data.account_value_history[0].root[0] == valid_timeframe_data["accountValueHistory"][0][0]
    )
    assert len(data.pnl_history) == len(valid_timeframe_data["pnlHistory"])
    assert data.pnl_history[0].root[1] == valid_timeframe_data["pnlHistory"][0][1]
    assert data.vlm == valid_timeframe_data["vlm"]


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("accountValueHistory", None, True),
        ("accountValueHistory", [[123, "valid"], ["invalid-ts", "1.0"]], False),
        ("pnlHistory", "not-a-list", False),
        ("vlm", "not-a-decimal", False),
        ("vlm", None, True),
    ],
)
def test_timeframe_data_invalid(
    valid_timeframe_data: dict[str, Any],
    field: str,
    value: str | float | bool | list[Any] | None,  # Invalid types for Pydantic
    is_missing_test: bool,
) -> None:
    """Test timeframe data invalid."""
    data_copy = valid_timeframe_data.copy()
    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioTimeframeData.model_validate(data_copy)


def test_timeframe_data_extra_field(valid_timeframe_data: dict[str, Any]) -> None:
    """Test timeframe data extra field."""
    data_copy = valid_timeframe_data.copy()
    data_copy["extra"] = "field"
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioTimeframeData.model_validate(data_copy)


# HyperliquidRawPortfolioTupleItem Tests
def test_portfolio_tuple_item_valid(valid_portfolio_tuple_item_data: list[Any]) -> None:
    """Test portfolio tuple item valid."""
    item = HyperliquidRawPortfolioTupleItem.model_validate(valid_portfolio_tuple_item_data)
    assert item.root[0] == valid_portfolio_tuple_item_data[0]
    # Accessing nested data correctly based on fixture structure
    timeframe_data_dict = valid_portfolio_tuple_item_data[1]
    assert isinstance(timeframe_data_dict, dict)  # Ensure it's a dict as expected
    assert item.root[1].vlm == timeframe_data_dict["vlm"]


@pytest.mark.parametrize(
    "value_list",
    [
        ["day"],
        [123, VALID_TIMEFRAME_DATA],
        ["day", "not-a-dict"],
        ["day", {"vlm": "invalid"}],
        [],
        ["day", VALID_TIMEFRAME_DATA, "extra"],
    ],
)
def test_portfolio_tuple_item_invalid(value_list: list[Any]) -> None:
    """Test portfolio tuple item invalid."""
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioTupleItem.model_validate(value_list)


# HyperliquidRawPortfolioResponse (RootModel) Tests
def test_portfolio_response_valid() -> None:
    """Test portfolio response valid."""
    response = HyperliquidRawPortfolioResponse.model_validate(VALID_PORTFOLIO_RESPONSE)
    assert len(response.root) == len(VALID_PORTFOLIO_RESPONSE)
    assert response.root[0].root[0] == VALID_PORTFOLIO_RESPONSE[0][0]
    # Accessing nested data correctly
    timeframe_data_dict_in_response = VALID_PORTFOLIO_RESPONSE[0][1]
    assert isinstance(timeframe_data_dict_in_response, dict)
    assert response.root[0].root[1].vlm == timeframe_data_dict_in_response["vlm"]


@pytest.mark.parametrize(
    "invalid_root_data",
    [
        "not-a-list",
        [[["day", VALID_TIMEFRAME_DATA]]],
        [{"timeframe": "day", "data": VALID_TIMEFRAME_DATA}],
        [["day", {"vlm": "invalid-decimal"}]],
    ],
)
def test_portfolio_response_invalid(invalid_root_data: str | int | list[Any] | None) -> None:
    """Test portfolio response invalid."""
    with pytest.raises(ValidationError):
        HyperliquidRawPortfolioResponse.model_validate(invalid_root_data)


def test_portfolio_response_empty_list_valid() -> None:
    """Test portfolio response empty list valid."""
    response = HyperliquidRawPortfolioResponse.model_validate([])
    assert response.root == []
