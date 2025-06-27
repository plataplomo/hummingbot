"""Unit tests for Hyperliquid Raw Exchange Action Models."""

from __future__ import annotations

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
    HyperliquidRawOrderItemSpec,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)


# --- Test Data ---

VALID_ETH_ADDRESS = "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B"
INVALID_ETH_ADDRESS_SHORT = "0x123"
INVALID_ETH_ADDRESS_NOHEX = "Ab5801a7D398351b8bE11C439e05C5B3259aeC9B"
INVALID_ETH_ADDRESS_NONHEXCHARS = "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9X"  # X is invalid

VALID_DECIMAL_STR = "123.456"
VALID_POSITIVE_DECIMAL_STR = "10.5"
INVALID_DECIMAL_STR_NON_FINITE = "Infinity"
INVALID_DECIMAL_STR_EMPTY = ""
INVALID_DECIMAL_STR_NEGATIVE = "-1.0"

VALID_LIMIT_ORDER_TYPE_DETAILS_GTC = {"limit": {"tif": "Gtc"}}
VALID_MARKET_ORDER_TYPE_DETAILS: dict[str, dict[str, Any]] = {"market": {}}


# --- Helper Functions ---


def set_nested_value(
    data_dict: dict[str, Any],
    path: tuple[str | int, ...],
    value: object,  # Accept any test value for Pydantic validation testing
) -> None:
    """Set nested values in dict/list structures for testing.

    This function dynamically traverses nested dict/list structures using mixed
    str/int path elements. The runtime isinstance checks ensure type safety
    for dynamic traversal and assignment.
    """
    current_level: Any = data_dict  # Start as Any, narrow through type guards

    for i, key_or_index in enumerate(path):
        is_final_element = i == len(path) - 1

        # Handle string keys (for dicts)
        if isinstance(key_or_index, str):
            # DEFENSIVE CHECK: Ensure current_level is a dict before string key access
            if not isinstance(current_level, dict):
                raise TypeError(
                    f"Path element '{key_or_index}' requires a dictionary at this level, "
                    f"but found {type(current_level).__name__} at path {path[: i + 1]}",
                )
            # After isinstance check, explicitly cast for Pyright
            current_dict: dict[str, Any] = cast("dict[str, Any]", current_level)

            if is_final_element:
                # Final element: set the value (cast for test compatibility)
                current_dict[key_or_index] = cast("Any", value)
                return
            # Traversal: get next level and validate it's a container
            next_level_val: Any = current_dict[key_or_index]
            # DEFENSIVE CHECK: Ensure we can traverse into next_level_val
            if not isinstance(next_level_val, dict | list):
                raise TypeError(
                    f"Cannot traverse non-container type {type(next_level_val).__name__} "
                    f"at path {path[: i + 1]}",
                )
            current_level = cast("dict[str, Any] | list[Any]", next_level_val)

        # Handle integer indices (for lists)
        # DEFENSIVE CHECK: isinstance needed to distinguish int from str in Union.
        elif isinstance(key_or_index, int):  # pyright: ignore[reportUnnecessaryIsInstance]
            # DEFENSIVE CHECK: Ensure current_level is a list before int index access
            if not isinstance(current_level, list):
                raise TypeError(
                    f"Path index {key_or_index} requires a list at this level, "
                    f"but found {type(current_level).__name__} at path {path[: i + 1]}",
                )
            # After isinstance check, pyright understands the type
            # After isinstance check, we know it's a list
            current_list: list[Any] = current_level  # pyright: ignore[reportUnknownVariableType]

            if is_final_element:
                # Final element: set the value (cast for test compatibility)
                current_list[key_or_index] = cast("Any", value)
                return
            # Traversal: get next level and validate it's a container
            next_level_list_val: Any = current_list[key_or_index]
            # DEFENSIVE CHECK: Ensure we can traverse into next_level_list_val
            if not isinstance(next_level_list_val, dict | list):
                raise TypeError(
                    f"Cannot traverse non-container type {type(next_level_list_val).__name__} "
                    f"at path {path[: i + 1]}",
                )
            current_level = cast("dict[str, Any] | list[Any]", next_level_list_val)
        else:
            # This should be unreachable given path type annotation
            raise TypeError(f"Path element must be str or int, got {type(key_or_index).__name__}")


# --- HyperliquidRawEthWithdrawalActionPayload Tests ---


def test_eth_withdrawal_payload_valid() -> None:
    """Test eth withdrawal payload valid."""
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS}
    payload = HyperliquidRawEthWithdrawalActionPayload.model_validate(data)
    assert payload.amount == VALID_DECIMAL_STR
    # Business logic normalizes ETH addresses to lowercase
    assert payload.destination == VALID_ETH_ADDRESS.lower()
    assert payload.model_config.get("extra") == "forbid"
    assert payload.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field, value, expected_error_part",
    [
        ("amount", INVALID_DECIMAL_STR_NON_FINITE, "must be a parseable finite decimal string"),
        ("amount", INVALID_DECIMAL_STR_EMPTY, "String cannot be empty"),
        ("amount", None, "Field required"),
        ("destination", INVALID_ETH_ADDRESS_SHORT, "Must be exactly 42 characters long"),
        ("destination", INVALID_ETH_ADDRESS_NOHEX, "Must start with '0x'"),
        (
            "destination",
            INVALID_ETH_ADDRESS_NONHEXCHARS,
            "must be a valid 0x-prefixed hexadecimal string",
        ),
        ("destination", None, "Field required"),
    ],
)
def test_eth_withdrawal_payload_invalid_fields(
    field: str,
    value: str | None,
    expected_error_part: str,
) -> None:
    """Test eth withdrawal payload invalid fields."""
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS}
    if value is None:
        del data[field]
    else:
        data[field] = value
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawEthWithdrawalActionPayload.model_validate(data)
    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_eth_withdrawal_payload_extra_field() -> None:
    """Test eth withdrawal payload extra field."""
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS, "extra": "field"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawEthWithdrawalActionPayload.model_validate(data)


# --- HyperliquidRawOrderItemSpec Tests ---


def test_order_item_spec_valid_limit() -> None:
    """Test order item spec valid limit."""
    data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
        "client_order_id": "0x" + "0" * 30 + "a" * 2,  # Valid 128-bit hex string
    }
    item_spec = HyperliquidRawOrderItemSpec.model_validate(data)
    assert item_spec.a == 0
    assert item_spec.b is True
    assert item_spec.p == VALID_DECIMAL_STR
    assert item_spec.s == "1"
    assert item_spec.r is False
    # Test that order type is properly deserialized as HyperliquidRawOrderType
    assert item_spec.t.limit is not None
    assert item_spec.t.limit.tif == "Gtc"
    assert item_spec.c == "0x" + "0" * 30 + "a" * 2
    assert item_spec.model_config.get("extra") == "forbid"
    assert item_spec.model_config.get("frozen") is True
    assert item_spec.model_config.get("populate_by_name") is True


def test_order_item_spec_valid_market_no_cloid() -> None:
    """Test order item spec valid market no cloid."""
    data = {
        "asset_index": 1,
        "is_buy": False,
        "limit_px": "0",  # Market orders use "0" for limit_px
        "size": "0.5",
        "reduce_only": True,
        "order_type_details": VALID_MARKET_ORDER_TYPE_DETAILS,
    }
    item_spec = HyperliquidRawOrderItemSpec.model_validate(data)
    assert item_spec.a == 1
    assert item_spec.b is False
    assert item_spec.p == "0"
    assert item_spec.s == "0.5"
    assert item_spec.r is True
    # Test that order type is properly deserialized as HyperliquidRawOrderType
    assert item_spec.t.market is not None
    assert item_spec.c is None


@pytest.mark.parametrize(
    "field_alias, value, expected_error_part",
    [
        ("asset_index", -1, "cannot be negative"),
        ("asset_index", "not-an-int", "Must be an integer"),
        ("is_buy", "not-a-bool", "Must be a boolean"),
        ("is_buy", None, "Field required"),
        ("limit_px", INVALID_DECIMAL_STR_NON_FINITE, "finite decimal string"),
        ("limit_px", None, "Field required"),
        ("size", INVALID_DECIMAL_STR_EMPTY, "String cannot be empty"),
        ("size", None, "Field required"),
        ("reduce_only", "True", "Must be a boolean"),  # String "True" is not bool True
        ("reduce_only", None, "Field required"),
        (
            "order_type_details",
            {"limit": {"tif": "InvalidTIF"}},
            "value error, tif: invalid value",
        ),
        # Removed test for both None - raw models no longer validate business logic
        ("order_type_details", None, "Field required"),
        ("client_order_id", "", "Must start with '0x' prefix"),  # Empty string fails validation
        (
            "client_order_id",
            "0x" + "a" * 33,
            "Must be exactly 34 characters",
        ),  # Too long hex string (35 total chars)
    ],
)
def test_order_item_spec_invalid_fields(
    field_alias: str,
    value: object,
    expected_error_part: str,
) -> None:
    """Test order item spec invalid fields."""
    base_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
        "client_order_id": "0x" + "0" * 30 + "b" * 2,  # Valid 128-bit hex string
    }
    if value is None and field_alias in base_data:  # Test missing required field
        del base_data[field_alias]
    else:
        base_data[field_alias] = cast("Any", value)  # Cast for test compatibility

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderItemSpec.model_validate(base_data)
    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_order_item_spec_extra_field() -> None:
    """Test order item spec extra field."""
    data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
        "extra_field": "value",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawOrderItemSpec.model_validate(data)


# --- HyperliquidRawBatchPlaceOrderActionPayload Tests (Model removed - skipping) ---
"""


def test_batch_place_order_payload_valid() -> None:
    Test batch place order payload valid.
    order_item_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
    }
    data = {
        "type": "order",
        "grouping": "na",
        "orders": [order_item_data],
    }
    payload = HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
    assert payload.type == "order"
    assert payload.grouping == "na"
    assert len(payload.orders) == 1
    assert isinstance(payload.orders[0], HyperliquidRawOrderItemSpec)
    assert payload.model_config.get("extra") == "forbid"
    assert payload.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field_path, value, expected_error_part",
    [
        # Test invalid type for asset_index
        (("orders", 0, "asset_index"), "not-an-int", "Must be an integer"),
        # Test invalid type for is_buy
        (("orders", 0, "is_buy"), "not-a-bool", "Must be a boolean"),
        # Test invalid format for limit_px (not a string)
        (("orders", 0, "limit_px"), 123.45, "Expected string, got float"),
        # FIXME: limitPx uses RawFiniteDecimalStr, which allows negative values.
        # This test expects non-negative, which is incorrect for this raw type.
        # (
        #     ("orders", 0, "limit_px"),
        #     INVALID_DECIMAL_STR_NEGATIVE,
        #     "must be non-negative",
        # ),
        # Test invalid format for sz (not parseable to decimal)
        (("orders", 0, "size"), "not-a-decimal", "Cannot convert 'not-a-decimal' to Decimal"),
        # FIXME: sz uses RawFiniteDecimalStr, which allows negative values.
        # This test expects non-negative, which is incorrect for this raw type.
        # (
        #     ("orders", 0, "size"),
        #     INVALID_DECIMAL_STR_NEGATIVE,
        #     "must be non-negative",
        # ),
        # Test invalid type for reduce_only
        (("orders", 0, "reduce_only"), "not-a-bool", "Must be a boolean"),
        # Removed test for invalid order type structure - raw models no longer validate
        # business logic
        # Test invalid tif value within limit order_type
        (
            ("orders", 0, "order_type_details", "limit", "tif"),
            "InvalidTif",
            "Invalid value 'InvalidTif'. Expected one of",
        ),
    ],
)
def test_batch_place_order_payload_invalid_fields(
    field_path: tuple[str | int, ...],
    value: object,
    expected_error_part: str,
) -> None:
    Test batch place order payload invalid fields.
    # Base valid data structure for a batch order item
    # Note: The model HyperliquidRawOrderItemSpec expects `order_type_details`
    # (alias for field `t`) as the JSON key for order type information.
    base_order_item_data: dict[str, Any] = {
        "asset_index": 0,  # Using alias directly for test data setup simplicity
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,  # Correct alias for field 't'
    }

    base_batch_data: dict[str, Any] = {
        "type": "order",
        "grouping": "na",
        "orders": [base_order_item_data.copy()],  # Start with one valid order
    }

    # Apply the invalid value at the specified path
    modified_batch_data = base_batch_data.copy()
    # Ensure orders list exists and has an item if path targets it
    if field_path[0] == "orders" and isinstance(field_path[1], int):
        while len(modified_batch_data["orders"]) <= field_path[1]:
            modified_batch_data["orders"].append(base_order_item_data.copy())

    set_nested_value(modified_batch_data, field_path, value)

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawBatchPlaceOrderActionPayload.model_validate(modified_batch_data)

    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_batch_place_order_payload_orders_empty_list_valid() -> None:
    Test batch place order payload orders empty list valid.
    data: dict[str, str | list[dict[str, Any]]] = {"type": "order", "grouping": "na", "orders": []}
    payload = HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
    assert payload.orders == []


def test_batch_place_order_payload_extra_field() -> None:
    Test batch place order payload extra field.
    data: dict[str, str | list[dict[str, Any]] | Any] = {
        "type": "order",
        "grouping": "na",
        "orders": [],
        "extra_field": "value",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawBatchPlaceOrderActionPayload.model_validate(data)
"""  # End of commented out batch tests

# --- HyperliquidRawL2UsdTransferActionDetails Tests (New) ---


def test_l2_usd_transfer_action_details_valid() -> None:
    """Test l2 usd transfer action details valid."""
    payload_data = {
        "destination": VALID_ETH_ADDRESS,
        "token": "USDC",
        "amount": VALID_POSITIVE_DECIMAL_STR,
    }
    data = {"chain": "L2", "payload": payload_data}
    action_details = HyperliquidRawL2UsdTransferActionDetails.model_validate(data)
    assert action_details.chain == "L2"
    assert isinstance(action_details.payload, HyperliquidRawL2UsdTransferPayload)
    assert action_details.payload.destination == VALID_ETH_ADDRESS.lower()
    assert action_details.payload.token == "USDC"
    assert action_details.payload.amount == VALID_POSITIVE_DECIMAL_STR
    assert action_details.model_config.get("extra") == "forbid"
    assert action_details.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field, value, expected_error_part",
    [
        ("chain", "L1", "Input should be 'L2'"),
        ("chain", None, "Field required"),
        ("payload", None, "Field required"),
        (
            "payload",
            {"destination": VALID_ETH_ADDRESS, "token": "DAI", "amount": "1"},
            "Input should be 'USDC'",
        ),
    ],
)
def test_l2_usd_transfer_action_details_invalid(
    field: str,
    value: object,
    expected_error_part: str,
) -> None:
    """Test l2 usd transfer action details invalid."""
    base_payload_data = {
        "destination": VALID_ETH_ADDRESS,
        "token": "USDC",
        "amount": VALID_POSITIVE_DECIMAL_STR,
    }
    base_data: dict[str, Any] = {"chain": "L2", "payload": base_payload_data}

    if value is None and field in base_data:
        del base_data[field]
    else:
        base_data[field] = cast("Any", value)  # Cast for test compatibility

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawL2UsdTransferActionDetails.model_validate(base_data)
    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_l2_usd_transfer_action_details_extra_field() -> None:
    """Test l2 usd transfer action details extra field."""
    data: dict[str, str | dict[str, Any] | Any] = {"chain": "L2", "payload": {}, "extra": "field"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawL2UsdTransferActionDetails.model_validate(data)


# --- HyperliquidRawCancelOrderAction Tests (Model renamed - skipping) ---
"""

def test_cancel_order_action_valid() -> None:
    Test cancel order action valid.
    data = {"asset": 0, "oid": 12345}
    action = HyperliquidRawCancelOrderAction.model_validate(data)
    assert action.asset == 0
    assert action.oid == 12345
    assert action.model_config.get("extra") == "forbid"
    assert action.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field, value, expected_error_part",
    [
        ("asset", -1, "cannot be negative"),
        ("asset", None, "Field required"),
        ("oid", -1, "cannot be negative"),
        ("oid", "not-an-int", "Must be an integer"),
        ("oid", None, "Field required"),
    ],
)
def test_cancel_order_action_invalid(field: str, value: object, expected_error_part: str) -> None:
    Test cancel order action invalid.
    base_data: dict[str, Any] = {"asset": 0, "oid": 12345}
    if value is None and field in base_data:
        del base_data[field]
    else:
        base_data[field] = cast("Any", value)  # Cast for test compatibility

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawCancelOrderAction.model_validate(base_data)
    assert any(
        expected_error_part.lower() in err_detail["msg"].lower()
        for err_detail in exc_info.value.errors()
    )


def test_cancel_order_action_extra_field() -> None:
    Test cancel order action extra field.
    data = {"asset": 0, "oid": 12345, "extra": "field"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawCancelOrderAction.model_validate(data)
"""  # End of commented out cancel tests
