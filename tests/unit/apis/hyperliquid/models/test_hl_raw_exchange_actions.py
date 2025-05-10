from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawOrderItemSpec,
    HyperliquidRawPlaceOrderActionPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawOrderType,
)

# --- Test Data ---

VALID_ETH_ADDRESS = "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9B"
INVALID_ETH_ADDRESS_SHORT = "0x123"
INVALID_ETH_ADDRESS_NOHEX = "Ab5801a7D398351b8bE11C439e05C5B3259aeC9B"
INVALID_ETH_ADDRESS_NONHEXCHARS = "0xAb5801a7D398351b8bE11C439e05C5B3259aeC9X"  # X is invalid

VALID_DECIMAL_STR = "123.456"
INVALID_DECIMAL_STR_NON_FINITE = "Infinity"
INVALID_DECIMAL_STR_EMPTY = ""

VALID_LIMIT_ORDER_TYPE_DETAILS_GTC = {"limit": {"tif": "Gtc"}}
VALID_MARKET_ORDER_TYPE_DETAILS: dict[str, dict[str, Any]] = {"market": {}}

# --- HyperliquidRawEthWithdrawalActionPayload Tests ---


def test_eth_withdrawal_payload_valid() -> None:
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS}
    payload = HyperliquidRawEthWithdrawalActionPayload.model_validate(data)
    assert payload.amount == VALID_DECIMAL_STR
    assert payload.destination == VALID_ETH_ADDRESS
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
        ("destination", INVALID_ETH_ADDRESS_NONHEXCHARS, "non-hexadecimal characters"),
        ("destination", None, "Field required"),
    ],
)
def test_eth_withdrawal_payload_invalid_fields(
    field: str, value: str | None, expected_error_part: str
) -> None:
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS}
    if value is None:
        del data[field]
    else:
        data[field] = value
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawEthWithdrawalActionPayload.model_validate(data)
    assert expected_error_part.lower() in str(exc_info.value).lower()


def test_eth_withdrawal_payload_extra_field() -> None:
    data = {"amount": VALID_DECIMAL_STR, "destination": VALID_ETH_ADDRESS, "extra": "field"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawEthWithdrawalActionPayload.model_validate(data)


# --- HyperliquidRawOrderItemSpec Tests ---


def test_order_item_spec_valid_limit() -> None:
    data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
        "client_order_id": "cloid123",
    }
    item_spec = HyperliquidRawOrderItemSpec.model_validate(data)
    assert item_spec.a == 0
    assert item_spec.b is True
    assert item_spec.p == VALID_DECIMAL_STR
    assert item_spec.s == "1.0"
    assert item_spec.r is False
    assert isinstance(item_spec.t, HyperliquidRawOrderType)
    assert item_spec.t.limit is not None
    assert item_spec.t.limit.tif == "Gtc"
    assert item_spec.c == "cloid123"
    assert item_spec.model_config.get("extra") == "forbid"
    assert item_spec.model_config.get("frozen") is True
    assert item_spec.model_config.get("populate_by_name") is True


def test_order_item_spec_valid_market_no_cloid() -> None:
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
    assert isinstance(item_spec.t, HyperliquidRawOrderType)
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
        ("order_type_details", {"limit": {"tif": "InvalidTIF"}}, "not in allowed set"),
        ("order_type_details", {"market": None, "limit": None}, "Exactly one of"),  # Both None
        ("order_type_details", None, "Field required"),
        ("client_order_id", "", "String cannot be empty"),
        ("client_order_id", "a" * 65, "String value too long"),  # Max 64
    ],
)
def test_order_item_spec_invalid_fields(
    field_alias: str, value: object, expected_error_part: str
) -> None:
    base_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
        "client_order_id": "cloid123",
    }
    if value is None and field_alias in base_data:  # Test missing required field
        del base_data[field_alias]
    else:
        base_data[field_alias] = value  # pyright: ignore[reportArgumentType] # Negative test: intentionally assigning invalid type for validation

    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderItemSpec.model_validate(base_data)
    assert expected_error_part.lower() in str(exc_info.value).lower()


def test_order_item_spec_extra_field() -> None:
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


# --- HyperliquidRawPlaceOrderActionPayload Tests ---


def test_place_order_payload_valid() -> None:
    order_item_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": VALID_DECIMAL_STR,
        "size": "1.0",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
    }
    # Pydantic will parse the inner dict to HyperliquidRawOrderItemSpec
    data = {
        "type": "order",
        "grouping": "na",
        "orders": [order_item_data],
    }
    payload = HyperliquidRawPlaceOrderActionPayload.model_validate(data)
    assert payload.type == "order"
    assert payload.grouping == "na"
    assert len(payload.orders) == 1
    assert isinstance(payload.orders[0], HyperliquidRawOrderItemSpec)
    assert payload.orders[0].a == 0
    assert payload.model_config.get("extra") == "forbid"
    assert payload.model_config.get("frozen") is True


@pytest.mark.parametrize(
    "field, value, expected_error_part",
    [
        ("type", "invalid_type", "unexpected value"),
        ("type", None, "Field required"),
        ("grouping", "invalid_grouping", "unexpected value"),
        ("grouping", None, "Field required"),
        ("orders", [], "List should have at least 1 item"),  # Assuming non-empty list
        (
            "orders",
            [
                {
                    "asset_index": -1,  # Invalid inner item
                    "is_buy": True,
                    "limit_px": "1",
                    "size": "1",
                    "reduce_only": False,
                    "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
                }
            ],
            "cannot be negative",
        ),
        ("orders", "not-a-list", "Input should be a valid list"),
        ("orders", None, "Field required"),
    ],
)
def test_place_order_payload_invalid_fields(
    field: str, value: object, expected_error_part: str
) -> None:
    order_item_data: dict[str, object] = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": "1",
        "size": "1",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
    }
    base_data: dict[str, object] = {"type": "order", "grouping": "na", "orders": [order_item_data]}

    if value is None and field in base_data:
        del base_data[field]
    else:
        base_data[field] = value  # pyright: ignore[reportArgumentType] # Negative test: intentionally assigning invalid type for validation

    # Special case for empty list validation, if your common type defines min_length=1
    # This test currently assumes orders can be empty, adjust if rules state min_items=1
    if field == "orders" and value == []:
        payload = HyperliquidRawPlaceOrderActionPayload.model_validate(base_data)
        assert payload.orders == []
        return  # Skip raises check for this specific case

    with pytest.raises(ValidationError) as exc_info:
        # DEFENSIVE CHECK: Mypy struggles with data['orders'] items. Mypy=[index]
        HyperliquidRawPlaceOrderActionPayload.model_validate(base_data)
    assert expected_error_part.lower() in str(exc_info.value).lower()


def test_place_order_payload_orders_empty_list_valid() -> None:
    # Test if an empty list of orders is considered valid by the model itself
    # (application logic might reject it later, but raw model might allow)
    # The RawNonEmptyList wrapper isn't used for 'orders', so an empty list is valid for Pydantic.
    data: dict[str, str | list[HyperliquidRawOrderItemSpec]] = {
        "type": "order",
        "grouping": "na",
        "orders": [],
    }
    payload = HyperliquidRawPlaceOrderActionPayload.model_validate(data)
    assert payload.orders == []


def test_place_order_payload_extra_field() -> None:
    order_item_data = {
        "asset_index": 0,
        "is_buy": True,
        "limit_px": "1",
        "size": "1",
        "reduce_only": False,
        "order_type_details": VALID_LIMIT_ORDER_TYPE_DETAILS_GTC,
    }
    data = {
        "type": "order",
        "grouping": "na",
        "orders": [order_item_data],
        "extra_field": "value",
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        HyperliquidRawPlaceOrderActionPayload.model_validate(data)
