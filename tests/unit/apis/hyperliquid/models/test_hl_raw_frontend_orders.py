"""
Unit Tests for Hyperliquid Raw Frontend Open Orders Models
"""

from typing import Any

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_frontend_orders import (
    HyperliquidRawFrontendOpenOrder,
)

# --- Test Data --- #

VALID_FRONTEND_ORDER: dict[str, Any] = {
    "coin": "BTC",
    "isPositionTpsl": False,
    "isTrigger": False,
    "limitPx": "29792.0",
    "oid": 91490942,
    "orderType": "Limit",
    "origSz": "5.0",
    "reduceOnly": False,
    "side": "A",
    "sz": "5.0",
    "timestamp": 1681247412573,
    "triggerCondition": "N/A",
    "triggerPx": "0.0",
}


# --- Fixtures --- #


@pytest.fixture
def valid_frontend_order_data() -> dict[str, Any]:
    return VALID_FRONTEND_ORDER.copy()


# --- Test Cases for HyperliquidRawFrontendOpenOrder --- #


def test_frontend_order_valid(valid_frontend_order_data: dict[str, Any]) -> None:
    order = HyperliquidRawFrontendOpenOrder.model_validate(valid_frontend_order_data)
    assert order.coin == valid_frontend_order_data["coin"]
    assert order.is_position_tpsl == valid_frontend_order_data["isPositionTpsl"]
    assert order.is_trigger == valid_frontend_order_data["isTrigger"]
    assert order.limit_px == valid_frontend_order_data["limitPx"]
    assert order.oid == valid_frontend_order_data["oid"]
    assert order.order_type == valid_frontend_order_data["orderType"]
    assert order.orig_sz == valid_frontend_order_data["origSz"]
    assert order.reduce_only == valid_frontend_order_data["reduceOnly"]
    assert order.side == valid_frontend_order_data["side"]
    assert order.sz == valid_frontend_order_data["sz"]
    assert order.timestamp == valid_frontend_order_data["timestamp"]
    assert order.trigger_condition == valid_frontend_order_data["triggerCondition"]
    assert order.trigger_px == valid_frontend_order_data["triggerPx"]


@pytest.mark.parametrize(
    "field, value, is_missing_test",
    [
        ("coin", None, True),  # Required
        ("coin", "BTCTOOLONG" * 20, False),  # Too long
        ("isPositionTpsl", "not-a-bool", False),
        ("isTrigger", 123, False),
        ("limitPx", "not-a-decimal", False),
        ("limitPx", "Infinity", False),
        ("oid", -1, False),  # Negative OID
        ("oid", "not-an-int", False),
        ("orderType", None, True),  # Required
        ("origSz", "NaN", False),
        ("reduceOnly", "TrueString", False),  # Invalid bool string
        ("side", "C", False),  # Invalid side
        ("sz", None, True),  # Required
        ("timestamp", "invalid-ts", False),
        ("triggerCondition", None, True),  # Required, N/A is a valid string
        (
            "triggerPx",
            "-1.0",
            False,
        ),  # While 0.0 is common for inactive, negative could be an edge for non-finite check
    ],
)
def test_frontend_order_invalid_fields(
    valid_frontend_order_data: dict[str, Any],
    field: str,
    value: Any,
    is_missing_test: bool,  # noqa: ANN401
) -> None:
    data_copy = valid_frontend_order_data.copy()
    if is_missing_test:
        if field in data_copy:
            del data_copy[field]
    else:
        data_copy[field] = value

    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(data_copy)


def test_frontend_order_missing_required_fields() -> None:
    required_fields = VALID_FRONTEND_ORDER.keys()
    for field in required_fields:
        data_copy = VALID_FRONTEND_ORDER.copy()
        del data_copy[field]
        with pytest.raises(ValidationError, match=f"Field required.*{field}"):
            HyperliquidRawFrontendOpenOrder.model_validate(data_copy)


def test_frontend_order_extra_field(valid_frontend_order_data: dict[str, Any]) -> None:
    data_copy = valid_frontend_order_data.copy()
    data_copy["extraField"] = "someValue"
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(data_copy)


# Test for response being a list of orders (though model itself is single order)
# This tests how the handler would use it, not the model itself directly as RootModel


def test_frontend_orders_list_valid() -> None:
    orders_list_data = [VALID_FRONTEND_ORDER.copy(), VALID_FRONTEND_ORDER.copy()]
    validated_orders = [HyperliquidRawFrontendOpenOrder.model_validate(o) for o in orders_list_data]
    assert len(validated_orders) == 2
    assert validated_orders[0].oid == VALID_FRONTEND_ORDER["oid"]


def test_frontend_orders_list_invalid_item() -> None:
    invalid_order_item = VALID_FRONTEND_ORDER.copy()
    invalid_order_item["oid"] = "not-an-int"
    orders_list_data = [VALID_FRONTEND_ORDER.copy(), invalid_order_item]
    with pytest.raises(ValidationError):
        [HyperliquidRawFrontendOpenOrder.model_validate(o) for o in orders_list_data]


def test_frontend_orders_list_not_a_list() -> None:
    with pytest.raises(TypeError):  # Or other error depending on how it's passed
        HyperliquidRawFrontendOpenOrder.model_validate("not a list")
