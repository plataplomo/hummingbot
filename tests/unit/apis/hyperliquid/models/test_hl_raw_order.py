"""
Unit tests for Hyperliquid Raw Order Action Pydantic models.
Validates parsing, aliases, and error handling for raw order request structures.
"""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
    HyperliquidRawTriggerDetails,
)

# --- Test Data ---
VALID_LIMIT_ORDER_DATA: dict[str, str] = {"tif": "Gtc"}
VALID_MARKET_ORDER_DATA: dict[Any, Any] = {}

VALID_LIMIT_ORDER_TYPE_GTC: dict[str, dict[str, str]] = {"limit": VALID_LIMIT_ORDER_DATA}
VALID_LIMIT_ORDER_TYPE_IOC: dict[str, dict[str, str]] = {"limit": {"tif": "Ioc"}}
VALID_MARKET_ORDER_TYPE: dict[str, dict[Any, Any]] = {"market": VALID_MARKET_ORDER_DATA}

VALID_TRIGGER_DETAILS_TP_MARKET_DATA: dict[str, Any] = {
    "triggerPx": "100.50",
    "isMarket": True,
    "tpsl": "tp",
}
VALID_TRIGGER_DETAILS_SL_LIMIT_DATA: dict[str, Any] = {
    "triggerPx": "90.00",
    "isMarket": False,
    "tpsl": "sl",
}

# For HyperliquidRawPlaceOrderAction, the nested dicts are complex.
# We will type the outer dict as Dict[str, Any] and rely on Pydantic's parsing for internals.
# Type ignores might be needed at the point of **data expansion if Pyright still complains.

MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT: dict[str, Any] = {
    "asset": 1,
    "isBuy": True,
    "limitPx": "150.75",
    "sz": "10.2",
    "reduceOnly": False,
    "orderType": VALID_LIMIT_ORDER_TYPE_GTC,
}

FULL_VALID_PLACE_ORDER_ACTION_LIMIT_WITH_TRIGGER_AND_CLOID: dict[str, Any] = {
    "asset": 2,
    "isBuy": False,
    "limitPx": "2000.00",
    "sz": "0.5",
    "reduceOnly": True,
    "orderType": VALID_LIMIT_ORDER_TYPE_IOC,
    "trigger": VALID_TRIGGER_DETAILS_TP_MARKET_DATA,
    "cloid": "my_client_order_id_123",
}

MINIMAL_VALID_PLACE_ORDER_ACTION_MARKET: dict[str, Any] = {
    "asset": 3,
    "isBuy": True,
    "limitPx": "0",
    "sz": "5",
    "reduceOnly": False,
    "orderType": VALID_MARKET_ORDER_TYPE,
}


# --- Helper for common assertions ---
def assert_common_place_order_fields(
    parsed_model: HyperliquidRawPlaceOrderAction,
    raw_data: dict[str, Any],
    expected_python_types: dict[str, type],
) -> None:
    asset_val = raw_data["asset"]
    assert parsed_model.asset == asset_val
    assert isinstance(parsed_model.asset, expected_python_types["asset"])

    is_buy_val = raw_data["isBuy"]
    assert parsed_model.is_buy == is_buy_val
    assert isinstance(parsed_model.is_buy, expected_python_types["is_buy"])

    limit_px_val = raw_data["limitPx"]
    assert parsed_model.limit_px == str(limit_px_val)
    assert isinstance(parsed_model.limit_px, expected_python_types["limit_px"])

    sz_val = raw_data["sz"]
    assert parsed_model.sz == str(sz_val)
    assert isinstance(parsed_model.sz, expected_python_types["sz"])

    reduce_only_val = raw_data["reduceOnly"]
    assert parsed_model.reduce_only == reduce_only_val
    assert isinstance(parsed_model.reduce_only, expected_python_types["reduce_only"])

    cloid_val = raw_data.get("cloid")
    if cloid_val is not None:
        assert parsed_model.cloid == str(cloid_val)
        assert isinstance(parsed_model.cloid, expected_python_types["cloid"])
    else:
        assert parsed_model.cloid is None


# --- Test Cases ---


class TestHyperliquidRawOrderType:
    def test_valid_limit_order_type(self) -> None:
        limit_details = HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
        parsed = HyperliquidRawOrderType(limit=limit_details, market=None)
        assert parsed.limit is not None
        assert parsed.market is None
        assert parsed.limit.tif == "Gtc"
        assert isinstance(parsed.limit.tif, str)

    def test_valid_market_order_type(self) -> None:
        market_details = HyperliquidRawMarketOrderTypeDetails()
        parsed = HyperliquidRawOrderType(limit=None, market=market_details)
        assert parsed.market is not None
        assert parsed.limit is None

    def test_invalid_order_type_both_limit_and_market(self) -> None:
        with pytest.raises(
            ValidationError, match="Exactly one of 'limit' or 'market' must be provided"
        ):
            HyperliquidRawOrderType(
                limit=cast(HyperliquidRawLimitOrderTypeDetails, VALID_LIMIT_ORDER_DATA),
                market=cast(HyperliquidRawMarketOrderTypeDetails, VALID_MARKET_ORDER_DATA),
            )

    def test_invalid_order_type_neither_limit_nor_market(self) -> None:
        with pytest.raises(
            ValidationError, match="Exactly one of 'limit' or 'market' must be provided"
        ):
            HyperliquidRawOrderType(limit=None, market=None)

    def test_invalid_order_type_limit_null_market_present(self) -> None:
        with pytest.raises(
            ValidationError, match="Exactly one of 'limit' or 'market' must be provided"
        ):
            HyperliquidRawOrderType(
                limit=None,
                market=cast(HyperliquidRawMarketOrderTypeDetails, VALID_MARKET_ORDER_DATA),
            )

    def test_invalid_tif_string(self) -> None:
        with pytest.raises(ValidationError, match="Value 'InvalidTif' is not in allowed set"):
            HyperliquidRawOrderType(
                limit=cast(HyperliquidRawLimitOrderTypeDetails, {"tif": "InvalidTif"})
            )


class TestHyperliquidRawTriggerDetails:
    def test_valid_trigger_details_tp_market(self) -> None:
        parsed = HyperliquidRawTriggerDetails(**VALID_TRIGGER_DETAILS_TP_MARKET_DATA)
        assert parsed.trigger_px == "100.50"
        assert isinstance(parsed.trigger_px, str)
        assert parsed.is_market is True
        assert isinstance(parsed.is_market, bool)
        assert parsed.tpsl == "tp"
        assert isinstance(parsed.tpsl, str)

    def test_valid_trigger_details_sl_limit(self) -> None:
        parsed = HyperliquidRawTriggerDetails(**VALID_TRIGGER_DETAILS_SL_LIMIT_DATA)
        assert parsed.trigger_px == "90.00"
        assert isinstance(parsed.trigger_px, str)
        assert parsed.is_market is False
        assert isinstance(parsed.is_market, bool)
        assert parsed.tpsl == "sl"
        assert isinstance(parsed.tpsl, str)

    def test_invalid_trigger_px_not_decimal_string(self) -> None:
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["triggerPx"] = "not_a_number"
        with pytest.raises(
            ValidationError,
            match="trigger_px: Value 'not_a_number' must be a parseable finite decimal string.",
        ):
            HyperliquidRawTriggerDetails(**data)

    def test_invalid_trigger_px_infinite_string(self) -> None:
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["triggerPx"] = "inf"
        with pytest.raises(
            ValidationError,
            match="trigger_px: Value 'inf' must be a parseable finite decimal string.",
        ):
            HyperliquidRawTriggerDetails(**data)

    def test_valid_trigger_px_zero(self) -> None:
        data: dict[str, Any] = {"triggerPx": "0", "isMarket": True, "tpsl": "tp"}
        parsed = HyperliquidRawTriggerDetails(**data)
        assert parsed.trigger_px == "0"
        assert isinstance(parsed.trigger_px, str)

    def test_invalid_tpsl_value(self) -> None:
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["tpsl"] = "stop"
        with pytest.raises(ValidationError, match="tpsl: Value 'stop' is not in allowed set"):
            HyperliquidRawTriggerDetails(**data)

    def test_missing_trigger_px(self) -> None:
        data_missing_px: dict[str, Any] = {"isMarket": True, "tpsl": "tp"}
        with pytest.raises(ValidationError, match="Field required"):
            HyperliquidRawTriggerDetails(**data_missing_px)

    def test_invalid_is_market_type(self) -> None:
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["isMarket"] = "not_a_bool"
        with pytest.raises(ValidationError, match="is_market: Must be a boolean"):
            HyperliquidRawTriggerDetails(**data)


class TestHyperliquidRawPlaceOrderAction:
    EXPECTED_PYTHON_TYPES_AFTER_PARSING: dict[str, type] = {
        "asset": int,
        "is_buy": bool,
        "limit_px": str,
        "sz": str,
        "reduce_only": bool,
        "cloid": str,
    }

    def test_minimal_valid_limit_order(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)
        assert parsed.order_type.limit is not None
        order_type_data = cast(dict[str, Any], data["orderType"])
        limit_data = cast(dict[str, Any], order_type_data.get("limit"))
        assert parsed.order_type.limit.tif == limit_data.get("tif")
        assert parsed.trigger is None

    def test_full_valid_limit_order_with_trigger_and_cloid(self) -> None:
        data = FULL_VALID_PLACE_ORDER_ACTION_LIMIT_WITH_TRIGGER_AND_CLOID
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)

        assert parsed.order_type.limit is not None
        order_type_data = cast(dict[str, Any], data["orderType"])
        limit_data = cast(dict[str, Any], order_type_data.get("limit"))
        assert parsed.order_type.limit.tif == limit_data.get("tif")

        assert parsed.trigger is not None
        trigger_data = cast(dict[str, Any], data["trigger"])
        assert parsed.trigger.trigger_px == str(trigger_data.get("triggerPx"))
        assert parsed.trigger.is_market == trigger_data.get("isMarket")
        assert parsed.trigger.tpsl == trigger_data.get("tpsl")

        cloid_data = data["cloid"]
        assert parsed.cloid == cloid_data

    def test_minimal_valid_market_order(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_MARKET
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)
        assert parsed.order_type.market is not None
        assert parsed.order_type.limit is None
        assert parsed.trigger is None

    def test_missing_required_field_asset(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        del data["asset"]
        with pytest.raises(ValidationError, match="Field required"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_asset_type_string(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["asset"] = "not_an_int"
        with pytest.raises(ValidationError, match="asset: Must be an integer"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_limit_px_type_not_string(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["limitPx"] = 150.75
        with pytest.raises(ValidationError, match="limit_px: Expected a string value"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_sz_not_parseable_to_decimal(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["sz"] = "abc"
        with pytest.raises(
            ValidationError, match="sz: Value 'abc' must be a parseable finite decimal string."
        ):
            HyperliquidRawPlaceOrderAction(**data)

    def test_valid_sz_zero_string_for_raw_model(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["sz"] = "0"
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.sz == "0"

    def test_invalid_is_buy_type_string(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["isBuy"] = "TrueString"
        with pytest.raises(ValidationError, match="is_buy: Must be a boolean"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_extra_field_not_allowed(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["extraField"] = "some_value"
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_cloid_too_long(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = "a" * 65
        with pytest.raises(ValidationError, match="cloid: String value too long"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_cloid_empty_if_present(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = ""
        with pytest.raises(ValidationError, match="cloid: String cannot be empty"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_valid_cloid_none(self) -> None:
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = None
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.cloid is None

    def test_valid_cloid_present(self) -> None:
        cloid_val = "my-test-cloid-max-64-chars-padding-padding-padding-padding-pad"
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = cloid_val
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.cloid == cloid_val
