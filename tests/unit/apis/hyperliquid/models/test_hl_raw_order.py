"""Unit tests for Hyperliquid Raw Order Action Pydantic models.

Validates parsing, aliases, and error handling for raw order request structures.
"""

from typing import Any, cast

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerInfo
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawLimitOrderTypeDetails,
    HyperliquidRawMarketOrderTypeDetails,
    HyperliquidRawOrderType,
    HyperliquidRawPlaceOrderAction,
)

# --- Test Data ---
VALID_LIMIT_ORDER_DATA: dict[str, str] = {"tif": "Gtc"}
VALID_MARKET_ORDER_DATA: dict[str, Any] = {}

VALID_LIMIT_ORDER_TYPE_GTC: dict[str, dict[str, str]] = {"limit": VALID_LIMIT_ORDER_DATA}
VALID_LIMIT_ORDER_TYPE_IOC: dict[str, dict[str, str]] = {"limit": {"tif": "Ioc"}}
VALID_MARKET_ORDER_TYPE: dict[str, dict[str, Any]] = {"market": VALID_MARKET_ORDER_DATA}

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
    """Assert common place order fields."""
    asset_val = raw_data["asset"]
    assert parsed_model.asset == asset_val
    assert isinstance(parsed_model.asset, expected_python_types["asset"])

    is_buy_val = raw_data["isBuy"]
    assert parsed_model.isBuy == is_buy_val
    assert isinstance(parsed_model.isBuy, expected_python_types["is_buy"])

    # Business logic normalizes decimal strings
    if raw_data["limitPx"] == "2000.00":
        assert parsed_model.limitPx == "2000"  # Business logic normalizes
    else:
        assert parsed_model.limitPx == str(raw_data["limitPx"])
    assert isinstance(parsed_model.limitPx, expected_python_types["limit_px"])

    sz_val = raw_data["sz"]
    assert parsed_model.sz == str(sz_val)
    assert isinstance(parsed_model.sz, expected_python_types["sz"])

    reduce_only_val = raw_data["reduceOnly"]
    assert parsed_model.reduceOnly == reduce_only_val
    assert isinstance(parsed_model.reduceOnly, expected_python_types["reduce_only"])

    cloid_val = raw_data.get("cloid")
    if cloid_val is not None:
        assert parsed_model.cloid == str(cloid_val)
        assert isinstance(parsed_model.cloid, expected_python_types["cloid"])
    else:
        assert parsed_model.cloid is None


# --- Test Cases ---


class TestHyperliquidRawOrderType:
    """Test class for HyperliquidRawOrderType model."""

    def test_valid_limit_order_type(self) -> None:
        """Test valid limit order type."""
        limit_details = HyperliquidRawLimitOrderTypeDetails(tif="Gtc")
        parsed = HyperliquidRawOrderType(limit=limit_details, market=None)
        assert parsed.limit is not None
        assert parsed.market is None
        assert parsed.limit.tif == "Gtc"
        assert isinstance(parsed.limit.tif, str)

    def test_valid_market_order_type(self) -> None:
        """Test valid market order type."""
        market_details = HyperliquidRawMarketOrderTypeDetails()
        parsed = HyperliquidRawOrderType(limit=None, market=market_details)
        assert parsed.market is not None
        assert parsed.limit is None

    def test_order_type_both_limit_and_market_allowed(self) -> None:
        """Test order type both limit and market allowed."""
        # Raw models should accept data without business logic validation
        # The builder/service layer should ensure only one is set
        order_type = HyperliquidRawOrderType(
            limit=cast("HyperliquidRawLimitOrderTypeDetails", VALID_LIMIT_ORDER_DATA),
            market=cast("HyperliquidRawMarketOrderTypeDetails", VALID_MARKET_ORDER_DATA),
        )
        assert order_type.limit is not None
        assert order_type.market is not None

    def test_order_type_neither_limit_nor_market_allowed(self) -> None:
        """Test order type neither limit nor market allowed."""
        # Raw models should accept data without business logic validation
        # The builder/service layer should ensure at least one is set
        order_type = HyperliquidRawOrderType(limit=None, market=None)
        assert order_type.limit is None
        assert order_type.market is None

    def test_invalid_tif_string(self) -> None:
        """Test invalid tif string."""
        with pytest.raises(
            ValidationError,
            match=r"Invalid value 'InvalidTif'\. Expected one of \['Alo', 'Gtc', 'Ioc'\]",
        ):
            HyperliquidRawOrderType(
                limit=cast("HyperliquidRawLimitOrderTypeDetails", {"tif": "InvalidTif"}),
            )


class TestHyperliquidRawTriggerInfo:
    """Test class for HyperliquidRawTriggerInfo model."""

    def test_valid_trigger_details_tp_market(self) -> None:
        """Test valid trigger details tp market."""
        parsed = HyperliquidRawTriggerInfo(**VALID_TRIGGER_DETAILS_TP_MARKET_DATA)
        assert parsed.trigger_px == "100.5"  # Business logic normalizes decimal strings
        assert isinstance(parsed.trigger_px, str)
        assert parsed.is_market is True
        assert isinstance(parsed.is_market, bool)
        assert parsed.tpsl == "tp"
        assert isinstance(parsed.tpsl, str)

    def test_valid_trigger_details_sl_limit(self) -> None:
        """Test valid trigger details sl limit."""
        parsed = HyperliquidRawTriggerInfo(**VALID_TRIGGER_DETAILS_SL_LIMIT_DATA)
        assert parsed.trigger_px == "90"  # Business logic normalizes decimal strings
        assert isinstance(parsed.trigger_px, str)
        assert parsed.is_market is False
        assert isinstance(parsed.is_market, bool)
        assert parsed.tpsl == "sl"
        assert isinstance(parsed.tpsl, str)

    def test_invalid_trigger_px_not_decimal_string(self) -> None:
        """Test invalid trigger px not decimal string."""
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["triggerPx"] = "not_a_number"
        with pytest.raises(
            ValidationError,
            match=r"Cannot convert 'not_a_number' to Decimal",
        ):
            HyperliquidRawTriggerInfo(**data)

    def test_invalid_trigger_px_infinite_string(self) -> None:
        """Test invalid trigger px infinite string."""
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["triggerPx"] = "inf"
        with pytest.raises(
            ValidationError,
            match="trigger_px: Value 'inf' must be a parseable finite decimal string.",
        ):
            HyperliquidRawTriggerInfo(**data)

    def test_valid_trigger_px_zero(self) -> None:
        """Test valid trigger px zero."""
        data: dict[str, Any] = {"triggerPx": "0", "isMarket": True, "tpsl": "tp"}
        parsed = HyperliquidRawTriggerInfo(**data)
        assert parsed.trigger_px == "0"
        assert isinstance(parsed.trigger_px, str)

    def test_invalid_tpsl_value(self) -> None:
        """Test invalid tpsl value."""
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["tpsl"] = "stop"
        with pytest.raises(
            ValidationError,
            match=r"Invalid value 'stop'\. Expected one of \['sl', 'tp'\]",
        ):
            HyperliquidRawTriggerInfo(**data)

    def test_missing_trigger_px(self) -> None:
        """Test missing trigger px."""
        data_missing_px: dict[str, Any] = {"isMarket": True, "tpsl": "tp"}
        with pytest.raises(ValidationError, match="Field required"):
            HyperliquidRawTriggerInfo(**data_missing_px)

    def test_invalid_is_market_type(self) -> None:
        """Test invalid is market type."""
        data = VALID_TRIGGER_DETAILS_TP_MARKET_DATA.copy()
        data["isMarket"] = "not_a_bool"
        with pytest.raises(ValidationError, match="is_market: Must be a boolean"):
            HyperliquidRawTriggerInfo(**data)


class TestHyperliquidRawPlaceOrderAction:
    """Test class for HyperliquidRawPlaceOrderAction model."""

    EXPECTED_PYTHON_TYPES_AFTER_PARSING: dict[str, type] = {
        "asset": int,
        "is_buy": bool,
        "limit_px": str,
        "sz": str,
        "reduce_only": bool,
        "cloid": str,
    }

    def test_minimal_valid_limit_order(self) -> None:
        """Test minimal valid limit order."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)
        assert parsed.orderType.limit is not None
        order_type_data = cast("dict[str, Any]", data["orderType"])
        limit_data = cast("dict[str, Any]", order_type_data.get("limit"))
        assert parsed.orderType.limit.tif == limit_data.get("tif")
        assert parsed.trigger is None

    def test_full_valid_limit_order_with_trigger_and_cloid(self) -> None:
        """Test full valid limit order with trigger and cloid."""
        data = FULL_VALID_PLACE_ORDER_ACTION_LIMIT_WITH_TRIGGER_AND_CLOID
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)

        assert parsed.orderType.limit is not None
        order_type_data = cast("dict[str, Any]", data["orderType"])
        limit_data = cast("dict[str, Any]", order_type_data.get("limit"))
        assert parsed.orderType.limit.tif == limit_data.get("tif")

        assert parsed.trigger is not None
        trigger_data = cast("dict[str, Any]", data["trigger"])
        # Business logic normalizes "100.50" to "100.5"
        assert parsed.trigger.trigger_px == "100.5"
        assert parsed.trigger.is_market == trigger_data.get("isMarket")
        assert parsed.trigger.tpsl == trigger_data.get("tpsl")

        cloid_data = data["cloid"]
        assert parsed.cloid == cloid_data

    def test_minimal_valid_market_order(self) -> None:
        """Test minimal valid market order."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_MARKET
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert_common_place_order_fields(parsed, data, self.EXPECTED_PYTHON_TYPES_AFTER_PARSING)
        assert parsed.orderType.market is not None
        assert parsed.orderType.limit is None
        assert parsed.trigger is None

    def test_missing_required_field_asset(self) -> None:
        """Test missing required field asset."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        del data["asset"]
        with pytest.raises(ValidationError, match="Field required"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_asset_type_string(self) -> None:
        """Test invalid asset type string."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["asset"] = "not_an_int"
        with pytest.raises(ValidationError, match="asset: Must be an integer"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_limit_px_type_not_string(self) -> None:
        """Test invalid limit px type not string."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["limitPx"] = 150.75
        with pytest.raises(ValidationError, match=r"Expected string, got float"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_sz_not_parseable_to_decimal(self) -> None:
        """Test invalid sz not parseable to decimal."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["sz"] = "abc"
        with pytest.raises(ValidationError, match=r"Cannot convert 'abc' to Decimal"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_valid_sz_zero_string_for_raw_model(self) -> None:
        """Test valid sz zero string for raw model."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["sz"] = "0"
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.sz == "0"

    def test_invalid_is_buy_type_string(self) -> None:
        """Test invalid is buy type string."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["isBuy"] = "TrueString"
        with pytest.raises(ValidationError, match="isBuy: Must be a boolean"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_extra_field_not_allowed(self) -> None:
        """Test extra field not allowed."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["extraField"] = "some_value"
        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_cloid_too_long(self) -> None:
        """Test invalid cloid too long."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = "a" * 65
        with pytest.raises(ValidationError, match="cloid: String value too long"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_invalid_cloid_empty_if_present(self) -> None:
        """Test invalid cloid empty if present."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = ""
        with pytest.raises(ValidationError, match="cloid: String cannot be empty"):
            HyperliquidRawPlaceOrderAction(**data)

    def test_valid_cloid_none(self) -> None:
        """Test valid cloid none."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = None
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.cloid is None

    def test_valid_cloid_present(self) -> None:
        """Test valid cloid present."""
        data = MINIMAL_VALID_PLACE_ORDER_ACTION_LIMIT.copy()
        data["cloid"] = "test_cloid"
        parsed = HyperliquidRawPlaceOrderAction(**data)
        assert parsed.cloid == "test_cloid"


class TestHyperliquidRawOrder:
    """Tests for the HyperliquidRawOrder model itself (representing an existing order)."""

    def test_invalid_order_bad_status(self) -> None:
        """Test that an order with an invalid status raises ValidationError."""
        # ... existing code ...
