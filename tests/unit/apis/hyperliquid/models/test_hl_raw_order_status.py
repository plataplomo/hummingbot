"""
Unit Tests for HyperliquidRawOrderStatusResponse Model
"""

from typing import Any

import pytest
from pydantic import ValidationError

# Import the actual HyperliquidRawOrder model to reference its fields
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusResponse,
)

# Data structure matching HyperliquidRawOrder fields
VALID_RAW_ORDER_DATA = {
    "oid": 12345,
    "cloid": None,  # Optional client order ID
    "asset": "ETH",
    "side": "B",  # 'B' for Buy
    "limitPx": "2000.50",
    "sz": "0.5",
    "timestamp": 1700000000000,
    "orderType": {"limit": {"tif": "Gtc"}},  # Example valid orderType
    "reduceOnly": False,
    "remainingSz": "0.5",  # Required by HLRawOrder
    "status": "open",  # Required by HLRawOrder
    "statusTimestamp": 1700000000000,  # Required by HLRawOrder
}


def test_valid_order_status_response() -> None:
    """Test successful parsing of a valid order status response."""
    valid_data = {"order": VALID_RAW_ORDER_DATA}
    obj = HyperliquidRawOrderStatusResponse.model_validate(valid_data)
    assert obj.order is not None
    assert obj.order.oid == 12345
    assert obj.order.asset == "ETH"
    assert obj.order.limit_px == "2000.50"
    assert obj.order.sz == "0.5"
    assert obj.order.side == "B"
    assert obj.order.status == "open"
    assert obj.model_config.get("extra") == "forbid"
    assert obj.model_config.get("frozen") is True


def test_missing_order_field() -> None:
    """Test validation fails if the required 'order' field is missing."""
    invalid_data: dict[str, Any] = {}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)
    assert "Field required" in str(exc_info.value)
    assert "order" in str(exc_info.value)


def test_invalid_order_structure() -> None:
    """Test validation fails if the 'order' field has an invalid structure."""
    # Create a copy and invalidate a field within the nested order dict
    invalid_order_data = VALID_RAW_ORDER_DATA.copy()
    invalid_order_data["limitPx"] = "invalid-price"  # Invalid format
    invalid_data = {"order": invalid_order_data}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)
    assert "order.limitPx" in str(exc_info.value)  # Check nested error path
    assert "Cannot convert 'invalid-price' to Decimal" in str(exc_info.value)


def test_extra_field_forbidden() -> None:
    """Test validation fails if extra fields are provided at the top level."""
    invalid_data = {"order": VALID_RAW_ORDER_DATA, "extra_field": 123}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)
    assert "Extra inputs are not permitted" in str(exc_info.value)
    assert "extra_field" in str(exc_info.value)


def test_frozen_instance() -> None:
    """Test that the validated instance is frozen."""
    valid_data = {"order": VALID_RAW_ORDER_DATA}
    obj = HyperliquidRawOrderStatusResponse.model_validate(valid_data)
    with pytest.raises(ValidationError) as exc_info:
        obj.order = None  # type: ignore # Attempt invalid assignment
    assert "Instance is frozen" in str(exc_info.value)


def test_order_field_not_a_dictionary() -> None:
    """Test validation fails if the 'order' field is not a dictionary."""
    # Test with string
    invalid_data_str = {"order": "not_a_dict"}
    with pytest.raises(TypeError) as exc_info_str:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data_str)
    assert "Field 'order' must be a dictionary, got str." in str(exc_info_str.value)

    # Test with list
    invalid_data_list = {"order": [1, 2, 3]}
    with pytest.raises(TypeError) as exc_info_list:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data_list)
    assert "Field 'order' must be a dictionary, got list." in str(exc_info_list.value)

    # Test with integer
    invalid_data_int = {"order": 123}
    with pytest.raises(TypeError) as exc_info_int:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data_int)
    assert "Field 'order' must be a dictionary, got int." in str(exc_info_int.value)

    # Test with None (if None is not allowed by HyperliquidRawOrder, Pydantic will catch later)
    # The current validator only checks for dict type, so None would pass this specific validator
    # but fail HyperliquidRawOrder validation if it's not optional there.
    # If HyperliquidRawOrder can be None, this test would need adjustment or be covered elsewhere.
    # For now, assuming HyperliquidRawOrder is not Optional[...].
    invalid_data_none = {"order": None}
    with pytest.raises(TypeError) as exc_info_none:  # Expecting TypeError from our validator
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data_none)
    assert "Field 'order' must be a dictionary, got NoneType." in str(exc_info_none.value)
