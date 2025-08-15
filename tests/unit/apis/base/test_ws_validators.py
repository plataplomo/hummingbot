"""Property-based tests for WebSocket payload validators.

This module provides comprehensive property-based testing for WebSocket payload validators,
ensuring robust validation logic across all possible input combinations. Uses Hypothesis to
generate exhaustive test cases that would be impossible to cover with traditional testing.

Key Testing Areas:
- Dictionary and list payload validation with size constraints
- Required and optional field validation properties
- Symbol and topic format validation across all possible inputs
- Numeric string parsing with boundary testing
- Timestamp validation with range constraints
- Exchange-specific validation rules (Backpack, Hyperliquid)

SECURITY CRITICAL: Validators are the first line of defense against:
- Malformed payloads that could crash the system
- Injection attacks through unvalidated strings
- Buffer overflow through unbounded data structures
- Type confusion attacks through improper validation
"""

from __future__ import annotations

from typing import Any

import pytest
from hypothesis import assume, given, settings, strategies as st

from cyberdelta.apis.backpack.bp_validators import BackpackValidators
from cyberdelta.apis.hyperliquid.hl_validators import HyperliquidValidators
from cyberdelta.apis.websocket.ws_validators import WebSocketPayloadValidators
from tests.common_symbols import SOL_BP


# =============================================================================
# HYPOTHESIS STRATEGIES FOR WEBSOCKET VALIDATION TESTING
# =============================================================================


@st.composite
def valid_dict_payload_strategy(
    draw: st.DrawFn, min_keys: int = 0, max_keys: int = 10
) -> dict[str, Any]:
    """Generate valid dictionary payloads for testing.

    Returns:
        Valid dictionary with various key-value combinations.
    """
    num_keys = draw(st.integers(min_value=min_keys, max_value=max_keys))

    keys = draw(
        st.lists(
            st.text(
                min_size=1,
                max_size=20,
                alphabet=st.characters(
                    whitelist_categories=("Lu", "Ll", "Nd"), whitelist_characters="_-"
                ),
            ),
            min_size=num_keys,
            max_size=num_keys,
            unique=True,
        )
    )

    values = draw(
        st.lists(
            st.one_of(
                st.text(max_size=100),
                st.integers(),
                st.floats(allow_nan=False, allow_infinity=False),
                st.booleans(),
                st.none(),
            ),
            min_size=num_keys,
            max_size=num_keys,
        )
    )

    return dict(zip(keys, values))


@st.composite
def valid_list_payload_strategy(
    draw: st.DrawFn, min_length: int = 0, max_length: int = 20, item_type: type | None = None
) -> list[Any]:
    """Generate valid list payloads for testing.

    Returns:
        Valid list with specified constraints.
    """
    length = draw(st.integers(min_value=min_length, max_value=max_length))

    if item_type is None:
        # Mixed types
        item_strategy = st.one_of(
            st.text(max_size=100),
            st.integers(),
            st.floats(allow_nan=False, allow_infinity=False),
            st.booleans(),
        )
    elif item_type == str:
        item_strategy = st.text(max_size=100)
    elif item_type == int:
        item_strategy = st.integers()
    elif item_type == float:
        item_strategy = st.floats(allow_nan=False, allow_infinity=False)
    elif item_type == bool:
        item_strategy = st.booleans()
    else:
        item_strategy = st.none()

    return draw(st.lists(item_strategy, min_size=length, max_size=length))


@st.composite
def valid_symbol_string_strategy(draw: st.DrawFn) -> str:
    """Generate valid symbol strings.

    Returns:
        Valid symbol string following exchange conventions.
        Pattern: ^[A-Z0-9_-]{1,20}$
    """
    # Generate symbols matching the exact pattern
    symbol = draw(
        st.text(min_size=1, max_size=20, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    )
    return symbol


@st.composite
def valid_topic_string_strategy(draw: st.DrawFn) -> str:
    """Generate valid topic strings.

    Returns:
        Valid topic string for WebSocket subscriptions.
        Pattern: ^[a-zA-Z0-9._-]{1,50}$
    """
    # Generate topics matching the exact pattern
    topic = draw(
        st.text(
            min_size=1,
            max_size=50,
            alphabet="abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-",
        )
    )
    return topic


@st.composite
def numeric_string_strategy(draw: st.DrawFn, allow_negative: bool = True) -> str:
    """Generate valid numeric strings.

    Returns:
        String representation of a number.
    """
    # Choose between integer and decimal
    if draw(st.booleans()):
        # Integer
        value = draw(st.integers(min_value=-1000000 if allow_negative else 0, max_value=1000000))
        return str(value)
    else:
        # Decimal
        value = draw(
            st.floats(
                min_value=-1000000.0 if allow_negative else 0.0,
                max_value=1000000.0,
                allow_nan=False,
                allow_infinity=False,
            )
        )
        return str(value)


# =============================================================================
# PROPERTY-BASED TESTS FOR WEBSOCKET VALIDATORS
# =============================================================================


class TestWebSocketPayloadValidators:
    """Property-based tests for WebSocketPayloadValidators functionality."""

    @given(payload=valid_dict_payload_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_dict_payload_properties(self, payload: dict[str, Any]) -> None:
        """Property: Valid dictionary payloads should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_dict_payload(payload)

        # Property: Payload should be preserved exactly
        assert result == payload
        assert result is payload  # Should return same object

    @given(
        payload=valid_dict_payload_strategy(min_keys=1, max_keys=10),
        min_keys=st.integers(min_value=0, max_value=5),
        max_keys=st.integers(min_value=5, max_value=15),
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_dict_payload_with_constraints_properties(
        self, payload: dict[str, Any], min_keys: int, max_keys: int
    ) -> None:
        """Property: Dictionary validation should respect size constraints."""
        from cyberdelta.apis.websocket.exceptions import PayloadSizeError

        num_keys = len(payload)

        if min_keys <= num_keys <= max_keys:
            # Should succeed
            result = WebSocketPayloadValidators.validate_dict_payload(
                payload, min_keys=min_keys, max_keys=max_keys
            )
            assert result == payload
        else:
            # Should fail with PayloadSizeError
            with pytest.raises(PayloadSizeError):
                WebSocketPayloadValidators.validate_dict_payload(
                    payload, min_keys=min_keys, max_keys=max_keys
                )

    @given(
        invalid_payload=st.one_of(
            st.lists(st.integers()),  # List instead of dict
            st.text(),  # String instead of dict
            st.integers(),  # Integer instead of dict
            st.none(),  # None instead of dict
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_dict_payload_type_rejection_properties(self, invalid_payload: Any) -> None:
        """Property: Non-dictionary types should be rejected with InvalidPayloadTypeError."""
        from cyberdelta.apis.websocket.exceptions import InvalidPayloadTypeError

        with pytest.raises(InvalidPayloadTypeError):
            WebSocketPayloadValidators.validate_dict_payload(invalid_payload)

    @given(payload=valid_list_payload_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_list_payload_properties(self, payload: list[Any]) -> None:
        """Property: Valid list payloads should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_list_payload(payload)

        # Property: Payload should be preserved exactly
        assert result == payload
        assert result is payload  # Should return same object

    @given(
        payload=valid_list_payload_strategy(),
        min_length=st.integers(min_value=0, max_value=5),
        max_length=st.integers(min_value=5, max_value=15),
        item_type=st.sampled_from([str, int, float, bool, None]),
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_list_payload_with_constraints_properties(
        self, payload: list[Any], min_length: int, max_length: int, item_type: type | None
    ) -> None:
        """Property: List validation should respect length and type constraints."""
        assume(min_length <= max_length)  # Valid constraint range

        payload_length = len(payload)

        # Check if payload meets type constraints
        type_matches = item_type is None or all(isinstance(item, item_type) for item in payload)

        # Check if payload meets length constraints
        length_valid = min_length <= payload_length <= max_length

        if length_valid and type_matches:
            # Should succeed
            result = WebSocketPayloadValidators.validate_list_payload(
                payload, min_length=min_length, max_length=max_length, item_type=item_type
            )
            assert result == payload
        else:
            # Should fail with specific exception types
            from cyberdelta.apis.websocket.exceptions import InvalidItemTypeError, PayloadSizeError

            with pytest.raises((PayloadSizeError, InvalidItemTypeError)):
                WebSocketPayloadValidators.validate_list_payload(
                    payload, min_length=min_length, max_length=max_length, item_type=item_type
                )

    @given(
        payload_length=st.integers(min_value=0, max_value=20),
        min_constraint=st.integers(min_value=0, max_value=50),
        max_constraint=st.integers(min_value=0, max_value=50),
    )
    @settings(max_examples=200, deadline=None)
    def test_list_length_constraint_properties(
        self, payload_length: int, min_constraint: int, max_constraint: int
    ) -> None:
        """Property: Length constraints should be enforced correctly."""
        from cyberdelta.apis.websocket.exceptions import PayloadSizeError

        assume(min_constraint <= max_constraint)  # Valid constraint range

        payload = list(range(payload_length))

        if min_constraint <= payload_length <= max_constraint:
            # Should succeed
            result = WebSocketPayloadValidators.validate_list_payload(
                payload, min_length=min_constraint, max_length=max_constraint
            )
            assert result == payload
        else:
            # Should fail with PayloadSizeError
            with pytest.raises(PayloadSizeError):
                WebSocketPayloadValidators.validate_list_payload(
                    payload, min_length=min_constraint, max_length=max_constraint
                )

    @given(
        invalid_payload=st.one_of(
            st.dictionaries(st.text(), st.integers()),  # Dict instead of list
            st.text(),  # String instead of list
            st.integers(),  # Integer instead of list
            st.none(),  # None instead of list
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_list_payload_type_rejection_properties(self, invalid_payload: Any) -> None:
        """Property: Non-list types should be rejected with InvalidPayloadTypeError."""
        from cyberdelta.apis.websocket.exceptions import InvalidPayloadTypeError

        with pytest.raises(InvalidPayloadTypeError):
            WebSocketPayloadValidators.validate_list_payload(invalid_payload)

    @given(
        all_fields=st.lists(
            st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Ll",))),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        num_required=st.integers(min_value=0, max_value=10),
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_required_fields_properties(
        self, all_fields: list[str], num_required: int
    ) -> None:
        """Property: Required fields validation should correctly enforce field presence."""
        from cyberdelta.apis.websocket.exceptions import MissingRequiredFieldsError

        num_required = min(num_required, len(all_fields))  # Can't require more than available
        required_fields = all_fields[:num_required]

        # Create payload with all fields
        full_payload = {field: f"value_{field}" for field in all_fields}

        # Should succeed with all fields present
        result = WebSocketPayloadValidators.validate_required_fields(full_payload, required_fields)
        assert result == full_payload

        # Test with missing fields
        if required_fields:
            partial_payload = {field: f"value_{field}" for field in all_fields[num_required:]}

            if partial_payload != full_payload:  # Only if we actually removed fields
                with pytest.raises(MissingRequiredFieldsError):
                    WebSocketPayloadValidators.validate_required_fields(
                        partial_payload, required_fields
                    )

    @given(
        allowed_fields=st.lists(
            st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Ll",))),
            min_size=1,
            max_size=10,
            unique=True,
        ),
        extra_fields=st.lists(
            st.text(min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Ll",))),
            min_size=0,
            max_size=5,
            unique=True,
        ),
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_optional_fields_properties(
        self, allowed_fields: list[str], extra_fields: list[str]
    ) -> None:
        """Property: Optional fields validation should reject unexpected fields."""
        from cyberdelta.apis.websocket.exceptions import UnexpectedFieldsError

        # Ensure extra fields don't overlap with allowed
        extra_fields = [f for f in extra_fields if f not in allowed_fields]

        # Create payload with some allowed fields
        num_used = min(3, len(allowed_fields))
        used_fields = allowed_fields[:num_used]
        payload = {field: f"value_{field}" for field in used_fields}

        # Should succeed with only allowed fields
        result = WebSocketPayloadValidators.validate_optional_fields(payload, allowed_fields)
        assert result == payload

        # Test with unexpected fields
        if extra_fields:
            bad_payload = {**payload, **{field: f"extra_{field}" for field in extra_fields}}
            with pytest.raises(UnexpectedFieldsError):
                WebSocketPayloadValidators.validate_optional_fields(bad_payload, allowed_fields)

    @given(symbol=valid_symbol_string_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_symbol_properties(self, symbol: str) -> None:
        """Property: Valid symbols should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_symbol(symbol)

        # Property: Symbol should be preserved exactly
        assert result == symbol

        # Property: Should be uppercase alphanumeric with optional separators
        assert all(c.isupper() or c.isdigit() or c in "_-" for c in result)

    @given(
        invalid_symbol=st.one_of(
            st.integers(),  # Wrong type
            st.floats(),  # Wrong type
            st.just(""),  # Empty
            st.text(
                min_size=1, max_size=5, alphabet=st.characters(whitelist_categories=("Ll",))
            ),  # Lowercase
            st.text(min_size=25, max_size=50),  # Too long
            st.text(min_size=1, max_size=10).filter(lambda s: " " in s),  # Contains space
            st.text(min_size=1, max_size=10).filter(
                lambda s: any(c in "@#$%^&*()" for c in s)
            ),  # Special chars
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_symbol_rejection_properties(self, invalid_symbol: Any) -> None:
        """Property: Invalid symbols should be rejected."""
        from cyberdelta.apis.websocket.exceptions import InvalidFieldTypeError, InvalidFormatError

        with pytest.raises((InvalidFieldTypeError, InvalidFormatError)):
            WebSocketPayloadValidators.validate_symbol(invalid_symbol)

    @given(topic=valid_topic_string_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_topic_properties(self, topic: str) -> None:
        """Property: Valid topics should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_topic(topic)

        # Property: Topic should be preserved exactly
        assert result == topic

        # Property: Should be valid topic format
        assert len(result) > 0
        assert len(result) <= 50

    @given(
        invalid_topic=st.one_of(
            st.integers(),  # Wrong type
            st.floats(),  # Wrong type
            st.just(""),  # Empty
            st.text(min_size=51, max_size=100),  # Too long
            st.text(min_size=1, max_size=20).filter(lambda s: " " in s),  # Contains space
            st.text(min_size=1, max_size=20).filter(
                lambda s: any(c in "@#$%^&*()" for c in s)
            ),  # Special chars
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_topic_rejection_properties(self, invalid_topic: Any) -> None:
        """Property: Invalid topics should be rejected."""
        from cyberdelta.apis.websocket.exceptions import InvalidFieldTypeError, InvalidFormatError

        with pytest.raises((InvalidFieldTypeError, InvalidFormatError)):
            WebSocketPayloadValidators.validate_topic(invalid_topic)

    @given(numeric_str=numeric_string_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_numeric_string_properties(self, numeric_str: str) -> None:
        """Property: Valid numeric strings should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_numeric_string(numeric_str)

        # Property: String should be preserved exactly
        assert result == numeric_str

        # Property: Should be parseable as float
        parsed = float(result)
        assert parsed == float(numeric_str)

    @given(
        numeric_str=numeric_string_strategy(),
        min_value=st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False),
        max_value=st.floats(min_value=-1000, max_value=1000, allow_nan=False, allow_infinity=False),
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_numeric_string_range_properties(
        self, numeric_str: str, min_value: float, max_value: float
    ) -> None:
        """Property: Range constraints should be enforced correctly."""
        from cyberdelta.apis.websocket.exceptions import NumericRangeError

        assume(min_value <= max_value)  # Valid range

        numeric_value = float(numeric_str)

        if min_value <= numeric_value <= max_value:
            # Should succeed
            result = WebSocketPayloadValidators.validate_numeric_string(
                numeric_str, min_value=min_value, max_value=max_value
            )
            assert result == numeric_str
        else:
            # Outside range - should raise NumericRangeError
            with pytest.raises(NumericRangeError):
                WebSocketPayloadValidators.validate_numeric_string(
                    numeric_str, min_value=min_value, max_value=max_value
                )

    @given(
        invalid_numeric=st.one_of(
            st.integers(),  # Wrong type
            st.floats(),  # Wrong type
            st.just(""),  # Empty
            st.text(
                min_size=1, max_size=10, alphabet=st.characters(whitelist_categories=("Ll",))
            ),  # Letters
            st.just("123.45.67"),  # Multiple decimals
            # Note: "12e34" is actually valid scientific notation for Python's float()
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_numeric_string_rejection_properties(self, invalid_numeric: Any) -> None:
        """Property: Invalid numeric strings should be rejected."""
        from cyberdelta.apis.websocket.exceptions import (
            InvalidFieldTypeError,
            InvalidNumericValueError,
        )

        # Scientific notation is actually valid for Python's float()
        if invalid_numeric == "12e34":
            # This is valid, should not be in invalid test
            result = WebSocketPayloadValidators.validate_numeric_string(invalid_numeric)
            assert result == invalid_numeric
        else:
            with pytest.raises((InvalidFieldTypeError, InvalidNumericValueError)):
                WebSocketPayloadValidators.validate_numeric_string(invalid_numeric)

    @given(
        timestamp=st.integers(
            min_value=946684800,  # Year 2000
            max_value=2147483647,  # Max 32-bit timestamp (2038)
        )
    )
    @settings(max_examples=200, deadline=None)
    def test_validate_timestamp_properties(self, timestamp: int) -> None:
        """Property: Valid timestamps should be accepted and preserved."""
        result = WebSocketPayloadValidators.validate_timestamp(timestamp)

        # Property: Timestamp should be preserved exactly
        assert result == timestamp

        # Property: Should be positive and reasonable
        assert result > 0
        assert result >= 946684800  # After year 2000

    @given(
        invalid_timestamp=st.one_of(
            st.text(),  # Wrong type (string)
            st.floats(max_value=-1.0),  # Negative floats
            st.floats(min_value=0.0, max_value=946684799.9),  # Before year 2000 (floats)
            st.integers(max_value=-1),  # Negative
            st.integers(min_value=0, max_value=946684799),  # Before year 2000
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_timestamp_rejection_properties(self, invalid_timestamp: Any) -> None:
        """Property: Invalid timestamps should be rejected."""
        from cyberdelta.apis.websocket.exceptions import InvalidTimestampError

        # Strings will cause TypeError during comparison
        if isinstance(invalid_timestamp, str):
            with pytest.raises(TypeError):
                WebSocketPayloadValidators.validate_timestamp(invalid_timestamp)
        # Numeric types (int or float) that are out of range will raise InvalidTimestampError
        elif isinstance(invalid_timestamp, (int, float)):
            with pytest.raises(InvalidTimestampError):
                WebSocketPayloadValidators.validate_timestamp(invalid_timestamp)
        else:
            # Other types
            with pytest.raises(TypeError):
                WebSocketPayloadValidators.validate_timestamp(invalid_timestamp)


@st.composite
def backpack_topic_strategy(draw: st.DrawFn) -> str:
    """Generate valid Backpack topic strings.

    Returns:
        Valid Backpack topic string with format 'type.SYMBOL'.
    """
    topic_type = draw(st.sampled_from(["depth", "ticker", "trade", "trades"]))
    # Ensure we generate a valid symbol (uppercase, alphanumeric, _, -)
    symbol = draw(
        st.text(min_size=1, max_size=20, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-")
    )
    return f"{topic_type}.{symbol}"


class TestBackpackValidators:
    """Property-based tests for BackpackValidators functionality."""

    @given(topic=backpack_topic_strategy())
    @settings(max_examples=200, deadline=None)
    def test_validate_backpack_topic_properties(self, topic: str) -> None:
        """Property: Valid Backpack topics should be parsed correctly."""
        topic_type, symbol = BackpackValidators.validate_backpack_topic(topic)

        # Property: Should split correctly
        assert topic == f"{topic_type}.{symbol}"

        # Property: Topic type should be valid
        assert topic_type in ["depth", "ticker", "trade", "trades"]

        # Property: Symbol should be uppercase
        assert all(c.isupper() or c.isdigit() or c in "_-" for c in symbol)

    @given(
        invalid_topic=st.one_of(
            st.just("depth"),  # Missing symbol
            st.builds(
                lambda t, s: f"{t}.{s}",
                st.sampled_from(["depth", "ticker", "trade", "trades"]),
                st.text(
                    min_size=1, max_size=5, alphabet=st.characters(whitelist_categories=("Ll",))
                ),
            ),  # Lowercase symbol
            st.builds(
                lambda t, s: f"{t}.{s}",
                st.text(min_size=1, max_size=10).filter(
                    lambda x: x not in ["depth", "ticker", "trade", "trades"]
                ),
                st.text(min_size=1, max_size=20, alphabet="ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"),
            ),  # Invalid topic type
            st.just(""),  # Empty
            st.text(min_size=1, max_size=20).filter(lambda s: s.count(".") > 1),  # Too many dots
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_backpack_topic_rejection_properties(self, invalid_topic: str) -> None:
        """Property: Invalid Backpack topics should be rejected."""
        from cyberdelta.apis.common.base_types import InvalidTopicFormatError, InvalidTopicTypeError
        from cyberdelta.apis.websocket.exceptions import InvalidFormatError

        with pytest.raises((
            InvalidTopicFormatError,
            InvalidTopicTypeError,
            InvalidFormatError,
            ValueError,
        )):
            BackpackValidators.validate_backpack_topic(invalid_topic)


class TestHyperliquidValidators:
    """Property-based tests for HyperliquidValidators functionality."""

    @given(
        channel=st.sampled_from([
            "l2Book",
            "trades",
            "userEvents",
            "allMids",
            "notification",
            "webData2",
            "subscriptionResponse",
            "fills",
            "orders",
            "candle",
        ])
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_hyperliquid_channel_properties(self, channel: str) -> None:
        """Property: Valid Hyperliquid channels should be accepted and preserved."""
        result = HyperliquidValidators.validate_hyperliquid_channel(channel)

        # Property: Channel should be preserved exactly
        assert result == channel

    @given(
        invalid_channel=st.text(min_size=1, max_size=20).filter(
            lambda x: x
            not in [
                "l2Book",
                "trades",
                "userEvents",
                "allMids",
                "notification",
                "webData2",
                "subscriptionResponse",
                "fills",
                "orders",
                "candle",
            ]
        )
    )
    @settings(max_examples=100, deadline=None)
    def test_validate_hyperliquid_channel_rejection_properties(self, invalid_channel: str) -> None:
        """Property: Unknown Hyperliquid channels should be rejected."""
        from cyberdelta.apis.common.base_types import InvalidChannelError

        with pytest.raises(InvalidChannelError):
            HyperliquidValidators.validate_hyperliquid_channel(invalid_channel)
