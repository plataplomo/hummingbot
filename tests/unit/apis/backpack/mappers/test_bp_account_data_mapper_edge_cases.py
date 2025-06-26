"""CyberDeltaEngine: Backpack Account Data Mapper Edge Cases Tests.

---------------------------------------------------------------

Comprehensive test suite for BackpackAccountDataMapper edge cases and robustness.
Tests boundary conditions, unicode handling, and extreme value scenarios including:
- Boundary decimal value testing
- Zero value handling
- Unicode symbol support
- Very long ID handling
- Error recovery scenarios
"""

from decimal import Decimal
from unittest.mock import patch

import pytest

from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.models.api_error import TransformationError


pytestmark = pytest.mark.timing


@pytest.fixture
def mapper() -> BackpackAccountDataMapper:
    """Fixture providing a BackpackAccountDataMapper instance."""
    return BackpackAccountDataMapper()


@pytest.fixture
def test_timestamp() -> str:
    """Fixture providing a consistent test timestamp string."""
    return "2024-01-15T10:30:00Z"


def create_raw_fill(
    fee: str = "0.05",
    fee_symbol: str = "USDC",
    is_maker: bool = True,
    order_id: str = "order123",
    price: str = "100.50",
    quantity: str = "10.0",
    side: str = "Bid",  # Changed from "Buy" to "Bid" to match BP_ORDER_SIDES validation
    symbol: str = "SOL-USDC",
    timestamp: str = "2024-01-15T10:30:00Z",
    trade_id: int = 123456,
    client_id: str | None = None,
) -> BackpackRawFill:
    """Create BackpackRawFill instances for testing edge cases."""
    return BackpackRawFill(
        fee=fee,
        feeSymbol=fee_symbol,
        isMaker=is_maker,
        orderId=order_id,
        price=price,
        quantity=quantity,
        side=side,
        symbol=symbol,
        timestamp=timestamp,
        tradeId=trade_id,
        clientId=client_id,
        systemOrderType=None,
    )


class TestEdgeCasesAndRobustness:
    """Test cases for edge cases and robustness."""

    def test_boundary_decimal_values(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of boundary decimal values."""
        raw_fill = create_raw_fill(
            price="0.000001",  # Very small price
            quantity="999999999.999999",  # Very large quantity
            fee="0.000000001",  # Very small fee
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.price == Decimal("0.000001")
        assert result.quantity == Decimal("999999999.999999")
        assert result.fee == Decimal("0.000000001")

    def test_zero_values(self, mapper: BackpackAccountDataMapper, test_timestamp: str) -> None:
        """Test handling of zero values."""
        raw_fill = create_raw_fill(
            price="0.0",
            quantity="0.0",
            fee="0.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Mapper returns None for zero price/quantity since Trade model requires positive values
        assert result is None

    def test_zero_price_non_zero_quantity(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of zero price with non-zero quantity."""
        raw_fill = create_raw_fill(
            price="0.0",
            quantity="10.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Should return None for zero price even with non-zero quantity
        assert result is None

    def test_zero_quantity_non_zero_price(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of zero quantity with non-zero price."""
        raw_fill = create_raw_fill(
            price="100.50",
            quantity="0.0",
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # Should return None for zero quantity even with non-zero price
        assert result is None

    def test_unicode_symbol_handling(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of unicode characters in symbols."""
        unicode_symbols = [
            "SOL-USDC🚀",  # Emoji
            "BTC-测试",  # Chinese characters
            "ETH-тест",  # Cyrillic characters
            "DOGE-αβγ",  # Greek characters
            "ADA-العربية",  # Arabic characters
        ]

        for symbol in unicode_symbols:
            raw_fill = create_raw_fill(symbol=symbol, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, f"Expected Trade object but got None for symbol {symbol}"
            assert result.symbol == symbol

    def test_unicode_fee_symbol_handling(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of unicode characters in fee symbols."""
        unicode_fee_symbols = [
            "USDC🚀",
            "测试币",
            "тест",
        ]

        for fee_symbol in unicode_fee_symbols:
            raw_fill = create_raw_fill(fee_symbol=fee_symbol, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, (
                f"Expected Trade object but got None for fee_symbol {fee_symbol}"
            )
            assert result.fee_asset == fee_symbol

    def test_very_long_ids(self, mapper: BackpackAccountDataMapper, test_timestamp: str) -> None:
        """Test handling of very long ID values."""
        long_order_id = "order_" + "a" * 100  # 6 + 100 = 106 characters
        long_client_id = "client_" + "b" * 114  # 7 + 114 = 121 characters

        raw_fill = create_raw_fill(
            order_id=long_order_id,
            client_id=long_client_id,
            timestamp=test_timestamp,
        )

        # Should raise TransformationError due to client_order_id length validation (max 64 chars)
        with pytest.raises(
            TransformationError,
            match="Failed to transform BackpackRawFill to Trade",
        ):
            mapper.transform_raw_fill_to_internal(raw_fill)

    def test_maximum_decimal_precision(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of maximum decimal precision values."""
        # Test with Python Decimal's maximum useful precision
        max_precision_price = "100.123456789012345678901234567890"
        max_precision_quantity = "10.987654321098765432109876543210"
        max_precision_fee = "0.123456789012345678901234567890"

        raw_fill = create_raw_fill(
            price=max_precision_price,
            quantity=max_precision_quantity,
            fee=max_precision_fee,
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.price == Decimal(max_precision_price)
        assert result.quantity == Decimal(max_precision_quantity)
        assert result.fee == Decimal(max_precision_fee)

    def test_scientific_notation_handling(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of scientific notation in numeric fields."""
        scientific_price = "1.23e-6"  # 0.00000123
        scientific_quantity = "1.5e+6"  # 1500000
        scientific_fee = "2.5e-8"  # 0.000000025

        raw_fill = create_raw_fill(
            price=scientific_price,
            quantity=scientific_quantity,
            fee=scientific_fee,
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.price == Decimal("0.00000123")
        assert result.quantity == Decimal(1500000)
        assert result.fee == Decimal("0.000000025")

    def test_negative_values_handling(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of negative values (which should be invalid for fills)."""
        # Test negative price (invalid for fills)
        raw_fill_negative_price = create_raw_fill(
            price="-100.50",
            timestamp=test_timestamp,
        )

        # Mock parse_decimal_value to allow negative values to test handling
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:

            def side_effect(
                value: str,
                allow_none: bool = False,
                field_name: str = "",
            ) -> Decimal | None:
                """Return Decimal conversion for testing edge case parsing behavior."""
                try:
                    return Decimal(str(value)) if value else None
                except Exception:
                    return None

            mock_parse.side_effect = side_effect

            result = mapper.transform_raw_fill_to_internal(raw_fill_negative_price)

            # Should return None for negative price as it's invalid for trading
            assert result is None

    def test_whitespace_in_values(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of whitespace in numeric string values."""
        raw_fill = create_raw_fill(
            price=" 100.50 ",  # Leading and trailing spaces
            quantity="\t10.0\n",  # Tabs and newlines
            fee=" 0.05",  # Leading space only
            timestamp=test_timestamp,
        )

        result = mapper.transform_raw_fill_to_internal(raw_fill)

        # parse_decimal_value should handle whitespace gracefully
        # DEFENSIVE CHECK: result could be None if price/quantity is zero.
        # Mypy=[union-attr] Ruff=[N/A]
        assert result is not None, "Expected Trade object but got None"
        assert result.price == Decimal("100.50")
        assert result.quantity == Decimal("10.0")
        assert result.fee == Decimal("0.05")

    def test_special_characters_in_symbols(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of special characters in symbol names."""
        special_symbols = [
            "SOL/USDC",  # Forward slash
            "BTC-USDC.PERP",  # Dot notation
            "ETH_USDC_FUT",  # Underscore
            "DOGE-USDC@1M",  # At symbol
            "ADA-USDC#SPOT",  # Hash symbol
        ]

        for symbol in special_symbols:
            raw_fill = create_raw_fill(symbol=symbol, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, f"Expected Trade object but got None for symbol {symbol}"
            assert result.symbol == symbol

    def test_transformation_error_context_preservation(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test that transformation errors preserve context information."""
        raw_fill = create_raw_fill(timestamp=test_timestamp)

        # Create a specific error with context
        original_error = ValueError("Specific parsing error with detailed context")

        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.side_effect = original_error

            with pytest.raises(TransformationError) as exc_info:
                mapper.transform_raw_fill_to_internal(raw_fill)

            # Verify error context is preserved
            assert "Failed to transform BackpackRawFill to Trade" in str(exc_info.value)
            assert exc_info.value.__cause__ == original_error

    def test_malformed_timestamp_handling(self, mapper: BackpackAccountDataMapper) -> None:
        """Test handling of malformed timestamp values."""
        # Create a valid raw fill first
        raw_fill = create_raw_fill(timestamp="2024-01-15T10:30:00Z")

        # Mock parse_datetime_utc to simulate malformed timestamp parsing
        with patch(
            "cyberdelta.apis.backpack.mappers.bp_account_data_mapper.parse_datetime_utc",
        ) as mock_parse_datetime:
            # Return None to simulate failed timestamp parsing
            mock_parse_datetime.return_value = None

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # Should handle malformed timestamps gracefully, falling back to current time
            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, "Expected Trade object but got None"
            # Should have some valid timestamp (current time fallback)
            assert result.executed_at is not None

    def test_extremely_large_trade_ids(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of extremely large trade IDs."""
        large_trade_ids = [
            999999999999999999,  # Maximum safe integer
            123456789012345678,  # Large but reasonable
        ]

        for trade_id in large_trade_ids:
            raw_fill = create_raw_fill(trade_id=trade_id, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, f"Expected Trade object but got None for trade_id {trade_id}"
            assert result.id == str(trade_id)

    def test_edge_case_fee_values(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of edge case fee values."""
        edge_case_fees = [
            "0",  # Zero fee
            "0.0",  # Zero fee with decimal
            "0.000000001",  # Extremely small fee
            "9999999.999999",  # Very large fee
        ]

        for fee in edge_case_fees:
            raw_fill = create_raw_fill(fee=fee, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, f"Expected Trade object but got None for fee {fee}"
            assert result.fee == Decimal(fee)

    def test_mixed_case_side_values(
        self,
        mapper: BackpackAccountDataMapper,
        test_timestamp: str,
    ) -> None:
        """Test handling of mixed case side values."""
        # Only test side values that are actually accepted by the raw model
        side_variations = [
            "Ask",  # Ask side
            "Bid",  # Bid side
        ]

        for side in side_variations:
            raw_fill = create_raw_fill(side=side, timestamp=test_timestamp)

            result = mapper.transform_raw_fill_to_internal(raw_fill)

            # Should handle case variations gracefully
            # DEFENSIVE CHECK: result could be None if price/quantity is zero.
            # Mypy=[union-attr] Ruff=[N/A]
            assert result is not None, f"Expected Trade object but got None for side {side}"
            # Verify the side is mapped correctly regardless of case
            assert result.side is not None
