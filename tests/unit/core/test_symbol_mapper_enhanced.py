"""Enhanced tests for SymbolMapper improvements.

This module tests the enhanced features added to SymbolMapper including:
- Thread safety
- Input validation
- New validation methods
- Type safety improvements
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import pytest

from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.core.symbol_types import (
    ExchangeId,
    InternalSymbol,
    SymbolMapping,
)
from cyberdelta.exceptions import (
    InvalidSymbolFormatError,
    SymbolMappingError,
    SymbolMappingFieldError,
)


class TestSymbolMapperEnhancements:
    """Test suite for SymbolMapper enhancements."""

    @pytest.fixture
    def test_config(self) -> dict[str, Any]:
        """Create test configuration."""
        return {
            "hyperliquid": {
                "symbols": {
                    "BTC": "BTC",
                    "ETH": "ETH",
                    "SOL": "SOL",
                },
                "enabled": True,
            },
            "backpack": {
                "symbols": {
                    "BTC": "BTC_PERP",
                    "ETH": "ETH_PERP",
                    # Note: SOL is not available on backpack in this test
                },
                "enabled": True,
            },
        }

    @pytest.fixture
    def symbol_mapper(self, test_config: dict[str, Any]) -> SymbolMapper:
        """Create SymbolMapper instance."""
        return SymbolMapper(test_config)

    def test_input_validation_none_values(self, symbol_mapper: SymbolMapper) -> None:
        """Test that None inputs raise proper exceptions."""
        # Test get_exchange_symbol with None values
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol(None, "hyperliquid")  # type: ignore

        with pytest.raises(SymbolMappingFieldError, match="Exchange ID must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("BTC", None)  # type: ignore

        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol(None, None)  # type: ignore

        # Test get_internal_symbol with None values
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_internal_symbol(None, "hyperliquid")  # type: ignore

        with pytest.raises(SymbolMappingFieldError, match="Exchange ID must be a non-empty string"):
            symbol_mapper.get_internal_symbol("BTC", None)  # type: ignore

        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_internal_symbol(None, None)  # type: ignore

    def test_input_validation_empty_strings(self, symbol_mapper: SymbolMapper) -> None:
        """Test that empty strings raise proper exceptions."""
        # Test get_exchange_symbol with empty strings
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("", "hyperliquid")

        with pytest.raises(SymbolMappingFieldError, match="Exchange ID must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("BTC", "")

        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("", "")

        # Test get_internal_symbol with empty strings
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_internal_symbol("", "hyperliquid")

        with pytest.raises(SymbolMappingFieldError, match="Exchange ID must be a non-empty string"):
            symbol_mapper.get_internal_symbol("BTC", "")

        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_internal_symbol("", "")

    def test_is_symbol_supported(self, symbol_mapper: SymbolMapper) -> None:
        """Test is_symbol_supported method."""
        # Test supported symbols
        assert symbol_mapper.is_symbol_supported("BTC", "hyperliquid") is True
        assert symbol_mapper.is_symbol_supported("ETH", "backpack") is True

        # Test unsupported symbols
        assert symbol_mapper.is_symbol_supported("SOL", "backpack") is False
        assert symbol_mapper.is_symbol_supported("DOGE", "hyperliquid") is False
        assert symbol_mapper.is_symbol_supported("BTC", "unknown_exchange") is False

    def test_validate_symbol_pair(self, symbol_mapper: SymbolMapper) -> None:
        """Test validate_symbol_pair method."""
        # Test valid pairs - should not raise exceptions
        symbol_mapper.validate_symbol_pair("BTC", "backpack", "hyperliquid")
        symbol_mapper.validate_symbol_pair("ETH", "backpack", "hyperliquid")

        # Test invalid pairs - SOL not on backpack
        with pytest.raises(Exception, match="Symbol pair validation failed"):
            symbol_mapper.validate_symbol_pair("SOL", "backpack", "hyperliquid")

        # Test invalid pairs - unknown symbol
        with pytest.raises(SymbolMappingError):
            symbol_mapper.validate_symbol_pair("DOGE", "backpack", "hyperliquid")

        # Test invalid pairs - unknown exchange
        with pytest.raises(SymbolMappingError):
            symbol_mapper.validate_symbol_pair("BTC", "unknown", "hyperliquid")

    def test_get_symbol_coverage(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_symbol_coverage method."""
        # Test BTC - available on both
        coverage = symbol_mapper.get_symbol_coverage("BTC")
        assert coverage == {"hyperliquid": True, "backpack": True}

        # Test SOL - only on hyperliquid
        coverage = symbol_mapper.get_symbol_coverage("SOL")
        assert coverage == {"hyperliquid": True, "backpack": False}

        # Test unknown symbol (valid format but not in config)
        coverage = symbol_mapper.get_symbol_coverage("DOGE")
        assert coverage == {"hyperliquid": False, "backpack": False}

        # Test invalid symbol format should raise exception
        with pytest.raises(InvalidSymbolFormatError, match="must be 2-10 uppercase alphanumeric"):
            symbol_mapper.get_symbol_coverage("btc")  # lowercase

    def test_thread_safety_concurrent_reads(self, symbol_mapper: SymbolMapper) -> None:
        """Test thread safety with concurrent read operations."""
        results = []
        errors = []

        def read_symbols(thread_id: int) -> None:
            """Perform multiple read operations."""
            try:
                for _ in range(100):
                    # Multiple types of reads
                    symbol = symbol_mapper.get_exchange_symbol("BTC", "hyperliquid")
                    internal = symbol_mapper.get_internal_symbol("BTC_PERP", "backpack")
                    all_symbols = symbol_mapper.get_all_internal_symbols()
                    exchange_symbols = symbol_mapper.get_exchange_symbols_for_internal("ETH")
                    internal_symbols = symbol_mapper.get_internal_symbols_for_exchange(
                        "hyperliquid"
                    )

                    # Validate results
                    assert symbol == "BTC"
                    assert internal == "BTC"
                    assert "BTC" in all_symbols
                    assert exchange_symbols.get("backpack") == "ETH_PERP"
                    assert internal_symbols.get("ETH") == "ETH"

                results.append(f"Thread {thread_id} completed")
            except (SymbolMappingError, AssertionError) as e:
                errors.append(f"Thread {thread_id} error: {e}")

        # Run concurrent threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(read_symbols, i) for i in range(10)]
            for future in as_completed(futures):
                future.result()

        # Check results
        assert len(errors) == 0
        assert len(results) == 10

    def test_thread_safety_concurrent_mixed_operations(self, test_config: dict[str, Any]) -> None:
        """Test thread safety with mixed read/validation operations."""
        symbol_mapper = SymbolMapper(test_config)
        results = []
        errors = []
        lock = threading.Lock()

        def mixed_operations(thread_id: int) -> None:
            """Perform mixed operations."""
            try:
                for i in range(50):
                    if i % 3 == 0:
                        # Validation operation - should not raise for valid symbols
                        symbol_mapper.validate_symbol_pair("BTC", "backpack", "hyperliquid")
                    elif i % 3 == 1:
                        # Coverage check
                        coverage = symbol_mapper.get_symbol_coverage("ETH")
                        assert coverage["hyperliquid"] is True
                    else:
                        # Symbol lookup
                        symbol = symbol_mapper.get_exchange_symbol("SOL", "hyperliquid")
                        assert symbol == "SOL"

                with lock:
                    results.append(thread_id)
            except (SymbolMappingError, AssertionError) as e:
                with lock:
                    errors.append(f"Thread {thread_id}: {e}")

        # Run concurrent threads
        threads = []
        for i in range(20):
            thread = threading.Thread(target=mixed_operations, args=(i,))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        # Check for errors
        if errors:
            pytest.fail(f"Thread errors occurred: {errors}")

        assert len(results) == 20

    def test_type_aliases_usage(self) -> None:
        """Test that type aliases work correctly."""
        # Create typed values
        internal = InternalSymbol("BTC")
        exchange = ExchangeId("hyperliquid")

        # These should work without type errors
        assert isinstance(internal, str)
        assert isinstance(exchange, str)
        assert internal == "BTC"
        assert exchange == "hyperliquid"

    def test_symbol_mapping_validation(self) -> None:
        """Test SymbolMapping Pydantic model validation."""
        # Valid mapping
        mapping = SymbolMapping(
            internal_symbol="BTC", exchange_symbol="BTC_PERP", exchange_id="backpack"
        )
        assert mapping.internal_symbol == "BTC"
        assert mapping.exchange_symbol == "BTC_PERP"
        assert mapping.exchange_id == "backpack"

        # Invalid internal symbol format
        with pytest.raises(ValueError, match="pattern"):
            SymbolMapping(
                internal_symbol="btc",  # lowercase not allowed
                exchange_symbol="BTC_PERP",
                exchange_id="backpack",
            )

        # Empty values
        with pytest.raises(ValueError):
            SymbolMapping(internal_symbol="", exchange_symbol="BTC_PERP", exchange_id="backpack")

    def test_protocol_compliance(self, symbol_mapper: SymbolMapper) -> None:
        """Test that SymbolMapper complies with extended ISymbolMapper protocol."""
        # All these methods should exist and work
        assert hasattr(symbol_mapper, "get_exchange_symbol")
        assert hasattr(symbol_mapper, "get_internal_symbol")
        assert hasattr(symbol_mapper, "get_all_internal_symbols")
        assert hasattr(symbol_mapper, "get_exchange_symbols_for_internal")
        assert hasattr(symbol_mapper, "get_internal_symbols_for_exchange")
        assert hasattr(symbol_mapper, "is_symbol_supported")
        assert hasattr(symbol_mapper, "validate_symbol_pair")

        # Test they return expected types
        assert isinstance(symbol_mapper.get_all_internal_symbols(), list)
        assert isinstance(symbol_mapper.get_exchange_symbols_for_internal("BTC"), dict)
        assert isinstance(symbol_mapper.get_internal_symbols_for_exchange("hyperliquid"), dict)
        assert isinstance(symbol_mapper.is_symbol_supported("BTC", "hyperliquid"), bool)

        # validate_symbol_pair should not raise for valid pairs
        # In strict mode, no exception means success
        symbol_mapper.validate_symbol_pair("BTC", "backpack", "hyperliquid")
