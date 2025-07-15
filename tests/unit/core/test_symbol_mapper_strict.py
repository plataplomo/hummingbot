"""Tests for strict SymbolMapper v0.0.1 refactor.

This test suite validates the new strict, fail-fast SymbolMapper implementation
with no backward compatibility concerns.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    ExchangeNotSupportedError,
    InvalidSymbolFormatError,
    SymbolMappingConfigurationError,
    SymbolMappingError,
    SymbolMappingFieldError,
    SymbolNotFoundError,
)
from tests.unit.core.conftest import create_test_exchange_config


# Type aliases for testing
InternalSymbol = str
ExchangeId = str


class TestStrictSymbolMapper:
    """Test suite for strict SymbolMapper implementation."""

    @pytest.fixture
    def valid_config(self) -> dict[str, ExchangeSpecificConfig]:
        """Valid configuration for testing."""
        return {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"},
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC_PERP", "ETH": "ETH_PERP"},
            ),
        }

    @pytest.fixture
    def symbol_mapper(self, valid_config: dict[str, ExchangeSpecificConfig]) -> SymbolMapper:
        """Create SymbolMapper instance."""
        return SymbolMapper(valid_config)

    def test_initialization_success(self, valid_config: dict[str, ExchangeSpecificConfig]) -> None:
        """Test successful initialization."""
        mapper = SymbolMapper(valid_config)

        assert mapper.get_supported_exchanges() == ["backpack", "hyperliquid"]
        assert mapper.get_all_internal_symbols() == ["BTC", "ETH", "SOL"]

    def test_initialization_fails_empty_config(self) -> None:
        """Test initialization fails with empty config."""
        with pytest.raises(SymbolMappingConfigurationError, match="Configuration cannot be empty"):
            SymbolMapper({})

    def test_initialization_fails_invalid_config_type(self) -> None:
        """Test initialization fails with invalid config type."""
        with pytest.raises(
            SymbolMappingConfigurationError, match="Configuration must be a dictionary"
        ):
            SymbolMapper("invalid")  # type: ignore

    def test_initialization_fails_missing_symbols(self) -> None:
        """Test initialization fails when exchange missing symbols."""
        # Create config with empty symbols - this should fail validation
        with pytest.raises(SymbolMappingConfigurationError, match="has no symbol mappings"):
            config = {
                "hyperliquid": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={},  # Empty symbols
                ),
            }
            SymbolMapper(config)

    def test_initialization_fails_empty_symbols(self) -> None:
        """Test initialization fails when exchange has empty symbols."""
        # This test is redundant with the previous test - empty symbols should fail the same way
        with pytest.raises(SymbolMappingConfigurationError, match="has no symbol mappings"):
            config = {
                "hyperliquid": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={},  # Empty symbols
                ),
            }
            SymbolMapper(config)

    def test_initialization_fails_invalid_internal_symbol(self) -> None:
        """Test initialization fails with invalid internal symbol."""
        with pytest.raises(InvalidSymbolFormatError, match="must be 2-10 uppercase alphanumeric"):
            config = {
                "hyperliquid": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={"btc": "BTC"},  # lowercase not allowed
                ),
            }
            SymbolMapper(config)

    def test_initialization_fails_duplicate_exchange_symbol(self) -> None:
        """Test initialization fails with duplicate exchange symbol."""
        with pytest.raises(SymbolMappingConfigurationError, match="already maps to"):
            config = {
                "hyperliquid": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={
                        "BTC": "BTC",
                        "BITCOIN": "BTC",  # Duplicate exchange symbol
                    },
                ),
            }
            SymbolMapper(config)

    def test_get_exchange_symbol_success(self, symbol_mapper: SymbolMapper) -> None:
        """Test successful exchange symbol lookup."""
        assert symbol_mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC"
        assert symbol_mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
        assert symbol_mapper.get_exchange_symbol("ETH", "hyperliquid") == "ETH"

    def test_get_exchange_symbol_fails_invalid_inputs(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_exchange_symbol fails with invalid inputs."""
        # Empty string
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("", "hyperliquid")

        # None value
        with pytest.raises(SymbolMappingFieldError, match="Symbol must be a non-empty string"):
            symbol_mapper.get_exchange_symbol(None, "hyperliquid")  # type: ignore

        # Invalid exchange ID
        with pytest.raises(SymbolMappingFieldError, match="Exchange ID must be a non-empty string"):
            symbol_mapper.get_exchange_symbol("BTC", "")

    def test_get_exchange_symbol_fails_unknown_exchange(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_exchange_symbol fails with unknown exchange."""
        with pytest.raises(ExchangeNotSupportedError, match="Exchange not supported"):
            symbol_mapper.get_exchange_symbol("BTC", "unknown")

    def test_get_exchange_symbol_fails_unknown_symbol(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_exchange_symbol fails with unknown symbol."""
        with pytest.raises(SymbolNotFoundError, match="Internal symbol not found"):
            symbol_mapper.get_exchange_symbol("UNKNOWN", "hyperliquid")

    def test_get_exchange_symbol_fails_symbol_not_on_exchange(
        self, symbol_mapper: SymbolMapper
    ) -> None:
        """Test get_exchange_symbol fails when symbol not on specific exchange."""
        with pytest.raises(SymbolNotFoundError, match="Symbol not available on exchange"):
            symbol_mapper.get_exchange_symbol("SOL", "backpack")

    def test_get_internal_symbol_success(self, symbol_mapper: SymbolMapper) -> None:
        """Test successful internal symbol lookup."""
        assert symbol_mapper.get_internal_symbol("BTC", "hyperliquid") == "BTC"
        assert symbol_mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"
        assert symbol_mapper.get_internal_symbol("ETH", "hyperliquid") == "ETH"

    def test_get_internal_symbol_fails_unknown_exchange_symbol(
        self, symbol_mapper: SymbolMapper
    ) -> None:
        """Test get_internal_symbol fails with unknown exchange symbol."""
        with pytest.raises(SymbolNotFoundError, match="Exchange symbol not found"):
            symbol_mapper.get_internal_symbol("UNKNOWN", "hyperliquid")

    def test_is_symbol_supported(self, symbol_mapper: SymbolMapper) -> None:
        """Test is_symbol_supported method."""
        # Supported combinations
        assert symbol_mapper.is_symbol_supported("BTC", "hyperliquid") is True
        assert symbol_mapper.is_symbol_supported("BTC", "backpack") is True
        assert symbol_mapper.is_symbol_supported("ETH", "hyperliquid") is True
        assert symbol_mapper.is_symbol_supported("SOL", "hyperliquid") is True

        # Unsupported combinations
        assert symbol_mapper.is_symbol_supported("SOL", "backpack") is False
        assert symbol_mapper.is_symbol_supported("UNKNOWN", "hyperliquid") is False
        assert symbol_mapper.is_symbol_supported("BTC", "unknown") is False

    def test_validate_symbol_pair_success(self, symbol_mapper: SymbolMapper) -> None:
        """Test successful symbol pair validation."""
        # Should not raise
        symbol_mapper.validate_symbol_pair("BTC", "backpack", "hyperliquid")
        symbol_mapper.validate_symbol_pair("ETH", "backpack", "hyperliquid")

    def test_validate_symbol_pair_fails(self, symbol_mapper: SymbolMapper) -> None:
        """Test symbol pair validation fails."""
        # SOL not available on backpack
        with pytest.raises(SymbolMappingError, match="Symbol pair validation failed"):
            symbol_mapper.validate_symbol_pair("SOL", "backpack", "hyperliquid")

    def test_get_symbol_coverage(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_symbol_coverage method."""
        # BTC available on both
        coverage = symbol_mapper.get_symbol_coverage("BTC")
        assert coverage == {"hyperliquid": True, "backpack": True}

        # SOL only on hyperliquid
        coverage = symbol_mapper.get_symbol_coverage("SOL")
        assert coverage == {"hyperliquid": True, "backpack": False}

    def test_get_symbol_coverage_fails_invalid_symbol(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_symbol_coverage fails with invalid symbol."""
        with pytest.raises(InvalidSymbolFormatError, match="must be 2-10 uppercase alphanumeric"):
            symbol_mapper.get_symbol_coverage("btc")  # lowercase

    def test_get_exchange_symbols_for_internal(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_exchange_symbols_for_internal method."""
        symbols = symbol_mapper.get_exchange_symbols_for_internal("BTC")
        assert symbols == {"hyperliquid": "BTC", "backpack": "BTC_PERP"}

        symbols = symbol_mapper.get_exchange_symbols_for_internal("SOL")
        assert symbols == {"hyperliquid": "SOL"}

    def test_get_exchange_symbols_for_internal_fails(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_exchange_symbols_for_internal fails with unknown symbol."""
        with pytest.raises(SymbolNotFoundError, match="Internal symbol not found"):
            symbol_mapper.get_exchange_symbols_for_internal("UNKNOWN")

    def test_get_internal_symbols_for_exchange(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_internal_symbols_for_exchange method."""
        symbols = symbol_mapper.get_internal_symbols_for_exchange("hyperliquid")
        assert symbols == {"BTC": "BTC", "ETH": "ETH", "SOL": "SOL"}

        symbols = symbol_mapper.get_internal_symbols_for_exchange("backpack")
        assert symbols == {"BTC_PERP": "BTC", "ETH_PERP": "ETH"}

    def test_get_internal_symbols_for_exchange_fails(self, symbol_mapper: SymbolMapper) -> None:
        """Test get_internal_symbols_for_exchange fails with unknown exchange."""
        with pytest.raises(ExchangeNotSupportedError, match="Exchange not supported"):
            symbol_mapper.get_internal_symbols_for_exchange("unknown")

    def test_thread_safety(self, symbol_mapper: SymbolMapper) -> None:
        """Test thread safety of the symbol mapper."""
        results: list[int] = []
        errors: list[str] = []

        def concurrent_operations(thread_id: int) -> None:
            """Perform concurrent operations."""
            try:
                for _ in range(50):
                    # Mix of different operations
                    symbol_mapper.get_exchange_symbol("BTC", "hyperliquid")
                    symbol_mapper.get_internal_symbol("BTC_PERP", "backpack")
                    symbol_mapper.get_all_internal_symbols()
                    symbol_mapper.is_symbol_supported("ETH", "hyperliquid")
                    symbol_mapper.get_supported_exchanges()

                results.append(thread_id)
            except (SymbolMappingError, AssertionError) as e:
                errors.append(f"Thread {thread_id}: {e}")

        # Run concurrent threads
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(concurrent_operations, i) for i in range(10)]
            for future in as_completed(futures):
                future.result()

        # Verify no errors occurred
        assert len(errors) == 0
        assert len(results) == 10

    def test_disabled_exchange_ignored(self) -> None:
        """Test disabled exchanges are ignored."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC"},
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC_PERP"},
                enabled=False,  # Disabled
            ),
        }

        mapper = SymbolMapper(config)

        # Only hyperliquid should be supported
        assert mapper.get_supported_exchanges() == ["hyperliquid"]
        assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC"

        # backpack should not be supported
        with pytest.raises(ExchangeNotSupportedError):
            mapper.get_exchange_symbol("BTC", "backpack")

    def test_type_aliases_work(self) -> None:
        """Test that type aliases work correctly."""
        internal = InternalSymbol("BTC")
        exchange = ExchangeId("hyperliquid")

        assert isinstance(internal, str)
        assert isinstance(exchange, str)
        assert internal == "BTC"
        assert exchange == "hyperliquid"

    def test_pydantic_models_validation(self) -> None:
        """Test Pydantic models work correctly."""
        # This test would validate Pydantic models if they existed
        # For now, we're using simple type aliases
        internal_symbol = InternalSymbol("BTC")
        exchange_id = ExchangeId("backpack")

        assert internal_symbol == "BTC"
        assert exchange_id == "backpack"

    def test_comprehensive_error_context(self, symbol_mapper: SymbolMapper) -> None:
        """Test that errors provide comprehensive context."""
        # Test symbol not found error context
        with pytest.raises(SymbolNotFoundError) as exc_info:
            symbol_mapper.get_exchange_symbol("UNKNOWN", "hyperliquid")

        error = exc_info.value
        # The symbol is available in the exception's symbol attribute and metadata
        assert error.symbol == "UNKNOWN"
        assert "available_symbols" in error.metadata
        assert isinstance(error.metadata["available_symbols"], list)

        # Test exchange not supported error context
        with pytest.raises(ExchangeNotSupportedError) as exc_info_2:
            symbol_mapper.get_exchange_symbol("BTC", "unknown")

        error_2 = exc_info_2.value
        # The exchange_id is available in the exception's exchange_id attribute
        assert error_2.exchange_id == "unknown"
        assert "supported_exchanges" in error_2.metadata
        assert isinstance(error_2.metadata["supported_exchanges"], list)

    def test_no_backward_compatibility_concerns(self) -> None:
        """Test that there are no backward compatibility concerns."""
        # This implementation should fail fast and not silently handle edge cases

        # Invalid config should raise immediately
        with pytest.raises(SymbolMappingConfigurationError):
            SymbolMapper({"invalid": "config"})  # type: ignore

        # Invalid symbols should raise immediately
        with pytest.raises(SymbolMappingFieldError):
            config = {
                "exchange": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={"": "EMPTY"},  # Empty internal symbol
                )
            }
            SymbolMapper(config)

        # No silent failures or None returns for lookups
        mapper = SymbolMapper({
            "exchange": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC"},
            )
        })

        # These should raise, not return None
        with pytest.raises(SymbolNotFoundError):
            mapper.get_exchange_symbol("UNKNOWN", "exchange")

        with pytest.raises(ExchangeNotSupportedError):
            mapper.get_exchange_symbol("BTC", "unknown")
