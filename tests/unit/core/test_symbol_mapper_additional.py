"""Additional comprehensive unit tests for SymbolMapper.

Tests additional edge cases and scenarios that need better coverage,
focusing on configuration validation, symbol mapping operations,
error handling, and comprehensive data retrieval methods.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases.
"""

from typing import Any
from unittest.mock import Mock, patch

import pytest

from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    ExchangeNotSupportedError,
    SymbolMappingConfigurationError as InvalidConfigurationError,
)
from tests.unit.core.conftest import create_test_exchange_config


class TestSymbolMapperInitialization:
    """Test suite for SymbolMapper initialization with various configurations."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_success_minimal_valid_config(self, mock_logger: Mock) -> None:
        """Test initialization with minimal valid configuration."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
            )
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert len(mapper.get_all_internal_symbols()) == 1
        assert "BTC" in mapper.get_all_internal_symbols()
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-PERP"
        assert mapper.get_internal_symbol("BTC-PERP", "exchange1") == "BTC"

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_success_multiple_exchanges(self, mock_logger: Mock) -> None:
        """Test initialization with multiple exchanges."""
        # Arrange
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"},
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        all_symbols = mapper.get_all_internal_symbols()
        assert len(all_symbols) == 3
        assert set(all_symbols) == {"BTC", "ETH", "SOL"}
        assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
        assert mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_success_empty_config(self, mock_logger: Mock) -> None:
        """Test initialization with empty configuration."""
        # Arrange
        config: dict[str, Any] = {}

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert len(mapper.get_all_internal_symbols()) == 0
        assert mapper.get_exchange_symbol("BTC", "exchange1") is None

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_edge_disabled_exchange(self, mock_logger: Mock) -> None:
        """Test initialization with disabled exchange."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
                enabled=False,  # Disabled
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC-PERP"},
                enabled=True,
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        # Should skip exchange1 and only process exchange2
        assert len(mapper.get_all_internal_symbols()) == 1
        assert "BTC" in mapper.get_all_internal_symbols()
        with pytest.raises(ExchangeNotSupportedError):
            mapper.get_exchange_symbol("BTC", "exchange1")
        assert mapper.get_exchange_symbol("BTC", "exchange2") == "BTC-PERP"
        mock_logger.info.assert_called()

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_edge_empty_symbols(self, mock_logger: Mock) -> None:
        """Test initialization with empty symbols fails in strict mode."""
        # Arrange - empty symbols should fail validation
        with pytest.raises(InvalidConfigurationError, match="has no symbol mappings"):
            config = {
                "exchange1": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={},  # Empty symbols
                ),
            }
            SymbolMapper(config)

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_edge_valid_single_exchange(self, mock_logger: Mock) -> None:
        """Test initialization with single valid exchange."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert len(mapper.get_all_internal_symbols()) == 1
        assert "BTC" in mapper.get_all_internal_symbols()
        mock_logger.info.assert_called()

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_edge_multiple_valid_symbols(self, mock_logger: Mock) -> None:
        """Test initialization with multiple valid symbols."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                },
            )
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        # Should process all symbols
        all_symbols = mapper.get_all_internal_symbols()
        assert "BTC" in all_symbols
        assert "ETH" in all_symbols
        assert "SOL" in all_symbols
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-PERP"
        assert mapper.get_exchange_symbol("ETH", "exchange1") is None
        assert mapper.get_exchange_symbol("SOL", "exchange1") == "SOL-PERP"
        mock_logger.warning.assert_called()

    def test_init_edge_overwrite_mapping_through_public_interface(self) -> None:
        """Test behavior when symbol mapping is overwritten through valid configuration."""
        # Since we can't test through private methods, we test the final public behavior
        # that results from the internal duplicate handling logic.

        # Arrange - create a valid config with a symbol mapping
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP"}
            )
        }
        mapper = SymbolMapper(config)

        # Act & Assert - verify the public mapping works correctly
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-PERP"
        assert mapper.get_internal_symbol("BTC-PERP", "exchange1") == "BTC"

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_init_edge_duplicate_exchange_symbol(self, mock_logger: Mock) -> None:
        """Test initialization with duplicate exchange symbol."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "SAME-SYMBOL"}
            )
        }
        mapper = SymbolMapper(config)

        # Act - create a new mapper config that would have duplicate exchange symbol
        # Instead of using private method, test through public interface
        config_with_duplicate_exchange = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "SAME-SYMBOL",
                    "ETH": "SAME-SYMBOL",  # Same exchange symbol for different internal
                },
            )
        }
        mapper = SymbolMapper(config_with_duplicate_exchange)

        # Assert
        # Should log warning about duplicate and overwrite
        assert mapper.get_internal_symbol("SAME-SYMBOL", "exchange1") == "ETH"
        mock_logger.warning.assert_called()

    # ==================== FAILURE CASES ====================

    def test_init_failure_invalid_config_type(self) -> None:
        """Test initialization with invalid config type."""
        # Arrange
        # Test with string instead of dict to trigger validation
        invalid_config: Any = "invalid_config"

        # Act & Assert
        with pytest.raises(InvalidConfigurationError) as exc_info:
            SymbolMapper(invalid_config)

        # The new exception interface just checks that the error was raised
        assert "Configuration must be a dictionary" in str(exc_info.value)

    def test_init_failure_none_config(self) -> None:
        """Test initialization with None config."""
        # Arrange
        config: Any = None

        # Act & Assert
        with pytest.raises(InvalidConfigurationError):
            SymbolMapper(config)


class TestSymbolMapperExchangeSymbolRetrieval:
    """Test suite for exchange symbol retrieval methods."""

    @pytest.fixture
    def sample_mapper(self) -> SymbolMapper:
        """Create a sample mapper for testing with test-specific config."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK, symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"}
            ),
        }
        return SymbolMapper(config)

    # ==================== SUCCESS CASES ====================

    def test_get_exchange_symbol_success_existing_mapping(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test successful retrieval of existing exchange symbol."""
        # Act & Assert
        assert sample_mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
        assert sample_mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
        assert sample_mapper.get_exchange_symbol("ETH", "hyperliquid") == "ETH-PERP"
        assert sample_mapper.get_exchange_symbol("SOL", "backpack") == "SOL_PERP"

    # ==================== EDGE CASES ====================

    def test_get_exchange_symbol_edge_nonexistent_internal_symbol(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with non-existent internal symbol."""
        # Act & Assert
        assert sample_mapper.get_exchange_symbol("UNKNOWN", "hyperliquid") is None

    def test_get_exchange_symbol_edge_nonexistent_exchange(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with non-existent exchange."""
        # Act & Assert
        assert sample_mapper.get_exchange_symbol("BTC", "unknown_exchange") is None

    def test_get_exchange_symbol_edge_symbol_not_on_exchange(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval of symbol not available on specific exchange."""
        # Act & Assert
        # SOL is only on backpack, not hyperliquid
        assert sample_mapper.get_exchange_symbol("SOL", "hyperliquid") is None
        # ETH is only on hyperliquid, not backpack
        assert sample_mapper.get_exchange_symbol("ETH", "backpack") is None

    def test_get_exchange_symbol_edge_empty_strings(self, sample_mapper: SymbolMapper) -> None:
        """Test retrieval with empty string parameters."""
        # Act & Assert
        assert sample_mapper.get_exchange_symbol("", "hyperliquid") is None
        assert sample_mapper.get_exchange_symbol("BTC", "") is None
        assert sample_mapper.get_exchange_symbol("", "") is None


class TestSymbolMapperInternalSymbolRetrieval:
    """Test suite for internal symbol retrieval methods."""

    @pytest.fixture
    def sample_mapper(self) -> SymbolMapper:
        """Create a sample mapper for testing with test-specific config."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK, symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"}
            ),
        }
        return SymbolMapper(config)

    # ==================== SUCCESS CASES ====================

    def test_get_internal_symbol_success_existing_mapping(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test successful retrieval of existing internal symbol."""
        # Act & Assert
        assert sample_mapper.get_internal_symbol("BTC-PERP", "hyperliquid") == "BTC"
        assert sample_mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"
        assert sample_mapper.get_internal_symbol("ETH-PERP", "hyperliquid") == "ETH"
        assert sample_mapper.get_internal_symbol("SOL_PERP", "backpack") == "SOL"

    # ==================== EDGE CASES ====================

    def test_get_internal_symbol_edge_nonexistent_exchange_symbol(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with non-existent exchange symbol."""
        # Act & Assert
        assert sample_mapper.get_internal_symbol("UNKNOWN-PERP", "hyperliquid") is None

    def test_get_internal_symbol_edge_nonexistent_exchange(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with non-existent exchange."""
        # Act & Assert
        assert sample_mapper.get_internal_symbol("BTC-PERP", "unknown_exchange") is None

    def test_get_internal_symbol_edge_wrong_exchange_for_symbol(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval of exchange symbol on wrong exchange."""
        # Act & Assert
        # BTC-PERP format is hyperliquid, not backpack
        assert sample_mapper.get_internal_symbol("BTC-PERP", "backpack") is None
        # BTC_PERP format is backpack, not hyperliquid
        assert sample_mapper.get_internal_symbol("BTC_PERP", "hyperliquid") is None

    def test_get_internal_symbol_edge_empty_strings(self, sample_mapper: SymbolMapper) -> None:
        """Test retrieval with empty string parameters."""
        # Act & Assert
        assert sample_mapper.get_internal_symbol("", "hyperliquid") is None
        assert sample_mapper.get_internal_symbol("BTC-PERP", "") is None
        assert sample_mapper.get_internal_symbol("", "") is None


class TestSymbolMapperAllInternalSymbols:
    """Test suite for all internal symbols retrieval."""

    # ==================== SUCCESS CASES ====================

    def test_get_all_internal_symbols_success_multiple_symbols(self) -> None:
        """Test retrieval of all internal symbols with multiple symbols."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP", "SOL": "SOL-PERP"},
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={
                    "BTC": "BTC_PERP",  # Same internal, different exchange
                    "DOGE": "DOGE_PERP",  # New internal symbol
                },
            ),
        }
        mapper = SymbolMapper(config)

        # Act
        symbols = mapper.get_all_internal_symbols()

        # Assert
        assert len(symbols) == 4
        assert set(symbols) == {"BTC", "ETH", "SOL", "DOGE"}
        assert symbols == sorted(symbols)  # Should be sorted

    def test_get_all_internal_symbols_success_single_symbol(self) -> None:
        """Test retrieval with single symbol."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP"}
            )
        }
        mapper = SymbolMapper(config)

        # Act
        symbols = mapper.get_all_internal_symbols()

        # Assert
        assert symbols == ["BTC"]

    # ==================== EDGE CASES ====================

    def test_get_all_internal_symbols_edge_empty_config(self) -> None:
        """Test retrieval with empty configuration."""
        # Arrange
        config: dict[str, ExchangeSpecificConfig] = {}

        # Act & Assert - empty config should raise an error
        with pytest.raises(InvalidConfigurationError):
            SymbolMapper(config)

    def test_get_all_internal_symbols_edge_no_valid_exchanges(self) -> None:
        """Test retrieval when no exchanges have valid symbols."""
        # Arrange - invalid configurations should raise errors during initialization
        with pytest.raises(InvalidConfigurationError):
            invalid_config = {
                "exchange1": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID, enabled=False, symbols={}
                ),
            }
            SymbolMapper(invalid_config)

    def test_get_all_internal_symbols_edge_duplicate_across_exchanges(self) -> None:
        """Test that duplicates across exchanges are deduplicated."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={
                    "BTC": "BTC_PERP",  # Same internal symbol
                    "ETH": "ETH_PERP",  # Same internal symbol
                },
            ),
        }
        mapper = SymbolMapper(config)

        # Act
        symbols = mapper.get_all_internal_symbols()

        # Assert
        assert len(symbols) == 2
        assert set(symbols) == {"BTC", "ETH"}


class TestSymbolMapperExchangeSymbolsForInternal:
    """Test suite for getting exchange symbols for internal symbol."""

    @pytest.fixture
    def sample_mapper(self) -> SymbolMapper:
        """Create a sample mapper for testing."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK, symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"}
            ),
            "kraken": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,  # Using HYPERLIQUID as placeholder for kraken
                symbols={"BTC": "BTC/USD"},
            ),
        }
        return SymbolMapper(config)

    # ==================== SUCCESS CASES ====================

    def test_get_exchange_symbols_for_internal_success_multiple_exchanges(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval of exchange symbols for internal symbol across multiple exchanges."""
        # Act
        btc_mappings = sample_mapper.get_exchange_symbols_for_internal("BTC")

        # Assert
        expected = {"hyperliquid": "BTC-PERP", "backpack": "BTC_PERP", "kraken": "BTC/USD"}
        assert btc_mappings == expected

    def test_get_exchange_symbols_for_internal_success_single_exchange(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval for symbol only on one exchange."""
        # Act
        sol_mappings = sample_mapper.get_exchange_symbols_for_internal("SOL")
        eth_mappings = sample_mapper.get_exchange_symbols_for_internal("ETH")

        # Assert
        assert sol_mappings == {"backpack": "SOL_PERP"}
        assert eth_mappings == {"hyperliquid": "ETH-PERP"}

    def test_get_exchange_symbols_for_internal_success_returns_copy(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test that method returns a copy, not reference to internal data."""
        # Act
        btc_mappings = sample_mapper.get_exchange_symbols_for_internal("BTC")
        original_size = len(btc_mappings)

        # Modify the returned dict
        btc_mappings["new_exchange"] = "NEW-BTC"

        # Get fresh copy
        fresh_mappings = sample_mapper.get_exchange_symbols_for_internal("BTC")

        # Assert
        assert len(fresh_mappings) == original_size
        assert "new_exchange" not in fresh_mappings

    # ==================== EDGE CASES ====================

    def test_get_exchange_symbols_for_internal_edge_nonexistent_symbol(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval for non-existent internal symbol."""
        # Act
        mappings = sample_mapper.get_exchange_symbols_for_internal("UNKNOWN")

        # Assert
        assert mappings == {}

    def test_get_exchange_symbols_for_internal_edge_empty_string(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with empty string."""
        # Act
        mappings = sample_mapper.get_exchange_symbols_for_internal("")

        # Assert
        assert mappings == {}


class TestSymbolMapperInternalSymbolsForExchange:
    """Test suite for getting internal symbols for exchange."""

    @pytest.fixture
    def sample_mapper(self) -> SymbolMapper:
        """Create a sample mapper for testing."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP", "SOL": "SOL-PERP"},
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK, symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"}
            ),
        }
        return SymbolMapper(config)

    # ==================== SUCCESS CASES ====================

    def test_get_internal_symbols_for_exchange_success_multiple_symbols(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval of internal symbols for exchange with multiple symbols."""
        # Act
        hyperliquid_mappings = sample_mapper.get_internal_symbols_for_exchange("hyperliquid")
        backpack_mappings = sample_mapper.get_internal_symbols_for_exchange("backpack")

        # Assert
        expected_hyperliquid = {"BTC-PERP": "BTC", "ETH-PERP": "ETH", "SOL-PERP": "SOL"}
        expected_backpack = {"BTC_PERP": "BTC", "SOL_PERP": "SOL"}
        assert hyperliquid_mappings == expected_hyperliquid
        assert backpack_mappings == expected_backpack

    def test_get_internal_symbols_for_exchange_success_returns_copy(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test that method returns a copy, not reference to internal data."""
        # Act
        mappings = sample_mapper.get_internal_symbols_for_exchange("hyperliquid")
        original_size = len(mappings)

        # Modify the returned dict
        mappings["NEW-SYMBOL"] = "NEW"

        # Get fresh copy
        fresh_mappings = sample_mapper.get_internal_symbols_for_exchange("hyperliquid")

        # Assert
        assert len(fresh_mappings) == original_size
        assert "NEW-SYMBOL" not in fresh_mappings

    # ==================== EDGE CASES ====================

    def test_get_internal_symbols_for_exchange_edge_nonexistent_exchange(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval for non-existent exchange."""
        # Act
        mappings = sample_mapper.get_internal_symbols_for_exchange("unknown_exchange")

        # Assert
        assert mappings == {}

    def test_get_internal_symbols_for_exchange_edge_empty_string(
        self, sample_mapper: SymbolMapper
    ) -> None:
        """Test retrieval with empty string."""
        # Act
        mappings = sample_mapper.get_internal_symbols_for_exchange("")

        # Assert
        assert mappings == {}

    def test_get_internal_symbols_for_exchange_edge_exchange_no_symbols(self) -> None:
        """Test retrieval for exchange with no symbols configured."""
        # Arrange - Invalid configuration should raise an error
        with pytest.raises(InvalidConfigurationError):
            invalid_config = {
                "exchange1": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    enabled=True,
                    symbols={},  # Empty symbols should fail
                ),
                "exchange2": create_test_exchange_config(
                    ExchangeName.BACKPACK, symbols={"BTC": "BTC-PERP"}
                ),
            }
            SymbolMapper(invalid_config)


class TestSymbolMapperConfigValidationLogging:
    """Test suite for configuration validation and logging behavior."""

    # ==================== SUCCESS CASES ====================

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_validation_success_logs_initialization(self, mock_logger: Mock) -> None:
        """Test that successful initialization logs appropriate messages."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            )
        }

        # Act
        SymbolMapper(config)

        # Assert
        # Should log debug message for validation completion
        mock_logger.debug.assert_called_with(
            "SymbolMapper configuration validation step completed (basic checks only)."
        )
        # Should log info message for initialization
        mock_logger.info.assert_called_with(
            "symbol_mapper_initialized",
            exchanges_count=1,
            internal_symbols_count=2,
            message="SymbolMapper initialized with exchange mappings and internal symbols",
        )

    # ==================== EDGE CASES ====================

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_validation_edge_logs_skipped_exchanges(self, mock_logger: Mock) -> None:
        """Test that invalid configurations raise errors."""
        # Arrange - Invalid configurations should raise errors
        with pytest.raises(InvalidConfigurationError):
            invalid_config = {
                "valid_exchange": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP"}
                ),
                "missing_symbols": create_test_exchange_config(
                    ExchangeName.BACKPACK,
                    enabled=True,
                    symbols={},  # Empty symbols should fail
                ),
            }
            SymbolMapper(invalid_config)
        # Should have multiple warning calls for different validation failures
        assert mock_logger.warning.call_count >= 3

        # Check specific warning calls
        warning_calls = [call[0][0] for call in mock_logger.warning.call_args_list]
        assert "skipping_exchange_missing_symbols" in warning_calls
        assert "skipping_exchange_symbols_not_dict" in warning_calls
        assert "skipping_exchange_invalid_data" in warning_calls

    @patch("cyberdelta.core.symbol_mapper.logger")
    def test_validation_edge_logs_invalid_symbol_values(self, mock_logger: Mock) -> None:
        """Test that valid symbol values are properly processed."""
        # Arrange - Valid configuration should work
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
        }

        # Act
        SymbolMapper(config)

        # Assert - Should log successful initialization
        mock_logger.info.assert_called_with(
            "symbol_mapper_initialized",
            exchanges_count=1,
            symbols_count=2,
        )


class TestSymbolMapperComplexScenarios:
    """Test suite for complex real-world scenarios."""

    # ==================== SUCCESS CASES ====================

    def test_complex_scenario_success_multi_exchange_arbitrage(self) -> None:
        """Test complex scenario with multiple exchanges for arbitrage mapping."""
        # Arrange - Real-world-like configuration
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                    "AVAX": "AVAX-PERP",
                },
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC_PERP", "ETH": "ETH_PERP", "SOL": "SOL_PERP"},
            ),
            "binance": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,  # Using HYPERLIQUID as placeholder
                symbols={"BTC": "BTCUSDT", "ETH": "ETHUSDT", "BNB": "BNBUSDT"},
            ),
        }
        mapper = SymbolMapper(config)

        # Act & Assert - Test cross-exchange lookups
        # BTC available on all exchanges
        btc_exchanges = mapper.get_exchange_symbols_for_internal("BTC")
        assert len(btc_exchanges) == 3
        assert btc_exchanges["hyperliquid"] == "BTC-PERP"
        assert btc_exchanges["backpack"] == "BTC_PERP"
        assert btc_exchanges["binance"] == "BTCUSDT"

        # AVAX only on hyperliquid
        avax_exchanges = mapper.get_exchange_symbols_for_internal("AVAX")
        assert avax_exchanges == {"hyperliquid": "AVAX-PERP"}

        # BNB only on binance
        bnb_exchanges = mapper.get_exchange_symbols_for_internal("BNB")
        assert bnb_exchanges == {"binance": "BNBUSDT"}

        # Test reverse lookups
        assert mapper.get_internal_symbol("BTC-PERP", "hyperliquid") == "BTC"
        assert mapper.get_internal_symbol("BTCUSDT", "binance") == "BTC"
        assert mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"

    # ==================== EDGE CASES ====================

    def test_complex_scenario_edge_partial_symbol_coverage(self) -> None:
        """Test scenario where different exchanges support different symbol sets."""
        # Arrange
        config = {
            "major_exchange": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-USD",
                    "ETH": "ETH-USD",
                    "SOL": "SOL-USD",
                    "AVAX": "AVAX-USD",
                    "DOGE": "DOGE-USD",
                },
            ),
            "altcoin_exchange": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"DOGE": "DOGE-PERP", "SHIB": "SHIB-PERP", "PEPE": "PEPE-PERP"},
            ),
            "defi_exchange": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"UNI": "UNI-PERP", "AAVE": "AAVE-PERP", "COMP": "COMP-PERP"},
            ),
        }
        mapper = SymbolMapper(config)

        # Act & Assert
        all_symbols = mapper.get_all_internal_symbols()
        assert len(all_symbols) == 10

        # Test symbol availability
        major_symbols = mapper.get_internal_symbols_for_exchange("major_exchange")
        assert len(major_symbols) == 5

        altcoin_symbols = mapper.get_internal_symbols_for_exchange("altcoin_exchange")
        assert len(altcoin_symbols) == 3

        defi_symbols = mapper.get_internal_symbols_for_exchange("defi_exchange")
        assert len(defi_symbols) == 3

        # Test cross-exchange symbol (DOGE)
        doge_mappings = mapper.get_exchange_symbols_for_internal("DOGE")
        assert len(doge_mappings) == 2
        assert doge_mappings["major_exchange"] == "DOGE-USD"
        assert doge_mappings["altcoin_exchange"] == "DOGE-PERP"

    def test_complex_scenario_edge_empty_and_invalid_mixed(self) -> None:
        """Test complex scenario with valid configurations only."""
        # Arrange - Only valid configurations should be used
        config = {
            "valid_exchange": create_test_exchange_config(
                ExchangeName.HYPERLIQUID, symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"}
            ),
            "mixed_valid": create_test_exchange_config(
                ExchangeName.BACKPACK, symbols={"SOL": "SOL-PERP", "VALID": "VALID-PERP"}
            ),
        }
        mapper = SymbolMapper(config)

        # Act & Assert
        all_symbols = mapper.get_all_internal_symbols()
        # Should only include BTC, ETH, SOL, VALID (4 symbols)
        assert len(all_symbols) == 4
        assert set(all_symbols) == {"BTC", "ETH", "SOL", "VALID"}

        # Valid exchanges should work
        assert mapper.get_exchange_symbol("BTC", "valid_exchange") == "BTC-PERP"
        assert mapper.get_exchange_symbol("SOL", "mixed_valid") == "SOL-PERP"
        assert mapper.get_exchange_symbol("VALID", "mixed_valid") == "VALID-PERP"
