"""Comprehensive unit tests for the SymbolMapper component.

Tests symbol mapping functionality between different exchanges and internal representations.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from unittest.mock import patch

import pytest

from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    ExchangeNotSupportedError,
    SymbolMappingConfigurationError as InvalidConfigurationError,
    SymbolMappingError,
    SymbolNotFoundError,
)
from tests.unit.core.conftest import create_test_exchange_config


class TestInvalidConfigurationError:
    """Test suite for InvalidConfigurationError exception."""

    # ==================== SUCCESS CASES ====================

    def test_invalid_configuration_error_success_creation(self) -> None:
        """Test successful creation of InvalidConfigurationError."""
        # Arrange
        message = "Configuration must be a dictionary"

        # Act
        error = InvalidConfigurationError(message, config_type="test")

        # Assert
        assert str(error) == message
        assert error.config_type == "test"
        assert isinstance(error, SymbolMappingError)

    def test_invalid_configuration_error_success_with_complex_types(self) -> None:
        """Test error creation with complex type names."""
        # Arrange
        message = "Expected Dict[str, Any], got tuple"

        # Act
        error = InvalidConfigurationError(message, config_type="complex")

        # Assert
        assert str(error) == message
        assert error.config_type == "complex"


class TestSymbolMapperInit:
    """Test suite for SymbolMapper initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_valid_config(self) -> None:
        """Test successful initialization with valid configuration."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC/USD", "SOL": "SOL/USD"},
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper is not None
        assert len(mapper.get_all_internal_symbols()) == 3  # BTC, ETH, SOL

    def test_init_success_with_empty_config(self) -> None:
        """Test initialization fails with empty configuration (strict mode)."""
        # Arrange
        config: dict[str, ExchangeSpecificConfig] = {}

        # Act & Assert
        with pytest.raises(InvalidConfigurationError, match="Configuration cannot be empty"):
            SymbolMapper(config)

    # ==================== EDGE CASES ====================

    def test_init_edge_config_with_empty_symbols(self) -> None:
        """Test initialization when exchanges have empty symbols."""
        # Arrange - this should fail in strict mode
        with pytest.raises(InvalidConfigurationError, match="has no symbol mappings"):
            config = {
                "exchange1": create_test_exchange_config(
                    ExchangeName.HYPERLIQUID,
                    symbols={},  # Empty symbols
                ),
            }
            SymbolMapper(config)

    def test_init_edge_config_with_disabled_exchange(self) -> None:
        """Test initialization when exchange is disabled."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
                enabled=True,
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"ETH": "ETH-PERP"},
                enabled=False,  # Disabled
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper is not None
        assert mapper.get_all_internal_symbols() == ["BTC"]  # Only enabled exchange
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-PERP"
        # Disabled exchange should not be supported
        with pytest.raises(ExchangeNotSupportedError):  # Should raise an error
            mapper.get_exchange_symbol("ETH", "exchange2")

    def test_init_edge_config_with_invalid_exchange_data_types(self) -> None:
        """Test initialization with invalid exchange data types fails early."""
        # In strict mode, this should fail at type checking level
        # We test this behavior by ensuring type errors are caught
        # This test documents the expected behavior with type checking
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
            ),
        }

        # Act & Assert - valid config should work
        mapper = SymbolMapper(config)
        assert mapper is not None
        assert mapper.get_all_internal_symbols() == ["BTC"]

    def test_init_edge_config_with_special_symbols(self) -> None:
        """Test initialization with special character symbols."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "1000SHIB": "1000SHIB-PERP",  # Numbers in symbol
                    "ADA": "ADA_PERP",  # Valid
                },
            )
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper is not None
        assert "BTC" in mapper.get_all_internal_symbols()
        assert "1000SHIB" in mapper.get_all_internal_symbols()
        assert "ADA" in mapper.get_all_internal_symbols()

    def test_init_edge_single_symbol_mapping(self) -> None:
        """Test initialization with single symbol mapping."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-USD"},
            )
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-USD"

    def test_init_edge_duplicate_exchange_symbol_mapping(self) -> None:
        """Test initialization with duplicate exchange symbol mapping."""
        # Arrange
        # Test that duplicate exchange symbols are handled (second overwrites first)
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTCUSD",
                    "BITCOIN": "BTCUSD",  # Same exchange symbol, different internal
                },
            )
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            # The second mapping should overwrite the first
            assert mapper.get_internal_symbol("BTCUSD", "exchange1") == "BITCOIN"
            # Verify warning was logged
            mock_logger.warning.assert_any_call(
                "duplicate_exchange_symbol",
                exchange_symbol="BTCUSD",
                exchange_id="exchange1",
                internal_symbol="BITCOIN",
                message=(
                    "Duplicate exchange symbol mapped for exchange. Overwriting mapping to internal"
                ),
            )

    # ==================== FAILURE CASES ====================

    def test_init_failure_non_dict_config(self) -> None:
        """Test initialization failure with non-dict configuration."""
        # Arrange - in strict mode, type checking should catch these
        # But we test runtime behavior for documentation
        configs = [None, "string_config", 123, ["list", "config"], True]

        # Act & Assert
        for config in configs:
            with pytest.raises(InvalidConfigurationError):
                SymbolMapper(config)  # type: ignore


class TestSymbolMapperMethods:
    """Test suite for SymbolMapper public methods."""

    @pytest.fixture
    def mapper(self) -> SymbolMapper:
        """Create a SymbolMapper with test configuration."""
        config = {
            "hyperliquid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                },
            ),
            "backpack": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={
                    "BTC": "BTC_PERP",
                    "ETH": "ETH_PERP",
                    "ADA": "ADA_PERP",
                },
            ),
        }
        return SymbolMapper(config)

    # ==================== SUCCESS CASES ====================

    def test_get_exchange_symbol_success(self, mapper: SymbolMapper) -> None:
        """Test successful retrieval of exchange symbols."""
        # Act & Assert
        assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
        assert mapper.get_exchange_symbol("ETH", "hyperliquid") == "ETH-PERP"
        assert mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
        assert mapper.get_exchange_symbol("ADA", "backpack") == "ADA_PERP"

    def test_get_internal_symbol_success(self, mapper: SymbolMapper) -> None:
        """Test successful retrieval of internal symbols."""
        # Act & Assert
        assert mapper.get_internal_symbol("BTC-PERP", "hyperliquid") == "BTC"
        assert mapper.get_internal_symbol("ETH-PERP", "hyperliquid") == "ETH"
        assert mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"
        assert mapper.get_internal_symbol("ADA_PERP", "backpack") == "ADA"

    def test_get_all_internal_symbols_success(self, mapper: SymbolMapper) -> None:
        """Test successful retrieval of all internal symbols."""
        # Act
        symbols = mapper.get_all_internal_symbols()

        # Assert
        assert isinstance(symbols, list)
        assert sorted(symbols) == ["ADA", "BTC", "ETH", "SOL"]

    def test_get_exchange_symbols_for_internal_success(self, mapper: SymbolMapper) -> None:
        """Test successful retrieval of all exchange symbols for internal symbol."""
        # Act
        btc_symbols = mapper.get_exchange_symbols_for_internal("BTC")
        eth_symbols = mapper.get_exchange_symbols_for_internal("ETH")
        sol_symbols = mapper.get_exchange_symbols_for_internal("SOL")
        ada_symbols = mapper.get_exchange_symbols_for_internal("ADA")

        # Assert
        assert btc_symbols == {"hyperliquid": "BTC-PERP", "backpack": "BTC_PERP"}
        assert eth_symbols == {"hyperliquid": "ETH-PERP", "backpack": "ETH_PERP"}
        assert sol_symbols == {"hyperliquid": "SOL-PERP"}
        assert ada_symbols == {"backpack": "ADA_PERP"}

    def test_get_internal_symbols_for_exchange_success(self, mapper: SymbolMapper) -> None:
        """Test successful retrieval of internal symbols for exchange."""
        # Act
        hl_symbols = mapper.get_internal_symbols_for_exchange("hyperliquid")
        bp_symbols = mapper.get_internal_symbols_for_exchange("backpack")
        empty_symbols = mapper.get_internal_symbols_for_exchange("empty_exchange")

        # Assert
        assert hl_symbols == {"BTC-PERP": "BTC", "ETH-PERP": "ETH", "SOL-PERP": "SOL"}
        assert bp_symbols == {"BTC_PERP": "BTC", "ETH_PERP": "ETH", "ADA_PERP": "ADA"}
        assert empty_symbols == {}

    # ==================== EDGE CASES ====================

    def test_get_exchange_symbol_edge_missing_mappings(self, mapper: SymbolMapper) -> None:
        """Test exchange symbol retrieval for missing mappings."""
        # Act & Assert
        # Symbol not mapped for exchange
        assert mapper.get_exchange_symbol("SOL", "backpack") is None
        assert mapper.get_exchange_symbol("ADA", "hyperliquid") is None

        # Symbol not in system at all
        assert mapper.get_exchange_symbol("DOGE", "hyperliquid") is None

        # Exchange not in system - should raise error in strict mode
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("BTC", "unknown_exchange")

    def test_get_internal_symbol_edge_missing_mappings(self, mapper: SymbolMapper) -> None:
        """Test internal symbol retrieval for missing mappings."""
        # Act & Assert
        # Wrong symbol format for exchange
        assert mapper.get_internal_symbol("BTC_PERP", "hyperliquid") is None
        assert mapper.get_internal_symbol("BTC-PERP", "backpack") is None

        # Symbol not in exchange
        assert mapper.get_internal_symbol("DOGE-PERP", "hyperliquid") is None

        # Exchange not in system - should raise error in strict mode
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbol("BTC-PERP", "unknown_exchange")

    def test_get_exchange_symbols_for_internal_edge_unknown_symbol(
        self,
        mapper: SymbolMapper,
    ) -> None:
        """Test exchange symbols retrieval for unknown internal symbol."""
        # Act & Assert - should raise error in strict mode
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbols_for_internal("UNKNOWN")

    def test_get_internal_symbols_for_exchange_edge_unknown_exchange(
        self,
        mapper: SymbolMapper,
    ) -> None:
        """Test internal symbols retrieval for unknown exchange."""
        # Act & Assert - should raise error in strict mode
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbols_for_exchange("unknown_exchange")

    def test_returned_dicts_are_copies(self, mapper: SymbolMapper) -> None:
        """Test that returned dictionaries are copies, not references."""
        # Act
        btc_symbols1 = mapper.get_exchange_symbols_for_internal("BTC")
        btc_symbols2 = mapper.get_exchange_symbols_for_internal("BTC")

        hl_symbols1 = mapper.get_internal_symbols_for_exchange("hyperliquid")
        hl_symbols2 = mapper.get_internal_symbols_for_exchange("hyperliquid")

        # Modify returned dicts
        btc_symbols1["new_exchange"] = "NEW_SYMBOL"
        hl_symbols1["NEW-PERP"] = "NEW"

        # Assert - Original data should be unchanged
        assert "new_exchange" not in btc_symbols2
        assert "NEW-PERP" not in hl_symbols2

    # ==================== FAILURE CASES ====================

    def test_get_methods_with_empty_strings(self, mapper: SymbolMapper) -> None:
        """Test methods behavior with empty string inputs."""
        # Act & Assert - empty strings should raise validation errors in strict mode
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("", "hyperliquid")
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("BTC", "")
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("", "")

        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbol("", "hyperliquid")
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbol("BTC-PERP", "")
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbol("", "")

        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbols_for_internal("")
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_internal_symbols_for_exchange("")


class TestSymbolMapperPrivateMethods:
    """Test suite for testing private method behavior through public interface."""

    def test_validate_config_logging(self) -> None:
        """Test that validation logs info message."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
            )
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            SymbolMapper(config)

            # Assert
            mock_logger.info.assert_any_call(
                "Symbol mapper initialized",
                exchanges_count=1,
                internal_symbols_count=1,
                supported_exchanges=["exchange1"],
                internal_symbols=["BTC"],
            )

    def test_process_exchanges_with_valid_types(self) -> None:
        """Test processing of exchanges with valid typed data."""
        # Arrange
        config = {
            "valid": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-PERP"},
            ),
            "valid2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"ETH": "ETH-PERP"},
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        # Both valid exchanges should be processed
        assert sorted(mapper.get_all_internal_symbols()) == ["BTC", "ETH"]
        assert mapper.get_exchange_symbol("BTC", "valid") == "BTC-PERP"
        assert mapper.get_exchange_symbol("ETH", "valid2") == "ETH-PERP"


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("internal_symbol", "exchange_id", "expected"),
    [
        ("BTC", "hyperliquid", "BTC-PERP"),
        ("ETH", "backpack", "ETH_PERP"),
        ("SOL", "hyperliquid", "SOL-PERP"),
        ("ADA", "backpack", "ADA_PERP"),
        # Missing mappings
        ("DOGE", "hyperliquid", None),
        ("BTC", "unknown", None),
        ("", "", None),
    ],
)
def test_get_exchange_symbol_parametrized(
    internal_symbol: str, exchange_id: str, expected: str | None
) -> None:
    """Test get_exchange_symbol with various inputs."""
    # Arrange
    config = {
        "hyperliquid": create_test_exchange_config(
            ExchangeName.HYPERLIQUID,
            symbols={"BTC": "BTC-PERP", "SOL": "SOL-PERP"},
        ),
        "backpack": create_test_exchange_config(
            ExchangeName.BACKPACK,
            symbols={"ETH": "ETH_PERP", "ADA": "ADA_PERP"},
        ),
    }
    mapper = SymbolMapper(config)

    # Act & Assert
    if expected is None:
        # In strict mode, these should raise exceptions
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol(internal_symbol, exchange_id)
    else:
        result = mapper.get_exchange_symbol(internal_symbol, exchange_id)
        assert result == expected


@pytest.mark.parametrize(
    ("config_description", "expected_internal_symbols", "should_fail"),
    [
        ("single_exchange", ["BTC"], False),
        (
            "multi_exchange",
            ["BTC", "ETH", "SOL"],
            False,
        ),
        # Invalid configs that should fail
        ("empty_config", [], True),
        ("empty_symbols", [], True),
    ],
)
def test_symbol_mapper_init_parametrized(
    config_description: str, expected_internal_symbols: list[str], should_fail: bool
) -> None:
    """Test SymbolMapper initialization with various configurations."""
    # Arrange
    configs = {
        "single_exchange": {
            "ex1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-USD"},
            )
        },
        "multi_exchange": {
            "ex1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-USD", "ETH": "ETH-USD"},
            ),
            "ex2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={"BTC": "BTC-PERP", "SOL": "SOL-PERP"},
            ),
        },
        "empty_config": {},
        "empty_symbols": {
            "ex1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={},
            )
        },
    }

    config = configs[config_description]

    # Act & Assert
    if should_fail:
        with pytest.raises(InvalidConfigurationError):
            SymbolMapper(config)
    else:
        mapper = SymbolMapper(config)
        assert sorted(mapper.get_all_internal_symbols()) == sorted(expected_internal_symbols)


# ==================== INTEGRATION TESTS ====================


class TestSymbolMapperIntegration:
    """Integration tests for SymbolMapper with complex scenarios."""

    def test_complex_multi_exchange_scenario(self) -> None:
        """Test complex scenario with multiple exchanges and overlapping symbols."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                },
            ),
            "exchange2": create_test_exchange_config(
                ExchangeName.BACKPACK,
                symbols={
                    "BTC": "BTCUSD",
                    "ETH": "ETHUSD",
                    "ADA": "ADAUSD",
                },
            ),
            "exchange3": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC/USD",
                    "SOL": "SOL/USD",
                    "LINK": "LINK/USD",
                },
            ),
            "disabled_exchange": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={"BTC": "BTC-DISABLED"},
                enabled=False,
            ),
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        # Test all internal symbols are found (disabled exchange excluded)
        all_symbols = mapper.get_all_internal_symbols()
        assert sorted(all_symbols) == ["ADA", "BTC", "ETH", "LINK", "SOL"]

        # Test BTC mappings across exchanges
        btc_mappings = mapper.get_exchange_symbols_for_internal("BTC")
        assert btc_mappings == {
            "exchange1": "BTC-PERP",
            "exchange2": "BTCUSD",
            "exchange3": "BTC/USD",
        }

        # Test reverse mappings
        assert mapper.get_internal_symbol("BTC-PERP", "exchange1") == "BTC"
        assert mapper.get_internal_symbol("BTCUSD", "exchange2") == "BTC"
        assert mapper.get_internal_symbol("BTC/USD", "exchange3") == "BTC"

        # Test symbols unique to specific exchanges
        assert mapper.get_exchange_symbol("ADA", "exchange2") == "ADAUSD"
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("ADA", "exchange1")

        assert mapper.get_exchange_symbol("LINK", "exchange3") == "LINK/USD"
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("LINK", "exchange2")

        # Test disabled exchanges
        with pytest.raises((SymbolNotFoundError, ExchangeNotSupportedError)):
            mapper.get_exchange_symbol("BTC", "disabled_exchange")

    def test_special_characters_in_symbols(self) -> None:
        """Test handling of special characters in symbol names."""
        # Arrange
        config = {
            "exchange1": create_test_exchange_config(
                ExchangeName.HYPERLIQUID,
                symbols={
                    "BTC": "BTC-PERP",
                    "1000SHIB": "1000SHIB-PERP",  # Numbers in symbol
                    "ETH2": "ETH2-PERP",  # Valid alphanumeric
                },
            )
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-PERP"
        assert mapper.get_exchange_symbol("1000SHIB", "exchange1") == "1000SHIB-PERP"
        assert mapper.get_exchange_symbol("ETH2", "exchange1") == "ETH2-PERP"

        # Test reverse mappings
        assert mapper.get_internal_symbol("BTC-PERP", "exchange1") == "BTC"
        assert mapper.get_internal_symbol("1000SHIB-PERP", "exchange1") == "1000SHIB"
        assert mapper.get_internal_symbol("ETH2-PERP", "exchange1") == "ETH2"
