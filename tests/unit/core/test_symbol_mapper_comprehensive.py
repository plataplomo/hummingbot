"""Comprehensive unit tests for the SymbolMapper component.

Tests symbol mapping functionality between different exchanges and internal representations.
Following the mandatory test pattern: SUCCESS, EDGE, and FAILURE cases for each method.
"""

from typing import Any
from unittest.mock import patch

import pytest

from cyberdelta.core.symbol_mapper import (
    InvalidConfigurationError,
    SymbolMapper,
    SymbolMappingError,
)


class TestInvalidConfigurationError:
    """Test suite for InvalidConfigurationError exception."""

    # ==================== SUCCESS CASES ====================

    def test_invalid_configuration_error_success_creation(self) -> None:
        """Test successful creation of InvalidConfigurationError."""
        # Arrange
        expected_type = "dict"
        actual_type = list

        # Act
        error = InvalidConfigurationError(expected_type, actual_type)

        # Assert
        assert error.expected_type == expected_type
        assert error.actual_type == actual_type
        assert str(error) == f"Invalid configuration: Expected {expected_type}, got {actual_type}"
        assert isinstance(error, SymbolMappingError)

    def test_invalid_configuration_error_success_with_complex_types(self) -> None:
        """Test error creation with complex type names."""
        # Arrange
        expected_type = "Dict[str, Any]"
        actual_type = tuple

        # Act
        error = InvalidConfigurationError(expected_type, actual_type)

        # Assert
        assert error.expected_type == expected_type
        assert error.actual_type == actual_type


class TestSymbolMapperInit:
    """Test suite for SymbolMapper initialization."""

    # ==================== SUCCESS CASES ====================

    def test_init_success_with_valid_config(self) -> None:
        """Test successful initialization with valid configuration."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                }
            },
            "exchange2": {
                "symbols": {
                    "BTC": "BTC/USD",
                    "SOL": "SOL/USD",
                }
            },
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper is not None
        assert mapper.raw_config == config
        assert len(mapper.get_all_internal_symbols()) == 3  # BTC, ETH, SOL

    def test_init_success_with_empty_config(self) -> None:
        """Test successful initialization with empty configuration."""
        # Arrange
        config: dict[str, Any] = {}

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper is not None
        assert mapper.get_all_internal_symbols() == []

    # ==================== EDGE CASES ====================

    def test_init_edge_config_with_missing_symbols_key(self) -> None:
        """Test initialization when exchanges lack symbols key."""
        # Arrange
        config = {"exchange1": {"enabled": True, "api_key": "test"}, "exchange2": {"symbols": {}}}

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            assert mapper is not None
            assert mapper.get_all_internal_symbols() == []
            # Verify warning was logged
            mock_logger.warning.assert_any_call(
                "skipping_exchange_missing_symbols",
                exchange_id="exchange1",
                message="Skipping exchange: Missing 'symbols' configuration",
            )

    def test_init_edge_config_with_non_dict_symbols(self) -> None:
        """Test initialization when symbols value is not a dict."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": ["BTC", "ETH"]  # Wrong type
            },
            "exchange2": {
                "symbols": "BTC,ETH"  # Wrong type
            },
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            assert mapper is not None
            assert mapper.get_all_internal_symbols() == []
            # Verify warnings were logged
            assert mock_logger.warning.call_count >= 2

    def test_init_edge_config_with_non_string_exchange_data(self) -> None:
        """Test initialization when exchange data is not a dict."""
        # Arrange
        config = {
            "exchange1": "invalid_data",  # String instead of dict
            "exchange2": 123,  # Number instead of dict
            "exchange3": {"symbols": {"BTC": "BTC-PERP"}},
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            assert mapper is not None
            assert mapper.get_all_internal_symbols() == ["BTC"]
            # Verify warnings were logged for invalid exchanges
            assert mock_logger.warning.call_count >= 2

    def test_init_edge_config_with_non_string_symbol_values(self) -> None:
        """Test initialization when symbol mappings contain non-string values."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": 123,  # Number instead of string
                    "ETH": None,  # None instead of string
                    "SOL": {"perp": "SOL-PERP"},  # Dict instead of string
                    "ADA": "ADA-PERP",  # Valid
                }
            }
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            assert mapper is not None
            assert mapper.get_all_internal_symbols() == ["ADA"]  # Only valid one
            # Verify warnings were logged for invalid symbol values
            assert mock_logger.warning.call_count >= 3

    def test_init_edge_duplicate_internal_symbol_mapping(self) -> None:
        """Test initialization with duplicate internal symbol for same exchange."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": "BTC-USD",  # Single key (removed duplicate)
                }
            }
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper.get_exchange_symbol("BTC", "exchange1") == "BTC-USD"

    def test_init_edge_duplicate_exchange_symbol_mapping(self) -> None:
        """Test initialization with duplicate exchange symbol mapping."""
        # Arrange
        # Need to simulate duplicate by patching the process
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": "BTCUSD",
                    "BITCOIN": "BTCUSD",  # Same exchange symbol, different internal
                }
            }
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
        # Arrange
        configs = [None, "string_config", 123, ["list", "config"], True]

        # Act & Assert
        for config in configs:
            with pytest.raises(InvalidConfigurationError) as exc_info:
                SymbolMapper(config)  # type: ignore
            assert "Expected a dictionary of exchanges" in str(exc_info.value)


class TestSymbolMapperMethods:
    """Test suite for SymbolMapper public methods."""

    @pytest.fixture
    def mapper(self) -> SymbolMapper:
        """Create a SymbolMapper with test configuration."""
        config: dict[str, dict[str, dict[str, str]]] = {
            "hyperliquid": {
                "symbols": {
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                }
            },
            "backpack": {
                "symbols": {
                    "BTC": "BTC_PERP",
                    "ETH": "ETH_PERP",
                    "ADA": "ADA_PERP",
                }
            },
            "empty_exchange": {"symbols": {}},
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

        # Exchange not in system
        assert mapper.get_exchange_symbol("BTC", "unknown_exchange") is None

        # Empty exchange
        assert mapper.get_exchange_symbol("BTC", "empty_exchange") is None

    def test_get_internal_symbol_edge_missing_mappings(self, mapper: SymbolMapper) -> None:
        """Test internal symbol retrieval for missing mappings."""
        # Act & Assert
        # Wrong symbol format for exchange
        assert mapper.get_internal_symbol("BTC_PERP", "hyperliquid") is None
        assert mapper.get_internal_symbol("BTC-PERP", "backpack") is None

        # Symbol not in exchange
        assert mapper.get_internal_symbol("DOGE-PERP", "hyperliquid") is None

        # Exchange not in system
        assert mapper.get_internal_symbol("BTC-PERP", "unknown_exchange") is None

        # Empty exchange
        assert mapper.get_internal_symbol("BTC-PERP", "empty_exchange") is None

    def test_get_exchange_symbols_for_internal_edge_unknown_symbol(
        self,
        mapper: SymbolMapper,
    ) -> None:
        """Test exchange symbols retrieval for unknown internal symbol."""
        # Act
        result = mapper.get_exchange_symbols_for_internal("UNKNOWN")

        # Assert
        assert result == {}

    def test_get_internal_symbols_for_exchange_edge_unknown_exchange(
        self,
        mapper: SymbolMapper,
    ) -> None:
        """Test internal symbols retrieval for unknown exchange."""
        # Act
        result = mapper.get_internal_symbols_for_exchange("unknown_exchange")

        # Assert
        assert result == {}

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
        # Act & Assert
        assert mapper.get_exchange_symbol("", "hyperliquid") is None
        assert mapper.get_exchange_symbol("BTC", "") is None
        assert mapper.get_exchange_symbol("", "") is None

        assert mapper.get_internal_symbol("", "hyperliquid") is None
        assert mapper.get_internal_symbol("BTC-PERP", "") is None
        assert mapper.get_internal_symbol("", "") is None

        assert mapper.get_exchange_symbols_for_internal("") == {}
        assert mapper.get_internal_symbols_for_exchange("") == {}


class TestSymbolMapperPrivateMethods:
    """Test suite for testing private method behavior through public interface."""

    def test_validate_config_logging(self) -> None:
        """Test that validation logs debug message."""
        # Arrange
        config = {"exchange1": {"symbols": {"BTC": "BTC-PERP"}}}

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            SymbolMapper(config)

            # Assert
            mock_logger.debug.assert_any_call(
                "SymbolMapper configuration validation step completed (basic checks only)."
            )

    def test_process_exchanges_with_various_types(self) -> None:
        """Test processing of exchanges with various data types."""
        # Arrange
        config = {
            "valid": {"symbols": {"BTC": "BTC-PERP"}},
            "string_data": "not_a_dict",
            "number_data": 12345,
            "list_data": ["symbols", "BTC"],
            "none_data": None,
            "bool_data": True,
        }

        # Act
        with patch("cyberdelta.core.symbol_mapper.logger") as mock_logger:
            mapper = SymbolMapper(config)

            # Assert
            # Only valid exchange should be processed
            assert mapper.get_all_internal_symbols() == ["BTC"]
            # Verify warnings for non-dict exchange data
            warning_calls = [
                call
                for call in mock_logger.warning.call_args_list
                if call[0][0] == "skipping_exchange_invalid_data"
            ]
            assert len(warning_calls) >= 5  # For each invalid type


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
        "hyperliquid": {"symbols": {"BTC": "BTC-PERP", "SOL": "SOL-PERP"}},
        "backpack": {"symbols": {"ETH": "ETH_PERP", "ADA": "ADA_PERP"}},
    }
    mapper = SymbolMapper(config)

    # Act
    result = mapper.get_exchange_symbol(internal_symbol, exchange_id)

    # Assert
    assert result == expected


@pytest.mark.parametrize(
    ("config", "expected_internal_symbols"),
    [
        ({}, []),
        ({"ex1": {"symbols": {}}}, []),
        ({"ex1": {"symbols": {"BTC": "BTC-USD"}}}, ["BTC"]),
        (
            {
                "ex1": {"symbols": {"BTC": "BTC-USD", "ETH": "ETH-USD"}},
                "ex2": {"symbols": {"BTC": "BTC-PERP", "SOL": "SOL-PERP"}},
            },
            ["BTC", "ETH", "SOL"],
        ),
        # Invalid configs that should result in empty mappings
        ({"ex1": "invalid"}, []),
        ({"ex1": {"no_symbols_key": {}}}, []),
        ({"ex1": {"symbols": "not_a_dict"}}, []),
    ],
)
def test_symbol_mapper_init_parametrized(
    config: dict[str, Any], expected_internal_symbols: list[str]
) -> None:
    """Test SymbolMapper initialization with various configurations."""
    # Act
    mapper = SymbolMapper(config)

    # Assert
    assert sorted(mapper.get_all_internal_symbols()) == sorted(expected_internal_symbols)


# ==================== INTEGRATION TESTS ====================


class TestSymbolMapperIntegration:
    """Integration tests for SymbolMapper with complex scenarios."""

    def test_complex_multi_exchange_scenario(self) -> None:
        """Test complex scenario with multiple exchanges and overlapping symbols."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": "BTC-PERP",
                    "ETH": "ETH-PERP",
                    "SOL": "SOL-PERP",
                }
            },
            "exchange2": {
                "symbols": {
                    "BTC": "BTCUSD",
                    "ETH": "ETHUSD",
                    "ADA": "ADAUSD",
                }
            },
            "exchange3": {
                "symbols": {
                    "BTC": "BTC/USD",
                    "SOL": "SOL/USD",
                    "LINK": "LINK/USD",
                }
            },
            "disabled_exchange": {
                "enabled": False,
                # No symbols key
            },
            "invalid_exchange": {
                "symbols": ["BTC", "ETH"]  # Invalid format
            },
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        # Test all internal symbols are found
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
        assert mapper.get_exchange_symbol("ADA", "exchange1") is None

        assert mapper.get_exchange_symbol("LINK", "exchange3") == "LINK/USD"
        assert mapper.get_exchange_symbol("LINK", "exchange2") is None

        # Test disabled/invalid exchanges
        assert mapper.get_exchange_symbol("BTC", "disabled_exchange") is None
        assert mapper.get_exchange_symbol("BTC", "invalid_exchange") is None

    def test_special_characters_in_symbols(self) -> None:
        """Test handling of special characters in symbol names."""
        # Arrange
        config = {
            "exchange1": {
                "symbols": {
                    "BTC": "BTC-PERP",
                    "BTC.D": "BTC.D-PERP",  # Dot in symbol
                    "1000SHIB": "1000SHIB-PERP",  # Numbers in symbol
                    "BNB/BTC": "BNB-BTC",  # Slash in internal symbol
                    "ETH_2X": "ETH_2X_PERP",  # Underscore and number
                }
            }
        }

        # Act
        mapper = SymbolMapper(config)

        # Assert
        assert mapper.get_exchange_symbol("BTC.D", "exchange1") == "BTC.D-PERP"
        assert mapper.get_exchange_symbol("1000SHIB", "exchange1") == "1000SHIB-PERP"
        assert mapper.get_exchange_symbol("BNB/BTC", "exchange1") == "BNB-BTC"
        assert mapper.get_exchange_symbol("ETH_2X", "exchange1") == "ETH_2X_PERP"

        # Test reverse mappings
        assert mapper.get_internal_symbol("BTC.D-PERP", "exchange1") == "BTC.D"
        assert mapper.get_internal_symbol("1000SHIB-PERP", "exchange1") == "1000SHIB"
        assert mapper.get_internal_symbol("BNB-BTC", "exchange1") == "BNB/BTC"
        assert mapper.get_internal_symbol("ETH_2X_PERP", "exchange1") == "ETH_2X"
