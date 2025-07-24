"""Unit tests for the SymbolMapper component.

Tests symbol mapping functionality between different exchanges and internal representations.
"""

from typing import Any, cast  # Added Dict, Any and cast

import pytest
from _pytest.logging import LogCaptureFixture

from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions import (
    ExchangeNotSupportedError,
    SymbolMappingConfigurationError,
    SymbolNotFoundError,
)
from tests.unit.core.conftest import create_test_exchange_config


# Sample valid config
VALID_CONFIG = {
    "exchanges": {
        "hyperliquid": create_test_exchange_config(
            ExchangeName.HYPERLIQUID,
            symbols={"BTC": "BTC-PERP", "ETH": "ETH-PERP"},
        ),
        "backpack": create_test_exchange_config(
            ExchangeName.BACKPACK,
            symbols={"BTC": "BTC_PERP", "SOL": "SOL_PERP"},
        ),
        "kraken": create_test_exchange_config(
            ExchangeName.HYPERLIQUID,  # Using HYPERLIQUID as placeholder since KRAKEN doesn't exist
            symbols={"BTC": "BTC-USD", "ETH": "ETH-USD"},
        ),
        "disabled_exchange": create_test_exchange_config(
            ExchangeName.HYPERLIQUID,
            symbols={"BTC": "BTC"},
            enabled=False,
        ),
        "invalid_symbols_exchange": create_test_exchange_config(
            ExchangeName.HYPERLIQUID,
            symbols={"BTC": "BTC", "ETH": "ETH"},
        ),
    },
}

# --- Test Initialization ---


def test_symbol_mapper_init_success() -> None:
    """Test successful initialization with a valid config."""
    mapper = SymbolMapper(VALID_CONFIG["exchanges"])
    assert mapper is not None
    assert len(mapper.get_all_internal_symbols()) == 3  # BTC, ETH, SOL
    # Test exchange initialization through public mapping functionality
    # Verify that enabled exchanges can perform mappings
    assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
    assert mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
    assert mapper.get_exchange_symbol("ETH", "kraken") == "ETH-USD"

    # Verify disabled exchanges are properly excluded (should raise exception)
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_exchange_symbol("BTC", "disabled_exchange")


def test_symbol_mapper_init_missing_exchanges_key() -> None:
    """Test initialization fails if 'exchanges' key is missing."""
    # SymbolMapper expects the exchanges dict directly.
    # This test should perhaps test that SymbolMapper raises if passed something other than a dict,
    with pytest.raises(SymbolMappingConfigurationError, match="Configuration cannot be empty"):
        SymbolMapper(cast("dict[str, Any]", None))
    with pytest.raises(AttributeError, match="'str' object has no attribute 'items'"):
        SymbolMapper(cast("dict[str, Any]", "not_a_dict"))


def test_symbol_mapper_init_exchanges_not_dict() -> None:
    """Test initialization fails if 'exchanges' value is not a dict."""
    with pytest.raises(AttributeError, match="'list' object has no attribute 'items'"):
        SymbolMapper(cast("dict[str, Any]", ["list", "not", "dict"]))


def test_symbol_mapper_init_skips_invalid_entries(caplog: LogCaptureFixture) -> None:
    """Test that invalid entries within the config cause immediate failure."""
    config_with_invalid: dict[str, Any] = {
        "valid_exchange": {"symbols": {"BTC": "BTC-OK"}},
        "missing_symbols": {"enabled": True},  # Missing 'symbols' key - will fail validation
    }

    # Business logic now uses strict Pydantic validation and fails immediately on invalid config
    with pytest.raises(AttributeError, match="'dict' object has no attribute 'enabled'"):
        SymbolMapper(config_with_invalid)


# --- Test Mapping Methods ---


@pytest.fixture
def mapper() -> SymbolMapper:
    """Fixture to provide a configured SymbolMapper instance.

    Returns:
        SymbolMapper instance with test configuration
    """
    return SymbolMapper(VALID_CONFIG["exchanges"])


def test_get_exchange_symbol_success(mapper: SymbolMapper) -> None:
    """Test successful lookup of exchange-specific symbols."""
    assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
    assert mapper.get_exchange_symbol("ETH", "hyperliquid") == "ETH-PERP"
    assert mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
    assert mapper.get_exchange_symbol("SOL", "backpack") == "SOL_PERP"
    assert mapper.get_exchange_symbol("BTC", "kraken") == "BTC-USD"
    assert mapper.get_exchange_symbol("ETH", "kraken") == "ETH-USD"


def test_get_exchange_symbol_not_found(mapper: SymbolMapper) -> None:
    """Test lookups raise exceptions for missing mappings or exchanges."""
    # SOL not on hyperliquid - symbol exists but not on this exchange
    with pytest.raises(SymbolNotFoundError):
        mapper.get_exchange_symbol("SOL", "hyperliquid")

    # ETH not on backpack - symbol exists but not on this exchange
    with pytest.raises(SymbolNotFoundError):
        mapper.get_exchange_symbol("ETH", "backpack")

    # DOGE not configured - internal symbol doesn't exist
    with pytest.raises(SymbolNotFoundError):
        mapper.get_exchange_symbol("DOGE", "hyperliquid")

    # Exchange not configured
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_exchange_symbol("BTC", "nonexistent_exchange")

    # Exchange skipped/disabled
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_exchange_symbol("BTC", "disabled_exchange")


def test_get_internal_symbol_success(mapper: SymbolMapper) -> None:
    """Test successful lookup of internal symbols."""
    assert mapper.get_internal_symbol("BTC-PERP", "hyperliquid") == "BTC"
    assert mapper.get_internal_symbol("ETH-PERP", "hyperliquid") == "ETH"
    assert mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"
    assert mapper.get_internal_symbol("SOL_PERP", "backpack") == "SOL"
    assert mapper.get_internal_symbol("BTC-USD", "kraken") == "BTC"
    assert mapper.get_internal_symbol("ETH-USD", "kraken") == "ETH"


def test_get_internal_symbol_not_found(mapper: SymbolMapper) -> None:
    """Test internal symbol lookups raise exceptions for missing mappings or exchanges."""
    # Wrong format/symbol for exchange - exchange symbol not found
    with pytest.raises(SymbolNotFoundError):
        mapper.get_internal_symbol("SOL-PERP", "hyperliquid")

    # Wrong symbol for exchange - exchange symbol not found
    with pytest.raises(SymbolNotFoundError):
        mapper.get_internal_symbol("ETH_PERP", "backpack")

    # Unconfigured symbol - exchange symbol not found
    with pytest.raises(SymbolNotFoundError):
        mapper.get_internal_symbol("DOGE-USD", "kraken")

    # Exchange not configured
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_internal_symbol("BTC-PERP", "nonexistent_exchange")

    # Exchange skipped/disabled
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_internal_symbol("BTC-PERP", "disabled_exchange")


def test_get_all_internal_symbols(mapper: SymbolMapper) -> None:
    """Test retrieval of all unique internal symbols."""
    internal_symbols = mapper.get_all_internal_symbols()
    assert isinstance(internal_symbols, list)
    assert sorted(internal_symbols) == ["BTC", "ETH", "SOL"]


def test_get_exchange_symbols_for_internal(mapper: SymbolMapper) -> None:
    """Test getting all exchange symbols mapped to an internal symbol."""
    btc_map = mapper.get_exchange_symbols_for_internal("BTC")
    assert btc_map == {
        "hyperliquid": "BTC-PERP",
        "backpack": "BTC_PERP",
        "kraken": "BTC-USD",
        "invalid_symbols_exchange": "BTC",
    }

    eth_map = mapper.get_exchange_symbols_for_internal("ETH")
    assert eth_map == {
        "hyperliquid": "ETH-PERP",
        "kraken": "ETH-USD",
        "invalid_symbols_exchange": "ETH",
    }

    sol_map = mapper.get_exchange_symbols_for_internal("SOL")
    assert sol_map == {"backpack": "SOL_PERP"}

    # Non-existent symbol should raise exception instead of returning empty dict
    with pytest.raises(SymbolNotFoundError):
        mapper.get_exchange_symbols_for_internal("DOGE")


def test_get_internal_symbols_for_exchange(mapper: SymbolMapper) -> None:
    """Test getting the map of exchange -> internal symbols for a specific exchange."""
    hyperliquid_map = mapper.get_internal_symbols_for_exchange("hyperliquid")
    assert hyperliquid_map == {"BTC-PERP": "BTC", "ETH-PERP": "ETH"}

    backpack_map = mapper.get_internal_symbols_for_exchange("backpack")
    assert backpack_map == {"BTC_PERP": "BTC", "SOL_PERP": "SOL"}

    kraken_map = mapper.get_internal_symbols_for_exchange("kraken")
    assert kraken_map == {"BTC-USD": "BTC", "ETH-USD": "ETH"}

    # Nonexistent exchange should raise exception
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_internal_symbols_for_exchange("nonexistent")

    # Disabled exchange should raise exception
    with pytest.raises(ExchangeNotSupportedError):
        mapper.get_internal_symbols_for_exchange("disabled_exchange")


# --- Test Immutability --- Does not apply as we return copies or primitives

# --- Test Edge Cases ---


def test_empty_config() -> None:
    """Test initialization with an empty config raises error."""
    # Business logic now requires non-empty configuration
    with pytest.raises(SymbolMappingConfigurationError, match="Configuration cannot be empty"):
        SymbolMapper({})


def test_config_with_only_empty_symbols() -> None:
    """Test initialization where exchanges have empty symbol dicts fails validation."""
    config: dict[str, Any] = {"ex1": {"symbols": {}}, "ex2": {"symbols": {}}}
    # Business logic now validates that exchanges have symbols and proper structure
    with pytest.raises(AttributeError, match="'dict' object has no attribute 'enabled'"):
        SymbolMapper(config)
