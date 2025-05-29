import logging
from typing import Any  # Added Dict and Any

import pytest
from _pytest.logging import LogCaptureFixture

from cyberdelta.core.symbol_mapper import SymbolMapper, SymbolMappingError

# Sample valid config
VALID_CONFIG = {
    "exchanges": {
        "hyperliquid": {
            "symbols": {
                "BTC": "BTC-PERP",
                "ETH": "ETH-PERP",
            }
        },
        "backpack": {
            "symbols": {
                "BTC": "BTC_PERP",
                "SOL": "SOL_PERP",
            }
        },
        "kraken": {
            "symbols": {
                "BTC": "BTC/USD",
                "ETH": "ETH/USD",
            }
        },
        "disabled_exchange": {  # Example of exchange data without symbols key
            "enabled": False,
        },
        "invalid_symbols_exchange": {  # Example with non-dict symbols
            "symbols": ["BTC", "ETH"]
        },
    }
}

# --- Test Initialization ---


def test_symbol_mapper_init_success() -> None:
    """Test successful initialization with a valid config."""
    mapper = SymbolMapper(VALID_CONFIG["exchanges"])
    assert mapper is not None
    assert len(mapper.get_all_internal_symbols()) == 3  # BTC, ETH, SOL
    assert "hyperliquid" in mapper._exchange_to_internal
    assert "backpack" in mapper._exchange_to_internal
    assert "kraken" in mapper._exchange_to_internal
    assert "disabled_exchange" not in mapper._exchange_to_internal  # Skipped
    assert "invalid_symbols_exchange" not in mapper._exchange_to_internal  # Skipped


def test_symbol_mapper_init_missing_exchanges_key() -> None:
    """Test initialization fails if 'exchanges' key is missing."""
    # invalid_config: dict[str, Any] = {"some_other_key": {}} # This config doesn't have "exchanges"
    # SymbolMapper expects the exchanges dict directly.
    # This test should perhaps test that SymbolMapper raises if passed something other than a dict,
    with pytest.raises(SymbolMappingError, match="Invalid configuration: Expected a dictionary"):
        SymbolMapper(None)
    with pytest.raises(SymbolMappingError, match="Invalid configuration: Expected a dictionary"):
        SymbolMapper("not_a_dict")


def test_symbol_mapper_init_exchanges_not_dict() -> None:
    """Test initialization fails if 'exchanges' value is not a dict."""
    with pytest.raises(
        SymbolMappingError,
        match="Invalid configuration: Expected a dictionary of exchanges, got <class 'list'>",
    ):
        SymbolMapper(["list", "not", "dict"])


def test_symbol_mapper_init_skips_invalid_entries(caplog: LogCaptureFixture) -> None:
    """Test that invalid entries within the config are skipped with warnings."""
    config_with_invalid: dict[str, Any] = {
        "exchanges": {
            "valid_exchange": {"symbols": {"BTC": "BTC-OK"}},
            "missing_symbols": {"enabled": True},  # Missing 'symbols' key
            "invalid_symbols_type": {"symbols": "not_a_dict"},
            "invalid_entry_type": {"symbols": {123: "BTC-INVALID"}},  # Non-string key
            "invalid_value_type": {"symbols": {"ETH": 456}},  # Non-string value
        }
    }
    with caplog.at_level(logging.WARNING):  # Use logging.WARNING constant
        mapper = SymbolMapper(config_with_invalid["exchanges"])

    assert mapper is not None
    assert "valid_exchange" in mapper._exchange_to_internal
    assert "missing_symbols" not in mapper._exchange_to_internal
    assert "invalid_symbols_type" not in mapper._exchange_to_internal
    assert "ETH" not in mapper._internal_to_exchange  # Check symbol with invalid value not added

    # Check for specific warning logs
    assert "Skipping exchange 'missing_symbols': Missing 'symbols' configuration." in caplog.text
    assert (
        "Skipping exchange 'invalid_symbols_type': 'symbols' must be a dictionary." in caplog.text
    )
    assert (
        "Invalid symbol map key for ex 'invalid_entry_type': Skip (123). Must be str."
    ) in caplog.text
    assert (
        "Invalid symbol map value for ex 'invalid_value_type': Skip (ETH: 456). Value must be str."
    ) in caplog.text


# --- Test Mapping Methods ---


@pytest.fixture
def mapper() -> SymbolMapper:
    """Fixture to provide a configured SymbolMapper instance."""
    return SymbolMapper(VALID_CONFIG["exchanges"])


def test_get_exchange_symbol_success(mapper: SymbolMapper) -> None:
    """Test successful lookup of exchange-specific symbols."""
    assert mapper.get_exchange_symbol("BTC", "hyperliquid") == "BTC-PERP"
    assert mapper.get_exchange_symbol("ETH", "hyperliquid") == "ETH-PERP"
    assert mapper.get_exchange_symbol("BTC", "backpack") == "BTC_PERP"
    assert mapper.get_exchange_symbol("SOL", "backpack") == "SOL_PERP"
    assert mapper.get_exchange_symbol("BTC", "kraken") == "BTC/USD"
    assert mapper.get_exchange_symbol("ETH", "kraken") == "ETH/USD"


def test_get_exchange_symbol_not_found(mapper: SymbolMapper) -> None:
    """Test lookups return None for missing mappings or exchanges."""
    assert mapper.get_exchange_symbol("SOL", "hyperliquid") is None  # SOL not on hyperliquid
    assert mapper.get_exchange_symbol("ETH", "backpack") is None  # ETH not on backpack
    assert mapper.get_exchange_symbol("DOGE", "hyperliquid") is None  # DOGE not configured
    assert (
        mapper.get_exchange_symbol("BTC", "nonexistent_exchange") is None
    )  # Exchange not configured
    assert mapper.get_exchange_symbol("BTC", "disabled_exchange") is None  # Exchange skipped


def test_get_internal_symbol_success(mapper: SymbolMapper) -> None:
    """Test successful lookup of internal symbols."""
    assert mapper.get_internal_symbol("BTC-PERP", "hyperliquid") == "BTC"
    assert mapper.get_internal_symbol("ETH-PERP", "hyperliquid") == "ETH"
    assert mapper.get_internal_symbol("BTC_PERP", "backpack") == "BTC"
    assert mapper.get_internal_symbol("SOL_PERP", "backpack") == "SOL"
    assert mapper.get_internal_symbol("BTC/USD", "kraken") == "BTC"
    assert mapper.get_internal_symbol("ETH/USD", "kraken") == "ETH"


def test_get_internal_symbol_not_found(mapper: SymbolMapper) -> None:
    """Test internal symbol lookups return None for missing mappings or exchanges."""
    assert (
        mapper.get_internal_symbol("SOL-PERP", "hyperliquid") is None
    )  # Wrong format/symbol for exchange
    assert mapper.get_internal_symbol("ETH_PERP", "backpack") is None  # Wrong symbol for exchange
    assert mapper.get_internal_symbol("DOGE/USD", "kraken") is None  # Unconfigured symbol
    assert (
        mapper.get_internal_symbol("BTC-PERP", "nonexistent_exchange") is None
    )  # Exchange not configured
    assert mapper.get_internal_symbol("BTC-PERP", "disabled_exchange") is None  # Exchange skipped


def test_get_all_internal_symbols(mapper: SymbolMapper) -> None:
    """Test retrieval of all unique internal symbols."""
    internal_symbols = mapper.get_all_internal_symbols()
    assert isinstance(internal_symbols, list)
    assert sorted(internal_symbols) == ["BTC", "ETH", "SOL"]


def test_get_exchange_symbols_for_internal(mapper: SymbolMapper) -> None:
    """Test getting all exchange symbols mapped to an internal symbol."""
    btc_map = mapper.get_exchange_symbols_for_internal("BTC")
    assert btc_map == {"hyperliquid": "BTC-PERP", "backpack": "BTC_PERP", "kraken": "BTC/USD"}

    eth_map = mapper.get_exchange_symbols_for_internal("ETH")
    assert eth_map == {"hyperliquid": "ETH-PERP", "kraken": "ETH/USD"}

    sol_map = mapper.get_exchange_symbols_for_internal("SOL")
    assert sol_map == {"backpack": "SOL_PERP"}

    doge_map = mapper.get_exchange_symbols_for_internal("DOGE")
    assert doge_map == {}


def test_get_internal_symbols_for_exchange(mapper: SymbolMapper) -> None:
    """Test getting the map of exchange -> internal symbols for a specific exchange."""
    hyperliquid_map = mapper.get_internal_symbols_for_exchange("hyperliquid")
    assert hyperliquid_map == {"BTC-PERP": "BTC", "ETH-PERP": "ETH"}

    backpack_map = mapper.get_internal_symbols_for_exchange("backpack")
    assert backpack_map == {"BTC_PERP": "BTC", "SOL_PERP": "SOL"}

    kraken_map = mapper.get_internal_symbols_for_exchange("kraken")
    assert kraken_map == {"BTC/USD": "BTC", "ETH/USD": "ETH"}

    nonexistent_map = mapper.get_internal_symbols_for_exchange("nonexistent")
    assert nonexistent_map == {}

    disabled_map = mapper.get_internal_symbols_for_exchange("disabled_exchange")
    assert disabled_map == {}


# --- Test Immutability --- Does not apply as we return copies or primitives

# --- Test Edge Cases ---


def test_empty_config() -> None:
    """Test initialization with an empty but valid structure."""
    mapper = SymbolMapper({})
    assert mapper.get_all_internal_symbols() == []
    assert mapper.get_exchange_symbol("BTC", "any") is None
    assert mapper.get_internal_symbol("BTC-PERP", "any") is None


def test_config_with_only_empty_symbols() -> None:
    """Test initialization where exchanges have empty symbol dicts."""
    config: dict[str, Any] = {"ex1": {"symbols": {}}, "ex2": {"symbols": {}}}
    mapper = SymbolMapper(config)
    assert mapper.get_all_internal_symbols() == []
    assert "ex1" in mapper._exchange_to_internal
    assert "ex2" in mapper._exchange_to_internal
