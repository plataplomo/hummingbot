"""Symbol domain object fixtures for testing.

This module provides reusable pytest fixtures for Symbol objects,
leveraging the existing symbol factories to avoid repetitive symbol creation.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, cast

import pytest

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols import Symbol, exchanges
from cyberdelta.symbols.models import BaseSymbol
from tests.common_symbols import (
    BTC_BP,
    BTC_HL,
    BTC_USDC_BP,
    # BTC_USDC_HL,  # Doesn't exist
    ETH_BP,
    ETH_HL,
    SOL_BP,
    SOL_HL,
)
from tests.factories.symbol_factories import (
    InternalSymbolFactory,
    SymbolFactory,
    UnifiedSymbolFactory,
)


if TYPE_CHECKING:
    from collections.abc import Callable


# Common test symbols as fixtures using the factories
@pytest.fixture
def btc_perp_hl() -> Symbol:
    """BTC perpetual symbol for Hyperliquid.

    Returns:
        Symbol: BTC perpetual for Hyperliquid exchange.
    """
    return BTC_HL


@pytest.fixture
def btc_perp_bp() -> Symbol:
    """BTC perpetual symbol for Backpack.

    Returns:
        Symbol: BTC perpetual for Backpack exchange.
    """
    return BTC_BP


@pytest.fixture
def eth_perp_hl() -> Symbol:
    """ETH perpetual symbol for Hyperliquid.

    Returns:
        Symbol: ETH perpetual for Hyperliquid exchange.
    """
    return ETH_HL


@pytest.fixture
def eth_perp_bp() -> Symbol:
    """ETH perpetual symbol for Backpack.

    Returns:
        Symbol: ETH perpetual for Backpack exchange.
    """
    return ETH_BP


@pytest.fixture
def btc_spot_hl() -> Symbol:
    """BTC spot symbol for Hyperliquid.

    Returns:
        Symbol: BTC spot symbol for Hyperliquid exchange.
    """
    # Hyperliquid doesn't have spot symbols in the same way, use perp
    return BTC_HL


@pytest.fixture
def btc_spot_bp() -> Symbol:
    """BTC spot symbol for Backpack.

    Returns:
        Symbol: BTC spot symbol for Backpack exchange.
    """
    return BTC_USDC_BP


# Spot asset fixtures (single assets for balance tracking)
@pytest.fixture
def usdc_hl() -> Symbol:
    """USDC spot asset symbol for Hyperliquid.

    Returns:
        Symbol: USDC spot asset for Hyperliquid exchange.
    """
    return SymbolFactory.create_custom_hyperliquid("USDC")


@pytest.fixture
def usdc_bp() -> Symbol:
    """USDC spot asset symbol for Backpack.

    Returns:
        Symbol: USDC spot asset for Backpack exchange.
    """
    return SymbolFactory.create_custom_backpack("USDC")


@pytest.fixture
def btc_asset_hl() -> Symbol:
    """BTC spot asset symbol for Hyperliquid.

    Returns:
        Symbol: BTC spot asset for Hyperliquid exchange.
    """
    return SymbolFactory.create_custom_hyperliquid("BTC")


@pytest.fixture
def btc_asset_bp() -> Symbol:
    """BTC spot asset symbol for Backpack.

    Returns:
        Symbol: BTC spot asset for Backpack exchange.
    """
    return SymbolFactory.create_custom_backpack("BTC")


# Parameterized spot asset fixtures
@pytest.fixture
def spot_asset(any_exchange: ExchangeName) -> Callable[[str], Symbol]:
    """Factory for creating spot asset symbols for any exchange.

    Returns:
        Callable[[str], Symbol]: Function to create spot asset symbols.
    """

    def _create(asset: str) -> Symbol:
        # Use exchanges API for dynamic symbol creation
        factory = getattr(exchanges, any_exchange.value.lower())
        result = factory(f"{asset}_USDC")
        assert isinstance(result, BaseSymbol)
        # BaseSymbol is assignable to Symbol
        return cast(Symbol, result)

    return _create


# Generic symbol creation fixture
@pytest.fixture
def exchange_symbol() -> Callable[[str, ExchangeName], Symbol]:
    """Factory for creating exchange symbols.

    Returns:
        Callable[[str, ExchangeName], Symbol]: Function to create exchange symbols.
    """

    def _create(value: str, exchange: ExchangeName) -> Symbol:
        # Use exchanges API for dynamic symbol creation
        factory = getattr(exchanges, exchange.value.lower())
        result = factory(value)
        assert isinstance(result, BaseSymbol)
        # BaseSymbol is assignable to Symbol
        return cast(Symbol, result)

    return _create


# Internal symbol fixtures (for backward compatibility)
@pytest.fixture
def btc_perp_internal() -> Symbol:
    """BTC perpetual internal symbol.

    Returns:
        Symbol: BTC perpetual internal symbol.
    """
    return InternalSymbolFactory.create_btc_usd_perp()


@pytest.fixture
def eth_perp_internal() -> Symbol:
    """ETH perpetual internal symbol.

    Returns:
        Symbol: ETH perpetual internal symbol.
    """
    return InternalSymbolFactory.create_eth_usd_perp()


@pytest.fixture
def btc_spot_internal() -> Symbol:
    """BTC/USDC spot internal symbol.

    Returns:
        Symbol: BTC/USDC spot internal symbol.
    """
    return InternalSymbolFactory.create_btc_usdc_spot()


# Unified symbol fixtures (for backward compatibility)
@pytest.fixture
def btc_perp_unified() -> Symbol:
    """BTC perpetual unified symbol.

    Returns:
        Symbol: BTC perpetual unified symbol.
    """
    return UnifiedSymbolFactory.create_btc_perp()


@pytest.fixture
def eth_perp_unified() -> Symbol:
    """ETH perpetual unified symbol.

    Returns:
        Symbol: ETH perpetual unified symbol.
    """
    return UnifiedSymbolFactory.create_eth_perp()


@pytest.fixture
def btc_spot_unified() -> Symbol:
    """BTC/USDC spot unified symbol.

    Returns:
        Symbol: BTC/USDC spot unified symbol.
    """
    return UnifiedSymbolFactory.create_btc_usdc_spot()


# Symbol pair fixtures for arbitrage testing
@pytest.fixture
def btc_perp_pair() -> tuple[Symbol, Symbol]:
    """BTC perpetual symbol pair for arbitrage.

    Returns:
        tuple[Symbol, Symbol]: BTC perpetual symbol pair for arbitrage.
    """
    return (
        BTC_HL,
        BTC_BP,
    )


@pytest.fixture
def eth_perp_pair() -> tuple[Symbol, Symbol]:
    """ETH perpetual symbol pair for arbitrage.

    Returns:
        tuple[Symbol, Symbol]: ETH perpetual symbol pair for arbitrage.
    """
    return (
        ETH_HL,
        ETH_BP,
    )


@pytest.fixture
def sol_perp_pair() -> tuple[Symbol, Symbol]:
    """SOL perpetual symbol pair for arbitrage.

    Returns:
        tuple[Symbol, Symbol]: SOL perpetual symbol pair for arbitrage.
    """
    return (
        SOL_HL,
        SOL_BP,
    )


# Factory fixtures for dynamic symbol creation
@pytest.fixture
def symbol_factory() -> SymbolFactory:
    """Symbol factory instance for custom symbol creation.

    Returns:
        SymbolFactory: Symbol factory instance for custom symbol creation.
    """
    return SymbolFactory()


@pytest.fixture
def create_symbol() -> Callable[[str, ExchangeName], Symbol]:
    """Factory function for creating custom symbols.

    Returns:
        Callable[[str, ExchangeName], Symbol]: Function to create custom symbols.
    """

    def _create(value: str, exchange: ExchangeName) -> Symbol:
        if exchange == ExchangeName.HYPERLIQUID:
            return SymbolFactory.create_custom_hyperliquid(value)
        return SymbolFactory.create_custom_backpack(value)

    return _create


# Exchange-specific symbol factories
@pytest.fixture
def hyperliquid_symbol() -> Callable[[str], Symbol]:
    """Factory for creating Hyperliquid symbols.

    Returns:
        Callable[[str], Symbol]: Function to create Hyperliquid symbols.
    """

    def _create(value: str) -> Symbol:
        return SymbolFactory.create_custom_hyperliquid(value)

    return _create


@pytest.fixture
def backpack_symbol() -> Callable[[str], Symbol]:
    """Factory for creating Backpack symbols.

    Returns:
        Callable[[str], Symbol]: Function to create Backpack symbols.
    """

    def _create(value: str) -> Symbol:
        return SymbolFactory.create_custom_backpack(value)

    return _create


# Exchange name fixtures
@pytest.fixture
def any_exchange() -> ExchangeName:
    """Default exchange for testing (Hyperliquid).

    Returns:
        ExchangeName: Default exchange for testing.
    """
    return ExchangeName.HYPERLIQUID


# Parameterized symbol fixtures
@pytest.fixture
def btc_perp_any_exchange(any_exchange: ExchangeName) -> Symbol:
    """BTC perpetual symbol for any exchange.

    Returns:
        Symbol: BTC perpetual symbol for any exchange.
    """
    if any_exchange == ExchangeName.HYPERLIQUID:
        return SymbolFactory.create_btc_perp_hyperliquid()
    return SymbolFactory.create_btc_perp_backpack()


@pytest.fixture
def eth_perp_any_exchange(any_exchange: ExchangeName) -> Symbol:
    """ETH perpetual symbol for any exchange.

    Returns:
        Symbol: ETH perpetual symbol for any exchange.
    """
    if any_exchange == ExchangeName.HYPERLIQUID:
        return SymbolFactory.create_eth_perp_hyperliquid()
    return SymbolFactory.create_eth_perp_backpack()


# Invalid symbol fixtures
@pytest.fixture
def invalid_symbol_long() -> str:
    """Invalid symbol - too long.

    Returns:
        str: Invalid symbol that exceeds maximum length.
    """
    return "A" * 31  # Exceeds max length of 30


@pytest.fixture
def invalid_symbol_empty() -> str:
    """Invalid symbol - empty string.

    Returns:
        str: Empty string for testing invalid symbols.
    """
    return ""


@pytest.fixture
def invalid_symbol_special_chars() -> str:
    """Invalid symbol - contains special characters.

    Returns:
        str: Symbol with invalid special characters.
    """
    return "BTC$USD"


# Arbitrage pair fixtures
@pytest.fixture
def btc_arbitrage_pair() -> tuple[Symbol, Symbol]:
    """BTC arbitrage pair (Hyperliquid and Backpack).

    Returns:
        tuple[Symbol, Symbol]: BTC arbitrage pair for cross-exchange trading.
    """
    return (SymbolFactory.create_btc_perp_hyperliquid(), SymbolFactory.create_btc_perp_backpack())


@pytest.fixture
def eth_arbitrage_pair() -> tuple[Symbol, Symbol]:
    """ETH arbitrage pair (Hyperliquid and Backpack).

    Returns:
        tuple[Symbol, Symbol]: ETH arbitrage pair for cross-exchange trading.
    """
    return (SymbolFactory.create_eth_perp_hyperliquid(), SymbolFactory.create_eth_perp_backpack())


# Common test symbols collection
@pytest.fixture
def common_test_symbols() -> dict[str, Symbol]:
    """Collection of commonly used test symbols.

    Returns:
        dict[str, Symbol]: Dictionary of commonly used test symbols.
    """
    return {
        "btc_perp_hl": SymbolFactory.create_btc_perp_hyperliquid(),
        "btc_perp_bp": SymbolFactory.create_btc_perp_backpack(),
        "eth_perp_hl": SymbolFactory.create_eth_perp_hyperliquid(),
        "eth_perp_bp": SymbolFactory.create_eth_perp_backpack(),
        "btc_spot_hl": SymbolFactory.create_btc_usdc_spot_hyperliquid(),
        "btc_spot_bp": SymbolFactory.create_btc_usdc_spot_backpack(),
        "sol_perp_hl": SymbolFactory.create_sol_perp_hyperliquid(),
        "sol_perp_bp": SymbolFactory.create_sol_perp_backpack(),
    }
