"""Base Symbol fixtures for testing.

These fixtures provide the foundational Symbol system components
needed by all Symbol-aware tests.
"""

import pytest

from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.symbols import exchanges, get_registry, get_symbol_service
from cyberdelta.symbols.handlers.backpack import BackpackHandler
from cyberdelta.symbols.handlers.hyperliquid import HyperliquidHandler
from cyberdelta.symbols.models import BackpackMetadata, HyperliquidMetadata
from cyberdelta.symbols.protocols import ExchangeHandler
from cyberdelta.symbols.registry import SymbolRegistry
from cyberdelta.symbols.service import SymbolService


@pytest.fixture
def symbol_service() -> SymbolService:
    """Provide configured SymbolService for tests.

    Returns:
        SymbolService: Production symbol service instance
    """
    return get_symbol_service()


@pytest.fixture
def symbol_registry() -> SymbolRegistry:
    """Provide symbol registry for tests.

    Returns:
        SymbolRegistry: Production registry instance
    """
    return get_registry()


@pytest.fixture
def exchange_handlers() -> dict[
    ExchangeName, ExchangeHandler[HyperliquidMetadata] | ExchangeHandler[BackpackMetadata]
]:
    """Provide exchange handlers for tests.

    Returns:
        dict: Mapping of exchange names to handlers
    """
    registry = get_registry()
    return registry.get_handlers()


@pytest.fixture
def hyperliquid_handler() -> ExchangeHandler[HyperliquidMetadata]:
    """Provide Hyperliquid handler.

    Returns:
        ExchangeHandler: Hyperliquid-specific handler
    """
    return HyperliquidHandler()


@pytest.fixture
def backpack_handler() -> ExchangeHandler[BackpackMetadata]:
    """Provide Backpack handler.

    Returns:
        ExchangeHandler: Backpack-specific handler
    """
    return BackpackHandler()


@pytest.fixture
def exchanges_api() -> object:
    """Provide exchanges namespace for symbol creation.

    Returns:
        Exchanges: Exchange namespace API
    """
    return exchanges


@pytest.fixture
def supported_exchanges() -> list[ExchangeName]:
    """Provide list of supported exchanges.

    Returns:
        list: List of supported exchange names
    """
    return [ExchangeName.HYPERLIQUID, ExchangeName.BACKPACK]


@pytest.fixture
def exchange_symbol_formats() -> dict[ExchangeName, dict[str, str]]:
    """Provide exchange-specific symbol format examples.

    Returns:
        dict: Exchange to format type to example mapping
    """
    return {
        ExchangeName.HYPERLIQUID: {
            "perp": "BTC-PERP",
            "spot": "BTC-USDC",
            "separator": "-",
        },
        ExchangeName.BACKPACK: {
            "perp": "BTC_PERP",
            "spot": "BTC_USDC",
            "separator": "_",
        },
    }


@pytest.fixture
def canonical_formats() -> dict[str, str]:
    """Provide canonical format examples.

    Returns:
        dict: Market type to canonical format mapping
    """
    return {
        "perp": "BTC_PERP",
        "spot": "BTC_SPOT",
        "single": "BTC",
    }


@pytest.fixture
def test_assets() -> list[str]:
    """Provide common test assets.

    Returns:
        list: List of asset symbols for testing
    """
    return ["BTC", "ETH", "SOL", "AVAX", "MATIC", "LINK"]


@pytest.fixture
def test_quote_assets() -> list[str]:
    """Provide common quote assets.

    Returns:
        list: List of quote assets for testing
    """
    return ["USD", "USDC", "USDT", "EUR"]
