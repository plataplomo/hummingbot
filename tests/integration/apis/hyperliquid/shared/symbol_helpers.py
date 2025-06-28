"""Exchange-agnostic symbol helpers for integration tests.

This module provides functions to get available symbols from the exchange
rather than using hardcoded symbol lists that violate security rules.
"""

from typing import Any

from cyberdelta.apis.common import APIError
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetMarketsArgs


async def get_available_symbols(api: HyperliquidAPI, market_type: str = "perp") -> list[str]:
    """Get available trading symbols from exchange.

    Args:
        api: HyperliquidAPI instance
        market_type: Type of market ("perp" or "spot")

    Returns:
        List of available symbols from exchange

    Raises:
        RuntimeError: If unable to get symbols from exchange
    """
    try:
        # Get all available markets from exchange
        args = GetMarketsArgs()
        all_markets = await api.get_markets(args)
        if not all_markets:
            raise RuntimeError(
                f"Failed to get {market_type} markets from exchange. "
                "Tests require access to real market data.",
            )

        # Filter by market type if needed
        if market_type == "perp":
            symbols = [market.symbol for market in all_markets if market.market_type == "Perpetual"]
        elif market_type == "spot":
            symbols = [market.symbol for market in all_markets if market.market_type == "Spot"]
        else:
            symbols = [market.symbol for market in all_markets]

        if not symbols:
            raise RuntimeError(
                f"No {market_type} symbols available from exchange. "
                "Cannot run integration tests without available markets.",
            )

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get available {market_type} symbols from exchange: {e}. "
            "Integration tests must have access to real exchange data.",
        ) from e
    else:
        return symbols


async def get_test_symbol(api: HyperliquidAPI, market_type: str = "perp", index: int = 0) -> str:
    """Get a specific test symbol by index.

    Args:
        api: HyperliquidAPI instance
        market_type: Type of market ("perp" or "spot")
        index: Index of symbol to return (0 = first available)

    Returns:
        Symbol string from exchange

    Raises:
        RuntimeError: If unable to get symbol or index out of range
    """
    symbols = await get_available_symbols(api, market_type)

    if index >= len(symbols):
        raise RuntimeError(
            f"Symbol index {index} out of range. "
            f"Only {len(symbols)} {market_type} symbols available: {symbols}",
        )

    return symbols[index]


async def get_major_crypto_symbol(api: HyperliquidAPI, crypto: str = "BTC") -> str:
    """Get symbol for a major cryptocurrency if available.

    Args:
        api: HyperliquidAPI instance
        crypto: Cryptocurrency to find (e.g., "BTC", "ETH")

    Returns:
        Symbol string that matches the crypto

    Raises:
        RuntimeError: If crypto not available on exchange
    """
    symbols = await get_available_symbols(api, "perp")

    # Look for symbols containing the crypto name
    matching_symbols = [s for s in symbols if crypto in s.upper()]

    if not matching_symbols:
        raise RuntimeError(
            f"Cryptocurrency {crypto} not available on exchange. "
            f"Available symbols: {symbols[:10]}... "
            "Tests cannot use hardcoded symbols that don't exist on exchange.",
        )

    # Return the first match (usually the main perpetual)
    return matching_symbols[0]


def validate_symbol_format(symbol: str, exchange_name: str = "hyperliquid") -> bool:
    """Validate symbol format for specific exchange.

    Args:
        symbol: Symbol to validate
        exchange_name: Exchange name for format validation

    Returns:
        True if symbol format is valid for exchange
    """
    if exchange_name.lower() == "hyperliquid":
        # Hyperliquid uses simple symbols like "BTC", "ETH" for perps
        # and may use different formats for spot
        return len(symbol) > 0 and symbol.isalnum()

    return True  # Default to permissive for unknown exchanges


async def get_exchange_symbol_mapping(api: HyperliquidAPI) -> dict[str, Any]:
    """Get exchange-specific symbol mapping information.

    Args:
        api: HyperliquidAPI instance

    Returns:
        Dict with symbol mapping information from exchange

    Raises:
        RuntimeError: When failing to get exchange symbol mapping due to
            connectivity issues or API errors.
    """
    try:
        # Get market information that includes symbol formatting
        args = GetMarketsArgs()
        markets = await api.get_markets(args)

        return {
            "available_symbols": [m.symbol for m in markets],
            "perp_symbols": [m.symbol for m in markets if m.market_type == "Perpetual"],
            "spot_symbols": [m.symbol for m in markets if m.market_type == "Spot"],
            "symbol_details": {
                m.symbol: {
                    "tick_size": m.tick_size,
                    "step_size": m.step_size,
                    "min_quantity": m.min_quantity,
                    "max_quantity": m.max_quantity,
                    "market_type": m.market_type,
                }
                for m in markets
            },
        }

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get exchange symbol mapping: {e}. "
            "Tests require access to exchange symbol information.",
        ) from e
