"""Hyperliquid Spot Assets Enumeration.

This module provides structured access to spot asset mappings with proper
enum support for type safety and IDE autocomplete.
"""

from enum import IntEnum
from typing import Dict, Optional


class HyperliquidSpotAsset(IntEnum):
    """Enumeration of Hyperliquid spot assets with their indices.

    This provides type-safe access to spot assets and their indices.
    Only includes commonly used assets. Full mappings are in separate file.
    """

    # Core stablecoins and test tokens
    USDC = 0
    PURR = 0  # Same index as USDC on testnet
    TEST = 2

    # Popular test tokens
    JPL = 3
    BREAD = 4
    P = 5
    KOGU = 6
    WOOF = 34
    BTC = 69  # BTC/USDC pair

    # Add more as needed...


class SpotAssetMappings:
    """Manages spot asset symbol to index mappings."""

    def __init__(self, is_testnet: bool = True):
        self.is_testnet = is_testnet
        self._mappings: Optional[Dict[str, int]] = None
        self._reverse_mappings: Optional[Dict[int, list[str]]] = None

    def _load_mappings(self) -> Dict[str, int]:
        """Load mappings from file based on network."""
        if self.is_testnet:
            # Import testnet mappings
            from .testnet_spot_mappings import TESTNET_SPOT_MAPPINGS
            return TESTNET_SPOT_MAPPINGS
        else:
            # Import mainnet mappings when available
            from .mainnet_spot_mappings import MAINNET_SPOT_MAPPINGS
            return MAINNET_SPOT_MAPPINGS

    @property
    def mappings(self) -> Dict[str, int]:
        """Get symbol to index mappings (lazy loaded)."""
        if self._mappings is None:
            self._mappings = self._load_mappings()
        return self._mappings

    @property
    def reverse_mappings(self) -> Dict[int, list[str]]:
        """Get index to symbols mappings (lazy loaded)."""
        if self._reverse_mappings is None:
            self._reverse_mappings = {}
            for symbol, index in self.mappings.items():
                if index not in self._reverse_mappings:
                    self._reverse_mappings[index] = []
                self._reverse_mappings[index].append(symbol)
        return self._reverse_mappings

    def get_index(self, symbol: str) -> Optional[int]:
        """Get asset index for a symbol.

        Args:
            symbol: Asset symbol (e.g., "@1", "PURR/USDC")

        Returns:
            Asset index if found, None otherwise
        """
        # Handle @N format directly
        if symbol.startswith("@") and symbol[1:].isdigit():
            return int(symbol[1:])

        # Look up in mappings
        return self.mappings.get(symbol)

    def get_symbols(self, index: int) -> list[str]:
        """Get all symbols for an asset index.

        Args:
            index: Asset index

        Returns:
            List of symbols that map to this index
        """
        symbols = self.reverse_mappings.get(index, [])
        # Always include @N format
        at_symbol = f"@{index}"
        if at_symbol not in symbols:
            symbols = [at_symbol] + symbols
        return symbols

    def get_primary_symbol(self, index: int) -> str:
        """Get the primary symbol for an asset index.

        Args:
            index: Asset index

        Returns:
            Primary symbol (NAME/USDC format if available, else @N)
        """
        symbols = self.get_symbols(index)
        # Prefer NAME/USDC format over @N format
        for symbol in symbols:
            if "/" in symbol and not symbol.startswith("@"):
                return symbol
        return symbols[0] if symbols else f"@{index}"


# Singleton instances for convenience
TESTNET_MAPPINGS = SpotAssetMappings(is_testnet=True)
MAINNET_MAPPINGS = SpotAssetMappings(is_testnet=False)


def get_spot_asset_index(symbol: str, is_testnet: bool = True) -> Optional[int]:
    """Convenience function to get asset index.

    Args:
        symbol: Asset symbol
        is_testnet: Whether on testnet or mainnet

    Returns:
        Asset index if found
    """
    mappings = TESTNET_MAPPINGS if is_testnet else MAINNET_MAPPINGS
    return mappings.get_index(symbol)
