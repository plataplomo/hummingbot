"""Enhanced asset indexer snippet with spot mappings support.

This shows how to update the existing HyperliquidAssetIndexResolver to use
the discovered spot mappings based on whether it's testnet or mainnet.
"""

# Add this to the existing HyperliquidAssetIndexResolver class

# Import at the top of the file
from hyperliquid.spot_assets import SpotAssetMappings

# Add to __init__ method
def __init__(self, ...):
    # ... existing init code ...

    # Initialize spot mappings (lazy loaded)
    self._spot_mappings: Optional[SpotAssetMappings] = None

def _resolve_spot_symbol_direct(self, symbol: str) -> int | None:
    """Resolve spot symbols that have direct mappings.

    Handles:
    - @N format (e.g., "@1", "@2") -> direct asset index N
    - Known spot mappings based on network (testnet/mainnet)

    Returns:
        Asset index if directly resolvable, None otherwise
    """
    # Initialize spot mappings if needed
    if self._spot_mappings is None:
        is_testnet = self._is_testnet()
        self._spot_mappings = SpotAssetMappings(is_testnet=is_testnet)

    # Use the mappings class to resolve
    return self._spot_mappings.get_index(symbol)

def _is_testnet(self) -> bool:
    """Detect if we're connected to testnet.

    This checks the HTTP client's endpoint URL to determine network.
    """
    # The asset indexer has access to the requester function which uses the HTTP client
    # We can check if 'testnet' is in the URL by examining the exchange name log
    return 'testnet' in self._exchange_name_for_log.lower()

def get_spot_symbols_for_index(self, index: int) -> list[str]:
    """Get all spot symbols that map to a given asset index.

    Args:
        index: Asset index

    Returns:
        List of symbols (e.g., ["@69", "BTC/USDC"])
    """
    if self._spot_mappings is None:
        is_testnet = self._is_testnet()
        self._spot_mappings = SpotAssetMappings(is_testnet=is_testnet)

    return self._spot_mappings.get_symbols(index)
