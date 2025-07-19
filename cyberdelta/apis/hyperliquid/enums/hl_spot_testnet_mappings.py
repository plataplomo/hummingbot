"""Hyperliquid Testnet Spot Asset Mappings.

Auto-generated from testnet API discovery.
Last updated: 2025-07-18

This module contains testnet-specific spot symbol to asset index mappings.
"""

from enum import IntEnum


class HyperliquidTestnetSpotAssets(IntEnum):
    """Common testnet spot assets for type-safe access."""

    # Core tokens
    PURR = 0
    USDC = 0
    TEST = 2

    # Popular test tokens
    JPL = 3
    BREAD = 4
    P = 5
    KOGU = 6
    WOOF = 34
    HOWL = 44
    ONE = 50
    JEFF = 60
    MOGG = 61
    BTC = 69

    # Additional test tokens
    TestPascal1 = 7
    KORILA = 8
    CHUTORO = 11
    OTORO = 12
    ODDISH = 13


# Full testnet mappings dictionary
# Only includes named symbols (not @N format which is handled separately)
TESTNET_SPOT_SYMBOL_MAPPINGS = {
    # Asset index 0
    "PURR/USDC": 0,
    "USDC/USDC": 0,
    # Asset index 2-6
    "TEST/USDC": 2,
    "JPL /USDC": 3,  # Note: has trailing space
    "BREAD/USDC": 4,
    "P/USDC": 5,
    "KOGU/USDC": 6,
    # Asset index 7-33 (test tokens)
    "TestPascal1/USDC": 7,
    "KORILA/USDC": 8,
    "werfqwer/USDC": 9,
    "trwe/USDC": 10,
    "CHUTORO/USDC": 11,
    "OTORO/USDC": 12,
    "ODDISH/USDC": 13,
    "TEST0/USDC": 14,
    "TEST1/USDC": 15,
    "TEST2/USDC": 16,
    "PACOU/USDC": 17,
    "TEST3/USDC": 18,
    "TEST4/USDC": 19,
    "PACOU0/USDC": 20,
    "PKL/USDC": 21,
    "DDD/USDC": 22,
    "TSRSFG/USDC": 23,
    "TT/USDC": 24,
    "TT0/USDC": 25,
    "TT1/USDC": 26,
    "QQ/USDC": 27,
    "TSRSFG0/USDC": 28,
    "TSRSFG1/USDC": 29,
    "TSRSFGGGG/USDC": 30,
    "TSRSFGQQQQQ/USDC": 31,
    "QQ0/USDC": 32,
    "QQ1/USDC": 33,
    # Asset index 34-99 (popular tokens)
    "WOOF/USDC": 34,
    "WOOF0/USDC": 35,
    "WOOF1/USDC": 36,
    "QQ2/USDC": 37,
    "MYUNG/USDC": 38,
    "MYUNG0/USDC": 39,
    "TEST5/USDC": 40,
    "HOWL/USDC": 44,
    "YY/USDC": 45,
    "ONE/USDC": 50,
    "HEFFE/USDC": 51,
    "JEFF/USDC": 60,
    "MOGG/USDC": 61,
    "BTC/USDC": 69,
    "AA/USDC": 70,
    "LOLO/USDC": 71,
    "CABAL/USDC": 72,
    # Note: There are 1000+ more mappings discovered
    # but including only commonly used ones here
    # @N format handles all numeric indices directly
}
