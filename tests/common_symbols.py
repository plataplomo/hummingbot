"""Common symbols used throughout the test suite.

This module provides pre-instantiated symbol objects to reduce repetition
and improve maintainability across all test files.

Usage:
    from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL, BTC_BP, ETH_BP

    # Use in tests
    symbol = BTC_HL
    symbol_value = BTC_HL.value
"""

from cyberdelta.symbols import exchanges, symbols


# Hyperliquid symbols
BTC_HL = symbols.BTC.hyperliquid()
ETH_HL = symbols.ETH.hyperliquid()
SOL_HL = symbols.SOL.hyperliquid()
# NOTE: These symbols are not defined in CommonSymbols module
# Using exchanges API directly for these symbols

# Use exchanges API directly for these:
DOGE_HL = exchanges.hyperliquid("DOGE-PERP")
AVAX_HL = exchanges.hyperliquid("AVAX-PERP")
MATIC_HL = exchanges.hyperliquid("MATIC-PERP")
ADA_HL = exchanges.hyperliquid("ADA-PERP")
DOT_HL = exchanges.hyperliquid("DOT-PERP")

# Hyperliquid USD symbols (for tests that use -USD format)
BTC_USD_HL = exchanges.hyperliquid("BTC-USD")
ETH_USD_HL = exchanges.hyperliquid("ETH-USD")
SOL_USD_HL = exchanges.hyperliquid("SOL-USD")

# Backpack perpetual symbols
BTC_BP = symbols.BTC.backpack()
ETH_BP = symbols.ETH.backpack()
SOL_BP = symbols.SOL.backpack()
# NOTE: DOGE not defined in CommonSymbols, using exchanges API
DOGE_BP = exchanges.backpack("DOGE_USD_PERP")

# Backpack spot symbols
BTC_USDC_BP = exchanges.backpack("BTC_USDC")
ETH_USDC_BP = exchanges.backpack("ETH_USDC")
SOL_USDC_BP = exchanges.backpack("SOL_USDC")
DOGE_USDC_BP = exchanges.backpack("DOGE_USDC")
ETH_USDT_BP = exchanges.backpack("ETH_USDT")
BTC_USDT_BP = exchanges.backpack("BTC_USDT")
SOL_USDT_BP = exchanges.backpack("SOL_USDT")
DOGE_USDT_BP = exchanges.backpack("DOGE_USDT")
ADA_BTC_BP = exchanges.backpack("ADA_BTC")
ADA_USDC_BP = exchanges.backpack("ADA_USDC")

# Backpack USDC perpetual symbols
BTC_USDC_PERP_BP = exchanges.backpack("BTC_USDC_PERP")
ETH_USDC_PERP_BP = exchanges.backpack("ETH_USDC_PERP")
SOL_USDC_PERP_BP = exchanges.backpack("SOL_USDC_PERP")

# Generic test symbols
SYM_USDT_BP = exchanges.backpack("SYM_USDT")

# Base assets for balances
USDC_BP = exchanges.backpack("USDC")
BTC_ASSET_BP = exchanges.backpack("BTC")
USD_HL = exchanges.hyperliquid("USD")
BTC_ASSET_HL = exchanges.hyperliquid("BTC")

# Cross-asset pair
USDT_USDC_BP = exchanges.backpack("USDT_USDC")

# Cross-currency conversion symbols
USD_USDC_HL = exchanges.hyperliquid("USD-USDC")
USDC_USD_BP = exchanges.backpack("USDC-USD")

# Common symbol pairs for arbitrage tests (perp to perp)
ARBITRAGE_PAIRS = [
    (BTC_HL, BTC_BP),
    (ETH_HL, ETH_BP),
    (SOL_HL, SOL_BP),
]

# Funding arbitrage pairs (perp to spot)
FUNDING_ARB_PAIRS = [
    (BTC_HL, BTC_USDC_BP),
    (ETH_HL, ETH_USDC_BP),
    (SOL_HL, SOL_USDC_BP),
]

# Symbol mappings for strategies (perp to spot string)
SYMBOL_MAPPINGS = {
    BTC_HL.value: BTC_USDC_BP.value,
    ETH_HL.value: ETH_USDC_BP.value,
    SOL_HL.value: SOL_USDC_BP.value,
}

# Parametrized test symbol lists for efficient testing
COMMON_SPOT_SYMBOLS_BP = [SOL_USDC_BP, BTC_USDC_BP, ETH_USDC_BP]
COMMON_PERP_SYMBOLS_BP = [SOL_USDC_PERP_BP, BTC_USDC_PERP_BP, ETH_USDC_PERP_BP]
COMMON_SYMBOLS_HL = [SOL_HL, BTC_HL, ETH_HL]

# Invalid symbols for error testing
INVALID_SYMBOL_BP = exchanges.backpack("INVALID_SYMBOL")
INVALID_PERP_BP = exchanges.backpack("INVALID_PERP")
INVALID_SPOT_BP = exchanges.backpack("INVALID_SPOT")
INVALID_SYMBOL_HL = exchanges.hyperliquid("INVALID-USD")
