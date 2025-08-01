"""Common symbols used throughout the test suite.

This module provides pre-instantiated symbol objects to reduce repetition
and improve maintainability across all test files.

Usage:
    from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL, BTC_BP, ETH_BP
    
    # Use in tests
    symbol = BTC_HL
    symbol_value = BTC_HL.value
"""

from cyberdelta.core.symbols import symbols

# Hyperliquid symbols
BTC_HL = symbols.BTC.hyperliquid()
ETH_HL = symbols.ETH.hyperliquid()
SOL_HL = symbols.SOL.hyperliquid()

# Backpack perpetual symbols
BTC_BP = symbols.BTC.backpack()
ETH_BP = symbols.ETH.backpack()
SOL_BP = symbols.SOL.backpack()

# Backpack spot symbols
from cyberdelta.core.symbols import exchanges
BTC_USDC_BP = exchanges.backpack("BTC_USDC")
ETH_USDC_BP = exchanges.backpack("ETH_USDC") 
SOL_USDC_BP = exchanges.backpack("SOL_USDC")

# Base assets for balances
USDC_BP = exchanges.backpack("USDC")
BTC_ASSET_BP = exchanges.backpack("BTC")
USD_HL = exchanges.hyperliquid("USD")
BTC_ASSET_HL = exchanges.hyperliquid("BTC")

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