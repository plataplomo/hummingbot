"""Common symbols used throughout the test suite.

This module provides pre-instantiated symbol objects to reduce repetition
and improve maintainability across all test files.

Usage:
    from tests.common_symbols import BTC_HL, ETH_HL, SOL_HL, BTC_BP, ETH_BP
    
    # Use in tests
    symbol = BTC_HL
    symbol_value = BTC_HL.value
"""

from cyberdelta.core.symbols import symbols, exchanges

# Hyperliquid symbols
BTC_HL = symbols.BTC.hyperliquid()
ETH_HL = symbols.ETH.hyperliquid()
SOL_HL = symbols.SOL.hyperliquid()
DOGE_HL = symbols.DOGE.hyperliquid()
AVAX_HL = symbols.AVAX.hyperliquid()
MATIC_HL = symbols.MATIC.hyperliquid()

# Hyperliquid USD symbols (for tests that use -USD format)
BTC_USD_HL = exchanges.hyperliquid("BTC-USD")
ETH_USD_HL = exchanges.hyperliquid("ETH-USD")
SOL_USD_HL = exchanges.hyperliquid("SOL-USD")

# Backpack perpetual symbols
BTC_BP = symbols.BTC.backpack()
ETH_BP = symbols.ETH.backpack()
SOL_BP = symbols.SOL.backpack()
DOGE_BP = symbols.DOGE.backpack()

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