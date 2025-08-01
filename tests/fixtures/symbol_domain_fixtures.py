"""Domain-specific Symbol fixtures for testing.

These fixtures provide pre-configured Symbol sets for common
test scenarios like arbitrage, market data, and trading.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import pytest

from cyberdelta.core.symbols import Symbol, symbols, exchanges
from cyberdelta.enums.exchange_names import ExchangeName

if TYPE_CHECKING:
    from cyberdelta.core.symbols.service import SymbolService


@dataclass
class SymbolSet:
    """Complete symbol set for an asset."""
    
    perp_hl: Symbol
    perp_bp: Symbol
    spot_hl: Symbol | None = None
    spot_bp: Symbol | None = None
    canonical: str | None = None
    equivalences: list[Symbol] | None = None


@dataclass
class ArbitragePair:
    """Symbol pair for arbitrage testing."""
    
    long: Symbol
    short: Symbol
    spread_threshold: Decimal = Decimal("0.001")


@dataclass
class MarketDataSet:
    """Symbols with associated market data."""
    
    symbol: Symbol
    bid: Decimal
    ask: Decimal
    last: Decimal
    volume: Decimal
    funding_rate: Decimal | None = None


@pytest.fixture
def btc_symbols(symbol_service: SymbolService) -> SymbolSet:
    """Complete BTC symbol set for testing.
    
    Args:
        symbol_service: Symbol service instance
        
    Returns:
        SymbolSet: BTC symbols across exchanges and markets
    """
    perp_hl = symbols.BTC.hyperliquid()
    perp_bp = symbols.BTC.backpack()
    spot_hl = exchanges.hyperliquid("BTC-USDC")
    spot_bp = exchanges.backpack("BTC_USDC")
    
    return SymbolSet(
        perp_hl=perp_hl,
        perp_bp=perp_bp,
        spot_hl=spot_hl,
        spot_bp=spot_bp,
        canonical="BTC_PERP",
        equivalences=symbol_service.get_equivalent_symbols(perp_hl),
    )


@pytest.fixture
def eth_symbols(symbol_service: SymbolService) -> SymbolSet:
    """Complete ETH symbol set for testing.
    
    Args:
        symbol_service: Symbol service instance
        
    Returns:
        SymbolSet: ETH symbols across exchanges and markets
    """
    perp_hl = symbols.ETH.hyperliquid()
    perp_bp = symbols.ETH.backpack()
    spot_hl = exchanges.hyperliquid("ETH-USDC")
    spot_bp = exchanges.backpack("ETH_USDC")
    
    return SymbolSet(
        perp_hl=perp_hl,
        perp_bp=perp_bp,
        spot_hl=spot_hl,
        spot_bp=spot_bp,
        canonical="ETH_PERP",
        equivalences=symbol_service.get_equivalent_symbols(perp_hl),
    )


@pytest.fixture
def sol_symbols(symbol_service: SymbolService) -> SymbolSet:
    """Complete SOL symbol set for testing.
    
    Args:
        symbol_service: Symbol service instance
        
    Returns:
        SymbolSet: SOL symbols across exchanges and markets
    """
    perp_hl = symbols.SOL.hyperliquid()
    perp_bp = symbols.SOL.backpack()
    spot_hl = exchanges.hyperliquid("SOL-USDC")
    spot_bp = exchanges.backpack("SOL_USDC")
    
    return SymbolSet(
        perp_hl=perp_hl,
        perp_bp=perp_bp,
        spot_hl=spot_hl,
        spot_bp=spot_bp,
        canonical="SOL_PERP",
        equivalences=symbol_service.get_equivalent_symbols(perp_hl),
    )


@pytest.fixture
def all_symbol_sets(
    btc_symbols: SymbolSet,
    eth_symbols: SymbolSet,
    sol_symbols: SymbolSet,
) -> dict[str, SymbolSet]:
    """All standard symbol sets.
    
    Args:
        btc_symbols: BTC symbol set
        eth_symbols: ETH symbol set
        sol_symbols: SOL symbol set
        
    Returns:
        dict: Asset name to symbol set mapping
    """
    return {
        "BTC": btc_symbols,
        "ETH": eth_symbols,
        "SOL": sol_symbols,
    }


@pytest.fixture
def arbitrage_pairs() -> dict[str, ArbitragePair]:
    """Common arbitrage pairs for testing.
    
    Returns:
        dict: Asset to arbitrage pair mapping
    """
    return {
        "BTC": ArbitragePair(
            long=symbols.BTC.hyperliquid(),
            short=symbols.BTC.backpack(),
            spread_threshold=Decimal("0.001"),
        ),
        "ETH": ArbitragePair(
            long=symbols.ETH.hyperliquid(),
            short=symbols.ETH.backpack(),
            spread_threshold=Decimal("0.0015"),
        ),
        "SOL": ArbitragePair(
            long=symbols.SOL.hyperliquid(),
            short=symbols.SOL.backpack(),
            spread_threshold=Decimal("0.002"),
        ),
    }


@pytest.fixture
def btc_arbitrage_pair(arbitrage_pairs: dict[str, ArbitragePair]) -> ArbitragePair:
    """BTC arbitrage pair.
    
    Args:
        arbitrage_pairs: All arbitrage pairs
        
    Returns:
        ArbitragePair: BTC arbitrage pair
    """
    return arbitrage_pairs["BTC"]


@pytest.fixture
def eth_arbitrage_pair(arbitrage_pairs: dict[str, ArbitragePair]) -> ArbitragePair:
    """ETH arbitrage pair.
    
    Args:
        arbitrage_pairs: All arbitrage pairs
        
    Returns:
        ArbitragePair: ETH arbitrage pair
    """
    return arbitrage_pairs["ETH"]


@pytest.fixture
def market_data_symbols() -> list[MarketDataSet]:
    """Symbols with sample market data.
    
    Returns:
        list: Market data sets for testing
    """
    return [
        MarketDataSet(
            symbol=symbols.BTC.hyperliquid(),
            bid=Decimal("50000.00"),
            ask=Decimal("50010.00"),
            last=Decimal("50005.00"),
            volume=Decimal("1000.0"),
            funding_rate=Decimal("0.0001"),
        ),
        MarketDataSet(
            symbol=symbols.BTC.backpack(),
            bid=Decimal("50020.00"),
            ask=Decimal("50030.00"),
            last=Decimal("50025.00"),
            volume=Decimal("800.0"),
            funding_rate=Decimal("-0.0002"),
        ),
        MarketDataSet(
            symbol=symbols.ETH.hyperliquid(),
            bid=Decimal("3000.00"),
            ask=Decimal("3001.00"),
            last=Decimal("3000.50"),
            volume=Decimal("5000.0"),
            funding_rate=Decimal("0.00015"),
        ),
        MarketDataSet(
            symbol=symbols.ETH.backpack(),
            bid=Decimal("3002.00"),
            ask=Decimal("3003.00"),
            last=Decimal("3002.50"),
            volume=Decimal("4000.0"),
            funding_rate=Decimal("-0.0001"),
        ),
    ]


@pytest.fixture
def spot_asset_symbols() -> dict[str, dict[ExchangeName, Symbol]]:
    """Single asset symbols for spot trading.
    
    Returns:
        dict: Asset to exchange to symbol mapping
    """
    return {
        "USDC": {
            ExchangeName.HYPERLIQUID: exchanges.hyperliquid("USDC"),
            ExchangeName.BACKPACK: exchanges.backpack("USDC"),
        },
        "BTC": {
            ExchangeName.HYPERLIQUID: exchanges.hyperliquid("BTC"),
            ExchangeName.BACKPACK: exchanges.backpack("BTC"),
        },
        "ETH": {
            ExchangeName.HYPERLIQUID: exchanges.hyperliquid("ETH"),
            ExchangeName.BACKPACK: exchanges.backpack("ETH"),
        },
    }


@pytest.fixture
def trading_symbols() -> dict[str, Symbol]:
    """Common symbols for trading tests.
    
    Returns:
        dict: Key to symbol mapping for trading tests
    """
    return {
        # Perpetuals
        "btc_perp_hl": symbols.BTC.hyperliquid(),
        "btc_perp_bp": symbols.BTC.backpack(),
        "eth_perp_hl": symbols.ETH.hyperliquid(),
        "eth_perp_bp": symbols.ETH.backpack(),
        
        # Spot
        "btc_spot_hl": exchanges.hyperliquid("BTC-USDC"),
        "btc_spot_bp": exchanges.backpack("BTC_USDC"),
        "eth_spot_hl": exchanges.hyperliquid("ETH-USDC"),
        "eth_spot_bp": exchanges.backpack("ETH_USDC"),
        
        # Assets
        "usdc_hl": exchanges.hyperliquid("USDC"),
        "usdc_bp": exchanges.backpack("USDC"),
    }


@pytest.fixture
def symbol_pairs_by_market() -> dict[str, list[tuple[Symbol, Symbol]]]:
    """Symbol pairs grouped by market type.
    
    Returns:
        dict: Market type to list of symbol pairs
    """
    return {
        "perp": [
            (symbols.BTC.hyperliquid(), symbols.BTC.backpack()),
            (symbols.ETH.hyperliquid(), symbols.ETH.backpack()),
            (symbols.SOL.hyperliquid(), symbols.SOL.backpack()),
        ],
        "spot": [
            (exchanges.hyperliquid("BTC-USDC"), exchanges.backpack("BTC_USDC")),
            (exchanges.hyperliquid("ETH-USDC"), exchanges.backpack("ETH_USDC")),
            (exchanges.hyperliquid("SOL-USDC"), exchanges.backpack("SOL_USDC")),
        ],
    }