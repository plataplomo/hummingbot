"""CyberDeltaEngine: Hyperliquid Domain-Focused Data Mappers.

---------------------------------------------------------

This package contains domain-focused data transformation mappers for Hyperliquid Exchange.
Each mapper class is responsible for transforming validated Raw Pydantic Models
into Internal Domain Models for a specific business domain.

The mappers are organized into subdirectories by domain:
- account/: Account-related mappers (balances, positions, summaries, transactions)
- market_data/: Market data mappers (order books, tickers, historical data, metadata)
- trading/: Trading operation mappers (orders, order responses, enums)
- utils/: Shared utilities and common mapper functions

Import the specific mapper you need from its subdirectory, for example:
  from cyberdelta.apis.hyperliquid.mappers.account.hl_balance_mapper import HyperliquidBalanceMapper
  from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import HyperliquidOrderMapper
"""

# Export decomposed mappers for direct use
from .account.hl_account_summary_mapper import HyperliquidAccountSummaryMapper
from .account.hl_balance_mapper import HyperliquidBalanceMapper
from .account.hl_position_mapper import HyperliquidPositionMapper
from .account.hl_transaction_mapper import HyperliquidTransactionMapper
from .account.hl_transfer_mapper import HyperliquidTransferMapper
from .market_data.hl_historical_data_mapper import HyperliquidHistoricalDataMapper
from .market_data.hl_market_metadata_mapper import HyperliquidMarketMetadataMapper
from .market_data.hl_order_book_mapper import HyperliquidOrderBookMapper
from .market_data.hl_price_ticker_mapper import HyperliquidPriceTickerMapper
from .trading.hl_order_mapper import HyperliquidOrderMapper
from .trading.hl_order_response_mapper import HyperliquidOrderResponseMapper
from .trading.hl_trading_enum_mapper import HyperliquidTradingEnumMapper


__all__ = [
    "HyperliquidAccountSummaryMapper",
    "HyperliquidBalanceMapper",
    "HyperliquidHistoricalDataMapper",
    "HyperliquidMarketMetadataMapper",
    "HyperliquidOrderBookMapper",
    "HyperliquidOrderMapper",
    "HyperliquidOrderResponseMapper",
    "HyperliquidPositionMapper",
    "HyperliquidPriceTickerMapper",
    "HyperliquidTradingEnumMapper",
    "HyperliquidTransactionMapper",
    "HyperliquidTransferMapper",
]
