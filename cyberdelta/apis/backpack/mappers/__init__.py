"""CyberDeltaEngine: Backpack API Mappers Module.

--------------------------------------------

This module provides domain-focused mapper classes for transforming
Backpack Raw API models into CyberDeltaEngine Internal Domain Models.

The mappers are organized by domain responsibility:
- Decomposed market data mappers: Market data transformations (tickers, order books, trades,
  funding rates, candles) - now split into specialized mappers
- Decomposed account mappers: Account data transformations (balances, positions,
  account summaries, historical fills) - now split into specialized mappers
- BackpackOrderMapper: Order data transformations (orders, fills from active
  trading operations)

All mappers follow the standard transformation pattern:
- Take validated Raw Pydantic Models as input
- Return fully populated Internal Domain Models with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

from .account.bp_account_summary_mapper import BackpackAccountSummaryMapper
from .account.bp_balance_mapper import BackpackBalanceMapper
from .account.bp_position_mapper import BackpackPositionMapper
from .account.bp_transaction_mapper import BackpackTransactionMapper
from .account.bp_transfer_mapper import BackpackTransferMapper
from .market_data.bp_candle_mapper import BackpackCandleMapper
from .market_data.bp_funding_rate_mapper import BackpackFundingRateMapper
from .market_data.bp_market_mapper import BackpackMarketMapper
from .market_data.bp_order_book_mapper import BackpackOrderBookMapper
from .market_data.bp_ticker_mapper import BackpackTickerMapper
from .market_data.bp_trade_mapper import BackpackTradeMapper
from .trading.bp_order_mapper import BackpackOrderMapper
from .utils.common_mappers import BackpackCommonMappers


__all__ = [
    "BackpackAccountSummaryMapper",
    "BackpackBalanceMapper",
    "BackpackCandleMapper",
    "BackpackCommonMappers",
    "BackpackFundingRateMapper",
    "BackpackMarketMapper",
    "BackpackOrderBookMapper",
    "BackpackOrderMapper",
    "BackpackPositionMapper",
    "BackpackTickerMapper",
    "BackpackTradeMapper",
    "BackpackTransactionMapper",
    "BackpackTransferMapper",
]
