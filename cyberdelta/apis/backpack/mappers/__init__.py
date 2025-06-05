"""CyberDeltaEngine: Backpack API Mappers Module.

--------------------------------------------

This module provides domain-focused mapper classes for transforming
Backpack Raw API models into CyberDeltaEngine Internal Domain Models.

The mappers are organized by domain responsibility:
- BackpackMarketDataMapper: Market data transformations (tickers, order books, trades,
  funding rates, candles)
- BackpackAccountDataMapper: Account data transformations (balances, positions,
  account summaries, historical fills)
- BackpackTradingDataMapper: Trading data transformations (orders, fills from active
  trading operations)

All mappers follow the standard transformation pattern:
- Take validated Raw Pydantic Models as input
- Return fully populated Internal Domain Models with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

from .bp_account_data_mapper import BackpackAccountDataMapper
from .bp_market_data_mapper import BackpackMarketDataMapper
from .bp_trading_data_mapper import BackpackTradingDataMapper

__all__ = [
    "BackpackMarketDataMapper",
    "BackpackAccountDataMapper",
    "BackpackTradingDataMapper",
]
