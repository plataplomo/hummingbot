"""
CyberDeltaEngine: Backpack Raw Models Package
--------------------------------------------

This package provides strict, well-documented Pydantic models for validating and parsing
all major Backpack Exchange API (REST and WebSocket) responses. Each model mirrors the
Backpack OpenAPI spec or WS event schema as closely as possible, with:

- Accurate field names and types (using Field aliasing for exact JSON keys)
- Comprehensive docstrings for every model
- Strict validation (extra fields forbidden)
- Logical grouping by resource/domain (account, order, trade, etc.)

Usage:
    from cyberdelta.apis.backpack.models import BackpackRawOrder, BackpackRawTrade

Do not use these models for internal business logic—use core models for that. These are
for boundary validation and transformation only.
"""

from .bp_raw_account import BackpackRawAccount, BackpackRawBalance
from .bp_raw_error import BackpackRawApiError
from .bp_raw_funding import BackpackRawFundingRate, BackpackRawMarkPrice
from .bp_raw_margin_functions import BackpackRawImfFunction, BackpackRawMmfFunction
from .bp_raw_market import BackpackRawMarket, BackpackRawOpenInterest, BackpackRawTicker
from .bp_raw_order import BackpackRawOrder, BackpackRawOrderBook, BackpackRawOrderUpdate
from .bp_raw_position import BackpackRawPosition, BackpackRawPositionUpdate
from .bp_raw_trade import BackpackRawTrade, BackpackRawTradeEvent
from .bp_raw_transfer import BackpackRawDeposit, BackpackRawLiquidation, BackpackRawWithdrawal

__all__ = [
    "BackpackRawApiError",
    "BackpackRawAccount",
    "BackpackRawBalance",
    "BackpackRawPosition",
    "BackpackRawPositionUpdate",
    "BackpackRawOrder",
    "BackpackRawOrderBook",
    "BackpackRawOrderUpdate",
    "BackpackRawTrade",
    "BackpackRawTradeEvent",
    "BackpackRawFundingRate",
    "BackpackRawMarkPrice",
    "BackpackRawMarket",
    "BackpackRawTicker",
    "BackpackRawOpenInterest",
    "BackpackRawWithdrawal",
    "BackpackRawDeposit",
    "BackpackRawLiquidation",
    "BackpackRawImfFunction",
    "BackpackRawMmfFunction",
]
