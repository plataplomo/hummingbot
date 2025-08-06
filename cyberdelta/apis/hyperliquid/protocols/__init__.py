"""Hyperliquid API Protocol Definitions.

This module provides comprehensive protocol definitions for the Hyperliquid API,
following the proven Backpack pattern. These protocols ensure type safety,
runtime validation, and consistent interfaces across all components.

The protocol system is organized into four main categories:
1. Base protocols - Core interfaces for all components
2. Mapper protocols - Data transformation interfaces
3. Builder protocols - Request building interfaces
4. Handler protocols - Response handling interfaces
"""

# Base protocols
from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)

# Builder protocols
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)

# Handler protocols
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    AccountResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
)

# Mapper protocols
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    CandleMapperProtocol,
    FillMapperProtocol,
    FundingRateMapperProtocol,
    MarketDataMapperProtocol,
    MarketMapperProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TickerMapperProtocol,
)


# Export all protocols
__all__ = [
    # Builder protocols
    "AccountRequestBuilderProtocol",
    # Handler protocols
    "AccountResponseHandlerProtocol",
    "AccountSummaryMapperProtocol",
    # Mapper protocols
    "BalanceMapperProtocol",
    "CandleMapperProtocol",
    "FillMapperProtocol",
    "FundingRateMapperProtocol",
    # Base protocols
    "MapperProtocol",
    "MarketDataMapperProtocol",
    "MarketDataRequestBuilderProtocol",
    "MarketDataResponseHandlerProtocol",
    "MarketMapperProtocol",
    "OrderBookMapperProtocol",
    "OrderMapperProtocol",
    "PositionMapperProtocol",
    "RequestBuilderProtocol",
    "ResponseHandlerProtocol",
    "TickerMapperProtocol",
    "TradingRequestBuilderProtocol",
    "TradingResponseHandlerProtocol",
]
