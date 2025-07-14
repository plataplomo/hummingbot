"""Protocol definitions for Backpack API components.

This package contains protocol definitions that define expected interfaces
for different component types in the Backpack API implementation.

Protocols are organized into separate modules:
- base_protocols: Core protocol definitions
- mapper_protocols: Protocols for data transformation components
- builder_protocols: Protocols for request builder components
- handler_protocols: Protocols for response handler components
"""

# Import base protocols
from cyberdelta.apis.backpack.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)

# Import specific request builder protocols
from cyberdelta.apis.backpack.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)

# Import specific response handler protocols
from cyberdelta.apis.backpack.protocols.handler_protocols import (
    AccountResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
)

# Import specific mapper protocols
from cyberdelta.apis.backpack.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    CandleMapperProtocol,
    FundingRateMapperProtocol,
    MarketDataMapperProtocol,
    MarketMapperProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TickerMapperProtocol,
    TradeMapperProtocol,
    TransactionMapperProtocol,
    TransferMapperProtocol,
)


__all__ = [
    # Request builder protocols
    "AccountRequestBuilderProtocol",
    # Response handler protocols
    "AccountResponseHandlerProtocol",
    # Mapper protocols
    "AccountSummaryMapperProtocol",
    "BalanceMapperProtocol",
    "CandleMapperProtocol",
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
    "TradeMapperProtocol",
    "TradingRequestBuilderProtocol",
    "TradingResponseHandlerProtocol",
    "TransactionMapperProtocol",
    "TransferMapperProtocol",
]
