"""Base protocol definitions for all API components."""

from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
from cyberdelta.apis.base.protocols.mapper_protocols import (
    # Abstract protocols
    AbstractAccountSummaryMapperProtocol,
    AbstractBalanceMapperProtocol,
    AbstractCandleMapperProtocol,
    AbstractFundingRateMapperProtocol,
    AbstractMarketMapperProtocol,
    AbstractOrderBookMapperProtocol,
    AbstractOrderMapperProtocol,
    AbstractPositionMapperProtocol,
    AbstractTickerMapperProtocol,
    AbstractTradeMapperProtocol,
    # Mixin classes with shared utilities
    BalanceMapperMixin,
    CommonDataParserMixin,
    PositionMapperMixin,
)


__all__ = [
    # Abstract mapper protocols
    "AbstractAccountSummaryMapperProtocol",
    "AbstractBalanceMapperProtocol",
    "AbstractCandleMapperProtocol",
    "AbstractFundingRateMapperProtocol",
    "AbstractMarketMapperProtocol",
    "AbstractOrderBookMapperProtocol",
    "AbstractOrderMapperProtocol",
    "AbstractPositionMapperProtocol",
    "AbstractTickerMapperProtocol",
    "AbstractTradeMapperProtocol",
    # Mixin classes with shared utilities
    "BalanceMapperMixin",
    "CommonDataParserMixin",
    # Base protocols
    "MapperProtocol",
    "PositionMapperMixin",
    "RequestBuilderProtocol",
    "ResponseHandlerProtocol",
]
