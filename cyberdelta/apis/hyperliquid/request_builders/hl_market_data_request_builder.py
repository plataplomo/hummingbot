"""Hyperliquid Market Data Request Builder.

This module handles the construction of request payloads for market data operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Market metadata and asset context requests
- Price and mid price requests
- Order book and trade data requests
- Historical data and funding rate requests
- Candlestick/OHLCV data requests
"""

from __future__ import annotations

import time
from datetime import UTC, datetime

from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import (
    HyperliquidRawAllMidsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2BookRequestPayload
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawRecentTradesRequestPayload,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.models.service_args.hyperliquid import HyperliquidGetCandleSnapshotArgs
from cyberdelta.apis.models.service_args.market_data import (
    GetHistoricalFundingRatesArgs,
    GetL2BookArgs,
    GetRecentTradesArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol


logger = get_logger(__name__)


class HyperliquidMarketDataRequestBuilder(MarketDataRequestBuilderProtocol):
    """Focused request builder for Hyperliquid market data operations.

    This class contains static methods for constructing validated request payloads
    for all market data related API endpoints.
    """

    # Protocol method implementations
    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build request payload.

        Args:
            *args: Positional arguments for request building
            **kwargs: Keyword arguments for request building

        Returns:
            Dictionary containing the request payload
        """
        # This is a generic method that could dispatch to specific methods
        # For now, we'll implement a basic version
        if not args and not kwargs:
            # Default to all mids request
            return self.build_get_all_mids_params().model_dump(mode="json", by_alias=True)

        # This would need more sophisticated logic in a full implementation
        raise NotImplementedError("Generic build_request needs more context")

    # Protocol-specific methods
    @staticmethod
    def build_get_all_mids_params() -> HyperliquidRawAllMidsRequestPayload:
        """Build parameters for all mids (mid prices) retrieval.

        Returns:
            Validated Pydantic model containing request parameters
        """
        return HyperliquidMarketDataRequestBuilder.build_all_mids_request_payload()

    @staticmethod
    def build_get_l2_book_params(symbol: Symbol) -> HyperliquidRawL2BookRequestPayload:
        """Build parameters for L2 order book retrieval.

        Args:
            symbol: The trading Symbol domain object

        Returns:
            Validated Pydantic model containing request parameters
        """
        args = GetL2BookArgs(symbol=symbol)
        return HyperliquidMarketDataRequestBuilder.build_l2_book_request_payload(args)

    @staticmethod
    def build_get_recent_trades_params(symbol: Symbol) -> HyperliquidRawRecentTradesRequestPayload:
        """Build parameters for recent trades retrieval.

        Args:
            symbol: The trading Symbol domain object

        Returns:
            Validated Pydantic model containing request parameters
        """
        args = GetRecentTradesArgs(symbol=symbol)
        return HyperliquidMarketDataRequestBuilder.build_recent_trades_request_payload(args)

    @staticmethod
    def build_get_candles_params(
        symbol: Symbol,
        interval: str,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Build parameters for candle data retrieval.

        Args:
            symbol: The trading Symbol domain object
            interval: Candle interval (e.g., "1m", "5m", "1h")
            start_time: Optional start time (timestamp in milliseconds)
            end_time: Optional end time (timestamp in milliseconds)

        Returns:
            Validated Pydantic model containing request parameters
        """
        # Use current time if not provided
        if start_time is None:
            start_time = int((time.time() - 24 * 3600) * 1000)  # 24 hours ago
        if end_time is None:
            end_time = int(time.time() * 1000)  # Now

        args = HyperliquidGetCandleSnapshotArgs(
            symbol=symbol,
            timeframe=interval,  # Use 'timeframe' instead of 'interval'
            start_time_ms=start_time,  # Use 'start_time_ms' instead of 'start_time'
            end_time_ms=end_time,  # Use 'end_time_ms' instead of 'end_time'
        )
        return HyperliquidMarketDataRequestBuilder.build_candle_snapshot_payload(args)

    @staticmethod
    def build_get_funding_history_params(
        symbol: Symbol,
        start_time: int | None = None,
        end_time: int | None = None,
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Build parameters for funding history retrieval.

        Args:
            symbol: The trading Symbol domain object
            start_time: Optional start time (timestamp)
            end_time: Optional end time (timestamp)

        Returns:
            Validated Pydantic model containing request parameters
        """
        # Convert timestamps to datetime objects if provided
        start_dt = datetime.fromtimestamp(start_time, tz=UTC) if start_time else None
        end_dt = datetime.fromtimestamp(end_time, tz=UTC) if end_time else None

        args = GetHistoricalFundingRatesArgs(
            symbol=symbol,
            start_time=start_dt,
            end_time=end_dt,
        )
        return HyperliquidMarketDataRequestBuilder.build_historical_funding_rates_payload(args)

    @staticmethod
    def build_get_meta_params() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Build parameters for meta information retrieval.

        Returns:
            Validated Pydantic model containing request parameters
        """
        return HyperliquidMarketDataRequestBuilder.build_info_request_payload()

    @staticmethod
    def build_info_request_payload() -> HyperliquidRawMetaAndAssetCtxsRequestPayload:
        """Build the Pydantic model for fetching meta and asset contexts via /info.

        Architecture Compliance: Pure factory method returning validated Pydantic model.
        No args needed as this is a static request type.

        Returns:
            HyperliquidRawMetaAndAssetCtxsRequestPayload: Validated request payload

        Payload: {"type": "metaAndAssetCtxs"}
        """
        logger.debug(
            "building_info_request_payload",
            request_type="metaAndAssetCtxs",
            message="Building request payload for meta and asset contexts",
        )
        return HyperliquidRawMetaAndAssetCtxsRequestPayload(type="metaAndAssetCtxs")

    @staticmethod
    def build_all_mids_request_payload() -> HyperliquidRawAllMidsRequestPayload:
        """Build the Pydantic model for fetching all mid prices via /info.

        Architecture Compliance: Pure factory method returning validated Pydantic model.
        No args needed as this is a static request type.

        Returns:
            HyperliquidRawAllMidsRequestPayload: Validated request payload

        Payload: {"type": "allMids"}
        """
        logger.debug(
            "building_all_mids_request_payload",
            request_type="allMids",
            message="Building request payload for all mid prices",
        )
        return HyperliquidRawAllMidsRequestPayload(type="allMids")

    @staticmethod
    def build_l2_book_request_payload(args: GetL2BookArgs) -> HyperliquidRawL2BookRequestPayload:
        """Build the Pydantic model for fetching L2 order book data.

        Pure factory method returning validated Pydantic model.

        Args:
            args: Validated GetL2BookArgs containing symbol

        Returns:
            HyperliquidRawL2BookRequestPayload: Validated Raw API model
        """
        logger.debug(
            "building_l2_book_request_payload",
            symbol=args.symbol,
            request_type="l2Book",
            message="Building request payload for L2 order book",
        )
        return HyperliquidRawL2BookRequestPayload(type="l2Book", coin=str(args.symbol))

    @staticmethod
    def build_recent_trades_request_payload(
        args: GetRecentTradesArgs,
    ) -> HyperliquidRawRecentTradesRequestPayload:
        """Build the Pydantic model for fetching recent public trades.

        Pure factory method returning validated Pydantic model.

        Args:
            args: Validated GetRecentTradesArgs containing symbol

        Returns:
            HyperliquidRawRecentTradesRequestPayload: Validated Raw API model
        """
        logger.debug(
            "building_recent_trades_request_payload",
            symbol=args.symbol,
            request_type="recentTrades",
            message="Building request payload for recent trades",
        )
        return HyperliquidRawRecentTradesRequestPayload(type="recentTrades", coin=str(args.symbol))

    @staticmethod
    def build_candle_snapshot_payload(
        args: HyperliquidGetCandleSnapshotArgs,
    ) -> HyperliquidRawCandleSnapshotRequestPayload:
        """Build the Pydantic model for fetching candle/OHLCV data.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidGetCandleSnapshotArgs containing candle parameters

        Returns:
            HyperliquidRawCandleSnapshotRequestPayload: Validated Raw API model
        """
        logger.debug(
            "building_candle_snapshot_payload",
            symbol=args.symbol,
            timeframe=args.timeframe,
            start_time_ms=args.start_time_ms,
            end_time_ms=args.end_time_ms,
            message="Building request payload for candle snapshot",
        )

        request_details = HyperliquidRawCandleRequestDetails(
            coin=str(args.symbol),
            interval=args.timeframe,
            startTime=args.start_time_ms,
            endTime=args.end_time_ms,
        )

        return HyperliquidRawCandleSnapshotRequestPayload(
            type="candleSnapshot",
            req=request_details,
        )

    @staticmethod
    def build_historical_funding_rates_payload(
        args: GetHistoricalFundingRatesArgs,
    ) -> HyperliquidRawFundingHistoryRequestPayload:
        """Build the Pydantic model for fetching historical funding rates.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated GetHistoricalFundingRatesArgs containing funding history parameters

        Returns:
            HyperliquidRawFundingHistoryRequestPayload: Validated Raw API model
        """
        # Convert datetime to milliseconds
        start_time_ms = int(args.start_time.timestamp() * 1000) if args.start_time else None
        end_time_ms = int(args.end_time.timestamp() * 1000) if args.end_time else None

        logger.debug(
            "building_historical_funding_rates_payload",
            symbol=args.symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            message="Building request payload for historical funding rates",
        )

        # Ensure we have a valid start time (default to 30 days ago if None)
        if start_time_ms is None:
            start_time_ms = int((time.time() - 30 * 24 * 3600) * 1000)  # 30 days ago

        return HyperliquidRawFundingHistoryRequestPayload(
            type="fundingHistory",
            coin=str(args.symbol),
            startTime=start_time_ms,
            endTime=end_time_ms,
        )
