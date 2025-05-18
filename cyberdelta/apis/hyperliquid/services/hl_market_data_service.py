"""
CyberDeltaEngine: Hyperliquid Market Data Service
-------------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Hyperliquid Exchange. It uses the HttpClient, HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

# Typing and Pydantic
from typing import Any

from pydantic import ValidationError

# Project-specific imports for connectivity and base types
from cyberdelta.apis.connectivity.http_client import HttpClient
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService

# Hyperliquid-specific imports
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse,  # Import the type alias
)
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Utilities
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)


class HyperliquidMarketDataService:
    """
    Service class for handling Hyperliquid market data API endpoints.

    This class centralizes the logic for fetching market data such as tickers,
    order books, trades, funding rates, and candlestick data.
    It leverages shared components like HttpClient, HyperliquidRequestBuilder,
    HyperliquidResponseHandler, and RateLimiterService to perform its tasks.
    """

    _http_client: HttpClient
    _request_builder: HyperliquidRequestBuilder
    _response_handler: HyperliquidResponseHandler
    _rate_limiter_service: RateLimiterService
    _exchange_name: str = "Hyperliquid"

    def __init__(
        self,
        http_client: HttpClient,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        rate_limiter_service: RateLimiterService,
    ) -> None:
        """
        Initialize the HyperliquidMarketDataService.

        Args:
            http_client: An instance of HttpClient for making HTTP requests.
            request_builder: An instance of HyperliquidRequestBuilder for preparing
                API requests.
            response_handler: An instance of HyperliquidResponseHandler for validating
                API responses.
            rate_limiter_service: An instance of RateLimiterService for managing API call rates.
        """
        self._http_client = http_client
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._rate_limiter_service = rate_limiter_service

    async def get_all_asset_contexts(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """
        Retrieves the metadata for all listed assets and their current context
        (mark price, funding rate, etc.) by calling the /info endpoint.
        Hyperliquid's /info endpoint often returns multiple data types; the handler
        is responsible for extracting and validating the metaAndAssetCtxs part.

        Returns:
            A HyperliquidRawMetaAndAssetCtxsResponse object containing validated raw data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # HyperliquidRequestBuilder.build_info_request_payload() now returns a Pydantic model.
        request_payload_model = self._request_builder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,  # Use by_alias if model uses aliases
        )

        raw_response_content: RawJsonResponse | None = None
        try:
            # The HttpClient is assumed to be configured with INFO_URL as its base.
            raw_response_content, _, _ = await self._http_client.request(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data_dict,  # Pass the dumped dictionary
                rate_limiter_service=self._rate_limiter_service,
            )

            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    raw_response_content
                )
            )
            return validated_response

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Asset contexts response validation failed: {e_val}. "
                f"Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate asset contexts response: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching asset contexts: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing asset contexts: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_ticker(self, symbol: str) -> HyperliquidRawAssetCtx | None:
        """
        Retrieves the latest ticker/context information for a specific symbol.
        This involves fetching all asset contexts and then finding the specific one.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A HyperliquidRawAssetCtx object if the symbol is found, otherwise None.
            The object contains ticker-like data (mark price, funding, etc.).

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
                      (Note: original hl_api.get_ticker raised SYMBOL_NOT_FOUND specifically)
        """
        try:
            all_contexts_response = await self.get_all_asset_contexts()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return asset_ctx

            # Symbol not found in the contexts
            logger.warning(
                f"[{self._exchange_name}] Ticker data (asset context) not found "
                f"for symbol '{symbol}' after fetching all asset contexts."
            )
            return None  # Consistent with method signature if not found

        except APIError:  # Propagate APIErrors from get_all_asset_contexts
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_ticker "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            # To maintain consistency with original behavior of raising APIError for failures
            raise APIError(
                message=f"Unexpected error fetching ticker for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_book(self, symbol: str) -> HyperliquidRawL2Book:
        """
        Retrieves the L2 order book for a specific symbol using a POST request to /info
        with a specific payload: {"type": "l2Book", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A HyperliquidRawL2Book object containing the validated raw order book data.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # Assuming HyperliquidRequestBuilder has or will have this method:
        request_payload_model = self._request_builder.build_l2_book_request_payload(symbol=symbol)
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        raw_response_content: RawJsonResponse | None = None
        try:
            raw_response_content, _, _ = await self._http_client.request(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data,
                rate_limiter_service=self._rate_limiter_service,
            )

            # The handler expects the raw response and the symbol for context or validation.
            validated_book: HyperliquidRawL2Book = (
                self._response_handler.handle_info_l2_book_response(
                    raw_response_content, symbol=symbol
                )
            )
            return validated_book

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Order book response validation failed "
                f"for {symbol}: {e_val}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate order book response for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching order book "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing order book for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_recent_trades(
        self,
        symbol: str,
        # limit: int = 100, # Limit is not part of HL /info request for recentTrades
    ) -> list[HyperliquidRawPublicTrade]:
        """
        Retrieves recent public trades for a specific symbol using a POST request to /info
        with a payload: {"type": "recentTrades", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A list of HyperliquidRawPublicTrade objects.
            The number of trades is determined by the Hyperliquid API.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"
        # Assuming HyperliquidRequestBuilder has or will have this method:
        request_payload_model = self._request_builder.build_recent_trades_request_payload(
            symbol=symbol
        )
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        raw_response_content: RawJsonResponse | None = None
        try:
            raw_response_content, _, _ = await self._http_client.request(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data,
                rate_limiter_service=self._rate_limiter_service,
            )

            validated_trades: list[HyperliquidRawPublicTrade] = (
                self._response_handler.handle_info_recent_trades_response(
                    raw_response_content, symbol=symbol
                )
            )
            # The response handler is expected to return the full list of trades from the API.
            # Limiting should be done by the caller or a mapping layer if needed.
            return validated_trades

        except APIError:
            raise
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Recent trades response validation failed "
                f"for {symbol}: {e_val}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                message=f"Failed to validate recent trades response for {symbol}: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unhandled error fetching recent trades "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error processing recent trades for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_funding_rate(self, symbol: str) -> HyperliquidRawAssetCtx | None:
        """
        Retrieves the current funding rate information for a specific perpetual contract symbol.
        This is typically part of the broader asset context. It calls get_all_asset_contexts
        and extracts the relevant context.

        Args:
            symbol: The perpetual contract symbol (e.g., "ETH").

        Returns:
            A HyperliquidRawAssetCtx object if the symbol is found, otherwise None.
            The 'funding' field of this object contains the funding rate.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        try:
            all_contexts_response = await self.get_all_asset_contexts()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return asset_ctx  # The entire context is returned as per definition

            logger.warning(
                f"[{self._exchange_name}] Funding rate data (asset context) not found "
                f"for symbol '{symbol}' after fetching all asset contexts."
            )
            return None
        except APIError:  # Propagate APIErrors from get_all_asset_contexts
            raise
        except Exception as e_unhandled:
            logger.error(
                f"[{self._exchange_name}] Unexpected error in get_funding_rate "
                f"for {symbol}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error fetching funding rate for {symbol}: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_market_data(
        self, symbol: str, interval: str, start_time_ms: int, end_time_ms: int
    ) -> HyperliquidRawCandleSnapshot:
        """
        Retrieves historical kline (candlestick) data for a specific symbol and timeframe.
        Uses a POST request to /info with payload:
        {"type": "candleSnapshot",
         "req": {"coin": SYMBOL, "interval": INTERVAL,
                 "startTime": START_MS, "endTime": END_MS}}

        Args:
            symbol: The trading symbol (e.g., "ETH").
            interval: The kline interval (e.g., "1m", "1h", "1d"). Refer to Hyperliquid API docs.
            start_time_ms: Start timestamp in milliseconds.
            end_time_ms: End timestamp in milliseconds.

        Returns:
            A HyperliquidRawCandleSnapshot object containing lists of candle data points.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        endpoint_path = "/info"

        payload_model = self._request_builder.build_candle_snapshot_payload(
            symbol=symbol, timeframe=interval, start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        request_payload_data: dict[str, Any] = payload_model.model_dump(
            by_alias=True, exclude_none=True
        )

        raw_response_content: RawJsonResponse | None = None
        try:
            raw_response_content, _, _ = await self._http_client.request(
                method="POST",
                endpoint_path=endpoint_path,
                data=request_payload_data,
                rate_limiter_service=self._rate_limiter_service,
            )

            validated_snapshot = self._response_handler.handle_info_candle_snapshot_response(
                raw_response_content=raw_response_content, symbol=symbol, interval=interval
            )
            return validated_snapshot
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] Candle snapshot response validation failed "
                f"for {symbol}@{interval}: {e_val}. Raw: {raw_response_content!r}"
            )
            raise APIError(
                "Pydantic validation error during candle snapshot processing",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
            ) from e_val
        except APIError:  # Re-raise APIErrors from http_client or response_handler
            raise
        except Exception as e_unhandled:  # Catch any other unexpected errors
            logger.error(
                f"[{self._exchange_name}] Unexpected error processing candle snapshot "
                f"for {symbol}@{interval}: {e_unhandled}",
                exc_info=True,
            )
            raise APIError(
                "Unexpected error during candle snapshot processing",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled
