"""Hyperliquid Market Data Response Handler.

This module handles the validation and processing of market data API responses,
extracted from the monolithic response handler to improve maintainability and testability.

Focused on:
- Asset metadata and contexts responses
- Price ticker and mid prices responses
- Order book (L2 book) responses
- Recent trades responses
- Funding rate responses
- Historical funding rates responses
- Candle/kline data responses
"""

from __future__ import annotations

from collections.abc import Mapping

from pydantic import ValidationError

from cyberdelta.apis.common.api_error import APIError
from cyberdelta.apis.common.api_error_codes import APIErrorCode
from cyberdelta.apis.exceptions import MissingRequiredParameterError
from cyberdelta.apis.hyperliquid.models.hl_raw_all_mids import HyperliquidRawAllMids
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleSnapshot,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import (
    HyperliquidRawL2Book as HyperliquidRawOrderBookResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import (
    HyperliquidRawPublicTrade,
    HyperliquidRawRecentTradesResponse,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_response_handler_base import (
    HyperliquidResponseHandlerBase,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


class HyperliquidMarketDataResponseHandler(
    HyperliquidResponseHandlerBase, MarketDataResponseHandlerProtocol
):
    """Handles validation of market data JSON responses from Hyperliquid API.

    Uses Pydantic models to validate the structure and types of the raw data.
    Raises APIError if validation fails.
    """

    # Base protocol method implementation
    def handle_response(
        self, response: dict[str, object], status_code: int, headers: dict[str, str], context: str
    ) -> object:
        """Handle API response per base protocol.

        Args:
            response: Raw response data from API
            status_code: HTTP status code
            headers: Response headers
            context: Context information about the request

        Returns:
            Processed response object
        """
        # Route to appropriate handler based on context
        if context == "all_mids":
            return self.handle_all_mids_response(response, status_code, headers)
        if context == "l2_book":
            return self.handle_info_l2_book_response(response, "unknown", status_code, headers)
        if context == "recent_trades":
            return self.handle_info_recent_trades_response(
                response, "unknown", status_code, headers
            )
        if context == "candles":
            return self.handle_info_candle_snapshot_response(
                response, "unknown", "unknown", status_code, headers
            )
        if context == "funding_history":
            return self.handle_info_funding_rate_response(response, status_code, headers)
        if context == "meta":
            return self.handle_info_meta_and_asset_ctxs_response(response, status_code, headers)
        raise APIError(
            message=f"Unknown market data response context: {context}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    # Protocol method implementations
    @staticmethod
    def handle_get_all_mids_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawAllMids:
        """Handle all mids response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing mid prices data
        """
        handler = HyperliquidMarketDataResponseHandler()
        return handler.handle_all_mids_response(
            raw_data=raw_response_content, status_code=status_code
        )

    @staticmethod
    def handle_get_l2_book_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawOrderBookResponse:
        """Handle L2 order book response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing order book data
        """
        # Extract symbol from response or use a default - this would need refinement
        symbol = str(raw_response_content.get("coin", "UNKNOWN"))

        handler = HyperliquidMarketDataResponseHandler()
        # The handler returns HyperliquidRawOrderBookResponse which is aliased to
        # HyperliquidRawL2Book
        # Need to check if we need to convert
        return handler.handle_info_l2_book_response(
            raw_data=raw_response_content, symbol=symbol, status_code=status_code
        )

    @staticmethod
    def handle_get_recent_trades_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawRecentTradesResponse:
        """Handle recent trades response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing trade data
        """
        # Extract symbol from response or use a default - this would need refinement
        symbol = "UNKNOWN"  # Would need to be passed in or extracted differently

        handler = HyperliquidMarketDataResponseHandler()
        trades_list = handler.handle_info_recent_trades_response(
            raw_data=raw_response_content, coin=symbol, status_code=status_code
        )
        # Wrap in RootModel
        return HyperliquidRawRecentTradesResponse(trades_list)

    @staticmethod
    def handle_get_candles_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawCandleSnapshot:
        """Handle candle data response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing candle data
        """
        # Extract symbol and interval from response or use defaults
        symbol = "UNKNOWN"  # Would need to be passed in or extracted differently
        interval = "1m"  # Would need to be passed in or extracted differently

        handler = HyperliquidMarketDataResponseHandler()
        return handler.handle_info_candle_snapshot_response(
            raw_data=raw_response_content,
            symbol=symbol,
            interval=interval,
            status_code=status_code,
        )

    @staticmethod
    def handle_get_funding_history_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawFundingHistoryResponse:
        """Handle funding history response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing funding history data
        """
        handler = HyperliquidMarketDataResponseHandler()
        # The handler returns HyperliquidRawFundingHistoryResponse
        return handler.handle_historical_funding_rates_response(
            raw_data=raw_response_content, status_code=status_code
        )

    @staticmethod
    def handle_get_meta_response(
        raw_response_content: dict[str, object], status_code: int
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Handle meta information response according to protocol.

        Args:
            raw_response_content: Raw response data from API
            status_code: HTTP status code

        Returns:
            Validated Pydantic model containing meta information
        """
        handler = HyperliquidMarketDataResponseHandler()
        # The handler returns HyperliquidRawMetaAndAssetCtxsResponse
        return handler.handle_info_meta_and_asset_ctxs_response(
            raw_data=raw_response_content, status_code=status_code
        )

    def handle_info_meta_and_asset_ctxs_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Validate raw JSON response for metadata and asset contexts info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawMetaAndAssetCtxsResponse: Validated metadata and asset contexts

        Raises:
            APIError: If validation fails
        """
        context = "info_meta_and_asset_ctxs"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Response is expected to be a list
            response_list = ensure_list_response(raw_data, context, status_code)
            # Wrap in response model
            return HyperliquidRawMetaAndAssetCtxsResponse.model_validate(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_funding_rate_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawAssetCtx:
        """Validate raw JSON response for funding rate info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawAssetCtx: Validated asset context with funding rate

        Raises:
            APIError: If validation fails
        """
        context = "info_funding_rate"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Ensure we have a dict
            response_dict = ensure_dict_response(raw_data, context, status_code)
            # Validate with Pydantic
            return HyperliquidRawAssetCtx(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_l2_book_response(
        self,
        raw_data: ParsedJsonResponse,
        symbol: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawOrderBookResponse:
        """Validate raw JSON response for L2 order book info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            symbol: Symbol for the order book data
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawOrderBookResponse: Validated order book data

        Raises:
            APIError: If validation fails
        """
        context = f"info_l2_book ({symbol})"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Ensure we have a dict
            response_dict = ensure_dict_response(raw_data, context, status_code)
            # Validate with Pydantic
            return HyperliquidRawOrderBookResponse(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_info_recent_trades_response(
        self,
        raw_data: ParsedJsonResponse,
        coin: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> list[HyperliquidRawPublicTrade]:
        """Validate raw JSON response for recent trades info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            coin: Coin for the recent trades data
            status_code: HTTP status code
            headers: Response headers

        Returns:
            list[HyperliquidRawPublicTrade]: Validated recent trades

        Raises:
            APIError: If validation fails
        """
        context = f"recent trades ({coin})"
        try:
            # Use RootModel for validation - it handles list structure
            validated_response = HyperliquidRawRecentTradesResponse.model_validate(
                raw_data,
            )
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e
        else:
            return validated_response.root

    def handle_info_candle_snapshot_response(
        self,
        raw_data: ParsedJsonResponse,
        symbol: str,
        interval: str,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawCandleSnapshot:
        """Validate raw JSON response for candle snapshot info endpoint.

        Args:
            raw_data: Raw JSON response from the API
            symbol: Symbol for the candle data
            interval: Interval for the candle data
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawCandleSnapshot: Validated candle data

        Raises:
            APIError: If validation fails
        """
        context = f"candle snapshot ({symbol}, {interval})"
        try:
            # Direct Pydantic validation - preprocessing is handled by the model
            return HyperliquidRawCandleSnapshot.model_validate(raw_data)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_historical_funding_rates_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawFundingHistoryResponse:
        """Validate raw JSON response for historical funding rates endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawFundingHistoryResponse: Validated funding history

        Raises:
            APIError: If validation fails
        """
        context = "historical_funding_rates"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Response is expected to be a list
            response_list = ensure_list_response(raw_data, context, status_code)
            # Don't convert to items here, let the response model handle validation
            # Wrap in response model (it expects a list of dicts)
            return HyperliquidRawFundingHistoryResponse(response_list)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e

    def handle_all_mids_response(
        self,
        raw_data: ParsedJsonResponse,
        status_code: int | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> HyperliquidRawAllMids:
        """Validate raw JSON response for all mid prices endpoint.

        Args:
            raw_data: Raw JSON response from the API
            status_code: HTTP status code
            headers: Response headers

        Returns:
            HyperliquidRawAllMids: Validated mid prices data

        Raises:
            APIError: If validation fails
        """
        context = "all_mids"
        if status_code is None:
            raise MissingRequiredParameterError("status_code", context)

        try:
            # Ensure we have a dict
            response_dict = ensure_dict_response(raw_data, context, status_code)
            # Validate with Pydantic
            return HyperliquidRawAllMids(**response_dict)
        except ValidationError as e:
            raise self._handle_validation_error(e, context, raw_data, status_code, headers) from e
