"""
CyberDeltaEngine: Hyperliquid Market Data Service
-------------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Hyperliquid Exchange. It uses the HttpClient, HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

# Typing and Pydantic
import inspect
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

# Project-specific imports for connectivity and base types
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse

# Hyperliquid-specific imports
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)

# Mappers
from cyberdelta.apis.hyperliquid.mappers import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)

# Removed unused raw model imports as handlers return these directly now
# from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)

# from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
# from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Internal Domain Models
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle

# Utilities
from cyberdelta.utils.logging_config import get_logger

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]

if TYPE_CHECKING:
    pass


class HyperliquidMarketDataService:
    """
    Service class for handling Hyperliquid market data API endpoints.

    This class centralizes the logic for fetching market data such as tickers,
    order books, trades, funding rates, and candlestick data.
    It leverages shared components like HttpClient, HyperliquidRequestBuilder,
    HyperliquidResponseHandler, and RateLimiterService to perform its tasks.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: HyperliquidRequestBuilder
    _response_handler: HyperliquidResponseHandler
    _exchange_name: str
    _mapper: HyperliquidMarketDataMapper

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        mapper: HyperliquidMarketDataMapper,
        exchange_name: str,
    ) -> None:
        """
        Initialize the HyperliquidMarketDataService.

        Args:
            http_client_requester: An instance of HttpClientRequesterSig for making HTTP requests.
            request_builder: An instance of HyperliquidRequestBuilder for preparing
                API requests.
            response_handler: An instance of HyperliquidResponseHandler for validating
                API responses.
            mapper: An instance of HyperliquidMarketDataMapper for mapping raw data to
                internal models.
            exchange_name: The name of the exchange.
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
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

        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0
        headers: Mapping[str, str] = {}
        try:
            # Use the single HTTP client requester for /info endpoint
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data_dict,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw all_asset_contexts response: "
                f"{raw_response_content!r}, Status: {status_code}, Headers: {headers}"
            )

            if raw_response_content is None:
                _error_msg = (
                    f"No content received from HTTP client for metaAndAssetCtxs. "
                    f"Status: {status_code}"
                )
                logger.error(f"[{self._exchange_name}] {_error_msg}")
                raise APIError(
                    message=_error_msg,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    raw_response_content,
                    status_code=status_code,
                    headers=headers,
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
                http_status=status_code,
                exchange_message=str(raw_response_content),
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
                http_status=status_code,
                exchange_message=str(raw_response_content),
            ) from e_unhandled

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """
        Retrieves the latest ticker/context information for a specific symbol.
        This involves fetching all asset contexts and then finding the specific one.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A Ticker object if the symbol is found, otherwise None.
            The object contains ticker-like data (mark price, funding, etc.).

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
                      (Note: original hl_api.get_ticker raised SYMBOL_NOT_FOUND specifically)
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_ticker"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return self._mapper.transform_raw_asset_ctx_to_ticker(asset_ctx)

            # Symbol not found in the contexts
            logger.warning(
                f"[{self._exchange_name}] Ticker data (asset context) not found "
                f"for symbol '{symbol}' after fetching all asset contexts."
            )
            return None  # Consistent with method signature if not found

        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_order_book(self, symbol: str) -> OrderBook | None:
        """
        Retrieves the order book for a specific symbol using a POST request to /info
        with a payload: {"type": "l2Book", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            An OrderBook object or None if the symbol is not found.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_book"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/info"
            # Assuming HyperliquidRequestBuilder has or will have this method:
            try:
                request_payload_model = self._request_builder.build_l2_book_request_payload(
                    symbol=symbol
                )
            except Exception as e:
                # Wrap request builder exceptions in APIError
                logger.error(f"[{self._exchange_name}] Request builder failed for l2Book: {e}")
                raise APIError(
                    message=f"Failed to build l2Book request for symbol {symbol}: {str(e)}",
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

            request_payload_data: dict[str, Any] = request_payload_model.model_dump(
                by_alias=True, exclude_none=True
            )

            raw_response_content_parsed: ParsedJsonResponse | None = None
            headers: Mapping[str, str] = {}
            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data,
            )

            if raw_response_content_parsed is not None:
                raw_response_content = str(raw_response_content_parsed)

            logger.debug(
                f"[{self._exchange_name}] Raw l2 orderbook response for {symbol}: "
                f"{raw_response_content_parsed!r}, Status: {status_code}, Headers: {headers}"
            )

            if raw_response_content_parsed is None:
                _error_msg = (
                    f"No content received from HTTP client for l2Book for {symbol}. "
                    f"Status: {status_code}"
                )
                logger.error(f"[{self._exchange_name}] {_error_msg}")
                raise APIError(
                    message=_error_msg,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            validated_raw_book = self._response_handler.handle_info_l2_book_response(
                raw_response_content_parsed,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )
            return self._mapper.transform_raw_order_book_to_internal(validated_raw_book)

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_recent_trades(
        self,
        symbol: str,
        # limit: int = 100, # Limit is not part of HL /info request for recentTrades
    ) -> list[Trade]:
        """
        Retrieves recent public trades for a specific symbol using a POST request to /info
        with a payload: {"type": "recentTrades", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A list of Trade objects.
            The number of trades is determined by the Hyperliquid API.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_recent_trades"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/info"
            # Assuming HyperliquidRequestBuilder has or will have this method:
            request_payload_model = self._request_builder.build_recent_trades_request_payload(
                symbol=symbol
            )
            request_payload_data: dict[str, Any] = request_payload_model.model_dump(
                by_alias=True, exclude_none=True
            )

            raw_response_content_parsed: ParsedJsonResponse | None = None
            headers: Mapping[str, str] = {}
            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data,
            )

            if raw_response_content_parsed is not None:
                raw_response_content = str(raw_response_content_parsed)

            logger.debug(
                f"[{self._exchange_name}] Raw recent_trades response for {symbol}: "
                f"{raw_response_content_parsed!r}, Status: {status_code}, Headers: {headers}"
            )

            if raw_response_content_parsed is None:
                _error_msg = (
                    f"No content received from HTTP client for recentTrades for {symbol}. "
                    f"Status: {status_code}"
                )
                logger.error(f"[{self._exchange_name}] {_error_msg}")
                raise APIError(
                    message=_error_msg,
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            validated_raw_trades = self._response_handler.handle_info_recent_trades_response(
                raw_response_content_parsed,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )
            # The response handler is expected to return the full list of trades from the API.
            # Limiting should be done by the caller or a mapping layer if needed.
            internal_trades: list[Trade] = []
            for raw_trade in validated_raw_trades:
                try:
                    # Use static call to mapper - Name was correct
                    trade = self._mapper.transform_raw_public_trade_to_internal(raw_trade)
                    if trade:
                        internal_trades.append(trade)
                except (ValidationError, ValueError) as e_map_item:
                    logger.warning(
                        f"[{self._exchange_name}] Skipping mapping for recent trade item: "
                        f"{e_map_item}. Raw: {raw_trade!r}"
                    )

            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_trades)} recent_trades for {symbol}"
            )
            return internal_trades

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """
        Retrieves the current funding rate information for a specific perpetual contract symbol.
        This is typically part of the broader asset context. It calls get_all_asset_contexts
        and extracts the relevant context.

        Args:
            symbol: The perpetual contract symbol (e.g., "ETH").

        Returns:
            A FundingRate object if the symbol is found, otherwise None.
            The 'funding' field of this object contains the funding rate.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rate"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if all_contexts_response and all_contexts_response.asset_ctxs:
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol:
                        return self._mapper.transform_raw_asset_ctx_to_funding_rate(asset_ctx)

            logger.warning(
                f"[{self._exchange_name}] Funding rate data (from asset context) not found "
                f"for symbol '{symbol}'."
            )
            return None

        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_funding_rates(self, symbols: list[str] | None = None) -> list[FundingRate]:
        """
        Retrieves current funding rates for specified symbols, or all if None.

        Args:
            symbols: A list of symbols to get funding rates for. If None, fetches for all.

        Returns:
            A list of FundingRate objects.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rates"

        if symbols is not None:
            for symbol in symbols:
                if not symbol:
                    raise ValueError(
                        f"[{current_method}] All symbols in list must be non-empty strings."
                    )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            rates: list[FundingRate] = []
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if not all_contexts_response or not all_contexts_response.asset_ctxs:
                logger.warning(
                    f"[{self._exchange_name}] No asset contexts found to derive funding rates."
                )
                return []

            symbols_to_process: list[str]
            if symbols:
                symbols_to_process = symbols
            else:
                symbols_to_process = [
                    str(ctx.name) for ctx in all_contexts_response.asset_ctxs if ctx.name
                ]

            for symbol_name in symbols_to_process:
                found_ctx = False
                for asset_ctx in all_contexts_response.asset_ctxs:
                    if asset_ctx.name == symbol_name:
                        try:
                            rate = self._mapper.transform_raw_asset_ctx_to_funding_rate(asset_ctx)
                            if rate:
                                rates.append(rate)
                            found_ctx = True
                            break
                        except Exception as e_map:
                            logger.error(
                                f"[{self._exchange_name}] Error mapping funding rate for "
                                f"{symbol_name} from context: {e_map}. "
                                f"Context: {asset_ctx.model_dump_json(indent=2)}"
                            )
                if (
                    not found_ctx and symbols
                ):  # Only warn if specific symbols were requested and not found
                    logger.warning(
                        f"[{self._exchange_name}] Context for symbol '{symbol_name}' not found in "
                        f"fetched asset contexts."
                    )
            return rates

        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                f"{e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: "
                f"{e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def get_historical_funding_rates(
        self,
        symbol: str,
        start_time: datetime,
        end_time: datetime | None = None,
    ) -> list[FundingRate]:
        """Retrieves historical funding rates for a specific symbol and time range."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "get_historical_funding_rates"
        )

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")

        # Convert datetime to milliseconds
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms: int | None = None
        if end_time is not None:
            end_time_ms = int(end_time.timestamp() * 1000)

        # Validate time parameters
        if start_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'start_time_ms' must be positive.")
        if end_time_ms is not None:
            if end_time_ms <= 0:
                raise ValueError(f"[{current_method}] 'end_time_ms' must be positive.")
            if end_time_ms < start_time_ms:
                raise ValueError(
                    f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'."
                )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: ParsedJsonResponse | None = None

        try:
            logger.debug(
                f"[{self._exchange_name}] Getting historical funding rates for {symbol} "
                f"from {start_time_ms} to {end_time_ms if end_time_ms is not None else 'now'}."
            )

            endpoint_path = "/info"
            payload = self._request_builder.build_historical_funding_rates_payload(
                symbol=symbol, start_time_ms=start_time_ms, end_time_ms=end_time_ms
            )

            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload,
            )

            if raw_response_content is None:
                logger.warning(
                    f"[{self._exchange_name}] No content for historical funding rates "
                    f"for {symbol}. Status: {status_code}."
                )
                raise APIError(
                    message=f"No data received for historical funding rates for {symbol}, "
                    f"status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            raw_funding_history_items: list[HyperliquidRawFundingHistoryItem] = (
                self._response_handler.handle_historical_funding_rates_response(
                    raw_response_content=raw_response_content,
                    status_code=status_code,
                    headers=headers,
                )
            )

            internal_funding_rates: list[FundingRate] = []
            for raw_item in raw_funding_history_items:
                try:
                    internal_rate = self._mapper.transform_raw_funding_history_item_to_internal(
                        raw_item
                    )
                    internal_funding_rates.append(internal_rate)
                except (ValidationError, ValueError) as e:
                    logger.error(
                        f"[{self._exchange_name}] Error mapping historical funding rate item: {e}. "
                        f"Raw: {raw_item!r}"
                    )
                    raise APIError(
                        message=f"Processing historical funding rate data failed: {e}",
                        code=APIErrorCode.UNKNOWN.value,
                        original_exception=e,
                    ) from e

            return internal_funding_rates
        except APIError:
            raise  # Re-raise APIErrors
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] [{current_method}] TransformationError: {e_transform}. "
                f"Status: {status_code}, Raw: {raw_response_content}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code,
                exchange_message=str(raw_response_content)
                if raw_response_content is not None
                else None,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] [{current_method}] ValidationError: {e_val}. "
                f"Status: {status_code}, Raw: {raw_response_content}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_response_content)
                if raw_response_content is not None
                else None,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] [{current_method}] Service logic error: "
                f"{e_service_logic}. Status: {status_code}, Raw: {raw_response_content}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] [{current_method}] Unexpected error: {e_unexpected}. "
                f"Status: {status_code}, Raw: {raw_response_content}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code,
                exchange_message=str(raw_response_content)
                if raw_response_content is not None
                else None,
            ) from e_unexpected

    async def get_market_data(
        self, symbol: str, interval: str, start_time_ms: int, end_time_ms: int
    ) -> list[Candle]:
        """
        Retrieves historical kline/candlestick data for a symbol and timeframe.
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
            A list of Candle objects containing lists of candle data points.

        Raises:
            APIError: If the API request fails or the response is invalid.
        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market_data"

        if not symbol:
            raise ValueError(f"[{current_method}] 'symbol' must be a non-empty string.")
        if not interval:
            raise ValueError(f"[{current_method}] 'interval' must be a non-empty string.")
        if start_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'start_time_ms' must be positive.")
        if end_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'end_time_ms' must be positive.")
        if end_time_ms < start_time_ms:
            raise ValueError(f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            logger.debug(
                f"[{self._exchange_name}] Getting market data (candles) for {symbol}, "
                f"interval {interval}, start {start_time_ms}, end {end_time_ms}"
            )
            endpoint_path = "/info"
            try:
                payload = self._request_builder.build_candle_snapshot_payload(
                    symbol=symbol,
                    timeframe=interval,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                )
            except Exception as e:
                # Wrap request builder exceptions in APIError
                logger.error(
                    f"[{self._exchange_name}] Request builder failed for candle snapshot: {e}"
                )
                raise APIError(
                    message=(
                        f"Failed to build candle snapshot request for symbol {symbol}: {str(e)}"
                    ),
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=payload.model_dump(
                    by_alias=True, exclude_none=True
                ),  # Payload itself is a dict[str, Any]
            )

            if raw_response_content_parsed is not None:
                raw_response_content = str(raw_response_content_parsed)

            if raw_response_content_parsed is None:
                logger.error(
                    f"[{self._exchange_name}] No content received for candles {symbol}, "
                    f"status: {status_code}."
                )
                # Consider raising APIError or returning empty list based on desired strictness
                raise APIError(
                    message=f"No data received for market data (candles) for {symbol}, "
                    f"status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            # Assuming raw_response_content is list[dict[str, Any]] for candles
            # The handler expects RawJsonResponse which can be list.

            # The handler expects raw JSON, not already Pydantic validated models typically
            # For candles, it might be list of lists or list of dicts
            raw_candles = self._response_handler.handle_info_candle_snapshot_response(
                raw_response_content_parsed, symbol, interval, status_code, headers
            )
            return self._mapper.transform_raw_candle_snapshot_to_candles(
                raw_candles, symbol, interval
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {symbol}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {symbol}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Service internal logic error "
                f"for {symbol}: {e_service_logic}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {symbol}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
