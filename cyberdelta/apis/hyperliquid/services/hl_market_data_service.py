"""CyberDeltaEngine: Hyperliquid Market Data Service.

-------------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Hyperliquid Exchange. It uses the HttpClient, HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

# Typing and Pydantic
import inspect
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions.market_data_service import SymbolNotFoundError
from cyberdelta.apis.exceptions.response_validation import EmptyResponseError

# Hyperliquid-specific imports
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
)

# Mappers
from cyberdelta.apis.hyperliquid.mappers import HyperliquidMarketDataMapper

# Removed unused raw model imports as handlers return these directly now
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade
from cyberdelta.apis.models.service_args_models import (
    GetCandleSnapshotArgs,
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetL2BookArgs,
    GetMarketArgs,
    GetMarketDataArgs,
    GetMarketsArgs,
    GetRecentTradesArgs,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)

# Utilities
from cyberdelta.config.structlog_config import get_logger

# Internal Domain Models
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market import Market
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.models.market.mid_prices import MidPrices

# Project-specific imports for connectivity and base types
from cyberdelta.utils.parsing import timeframe_to_ms
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidMarketDataService:
    """Service class for handling Hyperliquid market data API endpoints.

    This class centralizes the logic for fetching market data such as tickers,
    order books, trades, funding rates, and candlestick data.
    It leverages shared components like HttpClient, HyperliquidRequestBuilder,
    and HyperliquidResponseHandler to perform its tasks.
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
        """Initialize the HyperliquidMarketDataService.

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

    def _validate_response_not_none(
        self,
        response: ParsedJsonResponse | None,
        response_type: str,
        operation: str,
        status_code: int,
    ) -> ParsedJsonResponse:
        """Validate that response is not None.

        Args:
            response: The response to validate
            response_type: Type of response expected
            operation: Operation that returned the response
            status_code: HTTP status code

        Returns:
            The validated non-None response for type narrowing

        Raises:
            EmptyResponseError: If response is None
        """
        if response is None:
            raise EmptyResponseError(
                response_type=response_type,
                operation=operation,
                http_status=status_code,
                exchange=self._exchange_name,
            )
        return response

    def _find_market_or_raise(
        self,
        symbol: str,
        all_markets: list[Market],
    ) -> Market:
        """Find market by symbol in the markets list or raise error.

        Args:
            symbol: Symbol to find
            all_markets: List of available markets

        Returns:
            Market if found

        Raises:
            SymbolNotFoundError: If symbol not found
        """
        for market in all_markets:
            if market.symbol == symbol:
                return market

        # Symbol not found
        raise SymbolNotFoundError(
            symbol=symbol,
            available_symbols=[m.symbol for m in all_markets],
            exchange=self._exchange_name,
        )

    async def get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Retrieve the metadata for all listed assets and their current context.

        (mark price, funding rate, etc.) by calling the /info endpoint.
        Hyperliquid's /info endpoint often returns multiple data types; the handler
        is responsible for extracting and validating the metaAndAssetCtxs part.

        Returns:
            A HyperliquidRawMetaAndAssetCtxsResponse object containing validated raw data.

        Raises:
            APIError: If the API request fails or the response is invalid.

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_asset_contexts_raw"

        # No input parameters to validate for this method

        # Initialize context for error handling
        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content_str: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/info"
            # HyperliquidRequestBuilder.build_info_request_payload() now returns a Pydantic model.
            request_payload_model = self._request_builder.build_info_request_payload()
            request_payload_data_dict = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,  # Use by_alias if model uses aliases
            )

            headers: Mapping[str, str] = {}

            # Use the single HTTP client requester for /info endpoint
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data_dict,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )

            if raw_response_content is not None:
                raw_response_content_str = str(raw_response_content)

            logger.debug(
                "raw_all_asset_contexts_response",
                action="get_all_asset_contexts_raw",
                exchange=self._exchange_name,
                raw_response=raw_response_content,
                status_code=status_code,
                headers=headers,
                message=("Raw all_asset_contexts response: %r, Status: %s, Headers: %s"),
                message_args=(raw_response_content, status_code, headers),
            )

            # Use centralized validation - metaAndAssetCtxs returns a list
            validated_raw_data = ensure_list_response(
                raw_response_content,
                "metaAndAssetCtxs",
                status_code,
            )

            validated_response: HyperliquidRawMetaAndAssetCtxsResponse = (
                self._response_handler.handle_info_meta_and_asset_ctxs_response(
                    validated_raw_data,
                    status_code=status_code,
                    headers=headers,
                )
            )
        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transformation_error",
                action=current_method,
                exchange=self._exchange_name,
                error=str(e_transform),
                message=f"Failed to transform exchange data: {e_transform}",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                action=current_method,
                exchange=self._exchange_name,
                error=str(e_val),
                message=f"Internal data validation failed: {e_val}",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                message="[%s] %s: Service internal logic error: %s",
                message_args=(self._exchange_name, current_method, e_service_logic),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                message="[%s] %s: Unexpected service failure: %s",
                message_args=(self._exchange_name, current_method, e_unexpected),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_unexpected
        else:
            return validated_response

    async def get_ticker(self, symbol: str) -> Ticker | None:
        """Retrieve the latest ticker/context information for a specific symbol.

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

        # Validate symbol
        self._validate_symbol(symbol, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if (
                all_contexts_response
                and all_contexts_response.asset_ctxs
                and all_contexts_response.meta
            ):
                # Match asset contexts with universe names by index
                # The asset contexts are in the same order as the universe
                universe = all_contexts_response.meta.universe
                for i, asset_def in enumerate(universe):
                    if asset_def.name == symbol and i < len(all_contexts_response.asset_ctxs):
                        asset_ctx = all_contexts_response.asset_ctxs[i]
                        # Create a copy with the name field populated for the mapper
                        asset_ctx_with_name = asset_ctx.model_copy(update={"name": symbol})
                        return self._mapper.transform_raw_asset_ctx_to_ticker(asset_ctx_with_name)

            # Symbol not found in the contexts
            logger.warning(
                "ticker_data_not_found",
                action="get_ticker",
                exchange=self._exchange_name,
                symbol=symbol,
                message=(
                    f"Ticker data (asset context) not found for symbol '{symbol}' "
                    "after fetching all asset contexts."
                ),
            )
        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "ticker_transform_error",
                message="[%s] %s: Failed to transform exchange data for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                message="[%s] %s: Internal data validation failed for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_service_logic),
                message=f"Service internal logic error for {symbol}: {e_service_logic}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_unexpected),
                message=f"Unexpected service failure for {symbol}: {e_unexpected}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected
        else:
            return None  # Consistent with method signature if not found

    def _validate_symbol(self, symbol: str, current_method: str) -> None:
        """Validate symbol parameter.

        Args:
            symbol: Trading symbol to validate
            current_method: Calling method name for error context

        Raises:
            ValueError: If symbol is invalid
        """
        if not symbol:
            error_msg = f"[{current_method}] 'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Strip and check again
        if not symbol.strip():
            error_msg = f"[{current_method}] 'symbol' cannot be empty or whitespace only."
            raise ValueError(error_msg)

    def _validate_candle_snapshot_params(
        self,
        args: GetCandleSnapshotArgs,
        current_method: str,
    ) -> None:
        """Validate candle snapshot parameters.

        Args:
            args: Candle snapshot arguments to validate
            current_method: Calling method name for error context

        Raises:
            ValueError: If parameters are invalid
        """
        # Validate symbol
        self._validate_symbol(args.symbol, current_method)

        # Validate timeframe
        if not args.timeframe:
            error_msg = f"[{current_method}] 'timeframe' must be a non-empty string."
            raise ValueError(error_msg)

        if not args.timeframe.strip():
            error_msg = f"[{current_method}] 'timeframe' cannot be empty or whitespace only."
            raise ValueError(error_msg)

        # Validate time range
        if args.start_time_ms < 0:
            error_msg = f"[{current_method}] Start time cannot be negative: {args.start_time_ms}"
            raise ValueError(error_msg)

        if args.end_time_ms < 0:
            error_msg = f"[{current_method}] End time cannot be negative: {args.end_time_ms}"
            raise ValueError(error_msg)

        if args.start_time_ms >= args.end_time_ms:
            error_msg = (
                f"[{current_method}] Start time ({args.start_time_ms}) must be before "
                f"end time ({args.end_time_ms})"
            )
            raise ValueError(error_msg)

    async def get_order_book(self, symbol: str) -> OrderBook | None:
        """Retrieve the order book for a specific symbol using a POST request to /info.

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

        # Validate symbol
        self._validate_symbol(symbol, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            endpoint_path = "/info"
            # Assuming HyperliquidRequestBuilder has or will have this method:
            try:
                request_payload_model = self._request_builder.build_l2_book_request_payload(
                    GetL2BookArgs(symbol=symbol),
                )
            except Exception as e:
                # Wrap request builder exceptions in APIError
                logger.exception(
                    "request_builder_failed",
                    action="get_order_book",
                    exchange=self._exchange_name,
                    request_type="l2Book",
                    error=str(e),
                    message=f"[{self._exchange_name}] Request builder failed for l2Book: {e}",
                )
                error_msg = f"Failed to build l2Book request for symbol {symbol}: {e}"
                raise APIError(
                    message=error_msg,
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

            request_payload_data: dict[str, Any] = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,
            )

            raw_response_content_parsed: ParsedJsonResponse | None = None
            headers: Mapping[str, str] = {}
            raw_response_content_parsed, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data,
                is_signed=False,
                endpoint_group="public",
                request_weight=1,
            )

            if raw_response_content_parsed is not None:
                raw_response_content = str(raw_response_content_parsed)

            logger.debug(
                "l2_orderbook_response",
                message="[%s] Raw l2 orderbook response for %s: %r, Status: %s, Headers: %s",
                message_args=(
                    self._exchange_name,
                    symbol,
                    raw_response_content_parsed,
                    status_code,
                    headers,
                ),
            )

            validated_response = self._validate_response_not_none(
                raw_response_content_parsed,
                "l2Book data",
                f"l2Book for {symbol}",
                status_code,
            )

            validated_raw_book = self._response_handler.handle_info_l2_book_response(
                validated_response,
                symbol=symbol,
                status_code=status_code,
                headers=headers,
            )
            return self._mapper.transform_raw_order_book_to_internal(validated_raw_book)

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_error",
                message="[%s] %s: Failed to transform exchange data for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                message="[%s] %s: Internal data validation failed for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_service_logic),
                message=f"Service internal logic error for {symbol}: {e_service_logic}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_unexpected),
                message=f"Unexpected service failure for {symbol}: {e_unexpected}",
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
    ) -> list[Trade]:
        """Retrieve recent public trades for a specific symbol using a POST request to /info.

        Uses a payload: {"type": "recentTrades", "coin": "SYMBOL"}.

        Args:
            symbol: The trading symbol (e.g., "ETH").

        Returns:
            A list of Trade objects.
            The number of trades is determined by the Hyperliquid API.

        Raises:
            APIError: If the API request fails or the response is invalid.

        """
        # Input validation
        if not symbol:
            error_msg = "'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            (
                raw_response_content_parsed,
                status_code,
                headers,
            ) = await self._fetch_recent_trades_data(symbol)
            validated_response = self._validate_response_not_none(
                raw_response_content_parsed,
                "recent trades data",
                f"recent trades for {symbol}",
                status_code,
            )

            validated_raw_trades = self._process_recent_trades_response(
                validated_response,
                symbol,
                status_code,
                headers,
            )
            internal_trades = self._map_recent_trades_to_internal(validated_raw_trades, symbol)

            logger.debug(
                "recent_trades_mapped",
                message="[%s] Mapped %s recent_trades for %s",
                message_args=(self._exchange_name, len(internal_trades), symbol),
            )
        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_recent_trades_transformation_error(
                e_transform,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_recent_trades_validation_error(
                e_val,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_recent_trades_service_logic_error(e_service_logic, symbol)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_recent_trades_unexpected_error(
                e_unexpected,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        else:
            return internal_trades

    async def get_funding_rate(self, symbol: str) -> FundingRate | None:
        """Retrieve the current funding rate information for a specific perpetual contract symbol.

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
            error_msg = f"[{current_method}] 'symbol' must be a non-empty string."
            raise ValueError(error_msg)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            return await self._get_funding_rate_from_contexts(symbol)
        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.exception(
                "transform_error",
                message="[%s] %s: Failed to transform exchange data for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.exception(
                "validation_error",
                message="[%s] %s: Internal data validation failed for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_val),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            logger.exception(
                "service_logic_error",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_service_logic),
                message=f"Service internal logic error for {symbol}: {e_service_logic}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e_service_logic,
            ) from e_service_logic
        except Exception as e_unexpected:
            logger.exception(
                "unexpected_service_failure",
                action=current_method,
                exchange=self._exchange_name,
                symbol=symbol,
                error=str(e_unexpected),
                message=f"Unexpected service failure for {symbol}: {e_unexpected}",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def _get_funding_rate_from_contexts(self, symbol: str) -> FundingRate | None:
        """Get funding rate for symbol from asset contexts."""
        all_contexts_response = await self.get_all_asset_contexts_raw()
        if (
            all_contexts_response
            and all_contexts_response.asset_ctxs
            and all_contexts_response.meta
        ):
            # Match asset contexts with universe names by index
            # The asset contexts are in the same order as the universe
            universe = all_contexts_response.meta.universe
            for i, asset_def in enumerate(universe):
                if asset_def.name == symbol and i < len(all_contexts_response.asset_ctxs):
                    asset_ctx = all_contexts_response.asset_ctxs[i]
                    # Create a copy with the name field populated for the mapper
                    asset_ctx_with_name = asset_ctx.model_copy(update={"name": symbol})
                    return self._mapper.transform_raw_asset_ctx_to_funding_rate(
                        asset_ctx_with_name,
                    )

        logger.warning(
            "funding_rate_data_not_found",
            message="[%s] Funding rate data (from asset context) not found for symbol '%s'.",
            message_args=(self._exchange_name, symbol),
        )
        return None

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Retrieve current funding rates for specified symbols, or all if None.

        Args:
            args: GetFundingRatesArgs containing symbols list or None for all.

        Returns:
            A list of FundingRate objects.

        Raises:
            APIError: If the underlying API request to fetch all contexts fails.

        """
        # Input validation
        self._validate_funding_rates_input(args.symbols)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            all_contexts_response = await self.get_all_asset_contexts_raw()
            if not all_contexts_response or not all_contexts_response.asset_ctxs:
                logger.warning(
                    "funding_rates_no_asset_contexts",
                    message="[%s] No asset contexts found to derive funding rates.",
                    message_args=(self._exchange_name,),
                )
                return []

            symbols_to_process = self._determine_symbols_to_process(
                args.symbols,
                all_contexts_response.asset_ctxs,
            )
            return self._process_funding_rates_for_symbols(
                symbols_to_process,
                all_contexts_response.asset_ctxs,
                args.symbols,
            )

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_funding_rates_transformation_error(
                e_transform,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_funding_rates_validation_error(e_val, status_code, raw_response_content)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_funding_rates_service_logic_error(e_service_logic)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_funding_rates_unexpected_error(
                e_unexpected,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Retrieve historical funding rates for a specific symbol and time range."""
        start_time_ms, end_time_ms = self._validate_and_convert_historical_funding_times(args)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: ParsedJsonResponse | None = None

        try:
            raw_funding_history_items = await self._fetch_historical_funding_rates_data(
                args.symbol,
                start_time_ms,
                end_time_ms,
            )
            return self._map_historical_funding_rates_to_internal(raw_funding_history_items)

        except APIError:
            raise  # Re-raise APIErrors
        except TransformationError as e_transform:
            self._handle_historical_funding_rates_transformation_error(
                e_transform,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_historical_funding_rates_validation_error(
                e_val,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_historical_funding_rates_service_logic_error(e_service_logic)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_historical_funding_rates_unexpected_error(
                e_unexpected,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    def _validate_and_convert_historical_funding_times(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> tuple[int, int | None]:
        """Validate and convert historical funding rate time parameters."""
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name
            if frame is not None
            else "_validate_and_convert_historical_funding_times"
        )

        # Hyperliquid requires start_time
        if args.start_time is None:
            error_msg = f"[{current_method}] 'start_time' is required for Hyperliquid."
            raise ValueError(error_msg)

        # Convert datetime to milliseconds
        start_time_ms = int(args.start_time.timestamp() * 1000)
        end_time_ms: int | None = None
        if args.end_time is not None:
            end_time_ms = int(args.end_time.timestamp() * 1000)

        # Validate time parameters
        if start_time_ms <= 0:
            error_msg = f"[{current_method}] 'start_time_ms' must be positive."
            raise ValueError(error_msg)
        if end_time_ms is not None:
            if end_time_ms <= 0:
                error_msg = f"[{current_method}] 'end_time_ms' must be positive."
                raise ValueError(error_msg)
            if end_time_ms < start_time_ms:
                error_msg = f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'."
                raise ValueError(error_msg)

        return start_time_ms, end_time_ms

    async def _fetch_historical_funding_rates_data(
        self,
        symbol: str,
        start_time_ms: int,
        end_time_ms: int | None,
    ) -> list[Any]:
        """Fetch historical funding rates data from the API."""
        logger.debug(
            "historical_funding_rates_request",
            message="[%s] Getting historical funding rates for %s from %s to %s.",
            message_args=(
                self._exchange_name,
                symbol,
                start_time_ms,
                end_time_ms if end_time_ms is not None else "now",
            ),
        )

        endpoint_path = "/info"

        # Build payload through request builder
        # Convert milliseconds back to datetime for the args
        start_dt = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC)
        end_dt = datetime.fromtimestamp(end_time_ms / 1000, tz=UTC) if end_time_ms else None

        payload = self._request_builder.build_historical_funding_rates_payload(
            GetHistoricalFundingRatesArgs(
                symbol=symbol,
                start_time=start_dt,
                end_time=end_dt,
            ),
        )

        raw_response_content, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        # Use centralized validation
        validated_raw_data = ensure_list_response(
            raw_response_content,
            f"historical funding rates for {symbol}",
            status_code,
        )

        raw_funding_history_items: list[HyperliquidRawFundingHistoryItem] = (
            self._response_handler.handle_historical_funding_rates_response(
                raw_response_content=validated_raw_data,
                status_code=status_code,
                headers=headers,
            )
        )
        return raw_funding_history_items

    def _map_historical_funding_rates_to_internal(
        self,
        raw_funding_history_items: list[Any],
    ) -> list[FundingRate]:
        """Map raw historical funding rate items to internal FundingRate objects."""
        internal_funding_rates: list[FundingRate] = []
        for raw_item in raw_funding_history_items:
            try:
                internal_rate = self._mapper.transform_raw_funding_history_item_to_internal(
                    raw_item,
                )
                internal_funding_rates.append(internal_rate)
            except (ValidationError, ValueError) as e:
                logger.exception(
                    "historical_funding_rate_mapping_error",
                    message="[%s] Error mapping historical funding rate item: %s. Raw: %r",
                    message_args=(self._exchange_name, e, raw_item),
                )
                error_msg = f"Processing historical funding rate data failed: {e}"
                raise APIError(
                    message=error_msg,
                    code=APIErrorCode.UNKNOWN.value,
                    original_exception=e,
                ) from e

        return internal_funding_rates

    def _handle_historical_funding_rates_transformation_error(
        self,
        error: TransformationError,
        status_code: int,
        raw_response_content: ParsedJsonResponse | None,
    ) -> None:
        """Handle transformation errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            "historical_funding_rates_transform_error",
            message="[%s] [%s] TransformationError: %s. Status: %s, Raw: %s",
            message_args=(
                self._exchange_name,
                current_method,
                error,
                status_code,
                raw_response_content,
            ),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code,
            exchange_message=str(raw_response_content)
            if raw_response_content is not None
            else None,
        ) from error

    def _handle_historical_funding_rates_validation_error(
        self,
        error: ValidationError,
        status_code: int,
        raw_response_content: ParsedJsonResponse | None,
    ) -> None:
        """Handle validation errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            "historical_funding_rates_validation_error",
            message="[%s] [%s] ValidationError: %s. Status: %s, Raw: %s",
            message_args=(
                self._exchange_name,
                current_method,
                error,
                status_code,
                raw_response_content,
            ),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code,
            exchange_message=str(raw_response_content)
            if raw_response_content is not None
            else None,
        ) from error

    def _handle_historical_funding_rates_service_logic_error(
        self,
        error: ValueError | TypeError,
    ) -> None:
        """Handle service logic errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            "service_logic_error",
            message="[%s] [%s] Service logic error: %s",
            message_args=(self._exchange_name, current_method, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_historical_funding_rates_unexpected_error(
        self,
        error: Exception,
        status_code: int,
        raw_response_content: ParsedJsonResponse | None,
    ) -> None:
        """Handle unexpected errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            "unexpected_error",
            message="[%s] [%s] Unexpected error: %s. Status: %s, Raw: %s",
            message_args=(
                self._exchange_name,
                current_method,
                error,
                status_code,
                raw_response_content,
            ),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code,
            exchange_message=str(raw_response_content)
            if raw_response_content is not None
            else None,
        ) from error

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Retrieve historical kline/candlestick data for a symbol and timeframe.

        Uses a POST request to /info with payload:
        {"type": "candleSnapshot",
         "req": {"coin": SYMBOL, "interval": INTERVAL,
                 "startTime": START_MS, "endTime": END_MS}}

        Args:
            args: GetMarketDataArgs containing symbol, timeframe, limit,
                 start_time_ms, and end_time_ms parameters.

        Returns:
            A list of Candle objects containing lists of candle data points.

        Raises:
            APIError: If the API request fails or the response is invalid.
            ValueError: If input parameters are invalid.

        """
        symbol, interval, start_time_ms, end_time_ms = (
            self._validate_and_prepare_market_data_params(args)
        )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            raw_candles = await self._fetch_market_data_from_api(
                symbol,
                interval,
                start_time_ms,
                end_time_ms,
            )
            return self._mapper.transform_raw_candle_snapshot_to_candles(
                raw_candles,
                symbol,
                interval,
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            self._handle_market_data_transformation_error(
                e_transform,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_market_data_validation_error(
                e_val,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_market_data_service_logic_error(e_service_logic, symbol)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_market_data_unexpected_error(
                e_unexpected,
                symbol,
                status_code,
                raw_response_content,
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    def _validate_and_prepare_market_data_params(
        self,
        args: GetMarketDataArgs,
    ) -> tuple[str, str, int, int]:
        """Validate and prepare market data parameters."""
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name
            if frame is not None
            else "_validate_and_prepare_market_data_params"
        )

        # Map timeframe to interval for Hyperliquid internal usage
        symbol = args.symbol
        interval = args.timeframe  # Hyperliquid uses same naming internally
        limit = args.limit
        start_time_ms = args.start_time_ms
        end_time_ms = args.end_time_ms

        # Calculate time range if not provided
        if start_time_ms is None or end_time_ms is None:
            interval_ms = timeframe_to_ms(interval)
            if interval_ms == 0:
                error_msg = f"[{current_method}] Invalid or unsupported timeframe: {interval}"
                raise ValueError(error_msg)

            current_time_ms = int(time.time() * 1000)
            end_time_ms = end_time_ms or current_time_ms
            start_time_ms = start_time_ms or (end_time_ms - (limit * interval_ms))

        if start_time_ms <= 0:
            error_msg = f"[{current_method}] 'start_time_ms' must be positive."
            raise ValueError(error_msg)
        if end_time_ms <= 0:
            error_msg = f"[{current_method}] 'end_time_ms' must be positive."
            raise ValueError(error_msg)
        if end_time_ms < start_time_ms:
            error_msg = f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'."
            raise ValueError(error_msg)

        return symbol, interval, start_time_ms, end_time_ms

    async def _fetch_market_data_from_api(
        self,
        symbol: str,
        interval: str,
        start_time_ms: int,
        end_time_ms: int,
    ) -> HyperliquidRawCandleSnapshot:
        """Fetch market data from the API."""
        logger.debug(
            "market_data_candles_request",
            message="[%s] Getting market data (candles) for %s, interval %s, start %s, end %s",
            message_args=(self._exchange_name, symbol, interval, start_time_ms, end_time_ms),
        )
        endpoint_path = "/info"
        try:
            payload = self._request_builder.build_candle_snapshot_payload(
                GetCandleSnapshotArgs(
                    symbol=symbol,
                    timeframe=interval,
                    start_time_ms=start_time_ms,
                    end_time_ms=end_time_ms,
                ),
            )
        except Exception as e:
            # Wrap request builder exceptions in APIError
            logger.exception(
                "candle_snapshot_request_builder_failed",
                message="[%s] Request builder failed for candle snapshot: %s",
                message_args=(self._exchange_name, e),
            )
            error_msg = f"Failed to build candle snapshot request for symbol {symbol}: {e}"
            raise APIError(
                message=error_msg,
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e

        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload.model_dump(
                by_alias=True,
                exclude_none=True,
            ),  # Payload itself is a dict[str, Any]
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        if raw_response_content_parsed is None:
            logger.error(
                "candles_no_content_received",
                message="[%s] No content received for candles %s, status: %s.",
                message_args=(self._exchange_name, symbol, status_code),
            )
            # Consider raising APIError or returning empty list based on desired strictness
            error_msg = (
                f"No data received for market data (candles) for {symbol}, status: {status_code}"
            )
            raise APIError(
                message=error_msg,
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        # Assuming raw_response_content is list[dict[str, Any]] for candles
        # The handler expects RawJsonResponse which can be list.

        # The handler expects raw JSON, not already Pydantic validated models typically
        # For candles, it might be list of lists or list of dicts
        return self._response_handler.handle_info_candle_snapshot_response(
            raw_response_content_parsed,
            symbol,
            interval,
            status_code,
            headers,
        )

    def _handle_market_data_transformation_error(
        self,
        error: TransformationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for market data."""
        current_method = "get_market_data"
        logger.error(
            "market_data_transform_error",
            message="[%s] %s: Failed to transform exchange data for %s: %s",
            message_args=(self._exchange_name, current_method, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_market_data_validation_error(
        self,
        error: ValidationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors for market data."""
        current_method = "get_market_data"
        logger.error(
            "market_data_validation_error",
            message="[%s] %s: Internal data validation failed for %s: %s",
            message_args=(self._exchange_name, current_method, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_market_data_service_logic_error(
        self,
        error: ValueError | TypeError,
        symbol: str,
    ) -> None:
        """Handle service logic errors for market data."""
        current_method = "get_market_data"
        logger.error(
            "market_data_service_logic_error",
            message="[%s] %s: Service internal logic error for %s: %s",
            message_args=(self._exchange_name, current_method, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_market_data_unexpected_error(
        self,
        error: Exception,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors for market data."""
        current_method = "get_market_data"
        logger.error(
            "market_data_unexpected_error",
            message="[%s] %s: Unexpected service failure for %s: %s",
            message_args=(self._exchange_name, current_method, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _validate_funding_rates_input(self, symbols: list[str] | None) -> None:
        """Validate input symbols for funding rates request."""
        if symbols is not None:
            for symbol in symbols:
                if not symbol:
                    error_msg = "All symbols in list must be non-empty strings."
                    raise ValueError(error_msg)

    def _determine_symbols_to_process(
        self,
        symbols: list[str] | None,
        asset_ctxs: list[Any],
    ) -> list[str]:
        """Determine which symbols to process for funding rates."""
        if symbols:
            return symbols
        return [str(ctx.name) for ctx in asset_ctxs if ctx.name]

    def _process_funding_rates_for_symbols(
        self,
        symbols_to_process: list[str],
        asset_ctxs: list[Any],
        requested_symbols: list[str] | None,
    ) -> list[FundingRate]:
        """Process funding rates for the given symbols."""
        rates: list[FundingRate] = []

        for symbol_name in symbols_to_process:
            found_ctx = False
            for asset_ctx in asset_ctxs:
                if asset_ctx.name == symbol_name:
                    try:
                        rate = self._mapper.transform_raw_asset_ctx_to_funding_rate(asset_ctx)
                        if rate:
                            rates.append(rate)
                        found_ctx = True
                        break
                    except Exception as e_map:
                        logger.exception(
                            "funding_rate_mapping_error",
                            message=(
                                "[%s] Error mapping funding rate for %s from context: %s. "
                                "Context: %s"
                            ),
                            message_args=(
                                self._exchange_name,
                                symbol_name,
                                e_map,
                                asset_ctx.model_dump_json(indent=2),
                            ),
                        )

            if not found_ctx and requested_symbols:
                # Only warn if specific symbols were requested and not found
                logger.warning(
                    "symbol_context_not_found",
                    message="[%s] Context for symbol '%s' not found in fetched asset contexts.",
                    message_args=(self._exchange_name, symbol_name),
                )

        return rates

    def _handle_funding_rates_transformation_error(
        self,
        error: TransformationError,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for funding rates."""
        logger.error(
            "funding_rates_transform_error",
            message="[%s] get_funding_rates: Failed to transform exchange data: %s",
            message_args=(self._exchange_name, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_funding_rates_validation_error(
        self,
        error: ValidationError,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors for funding rates."""
        logger.error(
            "funding_rates_validation_error",
            message="[%s] get_funding_rates: Internal data validation failed: %s",
            message_args=(self._exchange_name, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_funding_rates_service_logic_error(self, error: ValueError | TypeError) -> None:
        """Handle service logic errors for funding rates."""
        logger.error(
            "funding_rates_service_logic_error",
            message="[%s] get_funding_rates: Service internal logic error: %s",
            message_args=(self._exchange_name, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_funding_rates_unexpected_error(
        self,
        error: Exception,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors for funding rates."""
        logger.error(
            "funding_rates_unexpected_error",
            message="[%s] get_funding_rates: Unexpected service failure: %s",
            message_args=(self._exchange_name, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    async def _fetch_recent_trades_data(
        self,
        symbol: str,
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Fetch raw recent trades data from API."""
        endpoint_path = "/info"
        request_payload_model = self._request_builder.build_recent_trades_request_payload(
            GetRecentTradesArgs(symbol=symbol),
        )
        request_payload_data: dict[str, Any] = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,
        )

        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=request_payload_data,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        logger.debug(
            "recent_trades_raw_response",
            message="[%s] Raw recent_trades response for %s: %r, Status: %s, Headers: %s",
            message_args=(
                self._exchange_name,
                symbol,
                raw_response_content_parsed,
                status_code,
                headers,
            ),
        )

        if raw_response_content_parsed is None:
            error_msg = (
                f"No content received from HTTP client for recentTrades for {symbol}. "
                f"Status: {status_code}"
            )
            logger.error(
                "trade_history_empty_response",
                action="get_trade_history",
                exchange=self._exchange_name,
                symbol=symbol,
                status_code=status_code,
                message=f"[{self._exchange_name}] {error_msg}",
            )
            raise APIError(
                message=error_msg,
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )

        return raw_response_content_parsed, status_code, headers

    def _process_recent_trades_response(
        self,
        raw_response_content_parsed: ParsedJsonResponse,
        symbol: str,
        status_code: int,
        headers: Mapping[str, str],
    ) -> list[HyperliquidRawPublicTrade]:
        """Process the raw response and validate recent trades data."""
        return self._response_handler.handle_info_recent_trades_response(
            raw_response_content_parsed,
            symbol=symbol,
            status_code=status_code,
            headers=headers,
        )

    def _map_recent_trades_to_internal(
        self,
        validated_raw_trades: list[HyperliquidRawPublicTrade],
        symbol: str,
    ) -> list[Trade]:
        """Map validated raw trades to internal Trade objects."""
        internal_trades: list[Trade] = []
        for raw_trade in validated_raw_trades:
            try:
                trade = self._mapper.transform_raw_public_trade_to_internal(raw_trade)
                if trade:
                    internal_trades.append(trade)
            except (ValidationError, ValueError) as e_map_item:
                logger.warning(
                    "recent_trade_mapping_skipped",
                    message="[%s] Skipping mapping for recent trade item: %s. Raw: %r",
                    message_args=(self._exchange_name, e_map_item, raw_trade),
                )
        return internal_trades

    def _handle_recent_trades_transformation_error(
        self,
        error: TransformationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for recent trades."""
        logger.error(
            "recent_trades_transform_error",
            message="[%s] get_recent_trades: Failed to transform exchange data for %s: %s",
            message_args=(self._exchange_name, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_recent_trades_validation_error(
        self,
        error: ValidationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle validation errors for recent trades."""
        logger.error(
            "recent_trades_validation_error",
            message="[%s] get_recent_trades: Internal data validation failed for %s: %s",
            message_args=(self._exchange_name, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_recent_trades_service_logic_error(
        self,
        error: ValueError | TypeError,
        symbol: str,
    ) -> None:
        """Handle service logic errors for recent trades."""
        logger.error(
            "recent_trades_service_logic_error",
            message="[%s] get_recent_trades: Service internal logic error for %s: %s",
            message_args=(self._exchange_name, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_recent_trades_unexpected_error(
        self,
        error: Exception,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle unexpected errors for recent trades."""
        logger.error(
            "recent_trades_unexpected_error",
            message="[%s] get_recent_trades: Unexpected service failure for %s: %s",
            message_args=(self._exchange_name, symbol, error),
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    async def get_markets(self, args: GetMarketsArgs) -> list[Market]:
        """Retrieve market metadata for all available markets.

        Uses Hyperliquid's /info endpoint with metaAndAssetCtxs to get
        asset definitions and current contexts, then transforms them to
        internal Market models.

        Returns:
            List of Market objects with metadata for all available assets
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_markets"

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content_str: str | None = None

        try:
            # Get raw meta and asset contexts
            raw_meta_and_asset_ctxs = await self.get_all_asset_contexts_raw()

            # Transform to internal Market models using mapper
            markets = self._mapper.transform_raw_meta_and_asset_ctxs_to_markets(
                raw_meta_and_asset_ctxs,
            )

            logger.debug(
                "markets_transformed",
                message="[%s] Transformed %s markets from meta response",
                message_args=(self._exchange_name, len(markets)),
            )

        except APIError:
            # Re-raise APIErrors from get_all_asset_contexts_raw or mapper
            raise
        except TransformationError as e_transform:
            logger.exception(
                "markets_transform_error",
                message="[%s] %s: Failed to transform exchange data for markets: %s",
                message_args=(self._exchange_name, current_method, e_transform),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_transform
        except Exception as e_unhandled:
            logger.exception(
                "markets_unexpected_error",
                message="[%s] %s: Unexpected error for markets: %s",
                message_args=(self._exchange_name, current_method, e_unhandled),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
            ) from e_unhandled
        else:
            return markets

    async def get_market(self, args: GetMarketArgs) -> Market:
        """Retrieve market metadata for a specific symbol.

        Gets all market metadata and filters for the requested symbol.
        This is necessary because Hyperliquid doesn't have a single-market endpoint.

        Args:
            args: Parameters for market metadata request including symbol.

        Returns:
            Market object with metadata for the specified symbol

        Raises:
            APIError: If symbol is not found or API request fails
        """
        symbol = args.symbol
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market"

        # Service Input Parameter Validation is now handled by GetMarketArgs Pydantic model

        try:
            # Get all markets and filter for the requested symbol
            all_markets = await self.get_markets(GetMarketsArgs())

            # Find the specific market (will raise if not found)
            market = self._find_market_or_raise(symbol, all_markets)
        except APIError:
            # Re-raise APIErrors (including SYMBOL_NOT_FOUND)
            raise
        except Exception as e_unhandled:
            logger.exception(
                "market_unexpected_error",
                message="[%s] %s: Unexpected error for %s: %s",
                message_args=(self._exchange_name, current_method, symbol, e_unhandled),
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error occurred.",
                original_exception=e_unhandled,
            ) from e_unhandled
        else:
            logger.debug(
                "market_metadata_found",
                action="get_market_metadata",
                exchange=self._exchange_name,
                symbol=symbol,
                message=f"[{self._exchange_name}] Found market metadata for {symbol}",
            )
            return market

    async def get_all_mids(self) -> MidPrices:
        """Fetch all mid prices efficiently for market order pricing.

        Uses the /info endpoint with {"type": "allMids"} to get a mapping
        of all symbol mid prices in a single request.

        Returns:
            MidPrices: Model containing symbol to mid price mapping

        Raises:
            APIError: If the API request fails or the response is invalid
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_mids"

        # Initialize context for error handling
        raw_response_content: ParsedJsonResponse | None = None
        status_code: int = 0

        try:
            # Build request payload
            endpoint_path = "/info"
            request_payload_model = self._request_builder.build_all_mids_request_payload()
            request_payload_data_dict = request_payload_model.model_dump(
                by_alias=True,
                exclude_none=True,
            )

            headers: Mapping[str, str] = {}

            # Make the API request
            raw_response_content, status_code, headers = await self._http_client_requester(
                method="POST",
                endpoint=endpoint_path,
                data=request_payload_data_dict,
                is_signed=False,
                endpoint_group="public",
                request_weight=2,  # AllMids has weight 2
            )

            # Use centralized validation
            validated_raw_data = ensure_dict_response(raw_response_content, "all mids", status_code)

            # Parse response with proper validation
            raw_all_mids = self._response_handler.handle_all_mids_response(
                validated_raw_data,
                status_code,
                headers,
            )

            # Transform to internal MidPrices model
            return self._mapper.transform_raw_all_mids_to_internal(raw_all_mids)

        except APIError:
            # Re-raise APIErrors as-is
            raise
        except ValidationError as e:
            logger.exception(
                "all_mids_validation_error",
                message="[%s] %s: Validation error: %s. Status: %s, Raw: %s",
                message_args=(
                    self._exchange_name,
                    current_method,
                    e,
                    status_code,
                    raw_response_content,
                ),
            )
            error_msg = "Failed to validate AllMids response"
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=error_msg,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_response_content) if raw_response_content else None,
            ) from e
        except Exception as e:
            logger.exception(
                "all_mids_unexpected_error",
                message="[%s] %s: Unexpected error: %s. Status: %s, Raw: %s",
                message_args=(
                    self._exchange_name,
                    current_method,
                    e,
                    status_code,
                    raw_response_content,
                ),
            )
            error_msg = "Unexpected error fetching all mid prices"
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message=error_msg,
                original_exception=e,
                http_status=status_code,
                exchange_message=str(raw_response_content) if raw_response_content else None,
            ) from e
