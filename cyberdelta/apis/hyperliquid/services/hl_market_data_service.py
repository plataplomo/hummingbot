"""CyberDeltaEngine: Hyperliquid Market Data Service.

-------------------------------------------------

This service encapsulates the logic for fetching and processing market data
from the Hyperliquid Exchange. It uses the HttpClient, HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API and returns validated
Raw Pydantic Models.
"""

# Typing and Pydantic
import inspect
from collections.abc import Awaitable, Callable, Mapping
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

# Removed unused raw model imports as handlers return these directly now
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_public_trades import HyperliquidRawPublicTrade

# from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketDataArgs,
)

# Utilities
from cyberdelta.config.logging_config import get_logger

# Internal Domain Models
from cyberdelta.core.models import FundingRate, OrderBook, Ticker, Trade
from cyberdelta.core.models.market.candle import Candle

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the service will use.
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

if TYPE_CHECKING:
    pass


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
                f"[{self._exchange_name}] Raw all_asset_contexts response: "
                f"{raw_response_content!r}, Status: {status_code}, Headers: {headers}",
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
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
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
                exchange_message=raw_response_content_str,
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
                exchange_message=raw_response_content_str,
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
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content_str,
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
                exchange_message=raw_response_content_str,
            ) from e_unexpected

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
                f"for symbol '{symbol}' after fetching all asset contexts.",
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
                    symbol=symbol,
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
                f"[{self._exchange_name}] Raw l2 orderbook response for {symbol}: "
                f"{raw_response_content_parsed!r}, Status: {status_code}, Headers: {headers}",
            )

            if raw_response_content_parsed is None:
                raise APIError(
                    message=(
                        f"No content received from HTTP client for l2Book for {symbol}, "
                        f"status: {status_code}"
                    ),
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
            raise ValueError("'symbol' must be a non-empty string.")

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            (
                raw_response_content_parsed,
                status_code,
                headers,
            ) = await self._fetch_recent_trades_data(symbol)
            if raw_response_content_parsed is None:
                raise APIError(
                    message=(
                        f"No content received from HTTP client for recentTrades for {symbol}. "
                        f"Status: {status_code}"
                    ),
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            validated_raw_trades = self._process_recent_trades_response(
                raw_response_content_parsed, symbol, status_code, headers
            )
            internal_trades = self._map_recent_trades_to_internal(validated_raw_trades, symbol)

            logger.debug(
                f"[{self._exchange_name}] Mapped {len(internal_trades)} recent_trades for {symbol}",
            )
            return internal_trades

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_recent_trades_transformation_error(
                e_transform, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_recent_trades_validation_error(
                e_val, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_recent_trades_service_logic_error(e_service_logic, symbol)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_recent_trades_unexpected_error(
                e_unexpected, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

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
                f"for symbol '{symbol}'.",
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
                    f"[{self._exchange_name}] No asset contexts found to derive funding rates.",
                )
                return []

            symbols_to_process = self._determine_symbols_to_process(
                args.symbols, all_contexts_response.asset_ctxs
            )
            rates = self._process_funding_rates_for_symbols(
                symbols_to_process, all_contexts_response.asset_ctxs, args.symbols
            )
            return rates

        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_funding_rates_transformation_error(
                e_transform, status_code, raw_response_content
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
                e_unexpected, status_code, raw_response_content
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
                args.symbol, start_time_ms, end_time_ms
            )
            return self._map_historical_funding_rates_to_internal(raw_funding_history_items)

        except APIError:
            raise  # Re-raise APIErrors
        except TransformationError as e_transform:
            self._handle_historical_funding_rates_transformation_error(
                e_transform, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_historical_funding_rates_validation_error(
                e_val, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_historical_funding_rates_service_logic_error(e_service_logic)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_historical_funding_rates_unexpected_error(
                e_unexpected, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    def _validate_and_convert_historical_funding_times(
        self, args: GetHistoricalFundingRatesArgs
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
            raise ValueError(f"[{current_method}] 'start_time' is required for Hyperliquid.")

        # Convert datetime to milliseconds
        start_time_ms = int(args.start_time.timestamp() * 1000)
        end_time_ms: int | None = None
        if args.end_time is not None:
            end_time_ms = int(args.end_time.timestamp() * 1000)

        # Validate time parameters
        if start_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'start_time_ms' must be positive.")
        if end_time_ms is not None:
            if end_time_ms <= 0:
                raise ValueError(f"[{current_method}] 'end_time_ms' must be positive.")
            if end_time_ms < start_time_ms:
                raise ValueError(
                    f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'.",
                )

        return start_time_ms, end_time_ms

    async def _fetch_historical_funding_rates_data(
        self, symbol: str, start_time_ms: int, end_time_ms: int | None
    ) -> list[Any]:
        """Fetch historical funding rates data from the API."""
        from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
            HyperliquidRawFundingHistoryItem,
        )

        logger.debug(
            f"[{self._exchange_name}] Getting historical funding rates for {symbol} "
            f"from {start_time_ms} to {end_time_ms if end_time_ms is not None else 'now'}.",
        )

        endpoint_path = "/info"
        payload = self._request_builder.build_historical_funding_rates_payload(
            symbol=symbol,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

        raw_response_content, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload,
            is_signed=False,
            endpoint_group="public",
            request_weight=1,
        )

        if raw_response_content is None:
            logger.warning(
                f"[{self._exchange_name}] No content for historical funding rates "
                f"for {symbol}. Status: {status_code}.",
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
        return raw_funding_history_items

    def _map_historical_funding_rates_to_internal(
        self, raw_funding_history_items: list[Any]
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
                logger.error(
                    f"[{self._exchange_name}] Error mapping historical funding rate item: {e}. "
                    f"Raw: {raw_item!r}",
                )
                raise APIError(
                    message=f"Processing historical funding rate data failed: {e}",
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
            f"[{self._exchange_name}] [{current_method}] TransformationError: {error}. "
            f"Status: {status_code}, Raw: {raw_response_content}",
            exc_info=True,
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
            f"[{self._exchange_name}] [{current_method}] ValidationError: {error}. "
            f"Status: {status_code}, Raw: {raw_response_content}",
            exc_info=True,
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
        self, error: ValueError | TypeError
    ) -> None:
        """Handle service logic errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            f"[{self._exchange_name}] [{current_method}] Service logic error: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_historical_funding_rates_unexpected_error(
        self, error: Exception, status_code: int, raw_response_content: ParsedJsonResponse | None
    ) -> None:
        """Handle unexpected errors for historical funding rates."""
        current_method = "get_historical_funding_rates"
        logger.error(
            f"[{self._exchange_name}] [{current_method}] Unexpected error: {error}. "
            f"Status: {status_code}, Raw: {raw_response_content}",
            exc_info=True,
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
                symbol, interval, start_time_ms, end_time_ms
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
                e_transform, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except ValidationError as e_val:
            self._handle_market_data_validation_error(
                e_val, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except (ValueError, TypeError) as e_service_logic:
            self._handle_market_data_service_logic_error(e_service_logic, symbol)
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]
        except Exception as e_unexpected:
            self._handle_market_data_unexpected_error(
                e_unexpected, symbol, status_code, raw_response_content
            )
            raise  # DEFENSIVE CHECK: Ensure function returns on all paths. Mypy=[return] Ruff=[]

    def _validate_and_prepare_market_data_params(
        self, args: GetMarketDataArgs
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
            # Import timeframe_to_ms here to avoid circular import
            import time

            from cyberdelta.utils.parsing import timeframe_to_ms

            interval_ms = timeframe_to_ms(interval)
            if interval_ms == 0:
                raise ValueError(f"[{current_method}] Invalid or unsupported timeframe: {interval}")

            current_time_ms = int(time.time() * 1000)
            end_time_ms = end_time_ms or current_time_ms
            start_time_ms = start_time_ms or (end_time_ms - (limit * interval_ms))

        if start_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'start_time_ms' must be positive.")
        if end_time_ms <= 0:
            raise ValueError(f"[{current_method}] 'end_time_ms' must be positive.")
        if end_time_ms < start_time_ms:
            raise ValueError(f"[{current_method}] 'end_time_ms' cannot be before 'start_time_ms'.")

        return symbol, interval, start_time_ms, end_time_ms

    async def _fetch_market_data_from_api(
        self, symbol: str, interval: str, start_time_ms: int, end_time_ms: int
    ) -> HyperliquidRawCandleSnapshot:
        """Fetch market data from the API."""
        logger.debug(
            f"[{self._exchange_name}] Getting market data (candles) for {symbol}, "
            f"interval {interval}, start {start_time_ms}, end {end_time_ms}",
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
                f"[{self._exchange_name}] Request builder failed for candle snapshot: {e}",
            )
            raise APIError(
                message=(f"Failed to build candle snapshot request for symbol {symbol}: {str(e)}"),
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
                f"[{self._exchange_name}] No content received for candles {symbol}, "
                f"status: {status_code}.",
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
            raw_response_content_parsed,
            symbol,
            interval,
            status_code,
            headers,
        )
        return raw_candles

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
            f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
            f"data for {symbol}: {error}",
            exc_info=True,
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
            f"[{self._exchange_name}] {current_method}: Internal data validation "
            f"failed for {symbol}: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_market_data_service_logic_error(
        self, error: ValueError | TypeError, symbol: str
    ) -> None:
        """Handle service logic errors for market data."""
        current_method = "get_market_data"
        logger.error(
            f"[{self._exchange_name}] {current_method}: Service internal logic error "
            f"for {symbol}: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_market_data_unexpected_error(
        self, error: Exception, symbol: str, status_code: int, raw_response_content: str | None
    ) -> None:
        """Handle unexpected errors for market data."""
        current_method = "get_market_data"
        logger.error(
            f"[{self._exchange_name}] {current_method}: Unexpected service failure "
            f"for {symbol}: {error}",
            exc_info=True,
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
                    raise ValueError("All symbols in list must be non-empty strings.")

    def _determine_symbols_to_process(
        self, symbols: list[str] | None, asset_ctxs: list[Any]
    ) -> list[str]:
        """Determine which symbols to process for funding rates."""
        if symbols:
            return symbols
        else:
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
                        logger.error(
                            f"[{self._exchange_name}] Error mapping funding rate for "
                            f"{symbol_name} from context: {e_map}. "
                            f"Context: {asset_ctx.model_dump_json(indent=2)}",
                        )

            if not found_ctx and requested_symbols:
                # Only warn if specific symbols were requested and not found
                logger.warning(
                    f"[{self._exchange_name}] Context for symbol '{symbol_name}' not found in "
                    f"fetched asset contexts.",
                )

        return rates

    def _handle_funding_rates_transformation_error(
        self, error: TransformationError, status_code: int, raw_response_content: str | None
    ) -> None:
        """Handle transformation errors for funding rates."""
        logger.error(
            f"[{self._exchange_name}] get_funding_rates: Failed to transform exchange "
            f"data: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Failed to process/transform exchange data.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_funding_rates_validation_error(
        self, error: ValidationError, status_code: int, raw_response_content: str | None
    ) -> None:
        """Handle validation errors for funding rates."""
        logger.error(
            f"[{self._exchange_name}] get_funding_rates: Internal data validation failed: {error}",
            exc_info=True,
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
            f"[{self._exchange_name}] get_funding_rates: Service internal logic error: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_funding_rates_unexpected_error(
        self, error: Exception, status_code: int, raw_response_content: str | None
    ) -> None:
        """Handle unexpected errors for funding rates."""
        logger.error(
            f"[{self._exchange_name}] get_funding_rates: Unexpected service failure: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    async def _fetch_recent_trades_data(
        self, symbol: str
    ) -> tuple[ParsedJsonResponse | None, int, Mapping[str, str]]:
        """Fetch raw recent trades data from API."""
        endpoint_path = "/info"
        request_payload_model = self._request_builder.build_recent_trades_request_payload(
            symbol=symbol,
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
            f"[{self._exchange_name}] Raw recent_trades response for {symbol}: "
            f"{raw_response_content_parsed!r}, Status: {status_code}, Headers: {headers}",
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
        self, validated_raw_trades: list[HyperliquidRawPublicTrade], symbol: str
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
                    f"[{self._exchange_name}] Skipping mapping for recent trade item: "
                    f"{e_map_item}. Raw: {raw_trade!r}",
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
            f"[{self._exchange_name}] get_recent_trades: Failed to transform exchange "
            f"data for {symbol}: {error}",
            exc_info=True,
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
            f"[{self._exchange_name}] get_recent_trades: Internal data validation "
            f"failed for {symbol}: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_recent_trades_service_logic_error(
        self, error: ValueError | TypeError, symbol: str
    ) -> None:
        """Handle service logic errors for recent trades."""
        logger.error(
            f"[{self._exchange_name}] get_recent_trades: Service internal logic error "
            f"for {symbol}: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Service internal logic error.",
            original_exception=error,
        ) from error

    def _handle_recent_trades_unexpected_error(
        self, error: Exception, symbol: str, status_code: int, raw_response_content: str | None
    ) -> None:
        """Handle unexpected errors for recent trades."""
        logger.error(
            f"[{self._exchange_name}] get_recent_trades: Unexpected service failure "
            f"for {symbol}: {error}",
            exc_info=True,
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error
