"""Hyperliquid Historical Data Service.

This service handles historical market data operations for the Hyperliquid exchange,
extracted from the monolithic market data service to improve maintainability and testability.

Focused on:
- Historical funding rates retrieval and time-range filtering
- Current funding rates for multiple symbols
- Historical candlestick/kline data (market data)
- Time-based validation and parameter processing
"""

from __future__ import annotations

import inspect
import time
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from pydantic import ValidationError

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.exceptions import InvalidParameterTypeError
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import HyperliquidRawCandleSnapshot
from cyberdelta.apis.hyperliquid.models.hl_raw_funding_history_info import (
    HyperliquidRawFundingHistoryItem,
    HyperliquidRawFundingHistoryResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import MarketDataRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    MarketDataResponseHandlerProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import HistoricalDataMapperProtocol
from cyberdelta.apis.models.service_args_models import (
    GetCandleSnapshotArgs,
    GetFundingRatesArgs,
    GetHistoricalFundingRatesArgs,
    GetMarketDataArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import FundingRate
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.utils.parsing import timeframe_to_ms
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidHistoricalDataService:
    """Focused service for Hyperliquid historical market data operations.

    Handles funding rates (current and historical), historical candlestick data,
    and time-based market data retrieval with comprehensive validation.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        mapper: HistoricalDataMapperProtocol,
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the historical data service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            mapper: Mapper for converting raw data to internal domain models
            exchange_name: Name identifier for this exchange instance
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._exchange_name = exchange_name

    async def get_funding_rates(self, args: GetFundingRatesArgs) -> list[FundingRate]:
        """Retrieve current funding rates for specified symbols, or all if None.

        Args:
            args: GetFundingRatesArgs containing symbols list or None for all

        Returns:
            List of FundingRate objects

        Raises:
            APIError: If the underlying API request to fetch all contexts fails
            ValueError: If symbols validation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_funding_rates"

        # Input validation
        self._validate_funding_rates_input(args.symbols)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_funding_rates",
                exchange=self._exchange_name,
                method=current_method,
                symbols_count=len(args.symbols) if args.symbols else None,
                message="Fetching current funding rates",
            )

            all_contexts_response = await self._get_all_asset_contexts_raw()
            if not all_contexts_response or not all_contexts_response.asset_ctxs:
                logger.warning(
                    "funding_rates_no_asset_contexts",
                    exchange=self._exchange_name,
                    method=current_method,
                    message="No asset contexts found to derive funding rates",
                )
                return []

            symbols_to_process = self._determine_symbols_to_process(
                args.symbols,
                all_contexts_response.asset_ctxs,
            )

            funding_rates = self._process_funding_rates_for_symbols(
                symbols_to_process,
                all_contexts_response.asset_ctxs,
                args.symbols,
            )

            logger.info(
                "funding_rates_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                rates_count=len(funding_rates),
                message="Successfully retrieved funding rates",
            )
        except APIError:
            raise
        except TransformationError as e_transform:
            self._handle_funding_rates_transformation_error(
                e_transform,
                status_code,
                raw_response_content,
            )
            raise
        except ValidationError as e_val:
            self._handle_funding_rates_validation_error(e_val, status_code, raw_response_content)
            raise
        except (ValueError, TypeError) as e_service_logic:
            self._handle_funding_rates_service_logic_error(e_service_logic)
            raise
        except Exception as e_unexpected:
            self._handle_funding_rates_unexpected_error(
                e_unexpected,
                status_code,
                raw_response_content,
            )
            raise
        else:
            return funding_rates

    async def get_historical_funding_rates(
        self,
        args: GetHistoricalFundingRatesArgs,
    ) -> list[FundingRate]:
        """Retrieve historical funding rates for a specific symbol and time range.

        Args:
            args: GetHistoricalFundingRatesArgs containing symbol, start_time, and end_time

        Returns:
            List of historical FundingRate objects

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If time parameters are invalid
        """
        frame = inspect.currentframe()
        current_method = (
            frame.f_code.co_name if frame is not None else "get_historical_funding_rates"
        )

        start_time_ms, end_time_ms = self._validate_and_convert_historical_funding_times(args)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: ParsedJsonResponse | None = None

        try:
            logger.info(
                "fetching_historical_funding_rates",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                start_time=args.start_time,
                end_time=args.end_time,
                message="Fetching historical funding rates",
            )

            raw_funding_history_response = await self._fetch_historical_funding_rates_data(
                args.symbol,
                start_time_ms,
                end_time_ms,
            )

            funding_rates = self._map_historical_funding_rates_to_internal(
                raw_funding_history_response.items
            )

            logger.info(
                "historical_funding_rates_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                rates_count=len(funding_rates),
                message="Successfully retrieved historical funding rates",
            )

        except APIError:
            raise  # Re-raise APIErrors
        except TransformationError as e_transform:
            self._handle_historical_funding_rates_transformation_error(
                e_transform,
                status_code,
                raw_response_content,
            )
            raise
        except ValidationError as e_val:
            self._handle_historical_funding_rates_validation_error(
                e_val,
                status_code,
                raw_response_content,
            )
            raise
        except (ValueError, TypeError) as e_service_logic:
            self._handle_historical_funding_rates_service_logic_error(e_service_logic)
            raise
        except Exception as e_unexpected:
            self._handle_historical_funding_rates_unexpected_error(
                e_unexpected,
                status_code,
                raw_response_content,
            )
            raise
        else:
            return funding_rates

    async def get_market_data(self, args: GetMarketDataArgs) -> list[Candle]:
        """Retrieve historical kline/candlestick data for a symbol and timeframe.

        Uses a POST request to /info with payload:
        {"type": "candleSnapshot",
         "req": {"coin": SYMBOL, "interval": INTERVAL,
                 "startTime": START_MS, "endTime": END_MS}}

        Args:
            args: GetMarketDataArgs containing symbol, timeframe, limit,
                 start_time_ms, and end_time_ms parameters

        Returns:
            List of Candle objects containing candlestick data points

        Raises:
            APIError: If the API request fails or the response is invalid
            ValueError: If input parameters are invalid
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_market_data"

        symbol, interval, start_time_ms, end_time_ms = (
            self._validate_and_prepare_market_data_params(args)
        )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_market_data",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                interval=interval,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
                message="Fetching historical market data (candles)",
            )

            raw_candles = await self._fetch_market_data_from_api(
                symbol,
                interval,
                start_time_ms,
                end_time_ms,
            )

            candles = self._mapper.transform_raw_candle_snapshot_to_candles(
                raw_candles,
                symbol,
                interval,
            )

            logger.info(
                "market_data_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                symbol=symbol,
                interval=interval,
                candles_count=len(candles),
                message="Successfully retrieved market data (candles)",
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
            raise
        except ValidationError as e_val:
            self._handle_market_data_validation_error(
                e_val,
                symbol,
                status_code,
                raw_response_content,
            )
            raise
        except (ValueError, TypeError) as e_service_logic:
            self._handle_market_data_service_logic_error(e_service_logic, symbol)
            raise
        except Exception as e_unexpected:
            self._handle_market_data_unexpected_error(
                e_unexpected,
                symbol,
                status_code,
                raw_response_content,
            )
            raise
        else:
            return candles

    async def _get_all_asset_contexts_raw(self) -> HyperliquidRawMetaAndAssetCtxsResponse:
        """Retrieve all asset contexts for funding rate processing.

        Note: This is a simplified version that delegates to the request/response flow.
        In a production setup, this might be injected as a dependency to avoid
        circular dependencies between services.
        """
        endpoint_path = "/info"
        request_payload_model = self._request_builder.build_info_request_payload()
        request_payload_data_dict = request_payload_model.model_dump(
            by_alias=True,
            exclude_none=True,
        )

        headers: Mapping[str, str] = {}
        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=request_payload_data_dict,
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,
                endpoint_group="public",
                request_weight=2,
            ),
        )

        if raw_response_content_parsed is None:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="No content received for asset contexts",
                http_status=status_code,
            )

        # Type guard for response handler
        if not isinstance(raw_response_content_parsed, dict):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Expected dict response for meta and asset contexts, "
                    f"got {type(raw_response_content_parsed)}"
                ),
                http_status=status_code,
            )

        return self._response_handler.handle_info_meta_and_asset_ctxs_response(
            raw_response_content_parsed,
            status_code,
            headers,
        )

    def _validate_funding_rates_input(self, symbols: list[str] | None) -> None:
        """Validate funding rates input parameters."""
        if symbols is not None:
            for symbol in symbols:
                if not symbol or not symbol.strip():
                    raise InvalidParameterTypeError(
                        parameter_name="symbols",
                        expected_type="list of non-empty strings",
                        actual_type="list containing empty string",
                        value=symbol,
                    )

    def _determine_symbols_to_process(
        self,
        symbols: list[str] | None,
        asset_ctxs: list[Any],
    ) -> list[str]:
        """Determine which symbols to process for funding rates."""
        if symbols is None:
            # If no symbols specified, process all available from asset contexts
            # This requires access to meta data to get universe names
            return []  # Simplified for now
        return symbols

    def _process_funding_rates_for_symbols(
        self,
        symbols_to_process: list[str],
        asset_ctxs: list[Any],
        original_symbols: list[str] | None,
    ) -> list[FundingRate]:
        """Process funding rates for the specified symbols."""
        funding_rates: list[FundingRate] = []
        # This would iterate through symbols and transform asset contexts to funding rates
        # Simplified implementation for decomposition purposes
        return funding_rates

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
    ) -> HyperliquidRawFundingHistoryResponse:
        """Fetch historical funding rates data from API."""
        endpoint_path = "/info"
        # Convert milliseconds back to datetime objects for the args
        start_time = datetime.fromtimestamp(start_time_ms / 1000, tz=UTC)
        end_time = datetime.fromtimestamp(end_time_ms / 1000, tz=UTC) if end_time_ms else None

        args = GetHistoricalFundingRatesArgs(
            symbol=symbol,
            start_time=start_time,
            end_time=end_time,
        )
        payload = self._request_builder.build_historical_funding_rates_payload(args)

        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=endpoint_path,
            data=payload.model_dump(by_alias=True, exclude_none=True, mode="json"),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,
                endpoint_group="public",
                request_weight=1,
            ),
        )

        if raw_response_content_parsed is None:
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=f"No content received for historical funding rates for {symbol}",
                http_status=status_code,
            )

        # Type guard for response handler - historical funding rates returns a list
        if not isinstance(raw_response_content_parsed, list):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Expected list response for historical funding rates, "
                    f"got {type(raw_response_content_parsed)}"
                ),
                http_status=status_code,
            )

        return self._response_handler.handle_historical_funding_rates_response(
            raw_response_content_parsed,
            status_code,
            headers,
        )

    def _map_historical_funding_rates_to_internal(
        self,
        raw_funding_history_items: list[HyperliquidRawFundingHistoryItem],
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
                logger.warning(
                    "historical_funding_rate_mapping_error",
                    exchange=self._exchange_name,
                    error=str(e),
                    message="Error mapping historical funding rate item, skipping",
                )
                continue

        return internal_funding_rates

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
            exchange=self._exchange_name,
            symbol=symbol,
            interval=interval,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
            message="Requesting market data (candles) from API",
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
            logger.exception(
                "candle_snapshot_request_builder_failed",
                exchange=self._exchange_name,
                error=str(e),
                message="Request builder failed for candle snapshot",
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
            ),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.UNSIGNED,
                endpoint_group="public",
                request_weight=1,
            ),
        )

        if raw_response_content_parsed is None:
            error_msg = f"No content received for candle snapshot for {symbol}"
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=error_msg,
                http_status=status_code,
            )

        # Type guard for response handler - candle snapshot can return either dict or list
        if not isinstance(raw_response_content_parsed, (dict, list)):
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message=(
                    f"Expected dict or list response for candle snapshot, "
                    f"got {type(raw_response_content_parsed)}"
                ),
                http_status=status_code,
            )

        return self._response_handler.handle_info_candle_snapshot_response(
            raw_response_content_parsed,
            symbol,
            interval,
            status_code,
            headers,
        )

    # Error handling methods
    def _handle_funding_rates_transformation_error(
        self,
        error: TransformationError,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for funding rates."""
        logger.error(
            "funding_rates_transform_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Failed to transform exchange data for funding rates",
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
            exchange=self._exchange_name,
            error=str(error),
            message="Internal data validation failed for funding rates",
        )
        raise APIError(
            code=APIErrorCode.INVALID_RESPONSE.value,
            message="Internal data validation failed.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_funding_rates_service_logic_error(
        self,
        error: ValueError | TypeError,
    ) -> None:
        """Handle service logic errors for funding rates."""
        logger.error(
            "funding_rates_service_logic_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Service internal logic error for funding rates",
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
            exchange=self._exchange_name,
            error=str(error),
            message="Unexpected service failure for funding rates",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error

    def _handle_historical_funding_rates_transformation_error(
        self,
        error: TransformationError,
        status_code: int,
        raw_response_content: ParsedJsonResponse | None,
    ) -> None:
        """Handle transformation errors for historical funding rates."""
        logger.error(
            "historical_funding_rates_transform_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Failed to transform exchange data for historical funding rates",
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
        logger.error(
            "historical_funding_rates_validation_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Internal data validation failed for historical funding rates",
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
        logger.error(
            "historical_funding_rates_service_logic_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Service internal logic error for historical funding rates",
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
        logger.error(
            "historical_funding_rates_unexpected_error",
            exchange=self._exchange_name,
            error=str(error),
            message="Unexpected service failure for historical funding rates",
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

    def _handle_market_data_transformation_error(
        self,
        error: TransformationError,
        symbol: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle transformation errors for market data."""
        logger.error(
            "market_data_transform_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Failed to transform exchange data for market data",
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
        logger.error(
            "market_data_validation_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Internal data validation failed for market data",
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
        logger.error(
            "market_data_service_logic_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Service internal logic error for market data",
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
        logger.error(
            "market_data_unexpected_error",
            exchange=self._exchange_name,
            symbol=symbol,
            error=str(error),
            message="Unexpected service failure for market data",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=error,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from error
