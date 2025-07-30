"""Backpack Transaction History Service.

This service handles all transaction history operations for the Backpack exchange,
extracted from the monolithic account service to improve maintainability and testability.

Focused on:
- Order history retrieval with flexible filtering
- Trade history (fills) retrieval and processing
- Historical data validation and transformation
- Time-based and symbol-based filtering
"""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from pydantic import ValidationError

from cyberdelta.apis.backpack.mappers import BackpackTransactionMapper
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrderResponse
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.models.service_args.trading import GetOrderHistoryArgs, GetTradeHistoryArgs
from cyberdelta.apis.utils import ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order, Trade
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackTransactionHistoryService:
    """Focused service for Backpack transaction history operations.

    Handles retrieval of historical order and trade data with comprehensive
    filtering options and error handling.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackTradingRequestBuilder,
        response_handler: BackpackTradingResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str = "backpack",
        # NEW: Optional dependency injection for mapper
        mapper: BackpackTransactionMapper | None = None,
    ) -> None:
        """Initialize the transaction history service.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            mapper: Optional transaction mapper instance for dependency injection
                (creates default if not provided)
        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        # Provide sensible default if mapper not injected
        self._mapper = mapper or BackpackTransactionMapper()

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieves historical order data with flexible filtering options.

        Supports filtering by symbol, time range, limit, and specific order IDs.

        Args:
            args: Order history query parameters including symbol, time range, and filters

        Returns:
            List of historical Order objects

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order_history"

        # Initialize context for error handling
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            logger.info(
                "fetching_order_history",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                limit=args.limit,
                order_id=args.order_id,
                client_order_id=args.client_order_id,
                has_time_filter=args.start_time is not None or args.end_time is not None,
                message="Fetching historical order data",
            )

            # Execute order history request
            raw_data, status_code = await self._execute_order_history_request(args)

            # raw_data is guaranteed to be non-None due to ensure_list_response validation
            raw_response_content = str(raw_data)

            # Process and transform response
            internal_orders = self._process_order_history_response(raw_data, args, status_code)

            logger.info(
                "order_history_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                order_count=len(internal_orders),
                symbol=args.symbol,
                message="Successfully retrieved order history",
            )

        except APIError:
            # Re-raise APIErrors from lower layers
            raise
        except TransformationError as e:
            logger.exception(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform exchange data for order history",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        except ValidationError as e:
            logger.exception(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Internal data validation failed for order history",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        except (ValueError, TypeError) as e:
            # Service internal logic errors (input validation handled by Pydantic)
            logger.exception(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service internal logic error for order history",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        except Exception as e:
            logger.exception(
                "unexpected_service_failure",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Unexpected service failure for order history",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        else:
            return internal_orders

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieves historical trade data (fills) with filtering options.

        Fetches trade execution history from the fills endpoint with symbol
        and limit filtering capabilities.

        Args:
            args: Trade history query parameters including symbol and limit

        Returns:
            List of historical Trade objects

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_trade_history"

        raw_response_content: str | None = None
        status_code: int = 0

        try:
            logger.info(
                "fetching_trade_history",
                exchange=self._exchange_name,
                method=current_method,
                symbol=args.symbol,
                limit=args.limit,
                message="Fetching historical trade data (fills)",
            )

            # Execute trade history request
            raw_data, status_code = await self._execute_trade_history_request(args)
            raw_response_content = str(raw_data)

            # Process and transform response
            internal_trades = self._process_trade_history_response(raw_data, args, status_code)

            logger.info(
                "trade_history_retrieved",
                exchange=self._exchange_name,
                method=current_method,
                trade_count=len(internal_trades),
                symbol=args.symbol,
                message="Successfully retrieved trade history",
            )

        except APIError:
            raise
        except Exception as e:
            self._handle_trade_history_exceptions(
                e,
                current_method,
                status_code,
                raw_response_content,
            )
            # Defensive check - should never reach here
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected error in get_trade_history",
                original_exception=e,
                http_status=status_code,
            ) from e
        else:
            return internal_trades

    async def _execute_order_history_request(
        self,
        args: GetOrderHistoryArgs,
    ) -> tuple[ParsedJsonResponse, int]:
        """Execute the order history API request.

        Args:
            args: Order history query parameters

        Returns:
            Tuple of validated response data and status code
        """
        endpoint_path = "/wapi/v1/history/orders"

        # Convert datetime to milliseconds for API
        start_time_ms = int(args.start_time.timestamp() * 1000) if args.start_time else None
        end_time_ms = int(args.end_time.timestamp() * 1000) if args.end_time else None

        # Build request parameters
        params = self._request_builder.build_get_order_history_params(
            symbol=args.symbol,  # Pass Symbol object directly
            start_time=start_time_ms,
            end_time=end_time_ms,
            limit=args.limit or 100,
            order_id=args.order_id,
            client_id=args.client_order_id,
        )

        logger.debug(
            "requesting_order_history",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting order history from endpoint",
        )

        # Execute API request
        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        logger.debug(
            "raw_order_history_response",
            exchange=self._exchange_name,
            status_code=status_code,
            message="Raw order history response received",
        )

        # Validate response format
        validated_data = ensure_list_response(raw_data, "order history", status_code)
        return validated_data, status_code

    async def _execute_trade_history_request(
        self,
        args: GetTradeHistoryArgs,
    ) -> tuple[ParsedJsonResponse, int]:
        """Execute the trade history API request.

        Args:
            args: Trade history query parameters

        Returns:
            Tuple of validated response data and status code
        """
        endpoint_path = "/wapi/v1/history/fills"

        # Build request parameters (using fills endpoint)
        params = self._request_builder.build_get_trade_history_params(
            symbol=args.symbol,  # Pass Symbol object directly
            limit=args.limit or 100,
            start_time=None,  # Not in current service signature
            end_time=None,  # Not in current service signature
            from_id=None,  # Not in current service signature
        )

        logger.debug(
            "requesting_trade_history",
            exchange=self._exchange_name,
            endpoint_path=endpoint_path,
            params=params,
            message="Requesting trade history from endpoint",
        )

        # Execute API request
        raw_data, status_code, _ = await self._http_client_requester(
            method="GET",
            endpoint=endpoint_path,
            params=params.model_dump(by_alias=True, exclude_none=True),
            request_config=RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,
                endpoint_group="private",
                request_weight=1,
            ),
        )

        logger.debug(
            "raw_trade_history_response",
            exchange=self._exchange_name,
            status_code=status_code,
            message="Raw trade history response received",
        )

        # Validate response format
        validated_data = ensure_list_response(raw_data, "trade history", status_code)
        return validated_data, status_code

    def _process_order_history_response(
        self,
        raw_data: ParsedJsonResponse,
        args: GetOrderHistoryArgs,
        status_code: int,
    ) -> list[Order]:
        """Process and transform order history response.

        Args:
            raw_data: Raw response data from API
            args: Original order history arguments
            status_code: HTTP status code

        Returns:
            List of transformed Order objects
        """
        # Handle response through response handler
        raw_orders_list: list[BackpackRawOrderResponse] = (
            self._response_handler.handle_get_order_history_response(
                raw_data,
                args.symbol,  # Pass Symbol object directly
                status_code,
            )
        )

        # Transform raw orders to internal models
        internal_orders: list[Order] = []
        for raw_order_model in raw_orders_list:
            try:
                internal_order = self._mapper.transform_raw_order_to_internal(raw_order_model)
                internal_orders.append(internal_order)
            except (ValidationError, ValueError) as e:
                # Log and skip problematic orders rather than failing entirely
                logger.warning(
                    "skipping_order_mapping",
                    exchange=self._exchange_name,
                    order_id=raw_order_model.clientId or raw_order_model.id,
                    error=str(e),
                    message="Skipping order mapping due to error",
                )
                continue

        logger.debug(
            "order_history_mapped",
            exchange=self._exchange_name,
            order_count=len(internal_orders),
            total_raw_orders=len(raw_orders_list),
            message="Successfully mapped internal order history",
        )

        return internal_orders

    def _process_trade_history_response(
        self,
        raw_data: ParsedJsonResponse,
        args: GetTradeHistoryArgs,
        status_code: int,
    ) -> list[Trade]:
        """Process and transform trade history response.

        Since we're using /wapi/v1/history/fills endpoint, we get BackpackRawFill format.

        Args:
            raw_data: Raw response data from API
            args: Original trade history arguments
            status_code: HTTP status code

        Returns:
            List of transformed Trade objects
        """
        # Use fills handler since we're calling /wapi/v1/history/fills
        raw_fills_list = self._response_handler.handle_get_fills_response(
            raw_data,
            args.symbol,  # Pass Symbol object directly
            status_code,
        )

        # Transform raw fills to internal trade models
        internal_trades: list[Trade] = []
        for raw_fill_model in raw_fills_list:
            try:
                trade = self._mapper.transform_raw_fill_to_internal(raw_fill_model)
                if trade is not None:  # Mapper can return None
                    internal_trades.append(trade)
            except (ValidationError, ValueError) as e:
                # Log and skip problematic fills rather than failing entirely
                logger.warning(
                    "skipping_fill_mapping",
                    exchange=self._exchange_name,
                    order_id=raw_fill_model.order_id,
                    error=str(e),
                    message="Skipping fill mapping due to error",
                )
                continue

        logger.debug(
            "trades_mapped_from_fills",
            exchange=self._exchange_name,
            trade_count=len(internal_trades),
            total_raw_fills=len(raw_fills_list),
            message="Successfully mapped internal trades from fills",
        )

        return internal_trades

    def _handle_trade_history_exceptions(
        self,
        e: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> None:
        """Handle various trade history-related exceptions.

        Args:
            e: The exception that occurred
            current_method: Name of the calling method
            status_code: HTTP status code if available
            raw_response_content: Raw response content for context

        Raises:
            APIError: Wrapped exception with appropriate error code
        """
        if isinstance(e, APIError):
            raise e

        if isinstance(e, TransformationError):
            logger.error(
                "transform_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Failed to transform exchange data for trade history",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, ValidationError):
            logger.error(
                "validation_failed",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Internal data validation failed for trade history",
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        if isinstance(e, (ValueError, TypeError)):
            error_msg = str(e)
            # Re-raise validation errors from our own parameter validation
            if current_method in error_msg and "limit" in error_msg:
                raise e

            logger.error(
                "service_logic_error",
                exchange=self._exchange_name,
                method=current_method,
                error=str(e),
                message="Service internal logic error for trade history",
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Service internal logic error.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e

        # Default case for unexpected exceptions
        logger.error(
            "unexpected_service_failure",
            exchange=self._exchange_name,
            method=current_method,
            error=str(e),
            message="Unexpected service failure for trade history",
        )
        raise APIError(
            code=APIErrorCode.UNKNOWN.value,
            message="Unexpected service failure.",
            original_exception=e,
            http_status=status_code if status_code != 0 else None,
            exchange_message=raw_response_content,
        ) from e
