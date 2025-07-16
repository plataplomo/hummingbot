"""Hyperliquid Order Query Service.

This service handles all order query operations for the Hyperliquid exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order retrieval by ID
- Open orders retrieval (all or by symbol)
- Order status queries
- Historical order data
"""

import inspect
from collections.abc import Awaitable, Callable, Mapping
from typing import NoReturn

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.common import APIError, APIErrorCode, TransformationError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOrderStatusResponse,
    HyperliquidRawSimpleOpenOrder,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderMapperProtocol
from cyberdelta.apis.hyperliquid.services.trading.hl_base_trading_service import (
    HyperliquidBaseTradingService,
)
from cyberdelta.apis.models.service_args_models import (
    GetAllOpenOrdersArgs,
    GetOpenOrdersArgs,
    GetOrderArgs,
    HyperliquidGetOrderStatusArgs,
)
from cyberdelta.apis.utils.response_validation import ensure_dict_response, ensure_list_response
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# HTTP client signature type
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderQueryService(HyperliquidBaseTradingService):
    """Focused service for Hyperliquid order query operations.

    Handles retrieval and processing of order data with comprehensive
    error handling and data transformation.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        mapper: OrderMapperProtocol,
        error_mapper: HyperliquidErrorMapper,
        authenticator: IAuthenticator,
        wallet_address: str,
        info_endpoint: str = "/info",
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the order query service.

        Args:
            http_client_requester: HTTP client callable for making requests
            request_builder: Request builder for creating API payloads
            response_handler: Response handler for processing API responses
            mapper: Order mapper for transforming order data to internal models
            error_mapper: Error mapper for handling Hyperliquid-specific errors
            authenticator: Authentication interface for request signing
            wallet_address: Hyperliquid wallet address for user-specific requests
            info_endpoint: API endpoint for info requests
            exchange_name: Name of the exchange for logging
        """
        super().__init__(exchange_name)
        self._http_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._error_mapper = error_mapper
        self._authenticator = authenticator
        self._wallet_address = wallet_address
        self._info_endpoint = info_endpoint
        self._exchange_name = exchange_name

    @staticmethod
    def _raise_none_response_error(http_status: int) -> None:
        """Raise error for None response after validation."""
        raise APIError(
            message="Received None response after validation",
            code=APIErrorCode.INVALID_RESPONSE.value,
            http_status=http_status,
        )

    @staticmethod
    def _raise_internal_logic_error() -> NoReturn:
        """Raise error for internal logic error."""
        msg = "Internal error: raw_content is None after validation"
        raise RuntimeError(msg)

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Retrieve a specific order by ID for a given symbol.

        Args:
            args: Arguments containing order_id and symbol for order retrieval

        Returns:
            Order object if found, None otherwise

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If order_id is not a valid integer
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Convert and validate order_id
            try:
                order_id_int = int(args.order_id)
            except (ValueError, TypeError) as e:
                error_msg = (
                    f"[{current_method}] 'order_id' must be a valid integer, got '{args.order_id}'"
                )
                raise ValueError(error_msg) from e

            # Get raw order data
            raw_order_status_response = await self._get_order_status_raw(order_id_int)
            if raw_order_status_response is None:
                return None

            # Convert order status response to historical order format for mapper
            raw_historical_order = self._convert_order_status_to_historical_order(
                raw_order_status_response
            )

            # Transform to internal order model
            return self._mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order,
                trigger=None,
            )

        except APIError:
            # Re-raise APIErrors from downstream services
            raise
        except TransformationError as e:
            logger.exception(
                "order_transform_error",
                message="[%s] %s: Failed to transform exchange data for order %s: %s",
                message_args=(self._exchange_name, current_method, args.order_id, e),
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e
        except (ValidationError, ValueError, TypeError, Exception) as e:
            raise self._handle_service_error(
                e, current_method, f"order {args.order_id}", status_code, raw_response_content
            ) from e

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol.

        Args:
            symbol: Optional symbol filter for orders (case-insensitive)

        Returns:
            List of Order objects representing open orders

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If symbol is provided but empty
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        # Validate symbol parameter
        if symbol is not None and not symbol:
            error_msg = f"[{current_method}] 'symbol' must be a non-empty string when provided."
            raise ValueError(error_msg)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Get raw open orders
            raw_open_orders = await self._get_open_orders_raw()

            # Process and filter orders
            internal_orders = self._process_raw_orders_to_internal(raw_open_orders, symbol)

            logger.info(
                "open_orders_retrieved",
                message="[%s] Retrieved %s open orders (symbol filter: %s)",
                message_args=(self._exchange_name, len(internal_orders), symbol),
            )

        except APIError:
            # Re-raise APIErrors from downstream services
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            return self._handle_get_open_orders_error(
                e,
                current_method,
                status_code,
                raw_response_content,
                symbol,
            )
        else:
            return internal_orders

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Retrieve all open orders across all symbols.

        Args:
            args: Arguments for getting all open orders

        Returns:
            List of Order objects representing all open orders

        Raises:
            APIError: If API request fails or data transformation fails
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_open_orders"

        try:
            # Use get_open_orders without symbol filter to get all orders
            return await self.get_open_orders(symbol=None)

        except Exception as e:
            raise self._handle_service_error(e, current_method, "all open orders", 0, None) from e

    async def _get_order_status_raw(
        self, order_id: int
    ) -> HyperliquidRawOrderStatusResponse | None:
        """Get raw order status data from the API.

        Args:
            order_id: Integer order ID to query

        Returns:
            Raw order status response or None if not found

        Raises:
            APIError: If API request fails
        """
        try:
            # Build request payload
            request_payload = self._request_builder.build_order_status_payload(
                HyperliquidGetOrderStatusArgs(
                    wallet_address=self._wallet_address, order_id=order_id
                )
            )

            # Make API request
            raw_content, http_status, _headers = await self._http_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                ),
            )

            # Validate response structure
            response_dict = ensure_dict_response(
                raw_content, f"order status {order_id}", http_status
            )

            # Handle the response using response handler
            raw_historical_response = self._response_handler.handle_info_order_status_response(
                response_dict,
                http_status,
            )

            # Check if order was found
            if not raw_historical_response or not raw_historical_response.order:
                logger.debug(
                    "order_not_found",
                    order_id=order_id,
                    message="Order not found in order status response",
                )
                return None

        except APIError:
            raise
        except Exception as e:
            logger.exception(
                "order_status_request_failed",
                order_id=order_id,
                error=str(e),
                message="Failed to get order status",
            )
            raise APIError(
                message=f"Failed to retrieve order status for order {order_id}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        else:
            return raw_historical_response

    async def _get_open_orders_raw(self) -> list[HyperliquidRawSimpleOpenOrder]:
        """Get raw open orders data from the API.

        Returns:
            List of raw open order objects

        Raises:
            APIError: If API request fails
        """
        try:
            # Build request payload for open orders
            request_payload = self._request_builder.build_open_orders_payload(
                GetOpenOrdersArgs(wallet_address=self._wallet_address)
            )

            # Make API request
            raw_content, http_status, _headers = await self._http_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload,
                request_config=RequestConfiguration(
                    auth_mode=RequestAuthMode.UNSIGNED,
                ),
            )

            # Validate response structure
            ensure_list_response(raw_content, "open orders", http_status)

            # Handle the response using response handler
            # The response handler expects a list, not a dict
            # After ensure_list_response, raw_content should not be None
            if raw_content is None:
                self._raise_none_response_error(http_status)

            # At this point we know raw_content is not None due to the check above
            # If somehow it's still None, this is a logic error
            if raw_content is None:
                self._raise_internal_logic_error()

            # At this point raw_content is guaranteed to be not None
            # Type narrowing for mypy - raw_content is ParsedJsonResponse (not None)
            raw_open_orders_response = self._response_handler.handle_info_open_orders_response(
                raw_content,  # Type is now narrowed to exclude None
                http_status,
            )

        except APIError:
            raise
        except Exception as e:
            logger.exception(
                "open_orders_request_failed", error=str(e), message="Failed to get open orders"
            )
            raise APIError(
                message="Failed to retrieve open orders",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e,
            ) from e
        else:
            return raw_open_orders_response.root if raw_open_orders_response else []

    def _process_raw_orders_to_internal(
        self,
        raw_orders: list[HyperliquidRawSimpleOpenOrder],
        symbol_filter: str | None = None,
    ) -> list[Order]:
        """Process raw orders to internal Order models with optional symbol filtering.

        Args:
            raw_orders: List of raw open order objects
            symbol_filter: Optional symbol to filter by (case-insensitive)

        Returns:
            List of processed Order objects
        """
        if not raw_orders:
            return []

        internal_orders: list[Order] = []

        for raw_order in raw_orders:
            try:
                # Transform to internal order
                internal_order = self._mapper.transform_raw_simple_open_order_to_internal(raw_order)

                # Apply symbol filter if specified
                if symbol_filter is None or (
                    internal_order.symbol and internal_order.symbol.lower() == symbol_filter.lower()
                ):
                    internal_orders.append(internal_order)

            except (
                TransformationError,
                ValidationError,
                ValueError,
                TypeError,
                AttributeError,
            ) as e:
                logger.warning(
                    "order_transformation_skipped",
                    raw_order_id=getattr(raw_order, "oid", "unknown"),
                    error=str(e),
                    message="Skipping order due to transformation error",
                )
                continue

        return internal_orders

    def _convert_order_status_to_historical_order(
        self, order_status_response: HyperliquidRawOrderStatusResponse
    ) -> HyperliquidRawHistoricalOrder:
        """Convert order status response to historical order format for mapper.

        Args:
            order_status_response: Order status response from the API

        Returns:
            HyperliquidRawHistoricalOrder: Converted order in historical format
        """
        order_data = order_status_response.order.order
        order_status = order_status_response.order.status
        status_timestamp = order_status_response.order.status_timestamp

        # Create HyperliquidRawHistoricalOrder by flattening the nested structure
        # and adding the status and status_timestamp at the top level
        return HyperliquidRawHistoricalOrder(
            oid=order_data.oid,
            cloid=order_data.cloid,
            coin=order_data.coin,
            side=order_data.side,
            limitPx=order_data.limit_px,
            sz=order_data.sz,
            timestamp=order_data.timestamp,
            orderType=order_data.order_type,
            reduceOnly=order_data.reduce_only,
            origSz=order_data.orig_sz,
            tif=order_data.tif,
            triggerCondition=order_data.trigger_condition,
            isTrigger=order_data.is_trigger,
            triggerPx=order_data.trigger_px,
            children=list(order_data.children) if order_data.children else None,
            isPositionTpsl=order_data.is_position_tpsl,
            status=order_status,
            statusTimestamp=status_timestamp,
        )

    def _handle_get_open_orders_error(
        self,
        error: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
        symbol_filter: str | None,
    ) -> list[Order]:
        """Handle errors specific to get_open_orders operation.

        Args:
            error: The original exception
            current_method: Name of the method where error occurred
            status_code: HTTP status code if available
            raw_response_content: Raw response content if available
            symbol_filter: Symbol filter that was applied

        Returns:
            Empty list (fallback behavior)

        Raises:
            APIError: For non-recoverable errors
        """
        context = f"open orders (symbol: {symbol_filter})"

        # Check if this is a recoverable error (e.g., empty response)
        if isinstance(error, (TransformationError, ValidationError)):
            logger.warning(
                "get_open_orders_recoverable_error",
                method=current_method,
                symbol=symbol_filter,
                error=str(error),
                message="Recoverable error in get_open_orders, returning empty list",
            )
            return []

        # For other errors, convert to APIError and raise
        api_error = self._handle_service_error(
            error, current_method, context, status_code, raw_response_content
        )
        raise api_error
