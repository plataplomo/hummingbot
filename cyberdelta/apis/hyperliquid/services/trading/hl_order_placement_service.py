"""Hyperliquid Order Placement Service.

This service handles all order placement operations for the Hyperliquid exchange,
extracted from the monolithic trading service to improve maintainability and testability.

Focused on:
- Single order placement
- Batch order placement
- Order validation and processing
- Response handling and transformation
"""

import inspect
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import NoReturn

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
    SerializationMode,
)
from cyberdelta.apis.base.trading_execution_domain import OrderExecution
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.exceptions import OrderError, ServiceParameterError, SymbolNotFoundError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import OrderResponseMapperProtocol
from cyberdelta.apis.hyperliquid.services.market_data.hl_order_book_service import (
    HyperliquidOrderBookService,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_base_trading_service import (
    HyperliquidBaseTradingService,
)
from cyberdelta.apis.hyperliquid.services.utils.order_validation import (
    map_time_in_force_to_hyperliquid,
    validate_batch_orders,
    validate_place_order_params,
)
from cyberdelta.apis.hyperliquid.services.utils.status_processing import (
    check_error_response,
    process_exchange_status,
)
from cyberdelta.apis.models.service_args.trading import PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.utils.typing import ParsedJsonResponse, is_dict_response


logger = get_logger(__name__)

# HTTP client signature type
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidOrderPlacementService(HyperliquidBaseTradingService):
    """Focused service for Hyperliquid order placement operations.

    Handles validation, processing, and transformation of order placement requests
    with comprehensive error handling and status processing.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        mapper: OrderResponseMapperProtocol,
        error_mapper: HyperliquidErrorMapper,
        authenticator: IAuthenticator,
        get_asset_index_callable: Callable[[str], Awaitable[int | None]],
        order_book_service: HyperliquidOrderBookService | None = None,  # Optional
        action_endpoint: str = "/exchange",
        exchange_name: str = "hyperliquid",
    ) -> None:
        """Initialize the order placement service.

        Args:
            http_client_requester: HTTP client callable for making requests
            request_builder: Request builder for creating API payloads
            response_handler: Response handler for processing API responses
            mapper: Order response mapper for transforming order placement responses
            error_mapper: Error mapper for handling Hyperliquid-specific errors
            authenticator: Authentication interface for request signing
            get_asset_index_callable: Function to retrieve asset index for symbols
            order_book_service: Optional order book service for market order handling
            action_endpoint: API endpoint for exchange actions
            exchange_name: Name of the exchange for logging
        """
        super().__init__(exchange_name)
        self._http_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._mapper = mapper
        self._error_mapper = error_mapper
        self._authenticator = authenticator
        self._get_asset_index_callable = get_asset_index_callable
        self._order_book_service = order_book_service
        self._action_endpoint = action_endpoint
        self._exchange_name = exchange_name

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a single order and return the internal Order model.

        Args:
            args: Order placement parameters

        Returns:
            Order object with order details and status

        Note:
            This method delegates to _place_orders_core which handles all exceptions.
            See _place_orders_core for possible exceptions that may be raised.
        """
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        # Use the unified core method with a single order
        orders = await self._place_orders_core([args], current_method)
        return orders[0]

    async def _place_orders_core(
        self,
        orders: list[PlaceOrderArgs],
        current_method: str,
    ) -> list[Order]:
        """Core unified method for placing single or batch orders.

        Args:
            orders: List of order placement parameters
            current_method: Name of calling method for error context

        Returns:
            List of placed Order objects
        """
        self._validate_orders_list(orders, current_method)

        # Handle single market orders with special logic (backwards compatibility)
        if (
            len(orders) == 1
            and orders[0].order_type == OrderType.MARKET
            and self._order_book_service is not None
            and orders[0].price is None
        ):
            # Execute thin market order if order book service is available
            return [await self._execute_thin_market_order(orders[0], current_method)]
            # Otherwise fall through to normal processing (requires price)

        try:
            orders_with_indices, tif_mapping = await self._prepare_order_data(orders)
            place_order_payload = self._build_order_payload(
                orders,
                orders_with_indices,
                tif_mapping,
            )

            raw_exchange_response, http_status = await self._place_order_raw(place_order_payload)

            return await self._process_order_response(
                raw_exchange_response,
                http_status,
                orders,
                orders_with_indices,
            )

        except Exception as error:
            # Re-raise OrderError as it's a legitimate business logic error
            if isinstance(error, OrderError):
                raise
            api_error = self._handle_service_error(error, current_method, "order placement")
            raise api_error from error

    async def _place_order_raw(
        self,
        request_payload_model: HyperliquidApiPlaceOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Execute the raw order placement request.

        Args:
            request_payload_model: Validated request payload

        Returns:
            Tuple of (raw exchange response, HTTP status code)

        Raises:
            APIError: If the response is not a dictionary or if validation fails
        """
        request_config = RequestConfiguration(
            auth_mode=RequestAuthMode.SIGNED,
            serialization_mode=SerializationMode.EXPLICIT_NULL,
        )

        raw_content, http_status, _ = await self._http_requester(
            method="POST",
            endpoint=self._action_endpoint,
            data=request_payload_model,
            request_config=request_config,
        )

        if not is_dict_response(raw_content):
            error_msg = f"Exchange action ({request_payload_model.type}) returned invalid content"
            raise APIError(error_msg, APIErrorCode.INVALID_RESPONSE.value)

        exchange_response = self._response_handler.handle_exchange_response(
            raw_content,
            http_status,
        )

        return exchange_response, http_status

    async def _prepare_order_data(
        self,
        orders: list[PlaceOrderArgs],
    ) -> tuple[list[tuple[PlaceOrderArgs, int]], dict[str, str | None]]:
        """Prepare order data with asset indices and TIF mappings.

        Args:
            orders: List of order placement parameters

        Returns:
            Tuple of (orders with asset indices, time-in-force mapping)

        Raises:
            SymbolNotFoundError: If any symbol cannot be found or has no asset index
        """
        orders_with_indices: list[tuple[PlaceOrderArgs, int]] = []
        tif_mapping: dict[str, str | None] = {}

        for order_args in orders:
            asset_index = await self._get_asset_index_callable(order_args.symbol)
            if asset_index is None:
                raise SymbolNotFoundError(symbol=order_args.symbol, exchange="Hyperliquid")

            orders_with_indices.append((order_args, asset_index))

            # Build time-in-force mapping
            if order_args.time_in_force:
                tif_key = f"{order_args.symbol}_{order_args.time_in_force.value}"
                tif_mapping[tif_key] = map_time_in_force_to_hyperliquid(order_args.time_in_force)

        return orders_with_indices, tif_mapping

    def _build_order_payload(
        self,
        orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
        tif_mapping: dict[str, str | None],
    ) -> HyperliquidApiPlaceOrderRequest:
        """Build the order placement request payload.

        Args:
            orders: Original order parameters
            orders_with_indices: Orders with their indices
            tif_mapping: Time-in-force mapping

        Returns:
            Validated request payload model
        """
        return self._request_builder.build_place_order_request(
            orders,
            orders_with_indices,
            tif_mapping,
        )

    async def _process_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        orders: list[PlaceOrderArgs],
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
    ) -> list[Order]:
        """Process the order placement response and transform to internal models.

        Args:
            raw_exchange_response: Raw response from exchange
            http_status: HTTP status code
            orders: Original order parameters
            orders_with_indices: Orders with their indices

        Returns:
            List of processed Order objects

        Raises:
            APIError: If response data is empty or invalid
        """
        # Check for exchange-level errors
        check_error_response(raw_exchange_response, http_status, self._error_mapper)

        # Process response data
        if raw_exchange_response.response is None:
            raise APIError(
                message="Empty response data from order placement",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=http_status,
            )

        # Handle batch vs single order response
        if len(orders) == 1:
            return await self._process_single_order_response(
                raw_exchange_response,
                orders[0],
            )
        return await self._process_batch_order_response(
            raw_exchange_response,
            orders_with_indices,
        )

    async def _process_single_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        order_args: PlaceOrderArgs,
    ) -> list[Order]:
        """Process response for a single order placement.

        Args:
            raw_exchange_response: Raw response from exchange
            order_args: Original order parameters

        Returns:
            List containing single processed Order object

        Raises:
            APIError: If no status data is present in the response
        """
        action_description = f"place order {order_args.symbol}"

        # Extract the actual status from the nested response structure
        response_data = raw_exchange_response.response_data
        if not response_data or not response_data.statuses:
            raise APIError(
                message=f"No status data in response for {action_description}",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        processed_status = process_exchange_status(
            response_data.statuses[0],
            action_description,
        )

        # Transform to internal Order model
        order = await self._mapper.map_place_order_response_to_order(
            processed_status,
            order_args,
            datetime.now(UTC),
        )

        return [order]

    async def _process_batch_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        orders_with_indices: list[tuple[PlaceOrderArgs, int]],
    ) -> list[Order]:
        """Process response for batch order placement.

        Args:
            raw_exchange_response: Raw response from exchange
            orders_with_indices: Orders with their indices

        Returns:
            List of processed Order objects

        Raises:
            APIError: If no status data in response or response count mismatch
        """
        # Extract the actual status list from the nested response structure
        response_data = raw_exchange_response.response_data
        if not response_data or not response_data.statuses:
            raise APIError(
                message="No status data in response for batch order placement",
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        response_list = response_data.statuses

        if len(response_list) != len(orders_with_indices):
            raise APIError(
                message=(
                    f"Response count mismatch: expected {len(orders_with_indices)}, "
                    f"got {len(response_list)}"
                ),
                code=APIErrorCode.INVALID_RESPONSE.value,
            )

        orders: list[Order] = []
        timestamp = datetime.now(UTC)

        for i, (order_args, _original_index) in enumerate(orders_with_indices):
            action_description = f"place batch order {i} ({order_args.symbol})"

            processed_status = process_exchange_status(
                response_list[i],
                action_description,
            )

            # Transform to internal Order model
            order = await self._mapper.map_place_order_response_to_order(
                processed_status,
                order_args,
                timestamp,
            )

            orders.append(order)

        return orders

    def _validate_orders_list(self, orders: list[PlaceOrderArgs], current_method: str) -> None:
        """Validate the orders list and individual order parameters.

        Args:
            orders: List of order parameters to validate
            current_method: Name of calling method for error context

        Raises:
            ServiceParameterError: If any order fails validation

        Note:
            The underlying validate_batch_orders may raise ValueError for batch validation.
        """
        # Use utility function for batch validation
        validate_batch_orders(orders, current_method)

        # Validate each order individually
        for i, order in enumerate(orders):
            try:
                validate_place_order_params(order, current_method)
            except ValueError as e:
                raise ServiceParameterError(
                    parameter=f"orders[{i}]",
                    issue="failed validation",
                    value=order,
                    exchange="hyperliquid",
                    operation="order placement",
                    suggestion=str(e),
                ) from e

    async def _execute_thin_market_order(
        self,
        args: PlaceOrderArgs,
        current_method: str,
    ) -> Order:
        """Execute thin market order implementation - WARNING: MISSING RISK CONTROLS.

        This is a backwards compatibility implementation that converts market orders to
        aggressive IOC limit orders. It bypasses sophisticated risk management like
        slippage protection, liquidity validation, and price deviation checks.

        For proper market order execution with full risk controls, use:
        cyberdelta.core.execution.orders.MarketOrder

        Args:
            args: Market order arguments to convert
            current_method: Name of calling method for error context

        Returns:
            Executed order result

        Raises:
            APIError: If order book fetch or order execution fails
        """
        try:
            # Get order book using order book service
            if self._order_book_service is None:
                self._raise_order_book_service_missing_error()

            order_book = await self._order_book_service.get_order_book(args.symbol)

            if order_book is None:
                self._raise_order_book_fetch_error(args.symbol)

            # Extract aggressive price - use multiple levels if needed to ensure fill
            # WARNING: This uses up to 3 price levels to ensure IOC orders fill
            if args.side == OrderSide.BUY:
                if not order_book.asks:
                    self._raise_no_ask_levels_error(args.symbol)
                # Use the 3rd ask level (or best available) to ensure aggressive fill
                ask_index = min(2, len(order_book.asks) - 1)  # Index 2 = 3rd level
                aggressive_price = order_book.asks[ask_index][0]
            else:  # SELL
                if not order_book.bids:
                    self._raise_no_bid_levels_error(args.symbol)
                # Use the 3rd bid level (or best available) to ensure aggressive fill
                bid_index = min(2, len(order_book.bids) - 1)  # Index 2 = 3rd level
                aggressive_price = order_book.bids[bid_index][0]

            # Convert to IOC limit order
            limit_args = PlaceOrderArgs(
                symbol=args.symbol,
                side=args.side,
                order_type=OrderType.LIMIT,  # Convert to limit
                quantity=args.quantity,
                price=aggressive_price,  # Aggressive market-taking price
                time_in_force=TimeInForce.IOC,  # Immediate or cancel
                client_order_id=args.client_order_id,
                execution=OrderExecution(),  # Default: ANY liquidity, OPEN_OR_INCREASE position
            )

            # Log the conversion
            logger.info(
                "market_order_converted_to_ioc_limit",
                method=current_method,
                symbol=args.symbol,
                side=args.side.value,
                quantity=str(args.quantity),
                aggressive_price=str(aggressive_price),
                message="Converting market order to aggressive IOC limit order",
            )

            # Recursive call with limit order (no circular dependency because it's now LIMIT)
            return await self.place_order(limit_args)

        except APIError:
            raise
        except Exception as error:
            api_error = self._handle_service_error(
                error,
                current_method,
                f"thin market order for {args.symbol}",
            )
            raise api_error from error

    def _raise_order_book_service_missing_error(self) -> NoReturn:
        """Raise APIError when order book service is missing.

        Raises:
            APIError: Always raises with appropriate error message
        """
        raise APIError(
            message="Order book service is required for market orders",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    def _raise_order_book_fetch_error(self, symbol: str) -> NoReturn:
        """Raise APIError when order book fetch fails.

        Args:
            symbol: Trading symbol that failed to fetch

        Raises:
            APIError: Always raises with appropriate error message
        """
        raise APIError(
            message=f"Failed to get order book for {symbol}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    def _raise_no_ask_levels_error(self, symbol: str) -> NoReturn:
        """Raise APIError when no ask levels are available.

        Args:
            symbol: Trading symbol with no ask levels

        Raises:
            APIError: Always raises with appropriate error message
        """
        raise APIError(
            message=f"No ask levels available for market buy of {symbol}",
            code=APIErrorCode.ORDER_REJECTED.value,
        )

    def _raise_no_bid_levels_error(self, symbol: str) -> NoReturn:
        """Raise APIError when no bid levels are available.

        Args:
            symbol: Trading symbol with no bid levels

        Raises:
            APIError: Always raises with appropriate error message
        """
        raise APIError(
            message=f"No bid levels available for market sell of {symbol}",
            code=APIErrorCode.ORDER_REJECTED.value,
        )
