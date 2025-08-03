"""Backpack Trading Service - Composite service combining trading operations.

This service combines all trading-related operations from the decomposed services
to provide a unified interface for trading management, following Hyperliquid's
composite pattern.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.backpack.mappers import BackpackOrderMapper
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.backpack.services.trading.bp_batch_order_service import (
    BackpackBatchOrderService,
)
from cyberdelta.apis.backpack.services.trading.bp_order_cancellation_service import (
    BackpackOrderCancellationService,
)
from cyberdelta.apis.backpack.services.trading.bp_order_placement_service import (
    BackpackOrderPlacementService,
)
from cyberdelta.apis.backpack.services.trading.bp_order_query_service import (
    BackpackOrderQueryService,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.models import Order
from cyberdelta.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackTradingService:
    """Composite service for Backpack trading operations.

    Combines all trading-related decomposed services to provide a unified interface
    for trading operations including order placement, cancellation, and queries.

    This follows Hyperliquid's composite pattern where:
    - The composite owns instances of decomposed services
    - All operations are delegated to the appropriate decomposed service
    - Optional dependency injection is supported for mappers
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackTradingRequestBuilder,
        response_handler: BackpackTradingResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        # Optional mapper injection for testability
        order_mapper: BackpackOrderMapper | None = None,
    ) -> None:
        """Initialize the Backpack trading service.

        Creates and configures all decomposed service components.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            order_mapper: Optional order mapper instance for dependency injection
        """
        # Store core parameters
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name

        # Create mapper if not provided (following Hyperliquid pattern)
        self._order_mapper = order_mapper or BackpackOrderMapper()

        # Initialize decomposed service components
        self._order_placement_service = BackpackOrderPlacementService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        self._order_cancellation_service = BackpackOrderCancellationService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        self._order_query_service = BackpackOrderQueryService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        self._batch_order_service = BackpackBatchOrderService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        logger.info(
            "trading_service_initialized",
            exchange=exchange_name,
            mapper=type(self._order_mapper).__name__,
            message="Backpack trading service initialized with decomposed services",
        )

    # Order Placement Operations

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a new order.

        Delegates to the order placement service component.

        Args:
            args: Order placement parameters

        Returns:
            Order object with placement details
        """
        return await self._order_placement_service.place_order(args)

    # Order Cancellation Operations

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel a single order.

        Delegates to the order cancellation service component.

        Args:
            args: Order cancellation parameters

        Returns:
            CancelOrderResult with cancellation details
        """
        return await self._order_cancellation_service.cancel_order(args)

    async def cancel_all_orders(self, symbol: Symbol | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders.

        Delegates to the batch order service component.

        Args:
            symbol: Optional symbol to filter cancellations

        Returns:
            List of CancelOrderResult objects
        """
        return await self._batch_order_service.cancel_all_orders(symbol)

    async def cancel_batch_orders(self, order_ids: list[str]) -> list[CancelOrderResult]:
        """Cancel multiple orders in a batch.

        Note: Backpack doesn't support batch cancellation directly.
        This implementation cancels orders one by one.

        Args:
            order_ids: List of order IDs to cancel

        Returns:
            List of CancelOrderResult objects
        """
        results: list[CancelOrderResult] = []
        for order_id in order_ids:
            cancel_args = CancelOrderArgs(order_id=order_id)
            result = await self._order_cancellation_service.cancel_order(cancel_args)
            results.append(result)
        return results

    # Order Query Operations

    async def get_open_orders(self, symbol: Symbol | None = None) -> list[Order]:
        """Get all open orders.

        Delegates to the order query service component.

        Args:
            symbol: Optional symbol to filter orders

        Returns:
            List of open Order objects
        """
        return await self._order_query_service.get_open_orders(symbol)

    async def get_order(self, args: GetOrderArgs) -> Order:
        """Get a specific order by ID.

        Delegates to the order query service component.

        Args:
            args: Order query parameters

        Returns:
            Order object with current details
        """
        return await self._order_query_service.get_order(args)

    async def get_order_status(self, args: GetOrderArgs) -> Order | None:
        """Get the status of a specific order.

        This method returns the current status of an order, returning None if not found
        instead of raising an exception (to match the API interface contract).

        Args:
            args: GetOrderArgs containing order identification parameters

        Returns:
            Order | None: The order with current status, or None if not found
        """
        try:
            return await self._order_query_service.get_order(args)
        except (KeyError, ValueError, LookupError):
            # Convert order not found exceptions to None return
            # This matches the API interface contract
            return None

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Get all open orders with arguments.

        This method accepts GetAllOpenOrdersArgs for parameter validation consistency
        with the API interface.

        Args:
            args: Parameters for filtering open orders including optional symbol

        Returns:
            list[Order]: List of open orders, filtered by symbol if specified
        """
        return await self._order_query_service.get_open_orders(
            args.symbol,
        )

    # Batch Operations

    async def batch_order(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Execute multiple order operations in a batch.

        Note: Backpack doesn't support batch order placement directly.
        This implementation places orders one by one.

        Args:
            orders: List of order placement parameters

        Returns:
            List of placed Order objects
        """
        results: list[Order] = []
        for order_args in orders:
            result = await self._order_placement_service.place_order(order_args)
            results.append(result)
        return results

    # Operations Not Supported by Backpack

    async def modify_order(self, order_id: str, modifications: PlaceOrderArgs) -> Order:
        """Modify an existing order.

        Note: Backpack doesn't support order modification directly.
        This would need to be implemented as cancel + create.
        """
        raise NotImplementedError(
            "Modify order operation is not directly supported by Backpack exchange. "
            "Use cancel and create order instead.",
        )
