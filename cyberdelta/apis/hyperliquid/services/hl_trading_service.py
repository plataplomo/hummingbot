"""Hyperliquid Trading Service - Composite service combining trading operations.

This service combines all trading-related operations from the decomposed services
to provide a unified interface for trading activities.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.exceptions import ServiceParameterError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import (
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_response_mapper import (
    HyperliquidOrderResponseMapper,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import TradingRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import TradingResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    OrderMapperProtocol,
    OrderResponseMapperProtocol,
)
from cyberdelta.apis.hyperliquid.services.market_data.hl_order_book_service import (
    HyperliquidOrderBookService,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_batch_order_service import (
    HyperliquidBatchOrderService,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_cancellation_service import (
    HyperliquidOrderCancellationService,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_placement_service import (
    HyperliquidOrderPlacementService,
)
from cyberdelta.apis.hyperliquid.services.trading.hl_order_query_service import (
    HyperliquidOrderQueryService,
)
from cyberdelta.apis.models.service_args.trading import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetOrderArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

GetAssetIndexCallableSig = Callable[[str], Awaitable[int | None]]


class HyperliquidTradingService:
    """Composite service for Hyperliquid trading operations.

    Combines all trading-related decomposed services to provide a unified interface
    for trading operations including order placement, cancellation, and queries.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: TradingRequestBuilderProtocol,
        response_handler: TradingResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: GetAssetIndexCallableSig,
        order_book_service: HyperliquidOrderBookService | None = None,  # Optional
        # Optional dependency injection following Backpack pattern
        order_mapper: OrderMapperProtocol | None = None,
        order_response_mapper: OrderResponseMapperProtocol | None = None,
        error_mapper: HyperliquidErrorMapper | None = None,
    ) -> None:
        """Initialize the Hyperliquid trading service with protocol-based dependency injection.

        Creates and configures all decomposed service components following the Backpack
        pattern with optional protocol-based mapper injection.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Protocol-compliant builder for constructing trading API requests
            response_handler: Protocol-compliant handler for processing trading API responses
            authenticator: Authentication interface for signing requests
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for trading operations
            get_asset_index_callable: Function to retrieve asset index for symbols
            order_book_service: Optional order book service for market order handling
            order_mapper: Optional order mapper implementing OrderMapperProtocol
            order_response_mapper: Optional order response mapper for order placement responses
            error_mapper: Optional error mapper for handling error responses
        """
        # Validate required dependencies for authenticated services
        if authenticator is None:
            raise ServiceParameterError(
                parameter="authenticator",
                issue="is required for trading services",
                value=authenticator,
                exchange="hyperliquid",
                operation="initialize trading service",
                expected_type="IAuthenticator",
                suggestion="Provide a valid authenticator instance",
            )
        if wallet_address is None:
            raise ServiceParameterError(
                parameter="wallet_address",
                issue="is required for trading services",
                value=wallet_address,
                exchange="hyperliquid",
                operation="initialize trading service",
                expected_type="str",
                suggestion="Provide a valid wallet address",
            )

        # Store parameters
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        self._get_asset_index_callable = get_asset_index_callable

        # Create mappers if not provided
        self._order_mapper = order_mapper or HyperliquidOrderMapper()
        self._order_response_mapper = order_response_mapper or HyperliquidOrderResponseMapper()
        self._error_mapper = error_mapper or HyperliquidErrorMapper()

        # Initialize decomposed services
        self._order_placement_service = HyperliquidOrderPlacementService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_response_mapper,
            error_mapper=self._error_mapper,
            authenticator=authenticator,
            get_asset_index_callable=get_asset_index_callable,
            order_book_service=order_book_service,
            exchange_name=exchange_name,
        )

        self._order_query_service = HyperliquidOrderQueryService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            error_mapper=self._error_mapper,
            authenticator=authenticator,
            wallet_address=wallet_address,
            exchange_name=exchange_name,
        )

        self._order_cancellation_service = HyperliquidOrderCancellationService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            error_mapper=self._error_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
            get_asset_index_callable=get_asset_index_callable,
            order_query_service=self._order_query_service,
        )

        self._batch_order_service = HyperliquidBatchOrderService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_response_mapper,
            error_mapper=self._error_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
            get_asset_index_callable=get_asset_index_callable,
        )

        logger.info(
            "trading_service_initialized",
            exchange=exchange_name,
            wallet_address=wallet_address,
            message="Hyperliquid trading service initialized with decomposed services",
        )

    # Order Placement Operations

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Place a single order on the exchange."""
        return await self._order_placement_service.place_order(args)

    # Order Cancellation Operations

    async def cancel_order(self, args: CancelOrderArgs) -> CancelOrderResult:
        """Cancel a single order by ID."""
        return await self._order_cancellation_service.cancel_order(args)

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders, optionally filtered by symbol."""
        return await self._order_cancellation_service.cancel_all_orders(symbol)

    # Order Query Operations

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Retrieve a specific order by ID."""
        return await self._order_query_service.get_order(args)

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol."""
        return await self._order_query_service.get_open_orders(symbol)

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol (args version)."""
        return await self._order_query_service.get_open_orders(args.symbol)

    # Batch Operations

    async def place_batch_orders(self, orders: list[PlaceOrderArgs]) -> list[Order]:
        """Place multiple orders in a single batch request."""
        return await self._batch_order_service.place_batch_orders(orders)

    async def cancel_batch_orders(self, args: list[CancelOrderArgs]) -> list[CancelOrderResult]:
        """Cancel multiple orders in a single batch request."""
        return await self._batch_order_service.cancel_batch_orders(args)
