"""Hyperliquid Account Service - Composite service combining account operations.

This service combines all account-related operations from the decomposed services
to provide a unified interface for account management.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.exceptions.response_validation import NotImplementedOperationError
from cyberdelta.apis.hyperliquid.mappers import (
    HyperliquidAccountSummaryMapper,
    HyperliquidBalanceMapper,
    HyperliquidOrderMapper,
    HyperliquidPositionMapper,
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import AccountResponseHandlerProtocol
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TransactionMapperProtocol,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.services.account.hl_account_summary_service import (
    HyperliquidAccountSummaryService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_balance_service import (
    HyperliquidBalanceService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_clearinghouse_state_service import (
    HyperliquidClearinghouseStateService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_order_history_service import (
    HyperliquidOrderHistoryService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_position_service import (
    HyperliquidPositionService,
)
from cyberdelta.apis.hyperliquid.services.account.hl_trade_history_service import (
    HyperliquidTradeHistoryService,
)
from cyberdelta.apis.models.service_args_models import (
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    DerivativePosition,
    MarginAccountSummary,
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator
    from cyberdelta.core.models import (
        DerivativePosition,
        MarginAccountSummary,
        Order,  # For order history
        SpotBalance,
        Trade,  # For trade history
    )
    from cyberdelta.core.models.account_settings import AccountSettings
    from cyberdelta.core.models.operations import Transfer, Withdrawal

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidAccountService:
    """Composite service for Hyperliquid account operations.

    Combines all account-related decomposed services to provide a unified interface
    for account management operations including balances, positions, and trade history.
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: AccountRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None = None,
        # Optional dependency injection following Backpack pattern
        balance_mapper: BalanceMapperProtocol | None = None,
        position_mapper: PositionMapperProtocol | None = None,
        account_summary_mapper: AccountSummaryMapperProtocol | None = None,
        order_mapper: OrderMapperProtocol | None = None,
        transaction_mapper: TransactionMapperProtocol | None = None,
        trading_request_builder: TradingRequestBuilderProtocol | None = None,
    ) -> None:
        """Initialize the Hyperliquid account service with protocol-based dependency injection.

        Creates and configures all decomposed service components following the Backpack
        pattern with optional protocol-based mapper injection.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Protocol-compliant builder for constructing account API requests
            response_handler: Protocol-compliant handler for processing account API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for user state requests
            balance_mapper: Optional balance mapper implementing BalanceMapperProtocol
            position_mapper: Optional position mapper implementing PositionMapperProtocol
            account_summary_mapper: Optional account summary mapper implementing
                AccountSummaryMapperProtocol
            order_mapper: Optional order mapper implementing OrderMapperProtocol
            transaction_mapper: Optional transaction mapper implementing TransactionMapperProtocol
            trading_request_builder: Optional trading request builder implementing
                TradingRequestBuilderProtocol
        """
        # Store parameters
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        # Create mappers if not provided
        self._balance_mapper = balance_mapper or HyperliquidBalanceMapper()
        self._position_mapper = position_mapper or HyperliquidPositionMapper()
        self._account_summary_mapper = account_summary_mapper or HyperliquidAccountSummaryMapper()
        self._order_mapper = order_mapper or HyperliquidOrderMapper()
        self._transaction_mapper = transaction_mapper or HyperliquidTransactionMapper()

        # Initialize shared clearinghouse state service
        self._clearinghouse_service = HyperliquidClearinghouseStateService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
        )

        # Initialize decomposed service components with shared clearinghouse service
        self._balance_service = HyperliquidBalanceService(
            clearinghouse_service=self._clearinghouse_service,
            mapper=self._balance_mapper,
            exchange_name=exchange_name,
        )

        self._position_service = HyperliquidPositionService(
            clearinghouse_service=self._clearinghouse_service,
            mapper=self._position_mapper,
            exchange_name=exchange_name,
        )

        self._account_summary_service = HyperliquidAccountSummaryService(
            clearinghouse_service=self._clearinghouse_service,
            mapper=self._account_summary_mapper,
            exchange_name=exchange_name,
        )

        self._order_history_service = HyperliquidOrderHistoryService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=self._order_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
        )

        # Trade history needs a trading request builder
        if trading_request_builder is None:
            # Create default implementation if not injected
            trading_request_builder = HyperliquidTradingRequestBuilder()

        self._trade_history_service = HyperliquidTradeHistoryService(
            http_client_requester=http_client_requester,
            request_builder=trading_request_builder,
            response_handler=response_handler,
            mapper=self._transaction_mapper,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
        )

        logger.info(
            "account_service_initialized",
            exchange=exchange_name,
            wallet_address=wallet_address,
            message="Hyperliquid account service initialized with decomposed services",
        )

    # Balance Operations

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieve all account balances."""
        return await self._balance_service.get_balances()

    # Position Operations

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Retrieve derivative positions, optionally filtered by symbol."""
        return await self._position_service.get_positions(symbol)

    # Account Summary Operations

    async def get_account_summary(self) -> MarginAccountSummary:
        """Retrieve general account information summary."""
        return await self._account_summary_service.get_account_summary()

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits."""
        raise NotImplementedOperationError(
            operation="update_account_settings",
            service="HyperliquidAccountService",
            exchange=self._exchange_name,
        )

    # Order History Operations

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieve historical order data."""
        return await self._order_history_service.get_order_history(args)

    # Trade History Operations

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieve user trade history (fills)."""
        return await self._trade_history_service.get_trade_history(args)

    # Operations Not Implemented

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Perform an internal transfer - NOT IMPLEMENTED."""
        raise NotImplementedOperationError(
            operation="transfer",
            service="HyperliquidAccountService",
            exchange=self._exchange_name,
        )

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Initiate a withdrawal - NOT IMPLEMENTED."""
        raise NotImplementedOperationError(
            operation="withdraw",
            service="HyperliquidAccountService",
            exchange=self._exchange_name,
        )
