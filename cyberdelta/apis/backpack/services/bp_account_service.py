"""Backpack Account Service - Composite service combining account operations.

This service combines all account-related operations from the decomposed services
to provide a unified interface for account management, following Hyperliquid's
composite pattern.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING

from cyberdelta.apis.backpack.mappers import (
    BackpackAccountSummaryMapper,
    BackpackBalanceMapper,
    BackpackPositionMapper,
    BackpackTransactionMapper,
    BackpackTransferMapper,
)
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.backpack.services.account.bp_account_state_service import (
    BackpackAccountStateService,
)
from cyberdelta.apis.backpack.services.account.bp_account_summary_service import (
    BackpackAccountSummaryService,
)
from cyberdelta.apis.backpack.services.account.bp_balance_service import (
    BackpackBalanceService,
)
from cyberdelta.apis.backpack.services.account.bp_position_service import (
    BackpackPositionService,
)
from cyberdelta.apis.backpack.services.account.bp_transaction_history_service import (
    BackpackTransactionHistoryService,
)
from cyberdelta.apis.backpack.services.account.bp_transfer_service import (
    BackpackTransferService,
)
from cyberdelta.apis.models.service_args.account import (
    TransferArgs,
    UpdateAccountSettingsArgs,
    WithdrawArgs,
)
from cyberdelta.apis.models.service_args.trading import (
    GetOrderHistoryArgs,
    GetTradeHistoryArgs,
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
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator
    from cyberdelta.core.models.operations import Transfer, Withdrawal

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class BackpackAccountService:
    """Composite service for Backpack account operations.

    Combines all account-related decomposed services to provide a unified interface
    for account management operations including balances, positions, and trade history.

    This follows Hyperliquid's composite pattern where:
    - The composite owns instances of decomposed services
    - All operations are delegated to the appropriate decomposed service
    - Optional dependency injection is supported for all mappers
    """

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: BackpackAccountRequestBuilder,
        response_handler: BackpackAccountResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        # Additional builders/handlers for services that need trading endpoints
        trading_request_builder: BackpackTradingRequestBuilder | None = None,
        trading_response_handler: BackpackTradingResponseHandler | None = None,
        # Optional mapper injection for testability
        balance_mapper: BackpackBalanceMapper | None = None,
        position_mapper: BackpackPositionMapper | None = None,
        account_summary_mapper: BackpackAccountSummaryMapper | None = None,
        transaction_mapper: BackpackTransactionMapper | None = None,
        transfer_mapper: BackpackTransferMapper | None = None,
    ) -> None:
        """Initialize the Backpack account service.

        Creates and configures all decomposed service components.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Backpack API requests
            response_handler: Handler for processing Backpack API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            trading_request_builder: Optional trading request builder for order endpoints
            trading_response_handler: Optional trading response handler for order endpoints
            balance_mapper: Optional balance mapper instance for dependency injection
            position_mapper: Optional position mapper instance for dependency injection
            account_summary_mapper: Optional account summary mapper instance for
                dependency injection
            transaction_mapper: Optional transaction mapper instance for dependency injection
            transfer_mapper: Optional transfer mapper instance for dependency injection
        """
        # Store core parameters
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name

        # Create trading components if not provided
        self._trading_request_builder = trading_request_builder or BackpackTradingRequestBuilder()
        self._trading_response_handler = (
            trading_response_handler or BackpackTradingResponseHandler()
        )

        # Create mappers if not provided (following Hyperliquid pattern)
        self._balance_mapper = balance_mapper or BackpackBalanceMapper()
        self._position_mapper = position_mapper or BackpackPositionMapper()
        self._account_summary_mapper = account_summary_mapper or BackpackAccountSummaryMapper()
        self._transaction_mapper = transaction_mapper or BackpackTransactionMapper()
        self._transfer_mapper = transfer_mapper or BackpackTransferMapper()

        # Create shared account state service (following Hyperliquid clearinghouse pattern)
        self._account_state_service = BackpackAccountStateService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
        )

        # Initialize decomposed service components with shared state service injection
        self._balance_service = BackpackBalanceService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            account_state_service=self._account_state_service,  # Inject shared state service
            mapper=self._balance_mapper,
        )

        self._position_service = BackpackPositionService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            account_state_service=self._account_state_service,  # Inject shared state service
            mapper=self._position_mapper,
        )

        self._account_summary_service = BackpackAccountSummaryService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            account_state_service=self._account_state_service,  # Inject shared state service
            mapper=self._account_summary_mapper,
        )

        self._transaction_history_service = BackpackTransactionHistoryService(
            http_client_requester=http_client_requester,
            request_builder=self._trading_request_builder,
            response_handler=self._trading_response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            mapper=self._transaction_mapper,
        )

        self._transfer_service = BackpackTransferService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            mapper=self._transfer_mapper,
        )

        logger.info(
            "account_service_initialized",
            exchange=exchange_name,
            mappers={
                "balance": type(self._balance_mapper).__name__,
                "position": type(self._position_mapper).__name__,
                "account_summary": type(self._account_summary_mapper).__name__,
                "transaction": type(self._transaction_mapper).__name__,
                "transfer": type(self._transfer_mapper).__name__,
            },
            message="Backpack account service initialized with decomposed services",
        )

    # Balance Operations

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieve all account balances.

        Delegates to the balance service component.

        Returns:
            Dictionary mapping asset symbols to SpotBalance objects
        """
        return await self._balance_service.get_balances()

    # Position Operations

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Retrieve derivative positions, optionally filtered by symbol.

        Delegates to the position service component.

        Args:
            symbol: Optional symbol to filter positions

        Returns:
            List of DerivativePosition objects
        """
        return await self._position_service.get_positions(symbol)

    # Account Summary Operations

    async def get_account_summary(self) -> MarginAccountSummary:
        """Retrieve the account summary information.

        Delegates to the account summary service component.

        Returns:
            MarginAccountSummary object with account details
        """
        return await self._account_summary_service.get_account_summary()

    async def invalidate_account_cache(self, subaccount_id: int | None = None) -> None:
        """Invalidate cached account state data.

        This should be called after operations that modify account state
        (like placing orders) to ensure fresh data on subsequent queries.

        Args:
            subaccount_id: Optional subaccount ID (None for main account)
        """
        await self._account_state_service.invalidate_cache(subaccount_id)

    # Trade History Operations

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieve historical trades based on the provided arguments.

        Delegates to the transaction history service component.

        Args:
            args: Trade history query parameters

        Returns:
            List of Trade objects
        """
        return await self._transaction_history_service.get_trade_history(args)

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieve historical orders based on the provided arguments.

        Note: Backpack doesn't have a separate order history endpoint,
        so this is currently not implemented.

        Args:
            args: Order history query parameters

        Returns:
            Empty list (not implemented for Backpack)
        """
        # Backpack doesn't have order history endpoint
        return []

    # Transfer Operations

    async def transfer(self, transfer_args: TransferArgs) -> Transfer:
        """Execute a transfer operation.

        Delegates to the transfer service component.

        Args:
            transfer_args: Transfer operation parameters

        Returns:
            Transfer object with transaction details
        """
        return await self._transfer_service.transfer(transfer_args)

    # Operations Not Supported by Backpack

    async def withdraw(self, withdraw_args: WithdrawArgs) -> Withdrawal:
        """Withdraw is not supported by Backpack API."""
        raise NotImplementedError("Withdraw operation is not supported by Backpack exchange")

    async def update_account_settings(
        self,
        update_account_settings_args: UpdateAccountSettingsArgs,
    ) -> AccountSettings:
        """Update account settings is not supported by Backpack API."""
        raise NotImplementedError(
            "Update account settings operation is not supported by Backpack exchange",
        )
