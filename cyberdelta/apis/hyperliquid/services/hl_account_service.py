"""Hyperliquid Account Service - Composite service combining account operations.

This service combines all account-related operations from the decomposed services
to provide a unified interface for account management.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, NoReturn

from cyberdelta.apis.base.infrastructure_config_domain import (
    RequestAuthMode,
    RequestConfiguration,
)
from cyberdelta.apis.exceptions.connectivity import ResponseParsingError
from cyberdelta.apis.exceptions.request_validation import InvalidEnumValueError
from cyberdelta.apis.exceptions.response_validation import NotImplementedOperationError
from cyberdelta.apis.hyperliquid.mappers import (
    HyperliquidAccountSummaryMapper,
    HyperliquidBalanceMapper,
    HyperliquidOrderMapper,
    HyperliquidPositionMapper,
    HyperliquidTransactionMapper,
    HyperliquidTransferMapper,
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
from cyberdelta.models import (
    AccountSettings,
    DerivativePosition,
    MarginAccountSummary,
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.models.operations import Transfer, Withdrawal
from cyberdelta.symbols import exchanges
from cyberdelta.symbols.models import Symbol
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.apis.hyperliquid.models.hl_raw_usd_transfer_response import (
        HyperliquidRawUsdTransferResponse,
    )


if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator
    from cyberdelta.models import (
        DerivativePosition,
        MarginAccountSummary,
        Order,  # For order history
        SpotBalance,
        Trade,  # For trade history
    )
    from cyberdelta.models.account_settings import AccountSettings
    from cyberdelta.models.operations import Transfer, Withdrawal

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
        transfer_mapper: HyperliquidTransferMapper | None = None,
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
            transfer_mapper: Optional transfer mapper for transforming raw transfer responses
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
        self._transfer_mapper = transfer_mapper or HyperliquidTransferMapper()

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
        """Retrieve all account balances.

        Returns:
            Dictionary mapping asset symbols to their balance information

        """
        return await self._balance_service.get_balances()

    # Position Operations

    async def get_positions(self, symbol: Symbol | None = None) -> list[DerivativePosition]:
        """Retrieve derivative positions, optionally filtered by symbol.

        Returns:
            list[DerivativePosition]: List of derivative positions
        """
        return await self._position_service.get_positions(symbol)

    # Account Summary Operations

    async def get_account_summary(self) -> MarginAccountSummary:
        """Retrieve general account information summary.

        Returns:
            Account summary containing margin, equity, and other account metrics

        """
        return await self._account_summary_service.get_account_summary()

    async def update_account_settings(self, args: UpdateAccountSettingsArgs) -> AccountSettings:
        """Update account settings such as leverage limits.

        Args:
            args: Settings update arguments

        Raises:
            NotImplementedOperationError: This operation is not implemented

        """
        raise NotImplementedOperationError(
            operation="update_account_settings",
            service="HyperliquidAccountService",
            exchange=self._exchange_name,
        )

    # Order History Operations

    async def get_order_history(self, args: GetOrderHistoryArgs) -> list[Order]:
        """Retrieve historical order data.

        Args:
            args: Arguments specifying date range and filtering options

        Returns:
            List of historical order records

        """
        return await self._order_history_service.get_order_history(args)

    # Trade History Operations

    async def get_trade_history(self, args: GetTradeHistoryArgs) -> list[Trade]:
        """Retrieve user trade history (fills).

        Args:
            args: Arguments specifying date range and filtering options

        Returns:
            List of trade history records (fills)

        """
        return await self._trade_history_service.get_trade_history(args)

    # Operations Not Implemented

    async def transfer(self, args: TransferArgs) -> Transfer:
        """Perform internal USD transfer between spot and perp accounts.

        Follows exact Backpack service pattern: validation → request building → execution →
        response handling → mapper transformation.

        Args:
            args: Validated TransferArgs containing transfer parameters

        Returns:
            Transfer: Internal transfer model with Hyperliquid-specific details

        """
        logger.info(
            "hyperliquid_internal_transfer_start",
            exchange=self._exchange_name,
            asset=args.asset,
            amount=str(args.amount),
            from_account=args.from_account_type,
            to_account=args.to_account_type,
            client_id=args.client_transfer_id,
            message=(
                f"[{self._exchange_name}] Starting internal transfer: {args.amount} {args.asset} "
                f"from {args.from_account_type} to {args.to_account_type}"
            ),
        )

        # Step 1: Validate account types following Backpack pattern
        self._validate_account_types(args.from_account_type, args.to_account_type)

        # Step 2: Execute transfer request
        raw_response, status_code = await self._execute_transfer_request(args)

        # Step 3: Process response through handler
        validated_response = await self._process_transfer_response(raw_response, status_code)

        # Step 4: Transform to internal model via mapper
        transfer = self._transfer_mapper.transform_raw_transfer_to_internal(
            raw_response=validated_response,
            exchange_name=self._exchange_name,
            asset=args.asset,
            quantity=args.amount,
            from_account_type_raw=args.from_account_type,
            to_account_type_raw=args.to_account_type,
            client_transfer_id=args.client_transfer_id,
        )

        logger.info(
            "hyperliquid_internal_transfer_complete",
            exchange=self._exchange_name,
            transfer_id=transfer.id,
            status=transfer.status.value,
            asset=transfer.asset,
            amount=str(transfer.quantity),
            message=(
                f"[{self._exchange_name}] Internal transfer completed: {transfer.id} "
                f"with status {transfer.status.value}"
            ),
        )

        return transfer

    def _validate_account_types(self, from_account: str, to_account: str) -> None:
        """Validate account types for Hyperliquid internal transfers.

        Args:
            from_account: Source account type
            to_account: Destination account type

        Raises:
            InvalidEnumValueError: If account types are invalid for Hyperliquid

        """
        valid_accounts = {"spot", "perp"}

        if from_account.lower() not in valid_accounts:
            raise InvalidEnumValueError(
                parameter_name="from_account_type",
                value=from_account,
                valid_values=list(valid_accounts),
                enum_type="account_types",
            )

        if to_account.lower() not in valid_accounts:
            raise InvalidEnumValueError(
                parameter_name="to_account_type",
                value=to_account,
                valid_values=list(valid_accounts),
                enum_type="account_types",
            )

        if from_account.lower() == to_account.lower():
            raise InvalidEnumValueError(
                parameter_name="account_type_combination",
                value=f"{from_account}->{to_account}",
                valid_values=["spot->perp", "perp->spot"],
                enum_type="transfer_directions",
            )

    async def _execute_transfer_request(self, args: TransferArgs) -> tuple[ParsedJsonResponse, int]:
        """Execute the internal transfer request following Backpack pattern.

        Args:
            args: Validated transfer arguments

        Returns:
            Tuple of (raw response from Hyperliquid API, HTTP status code)

        """
        try:
            # Build internal transfer payload using static method
            # Convert asset string to Symbol object
            asset_symbol = exchanges.hyperliquid(args.asset)
            transfer_payload = self._request_builder.build_internal_transfer_payload(
                asset_symbol=asset_symbol,
                from_account_type=args.from_account_type,
                to_account_type=args.to_account_type,
                amount=args.amount,
            )

            # Execute signed request to /exchange endpoint
            request_config = RequestConfiguration(
                auth_mode=RequestAuthMode.SIGNED,  # Transfer requests require signing
                endpoint_group="exchange",
                request_weight=1,
            )
            response_data, status_code, _headers = await self._http_client_requester(
                method="POST",
                endpoint="/exchange",
                data=transfer_payload.model_dump(by_alias=True, exclude_none=True, mode="json"),
                request_config=request_config,
            )

            if response_data is None:
                self._raise_none_response_error(status_code)

        except Exception as e:
            logger.exception(
                "hyperliquid_transfer_request_failed",
                exchange=self._exchange_name,
                asset=args.asset,
                amount=str(args.amount),
                error=str(e),
                message=f"[{self._exchange_name}] Transfer request execution failed",
            )
            raise
        else:
            return response_data, status_code

    async def _process_transfer_response(
        self,
        raw_response: ParsedJsonResponse,
        status_code: int,
    ) -> HyperliquidRawUsdTransferResponse:
        """Process transfer response through response handler.

        Args:
            raw_response: Raw response from HTTP client
            status_code: HTTP status code from the response

        Returns:
            Validated HyperliquidRawUsdTransferResponse model

        """
        try:
            # For transfer responses, the raw_response should be the direct API response
            if isinstance(raw_response, dict):
                response_data = raw_response
            elif isinstance(raw_response, list):
                # Wrap list responses in a dict structure
                response_data = {"data": raw_response}
            else:  # str case
                # Wrap string responses in a dict structure
                response_data = {"data": raw_response}

            # Use the actual HTTP status code from the response

            # Process through response handler
            return self._response_handler.handle_transfer_response(
                raw_response_content=response_data,
                status_code=status_code,
            )

        except Exception as e:
            logger.exception(
                "hyperliquid_transfer_response_processing_failed",
                exchange=self._exchange_name,
                error=str(e),
                message=f"[{self._exchange_name}] Transfer response processing failed",
            )
            raise

    def _raise_none_response_error(self, status_code: int) -> NoReturn:
        """Raise ResponseParsingError for None response data.

        Args:
            status_code: HTTP status code from the response

        Raises:
            ResponseParsingError: Always raised to indicate None response data

        """
        raise ResponseParsingError(
            url="/exchange",
            status_code=status_code,
            reason="HTTP client returned None response data",
        )

    async def withdraw(self, args: WithdrawArgs) -> Withdrawal:
        """Initiate a withdrawal - NOT IMPLEMENTED.

        Args:
            args: Withdrawal arguments

        Raises:
            NotImplementedOperationError: This operation is not implemented

        """
        raise NotImplementedOperationError(
            operation="withdraw",
            service="HyperliquidAccountService",
            exchange=self._exchange_name,
        )
