"""BackpackAPIComponentsFactory: Centralized factory for Backpack exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Backpack exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.

REFACTORING NOTE: This factory has been updated to use composite services following
Hyperliquid's pattern. All services (Account, Trading, and Market Data) now use the
composite pattern, combining decomposed service components with optional dependency injection.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any, Literal, overload

from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.mappers import (
    BackpackAccountSummaryMapper,
    BackpackBalanceMapper,
    BackpackCandleMapper,
    BackpackFillMapper,
    BackpackFundingRateMapper,
    BackpackMarketMapper,
    BackpackOrderBookMapper,
    BackpackOrderMapper,
    BackpackPositionMapper,
    BackpackTickerMapper,
    BackpackTransactionMapper,
    BackpackTransferMapper,
)
from cyberdelta.apis.backpack.protocols import (
    AccountRequestBuilderProtocol,
    AccountResponseHandlerProtocol,
    BalanceMapperProtocol,
    CandleMapperProtocol,
    FillMapperProtocol,
    MarketDataRequestBuilderProtocol,
    MarketDataResponseHandlerProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    PositionMapperProtocol,
    TickerMapperProtocol,
    TradingRequestBuilderProtocol,
    TradingResponseHandlerProtocol,
)
from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import (
    BackpackMarketDataService,
)
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.backpack.utils.component_registry import BackpackComponentRegistry
from cyberdelta.apis.exceptions.authentication import InvalidPrivateKeyError
from cyberdelta.config.secrets_models import AnyExchangeSecrets, ApiKeyAuthSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import ExchangeName
from cyberdelta.utils.typing import ParsedJsonResponse


if TYPE_CHECKING:
    from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig


logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the factory will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

# Type aliases for component types (improves readability)
MapperComponent = (
    BackpackBalanceMapper
    | BackpackPositionMapper
    | BackpackAccountSummaryMapper
    | BackpackTransactionMapper
    | BackpackTransferMapper
    | BackpackCandleMapper
    | BackpackFundingRateMapper
    | BackpackMarketMapper
    | BackpackOrderBookMapper
    | BackpackTickerMapper
    | BackpackFillMapper
    | BackpackOrderMapper
)

BuilderComponent = (
    BackpackAccountRequestBuilder | BackpackMarketDataRequestBuilder | BackpackTradingRequestBuilder
)

HandlerComponent = (
    BackpackAccountResponseHandler
    | BackpackMarketDataResponseHandler
    | BackpackTradingResponseHandler
)

AnyComponent = MapperComponent | BuilderComponent | HandlerComponent

# Type-safe component names using Literal types
ComponentName = Literal[
    # Account mappers
    "balance_mapper",
    "position_mapper",
    "account_summary_mapper",
    "transaction_mapper",
    "transfer_mapper",
    # Market data mappers
    "candle_mapper",
    "funding_rate_mapper",
    "market_mapper",
    "order_book_mapper",
    "ticker_mapper",
    "trade_mapper",
    # Trading mappers
    "order_mapper",
    # Request builders
    "account_request_builder",
    "market_data_request_builder",
    "trading_request_builder",
    # Response handlers
    "account_response_handler",
    "market_data_response_handler",
    "trading_response_handler",
]


class BackpackAPIComponentsFactory:
    """Factory class for creating Backpack exchange API components and services.

    This factory centralizes the instantiation logic for all Backpack-specific
    components, making it easier to manage dependencies and simplify the main
    API client initialization.
    """

    def __init__(
        self,
        exchange_config: ExchangeSpecificConfig,
        exchange_secrets: AnyExchangeSecrets,
        component_registry: BackpackComponentRegistry | None = None,
    ) -> None:
        """Initialize the factory with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model
            exchange_secrets: Exchange secrets configuration model (discriminated union)
            component_registry: Optional component registry for dependency injection

        """
        self.exchange_config = exchange_config
        self.exchange_secrets = exchange_secrets

        # Use provided registry or create default
        self._registry = component_registry or BackpackComponentRegistry()

        # Log error if Backpack receives wrong auth type
        if not isinstance(exchange_secrets, ApiKeyAuthSecrets):
            logger.error(
                "backpack_wrong_auth_type",
                expected_auth_type="api_key",
                received_auth_type=exchange_secrets.auth_type,
                message="Backpack expects auth_type 'api_key' but received different type. "
                "ED25519 authentication will not work.",
            )

        # Create shared components once for resource optimization
        # Now pulls from registry when available
        self._shared_components = self._create_shared_components()

        # Validate components implement expected protocols (development aid)
        self._validate_component_protocols()

    def create_authenticator(self) -> BackpackEd25519Authenticator | None:
        """Create a BackpackEd25519Authenticator instance.

        The authenticator will perform its own cryptographic validation.

        Returns:
            Configured ED25519 authenticator instance or None if credentials are missing or invalid

        Raises:
            ValueError: If authentication credentials are invalid or cannot be processed

        """
        # Check if we have the correct secrets type for Backpack
        if not isinstance(self.exchange_secrets, ApiKeyAuthSecrets):
            logger.error(
                "backpack_authenticator_wrong_auth_type",
                expected_auth_type="api_key",
                received_auth_type=self.exchange_secrets.auth_type,
                action="create_authenticator",
                message="Cannot create Backpack authenticator: expected auth_type 'api_key' "
                "but received different type. Signed operations will fail.",
            )
            return None

        secrets: ApiKeyAuthSecrets = self.exchange_secrets  # Type cast for clarity

        if secrets.api_key and secrets.api_secret:  # Check if SecretStr objects themselves exist
            logger.info(
                "Attempting to create BackpackEd25519Authenticator (ED25519 authentication)",
            )
            try:
                return BackpackEd25519Authenticator(
                    api_key_b64_secret=secrets.api_key,  # Pass SecretStr for public key
                    private_key_b64_secret=secrets.api_secret,  # Pass SecretStr for private key
                )
            except ValueError as e:  # Catch init errors from Authenticator
                # Re-raise authentication-related errors to fail fast during API initialization
                if isinstance(e, InvalidPrivateKeyError):
                    logger.exception(
                        "authenticator_invalid_credentials",
                        action="init_authenticator",
                        authenticator_type="BackpackEd25519Authenticator",
                        error=str(e),
                        message="Invalid authentication credentials provided",
                    )
                    raise  # Re-raise the InvalidPrivateKeyError
                # For other ValueError types, log and return None as before
                logger.exception(
                    "authenticator_initialization_failed",
                    action="init_authenticator",
                    authenticator_type="BackpackEd25519Authenticator",
                    error=str(e),
                    message="Failed to initialize BackpackEd25519Authenticator",
                )
                return None
        else:
            logger.warning(
                "Backpack secrets (api_key or api_secret as SecretStr) not fully provided. "
                "Cannot create ED25519 authenticator.",
            )
            return None

    def _create_shared_components(self) -> dict[ComponentName, Any]:
        """Create shared component instances for resource optimization.

        Creates single instances of all stateless components that can be shared
        across multiple services, following Hyperliquid's shared component pattern.

        Components are retrieved from the registry when available, allowing for
        runtime customization and testing.

        Returns:
            Dictionary mapping component names to shared instances

        """
        # Try to get components from registry first, fall back to direct instantiation
        shared_components: dict[ComponentName, Any] = {
            # Account mappers - all stateless with static methods
            "balance_mapper": (
                self._registry.mappers.get("account.balance")
                if self._registry.mappers.is_registered("account.balance")
                else BackpackBalanceMapper()
            ),
            "position_mapper": (
                self._registry.mappers.get("account.position")
                if self._registry.mappers.is_registered("account.position")
                else BackpackPositionMapper()
            ),
            "account_summary_mapper": (
                self._registry.mappers.get("account.summary")
                if self._registry.mappers.is_registered("account.summary")
                else BackpackAccountSummaryMapper()
            ),
            "transaction_mapper": (
                self._registry.mappers.get("account.transaction")
                if self._registry.mappers.is_registered("account.transaction")
                else BackpackTransactionMapper()
            ),
            "transfer_mapper": (
                self._registry.mappers.get("account.transfer")
                if self._registry.mappers.is_registered("account.transfer")
                else BackpackTransferMapper()
            ),
            # Market data mappers - all stateless with static methods
            "candle_mapper": (
                self._registry.mappers.get("market.candle")
                if self._registry.mappers.is_registered("market.candle")
                else BackpackCandleMapper()
            ),
            "funding_rate_mapper": (
                self._registry.mappers.get("market.funding_rate")
                if self._registry.mappers.is_registered("market.funding_rate")
                else BackpackFundingRateMapper()
            ),
            "market_mapper": (
                self._registry.mappers.get("market.market")
                if self._registry.mappers.is_registered("market.market")
                else BackpackMarketMapper()
            ),
            "order_book_mapper": (
                self._registry.mappers.get("market.order_book")
                if self._registry.mappers.is_registered("market.order_book")
                else BackpackOrderBookMapper()
            ),
            "ticker_mapper": (
                self._registry.mappers.get("market.ticker")
                if self._registry.mappers.is_registered("market.ticker")
                else BackpackTickerMapper()
            ),
            "trade_mapper": (
                self._registry.mappers.get("market.trade")
                if self._registry.mappers.is_registered("market.trade")
                else BackpackFillMapper()
            ),
            # Trading mappers - all stateless with static methods
            "order_mapper": (
                self._registry.mappers.get("trading.order")
                if self._registry.mappers.is_registered("trading.order")
                else BackpackOrderMapper()
            ),
            # Request builders - all stateless
            "account_request_builder": (
                self._registry.request_builders.get("account")
                if self._registry.request_builders.is_registered("account")
                else BackpackAccountRequestBuilder()
            ),
            "market_data_request_builder": (
                self._registry.request_builders.get("market_data")
                if self._registry.request_builders.is_registered("market_data")
                else BackpackMarketDataRequestBuilder()
            ),
            "trading_request_builder": (
                self._registry.request_builders.get("trading")
                if self._registry.request_builders.is_registered("trading")
                else BackpackTradingRequestBuilder()
            ),
            # Response handlers - all stateless
            "account_response_handler": (
                self._registry.response_handlers.get("account")
                if self._registry.response_handlers.is_registered("account")
                else BackpackAccountResponseHandler()
            ),
            "market_data_response_handler": (
                self._registry.response_handlers.get("market_data")
                if self._registry.response_handlers.is_registered("market_data")
                else BackpackMarketDataResponseHandler()
            ),
            "trading_response_handler": (
                self._registry.response_handlers.get("trading")
                if self._registry.response_handlers.is_registered("trading")
                else BackpackTradingResponseHandler()
            ),
        }

        logger.info(
            "shared_components_created",
            component_count=len(shared_components),
            mappers_count=13,  # 5 account + 6 market data + 1 trading + 1 quote
            builders_count=3,
            handlers_count=3,
            from_registry={
                "mappers": len([
                    k
                    for k in shared_components
                    if k.endswith("_mapper")
                    and self._registry.mappers.is_registered(k.replace("_mapper", ""))
                ]),
                "builders": len([
                    k
                    for k in shared_components
                    if k.endswith("_builder")
                    and self._registry.request_builders.is_registered(
                        k.replace("_request_builder", ""),
                    )
                ]),
                "handlers": len([
                    k
                    for k in shared_components
                    if k.endswith("_handler")
                    and self._registry.response_handlers.is_registered(
                        k.replace("_response_handler", ""),
                    )
                ]),
            },
            message="Created shared component instances with registry support",
        )

        return shared_components

    def _validate_component_protocols(self) -> None:
        """Validate that components implement expected protocols.

        This is a development aid to ensure components follow consistent interfaces.
        Uses runtime_checkable protocols to verify implementation.
        """
        validations: list[tuple[ComponentName, type]] = [
            # Mapper protocol validations
            ("balance_mapper", BalanceMapperProtocol),
            ("position_mapper", PositionMapperProtocol),
            ("order_mapper", OrderMapperProtocol),
            ("ticker_mapper", TickerMapperProtocol),
            ("order_book_mapper", OrderBookMapperProtocol),
            ("trade_mapper", FillMapperProtocol),
            ("candle_mapper", CandleMapperProtocol),
            # Request builder protocol validations
            ("account_request_builder", AccountRequestBuilderProtocol),
            ("market_data_request_builder", MarketDataRequestBuilderProtocol),
            ("trading_request_builder", TradingRequestBuilderProtocol),
            # Response handler protocol validations
            ("account_response_handler", AccountResponseHandlerProtocol),
            ("market_data_response_handler", MarketDataResponseHandlerProtocol),
            ("trading_response_handler", TradingResponseHandlerProtocol),
        ]

        for component_name, protocol in validations:
            if component_name in self._shared_components:
                component = self._shared_components[component_name]
                if not isinstance(component, protocol):
                    logger.warning(
                        "component_protocol_mismatch",
                        component_name=component_name,
                        component_type=type(component).__name__,
                        expected_protocol=protocol.__name__,
                        message=(
                            f"Component {component_name} does not implement {protocol.__name__}"
                        ),
                    )

    def get_component_info(self) -> dict[str, dict[str, Any]]:
        """Get information about all registered components.

        Returns a dictionary with component names as keys and info dicts as values.
        Useful for debugging and introspection.

        Returns:
            dict[str, dict[str, Any]]: Component information including type and module
        """
        info: dict[str, dict[str, Any]] = {}
        for name, component in self._shared_components.items():
            # Get type information from component instance
            info[str(name)] = {
                "type": type(component).__name__,
                "module": type(component).__module__,
                "is_mapper": name.endswith("_mapper"),
                "is_builder": name.endswith("_request_builder"),
                "is_handler": name.endswith("_response_handler"),
            }
        return info

    def get_component_registry(self) -> BackpackComponentRegistry:
        """Get the component registry used by this factory.

        Returns:
            The component registry instance
        """
        return self._registry

    # Function overloads for type-safe component retrieval

    # Account mappers
    @overload
    def get_shared_component(
        self,
        component_name: Literal["balance_mapper"],
    ) -> BackpackBalanceMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["position_mapper"],
    ) -> BackpackPositionMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["account_summary_mapper"],
    ) -> BackpackAccountSummaryMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["transaction_mapper"],
    ) -> BackpackTransactionMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["transfer_mapper"],
    ) -> BackpackTransferMapper: ...

    # Market data mappers
    @overload
    def get_shared_component(
        self,
        component_name: Literal["candle_mapper"],
    ) -> BackpackCandleMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["funding_rate_mapper"],
    ) -> BackpackFundingRateMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["market_mapper"],
    ) -> BackpackMarketMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["order_book_mapper"],
    ) -> BackpackOrderBookMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["ticker_mapper"],
    ) -> BackpackTickerMapper: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["trade_mapper"],
    ) -> BackpackFillMapper: ...

    # Trading mappers
    @overload
    def get_shared_component(
        self,
        component_name: Literal["order_mapper"],
    ) -> BackpackOrderMapper: ...

    # Request builders
    @overload
    def get_shared_component(
        self,
        component_name: Literal["account_request_builder"],
    ) -> BackpackAccountRequestBuilder: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["market_data_request_builder"],
    ) -> BackpackMarketDataRequestBuilder: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["trading_request_builder"],
    ) -> BackpackTradingRequestBuilder: ...

    # Response handlers
    @overload
    def get_shared_component(
        self,
        component_name: Literal["account_response_handler"],
    ) -> BackpackAccountResponseHandler: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["market_data_response_handler"],
    ) -> BackpackMarketDataResponseHandler: ...

    @overload
    def get_shared_component(
        self,
        component_name: Literal["trading_response_handler"],
    ) -> BackpackTradingResponseHandler: ...

    # Implementation
    def get_shared_component(self, component_name: ComponentName) -> object:
        """Get a shared component instance by name with full type safety.

        Args:
            component_name: Name of the component to retrieve (type-safe with Literal types)

        Returns:
            The shared component instance with correct type inference

        Raises:
            KeyError: If component name is not found

        """
        if component_name not in self._shared_components:
            available_components = list(self._shared_components.keys())
            msg = (
                f"Component '{component_name}' not found in shared components. "
                f"Available components: {available_components}"
            )
            raise KeyError(msg)
        return self._shared_components[component_name]

    def create_error_mapper(self) -> BackpackErrorMapper:
        """Create a BackpackErrorMapper instance.

        Returns:
            Configured error mapper instance

        """
        return BackpackErrorMapper()

    def create_trading_request_builder(self) -> BackpackTradingRequestBuilder:
        """Create a BackpackTradingRequestBuilder instance.

        Returns:
            Configured trading request builder instance

        """
        return self.get_shared_component("trading_request_builder")

    def create_account_request_builder(self) -> BackpackAccountRequestBuilder:
        """Create a BackpackAccountRequestBuilder instance.

        Returns:
            Account request builder instance

        """
        return self.get_shared_component("account_request_builder")

    def create_market_data_request_builder(self) -> BackpackMarketDataRequestBuilder:
        """Create a BackpackMarketDataRequestBuilder instance.

        Returns:
            Market data request builder instance

        """
        return self.get_shared_component("market_data_request_builder")

    def create_trading_response_handler(self) -> BackpackTradingResponseHandler:
        """Create a BackpackTradingResponseHandler instance.

        Returns:
            Trading response handler instance

        """
        return self.get_shared_component("trading_response_handler")

    def create_account_response_handler(self) -> BackpackAccountResponseHandler:
        """Create a BackpackAccountResponseHandler instance.

        Returns:
            Account response handler instance

        """
        return self.get_shared_component("account_response_handler")

    def create_market_data_response_handler(self) -> BackpackMarketDataResponseHandler:
        """Create a BackpackMarketDataResponseHandler instance.

        Returns:
            Market data response handler instance

        """
        return self.get_shared_component("market_data_response_handler")

    def create_account_summary_mapper(self) -> BackpackAccountSummaryMapper:
        """Create a BackpackAccountSummaryMapper instance.

        Returns:
            BackpackAccountSummaryMapper: A shared mapper instance.
        """
        return self.get_shared_component("account_summary_mapper")

    def create_balance_mapper(self) -> BackpackBalanceMapper:
        """Create a BackpackBalanceMapper instance.

        Returns:
            BackpackBalanceMapper: A shared mapper instance.
        """
        return self.get_shared_component("balance_mapper")

    def create_position_mapper(self) -> BackpackPositionMapper:
        """Create a BackpackPositionMapper instance.

        Returns:
            BackpackPositionMapper: A shared mapper instance.
        """
        return self.get_shared_component("position_mapper")

    def create_transaction_mapper(self) -> BackpackTransactionMapper:
        """Create a BackpackTransactionMapper instance.

        Returns:
            BackpackTransactionMapper: A shared mapper instance.
        """
        return self.get_shared_component("transaction_mapper")

    def create_transfer_mapper(self) -> BackpackTransferMapper:
        """Create a BackpackTransferMapper instance.

        Returns:
            BackpackTransferMapper: A shared mapper instance.
        """
        return self.get_shared_component("transfer_mapper")

    def create_trading_data_mapper(self) -> BackpackOrderMapper:
        """Create a BackpackOrderMapper instance.

        Returns:
            Configured order mapper instance

        """
        return self.get_shared_component("order_mapper")

    def create_order_book_mapper(self) -> BackpackOrderBookMapper:
        """Create a BackpackOrderBookMapper instance.

        Returns:
            BackpackOrderBookMapper: A shared mapper instance.
        """
        return self.get_shared_component("order_book_mapper")

    def create_ticker_mapper(self) -> BackpackTickerMapper:
        """Create a BackpackTickerMapper instance.

        Returns:
            BackpackTickerMapper: A shared mapper instance.
        """
        return self.get_shared_component("ticker_mapper")

    def create_candle_mapper(self) -> BackpackCandleMapper:
        """Create a BackpackCandleMapper instance.

        Returns:
            BackpackCandleMapper: A shared mapper instance.
        """
        return self.get_shared_component("candle_mapper")

    def create_trade_mapper(self) -> BackpackFillMapper:
        """Create a BackpackFillMapper instance.

        Returns:
            BackpackFillMapper: A shared mapper instance.
        """
        return self.get_shared_component("trade_mapper")

    def create_market_mapper(self) -> BackpackMarketMapper:
        """Create a BackpackMarketMapper instance.

        Returns:
            BackpackMarketMapper: A shared mapper instance.
        """
        return self.get_shared_component("market_mapper")

    def create_funding_rate_mapper(self) -> BackpackFundingRateMapper:
        """Create a BackpackFundingRateMapper instance.

        Returns:
            BackpackFundingRateMapper: A shared mapper instance.
        """
        return self.get_shared_component("funding_rate_mapper")

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        exchange_name: ExchangeName,
        request_builder: BackpackMarketDataRequestBuilder | None = None,
        response_handler: BackpackMarketDataResponseHandler | None = None,
        # Optional mapper parameters for dependency injection
        ticker_mapper: BackpackTickerMapper | None = None,
        order_book_mapper: BackpackOrderBookMapper | None = None,
        trade_mapper: BackpackFillMapper | None = None,
        candle_mapper: BackpackCandleMapper | None = None,
        market_mapper: BackpackMarketMapper | None = None,
        funding_rate_mapper: BackpackFundingRateMapper | None = None,
    ) -> BackpackMarketDataService:
        """Create a BackpackMarketDataService instance.

        NOTE: This returns a composite service that combines all market data
        operations from decomposed service components.

        Args:
            http_client_requester: HTTP client request function
            exchange_name: Name of the exchange
            request_builder: Request builder instance (optional)
            response_handler: Response handler instance (optional)
            ticker_mapper: Optional ticker mapper for dependency injection
            order_book_mapper: Optional order book mapper for dependency injection
            trade_mapper: Optional trade mapper for dependency injection
            candle_mapper: Optional candle mapper for dependency injection
            market_mapper: Optional market mapper for dependency injection
            funding_rate_mapper: Optional funding rate mapper for dependency injection

        Returns:
            Configured market data service instance

        """
        # Use decomposed instances for the service
        market_data_request_builder = request_builder or self.create_market_data_request_builder()
        market_data_response_handler = (
            response_handler or self.create_market_data_response_handler()
        )

        return BackpackMarketDataService(
            http_client_requester=http_client_requester,
            request_builder=market_data_request_builder,
            response_handler=market_data_response_handler,
            exchange_name=exchange_name,
            # Pass optional mappers for dependency injection
            ticker_mapper=ticker_mapper,
            order_book_mapper=order_book_mapper,
            trade_mapper=trade_mapper,
            candle_mapper=candle_mapper,
            market_mapper=market_mapper,
            funding_rate_mapper=funding_rate_mapper,
        )

    def create_account_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: BackpackEd25519Authenticator | None,
        exchange_name: ExchangeName,
        request_builder: BackpackAccountRequestBuilder | None = None,
        response_handler: BackpackAccountResponseHandler | None = None,
        # NEW: Optional mapper parameters for dependency injection
        balance_mapper: BackpackBalanceMapper | None = None,
        position_mapper: BackpackPositionMapper | None = None,
        account_summary_mapper: BackpackAccountSummaryMapper | None = None,
        transaction_mapper: BackpackTransactionMapper | None = None,
        transfer_mapper: BackpackTransferMapper | None = None,
    ) -> BackpackAccountService:
        """Create a BackpackAccountService instance.

        NOTE: This returns a composite service that combines all account
        operations from decomposed service components.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            balance_mapper: Optional balance mapper for dependency injection
            position_mapper: Optional position mapper for dependency injection
            account_summary_mapper: Optional account summary mapper for dependency injection
            transaction_mapper: Optional transaction mapper for dependency injection
            transfer_mapper: Optional transfer mapper for dependency injection

        Returns:
            Configured account service instance

        """
        # Use decomposed instances for the service
        account_request_builder = request_builder or self.create_account_request_builder()
        account_response_handler = response_handler or self.create_account_response_handler()

        # Create trading components for transaction history service
        trading_request_builder = self.create_trading_request_builder()
        trading_response_handler = self.create_trading_response_handler()

        return BackpackAccountService(
            http_client_requester=http_client_requester,
            request_builder=account_request_builder,
            response_handler=account_response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            # Pass trading components for transaction history
            trading_request_builder=trading_request_builder,
            trading_response_handler=trading_response_handler,
            # Pass optional mappers for dependency injection
            balance_mapper=balance_mapper,
            position_mapper=position_mapper,
            account_summary_mapper=account_summary_mapper,
            transaction_mapper=transaction_mapper,
            transfer_mapper=transfer_mapper,
        )

    def create_trading_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: BackpackEd25519Authenticator | None,
        exchange_name: ExchangeName,
        request_builder: BackpackTradingRequestBuilder | None = None,
        response_handler: BackpackTradingResponseHandler | None = None,
        # Optional mapper parameter for dependency injection
        order_mapper: BackpackOrderMapper | None = None,
    ) -> BackpackTradingService:
        """Create a BackpackTradingService instance.

        NOTE: This returns a composite service that combines all trading
        operations from decomposed service components.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            exchange_name: Name of the exchange
            request_builder: Request builder instance (optional)
            response_handler: Response handler instance (optional)
            order_mapper: Optional order mapper for dependency injection

        Returns:
            Configured trading service instance

        """
        # Use decomposed instances for the service
        trading_request_builder = request_builder or self.create_trading_request_builder()
        trading_response_handler = response_handler or self.create_trading_response_handler()

        return BackpackTradingService(
            http_client_requester=http_client_requester,
            request_builder=trading_request_builder,
            response_handler=trading_response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            # Pass optional mapper for dependency injection
            order_mapper=order_mapper,
        )
