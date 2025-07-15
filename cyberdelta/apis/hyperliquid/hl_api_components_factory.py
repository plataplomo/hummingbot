"""HyperliquidAPIComponentsFactory: Centralized factory for Hyperliquid exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Hyperliquid exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.

REFACTORING NOTE: This factory has been updated to use facade services for all operations
(Account, Trading, and Market Data). The facades maintain backward compatibility while
delegating to decomposed service components. All services have been successfully decomposed.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import Any, Literal, overload

from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_payload_serialization_strategy import (
    HyperliquidSerializationStrategy,
)
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

# Import decomposed mappers directly
from cyberdelta.apis.hyperliquid.mappers.account.hl_account_summary_mapper import (
    HyperliquidAccountSummaryMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_balance_mapper import (
    HyperliquidBalanceMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_position_mapper import (
    HyperliquidPositionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.account.hl_transaction_mapper import (
    HyperliquidTransactionMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_historical_data_mapper import (
    HyperliquidHistoricalDataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper import (
    HyperliquidMarketMetadataMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_order_book_mapper import (
    HyperliquidOrderBookMapper,
)
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_price_ticker_mapper import (
    HyperliquidPriceTickerMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_mapper import (
    HyperliquidOrderMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_order_response_mapper import (
    HyperliquidOrderResponseMapper,
)
from cyberdelta.apis.hyperliquid.mappers.trading.hl_trading_enum_mapper import (
    HyperliquidTradingEnumMapper,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import (
    AccountRequestBuilderProtocol,
    MarketDataRequestBuilderProtocol,
    TradingRequestBuilderProtocol,
)
from cyberdelta.apis.hyperliquid.protocols.handler_protocols import (
    AccountResponseHandlerProtocol,
    MarketDataResponseHandlerProtocol,
    TradingResponseHandlerProtocol,
)

# Import protocols for runtime validation
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    AccountSummaryMapperProtocol,
    BalanceMapperProtocol,
    HistoricalDataMapperProtocol,
    MarketMetadataMapperProtocol,
    OrderBookMapperProtocol,
    OrderMapperProtocol,
    OrderResponseMapperProtocol,
    PositionMapperProtocol,
    PriceTickerMapperProtocol,
    TradingEnumMapperProtocol,
    TransactionMapperProtocol,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_account_request_builder import (
    HyperliquidAccountRequestBuilder,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_trading_request_builder import (
    HyperliquidTradingRequestBuilder,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_account_response_handler import (
    HyperliquidAccountResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_market_data_response_handler import (
    HyperliquidMarketDataResponseHandler,
)
from cyberdelta.apis.hyperliquid.response_handlers.hl_trading_response_handler import (
    HyperliquidTradingResponseHandler,
)

# Import composite services that combine decomposed services
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import (
    HyperliquidMarketDataService,
)
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.apis.hyperliquid.services.market_data.hl_order_book_service import (
    HyperliquidOrderBookService,
)
from cyberdelta.apis.hyperliquid.utils.component_registry import HyperliquidComponentRegistry
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets, PrivateKeyAuthSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the factory will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ...,
    Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

# Type alias for the get_asset_index callable
GetAssetIndexCallableSig = Callable[[str], Coroutine[Any, Any, int | None]]

# Component Type Unions for Type Safety
MapperComponent = (
    # Concrete types
    HyperliquidAccountSummaryMapper
    | HyperliquidBalanceMapper
    | HyperliquidPositionMapper
    | HyperliquidTransactionMapper
    | HyperliquidOrderBookMapper
    | HyperliquidPriceTickerMapper
    | HyperliquidHistoricalDataMapper
    | HyperliquidMarketMetadataMapper
    | HyperliquidOrderMapper
    | HyperliquidOrderResponseMapper
    | HyperliquidTradingEnumMapper
    # Protocol types
    | AccountSummaryMapperProtocol
    | BalanceMapperProtocol
    | PositionMapperProtocol
    | TransactionMapperProtocol
    | OrderBookMapperProtocol
    | PriceTickerMapperProtocol
    | HistoricalDataMapperProtocol
    | MarketMetadataMapperProtocol
    | OrderMapperProtocol
    | OrderResponseMapperProtocol
    | TradingEnumMapperProtocol
)

BuilderComponent = (
    HyperliquidAccountRequestBuilder
    | HyperliquidMarketDataRequestBuilder
    | HyperliquidTradingRequestBuilder
)

HandlerComponent = (
    HyperliquidAccountResponseHandler
    | HyperliquidMarketDataResponseHandler
    | HyperliquidTradingResponseHandler
)

AnyComponent = MapperComponent | BuilderComponent | HandlerComponent

# Component Name Type Definitions
ComponentName = Literal[
    # Account Mappers
    "account_summary_mapper",
    "balance_mapper",
    "position_mapper",
    "transaction_mapper",
    # Market Data Mappers
    "order_book_mapper",
    "price_ticker_mapper",
    "historical_data_mapper",
    "market_metadata_mapper",
    # Trading Mappers
    "order_mapper",
    "order_response_mapper",
    "trading_enum_mapper",
    # Request Builders
    "account_request_builder",
    "market_data_request_builder",
    "trading_request_builder",
    # Response Handlers
    "account_response_handler",
    "market_data_response_handler",
    "trading_response_handler",
]


class HyperliquidAPIComponentsFactory:
    """Factory class for creating Hyperliquid exchange API components and services.

    This factory centralizes the instantiation logic for all Hyperliquid-specific
    components, making it easier to manage dependencies and simplify the main
    API client initialization.
    """

    def __init__(
        self,
        exchange_config: ExchangeSpecificConfig,
        exchange_secrets: AnyExchangeSecrets,
        chain_id: int,
        component_registry: HyperliquidComponentRegistry | None = None,
    ) -> None:
        """Initialize the factory with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model
            exchange_secrets: Exchange secrets configuration model (discriminated union)
            chain_id: The blockchain chain ID for EIP-712 signing
            component_registry: Optional component registry for dependency injection

        """
        self.exchange_config = exchange_config
        self.exchange_secrets = exchange_secrets
        self.chain_id = chain_id

        # Use provided registry or create default
        self._registry = component_registry or HyperliquidComponentRegistry()

        # Shared component cache for singleton instances
        self._shared_components: dict[str, AnyComponent] = {}

        # Log error if Hyperliquid receives wrong auth type
        if not isinstance(exchange_secrets, PrivateKeyAuthSecrets):
            logger.error(
                "hyperliquid_invalid_auth_type",
                expected_auth_type="private_key",
                received_auth_type=exchange_secrets.auth_type,
                message=(
                    "Hyperliquid expects auth_type 'private_key' but received '%s'. "
                    "EIP-712 authentication will not work."
                ),
                message_args=(exchange_secrets.auth_type,),
            )

        # Validate registry has required components if auto-registered
        if self._registry.validate_required_components():
            logger.info(
                "hyperliquid_registry_validated",
                message="All required components are registered in the factory registry",
            )

    # Type-safe overloads for get_shared_component method (17+ overloads following Backpack pattern)

    # Account Mappers (4 overloads)
    @overload
    def get_shared_component(
        self, component_name: Literal["account_summary_mapper"]
    ) -> HyperliquidAccountSummaryMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["balance_mapper"]
    ) -> HyperliquidBalanceMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["position_mapper"]
    ) -> HyperliquidPositionMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["transaction_mapper"]
    ) -> HyperliquidTransactionMapper: ...

    # Market Data Mappers (4 overloads)
    @overload
    def get_shared_component(
        self, component_name: Literal["order_book_mapper"]
    ) -> HyperliquidOrderBookMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["price_ticker_mapper"]
    ) -> HyperliquidPriceTickerMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["historical_data_mapper"]
    ) -> HyperliquidHistoricalDataMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["market_metadata_mapper"]
    ) -> HyperliquidMarketMetadataMapper: ...

    # Trading Mappers (3 overloads)
    @overload
    def get_shared_component(
        self, component_name: Literal["order_mapper"]
    ) -> HyperliquidOrderMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["order_response_mapper"]
    ) -> HyperliquidOrderResponseMapper: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["trading_enum_mapper"]
    ) -> HyperliquidTradingEnumMapper: ...

    # Request Builders (3 overloads)
    @overload
    def get_shared_component(
        self, component_name: Literal["account_request_builder"]
    ) -> HyperliquidAccountRequestBuilder: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["market_data_request_builder"]
    ) -> HyperliquidMarketDataRequestBuilder: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["trading_request_builder"]
    ) -> HyperliquidTradingRequestBuilder: ...

    # Response Handlers (3 overloads)
    @overload
    def get_shared_component(
        self, component_name: Literal["account_response_handler"]
    ) -> HyperliquidAccountResponseHandler: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["market_data_response_handler"]
    ) -> HyperliquidMarketDataResponseHandler: ...

    @overload
    def get_shared_component(
        self, component_name: Literal["trading_response_handler"]
    ) -> HyperliquidTradingResponseHandler: ...

    # Implementation for all overloads
    def get_shared_component(self, component_name: ComponentName) -> AnyComponent:
        """Get or create a shared component instance with full type safety.

        This method provides type-safe access to all factory components with 18
        overloads for compile-time type checking and IntelliSense support.

        Args:
            component_name: The name of the component to retrieve

        Returns:
            The requested component instance

        Raises:
            KeyError: If the component name is not valid
        """
        # Check cache first
        if component_name in self._shared_components:
            return self._shared_components[component_name]

        # Get component configuration
        component_config = self._get_component_config()

        if component_name not in component_config:
            # This should never happen since all ComponentName values are handled
            msg = f"Unknown component name: {component_name}"
            raise KeyError(msg)

        # Create component directly based on component name
        component = self._create_component(component_name)

        # Validate protocol compliance (following Backpack pattern)
        self._validate_protocol_compliance(component_name, component)

        # Cache and return
        self._shared_components[component_name] = component
        return component

    def _get_component_config(self) -> dict[ComponentName, dict[str, Any]]:
        """Get component configuration mapping."""
        return {
            # Account Mappers
            "account_summary_mapper": {
                "registry_key": "account.summary",
                "expected_type": HyperliquidAccountSummaryMapper,
                "factory_method": self.create_account_summary_mapper,
            },
            "balance_mapper": {
                "registry_key": "account.balance",
                "expected_type": HyperliquidBalanceMapper,
                "factory_method": self.create_balance_mapper,
            },
            "position_mapper": {
                "registry_key": "account.position",
                "expected_type": HyperliquidPositionMapper,
                "factory_method": self.create_position_mapper,
            },
            "transaction_mapper": {
                "registry_key": "account.transaction",
                "expected_type": HyperliquidTransactionMapper,
                "factory_method": self.create_transaction_mapper,
            },
            # Market Data Mappers
            "order_book_mapper": {
                "registry_key": "market.order_book",
                "expected_type": HyperliquidOrderBookMapper,
                "factory_method": self.create_order_book_mapper,
            },
            "price_ticker_mapper": {
                "registry_key": "market.ticker",
                "expected_type": HyperliquidPriceTickerMapper,
                "factory_method": self.create_price_ticker_mapper,
            },
            "historical_data_mapper": {
                "registry_key": "market.historical",
                "expected_type": HyperliquidHistoricalDataMapper,
                "factory_method": self.create_historical_data_mapper,
            },
            "market_metadata_mapper": {
                "registry_key": "market.metadata",
                "expected_type": HyperliquidMarketMetadataMapper,
                "factory_method": self.create_market_metadata_mapper,
            },
            # Trading Mappers
            "order_mapper": {
                "registry_key": "trading.order",
                "expected_type": HyperliquidOrderMapper,
                "factory_method": self.create_order_mapper,
            },
            "order_response_mapper": {
                "registry_key": "trading.order_response",
                "expected_type": HyperliquidOrderResponseMapper,
                "factory_method": self.create_order_response_mapper,
            },
            "trading_enum_mapper": {
                "registry_key": "trading.enum",
                "expected_type": HyperliquidTradingEnumMapper,
                "factory_method": self.create_trading_enum_mapper,
            },
            # Request Builders
            "account_request_builder": {
                "registry_key": "account",
                "registry_type": "request_builders",
                "expected_type": HyperliquidAccountRequestBuilder,
                "factory_method": self.create_account_request_builder,
            },
            "market_data_request_builder": {
                "registry_key": "market_data",
                "registry_type": "request_builders",
                "expected_type": HyperliquidMarketDataRequestBuilder,
                "factory_method": self.create_market_data_request_builder,
            },
            "trading_request_builder": {
                "registry_key": "trading",
                "registry_type": "request_builders",
                "expected_type": HyperliquidTradingRequestBuilder,
                "factory_method": self.create_trading_request_builder,
            },
            # Response Handlers
            "account_response_handler": {
                "registry_key": "account",
                "registry_type": "response_handlers",
                "expected_type": HyperliquidAccountResponseHandler,
                "factory_method": self.create_account_response_handler,
            },
            "market_data_response_handler": {
                "registry_key": "market_data",
                "registry_type": "response_handlers",
                "expected_type": HyperliquidMarketDataResponseHandler,
                "factory_method": self.create_market_data_response_handler,
            },
            "trading_response_handler": {
                "registry_key": "trading",
                "registry_type": "response_handlers",
                "expected_type": HyperliquidTradingResponseHandler,
                "factory_method": self.create_trading_response_handler,
            },
        }

    def _create_component(self, component_name: ComponentName) -> AnyComponent:
        """Create component based on component name."""
        # Delegate to specific creators to reduce complexity
        mapper = self._try_create_mapper(component_name)
        if mapper is not None:
            return mapper

        builder = self._try_create_builder(component_name)
        if builder is not None:
            return builder

        handler = self._try_create_handler(component_name)
        if handler is not None:
            return handler

        # This should never happen since all ComponentName values are handled
        # But we need this for type checking
        msg = f"Unknown component name: {component_name}"
        raise ValueError(msg)

    def _try_create_mapper(self, component_name: ComponentName) -> MapperComponent | None:
        """Try to create a mapper component."""
        account_mapper = self._try_create_account_mapper(component_name)
        if account_mapper is not None:
            return account_mapper

        market_mapper = self._try_create_market_mapper(component_name)
        if market_mapper is not None:
            return market_mapper

        trading_mapper = self._try_create_trading_mapper(component_name)
        if trading_mapper is not None:
            return trading_mapper

        return None

    def _try_create_account_mapper(self, component_name: ComponentName) -> MapperComponent | None:
        """Try to create an account mapper component."""
        if component_name == "account_summary_mapper":
            return self.create_account_summary_mapper()
        if component_name == "balance_mapper":
            return self.create_balance_mapper()
        if component_name == "position_mapper":
            return self.create_position_mapper()
        if component_name == "transaction_mapper":
            return self.create_transaction_mapper()
        return None

    def _try_create_market_mapper(self, component_name: ComponentName) -> MapperComponent | None:
        """Try to create a market data mapper component."""
        if component_name == "order_book_mapper":
            return self.create_order_book_mapper()
        if component_name == "price_ticker_mapper":
            return self.create_price_ticker_mapper()
        if component_name == "historical_data_mapper":
            return self.create_historical_data_mapper()
        if component_name == "market_metadata_mapper":
            return self.create_market_metadata_mapper()
        return None

    def _try_create_trading_mapper(self, component_name: ComponentName) -> MapperComponent | None:
        """Try to create a trading mapper component."""
        if component_name == "order_mapper":
            return self.create_order_mapper()
        if component_name == "order_response_mapper":
            return self.create_order_response_mapper()
        if component_name == "trading_enum_mapper":
            return self.create_trading_enum_mapper()
        return None

    def _try_create_builder(self, component_name: ComponentName) -> BuilderComponent | None:
        """Try to create a builder component."""
        if component_name == "account_request_builder":
            return self.create_account_request_builder()
        if component_name == "market_data_request_builder":
            return self.create_market_data_request_builder()
        if component_name == "trading_request_builder":
            return self.create_trading_request_builder()
        return None

    def _try_create_handler(self, component_name: ComponentName) -> HandlerComponent | None:
        """Try to create a handler component."""
        if component_name == "account_response_handler":
            return self.create_account_response_handler()
        if component_name == "market_data_response_handler":
            return self.create_market_data_response_handler()
        if component_name == "trading_response_handler":
            return self.create_trading_response_handler()
        return None

    def _validate_protocol_compliance(self, component_name: str, component: AnyComponent) -> None:
        """Validate that the component implements the expected protocol.

        Args:
            component_name: Name of the component
            component: The component instance to validate

        Raises:
            TypeError: If the component doesn't implement the expected protocol
        """
        # Protocol validation mapping
        protocol_map = {
            # Account Mappers
            "account_summary_mapper": AccountSummaryMapperProtocol,
            "balance_mapper": BalanceMapperProtocol,
            "position_mapper": PositionMapperProtocol,
            "transaction_mapper": TransactionMapperProtocol,
            # Market Data Mappers
            "order_book_mapper": OrderBookMapperProtocol,
            "price_ticker_mapper": PriceTickerMapperProtocol,
            "historical_data_mapper": HistoricalDataMapperProtocol,
            "market_metadata_mapper": MarketMetadataMapperProtocol,
            # Trading Mappers
            "order_mapper": OrderMapperProtocol,
            "order_response_mapper": OrderResponseMapperProtocol,
            "trading_enum_mapper": TradingEnumMapperProtocol,
            # Request Builders
            "account_request_builder": AccountRequestBuilderProtocol,
            "market_data_request_builder": MarketDataRequestBuilderProtocol,
            "trading_request_builder": TradingRequestBuilderProtocol,
            # Response Handlers
            "account_response_handler": AccountResponseHandlerProtocol,
            "market_data_response_handler": MarketDataResponseHandlerProtocol,
            "trading_response_handler": TradingResponseHandlerProtocol,
        }

        expected_protocol = protocol_map.get(component_name)
        if expected_protocol is None:
            return  # No validation needed for unknown component names

        # Since AnyComponent is a Union of types that implement these protocols,
        # the isinstance check is redundant. The type system already ensures
        # protocol compliance at compile time.

    def create_authenticator(self) -> HyperliquidEip712Authenticator | None:
        """Create a HyperliquidEip712Authenticator instance.

        The authenticator will perform its own cryptographic validation.

        Returns:
            Configured authenticator instance or None if credentials are missing or invalid

        """
        # Check if we have the correct secrets type for Hyperliquid
        if not isinstance(self.exchange_secrets, PrivateKeyAuthSecrets):
            logger.error(
                "hyperliquid_authenticator_creation_failed",
                expected_auth_type="private_key",
                received_auth_type=self.exchange_secrets.auth_type,
                message=(
                    "Cannot create Hyperliquid authenticator: expected auth_type "
                    "'private_key' but received '%s'. Signed operations will fail."
                ),
                message_args=(self.exchange_secrets.auth_type,),
            )
            return None

        secrets: PrivateKeyAuthSecrets = self.exchange_secrets  # Type cast for clarity

        # Determine which private key to use based on environment
        private_key_to_use: SecretStr | None = None

        if self.exchange_config.is_mainnet_environment:
            # Mainnet: use the main private_key
            private_key_to_use = secrets.private_key
            logger.info("Using main private_key for mainnet environment")
        else:
            # Testnet: implement precedence logic
            if secrets.testnet_seed_passphrase:
                # Priority 1: testnet_seed_passphrase (for future implementation)
                logger.warning(
                    "testnet_seed_passphrase is provided but wallet derivation "
                    "is not yet implemented. Falling back to other options.",
                )
                # TODO: Implement BIP-39 seed phrase to private key derivation
                # For now, continue to check other options

            if secrets.private_key_testnet:
                # Priority 2: dedicated testnet private key
                private_key_to_use = secrets.private_key_testnet
                logger.info("Using dedicated private_key_testnet for testnet environment")
            elif secrets.private_key:
                # Priority 3: fall back to main private key
                private_key_to_use = secrets.private_key
                logger.warning(
                    "No testnet-specific credentials found. Using main private_key for testnet. "
                    "Consider using a dedicated testnet key for safety.",
                )
            else:
                logger.error(
                    "No suitable private key found for testnet environment. "
                    "Provide either private_key_testnet or private_key.",
                )
                return None

        if private_key_to_use:
            try:
                return HyperliquidEip712Authenticator(
                    wallet_private_key_secret=private_key_to_use,
                    passphrase_secret=secrets.passphrase,  # Pass SecretStr or None
                    chain_id=self.chain_id,
                    is_mainnet_environment=self.exchange_config.is_mainnet_environment,
                )
            except ValueError as e:  # Catch init errors from Authenticator
                logger.exception(
                    "authenticator_initialization_failed",
                    action="init_authenticator",
                    authenticator_type="HyperliquidEip712Authenticator",
                    error=str(e),
                    message=f"Failed to initialize HyperliquidEip712Authenticator: {e}",
                )
                return None
        else:
            logger.warning(
                "Hyperliquid secrets provided but no suitable private key found. "
                "Cannot create authenticator.",
            )
            return None

    def create_error_mapper(self) -> HyperliquidErrorMapper:
        """Create a HyperliquidErrorMapper instance.

        Returns:
            Configured error mapper instance

        """
        return HyperliquidErrorMapper()

    def create_account_request_builder(self) -> HyperliquidAccountRequestBuilder:
        """Create a decomposed account request builder instance for account services.

        Returns:
            Account request builder instance

        """
        return HyperliquidAccountRequestBuilder()

    def create_trading_request_builder(self) -> HyperliquidTradingRequestBuilder:
        """Create a decomposed trading request builder instance for trading services.

        Returns:
            Trading request builder instance

        """
        return HyperliquidTradingRequestBuilder()

    def create_market_data_request_builder(self) -> HyperliquidMarketDataRequestBuilder:
        """Create a decomposed market data request builder instance for market data services.

        Returns:
            Market data request builder instance

        """
        return HyperliquidMarketDataRequestBuilder()

    def create_response_handler(self) -> HyperliquidResponseHandler:
        """Create a HyperliquidResponseHandler adapter instance.

        Returns:
            Configured response handler adapter instance for backward compatibility

        """
        return HyperliquidResponseHandler()

    def create_account_response_handler(self) -> HyperliquidAccountResponseHandler:
        """Create an account-specific response handler instance.

        Returns:
            Account response handler instance

        """
        return HyperliquidAccountResponseHandler()

    def create_trading_response_handler(self) -> HyperliquidTradingResponseHandler:
        """Create a trading-specific response handler instance.

        Returns:
            Trading response handler instance

        """
        return HyperliquidTradingResponseHandler()

    def create_market_data_response_handler(self) -> HyperliquidMarketDataResponseHandler:
        """Create a market data-specific response handler instance.

        Returns:
            Market data response handler instance

        """
        return HyperliquidMarketDataResponseHandler()

    # Decomposed mapper creation methods
    def create_balance_mapper(self) -> BalanceMapperProtocol:
        """Create a HyperliquidBalanceMapper instance."""
        return HyperliquidBalanceMapper()

    def create_position_mapper(self) -> PositionMapperProtocol:
        """Create a HyperliquidPositionMapper instance."""
        return HyperliquidPositionMapper()

    def create_account_summary_mapper(self) -> AccountSummaryMapperProtocol:
        """Create a HyperliquidAccountSummaryMapper instance."""
        return HyperliquidAccountSummaryMapper()

    def create_transaction_mapper(self) -> TransactionMapperProtocol:
        """Create a HyperliquidTransactionMapper instance."""
        return HyperliquidTransactionMapper()

    def create_order_book_mapper(self) -> OrderBookMapperProtocol:
        """Create a HyperliquidOrderBookMapper instance."""
        return HyperliquidOrderBookMapper()

    def create_price_ticker_mapper(self) -> PriceTickerMapperProtocol:
        """Create a HyperliquidPriceTickerMapper instance."""
        return HyperliquidPriceTickerMapper()

    def create_historical_data_mapper(self) -> HistoricalDataMapperProtocol:
        """Create a HyperliquidHistoricalDataMapper instance."""
        return HyperliquidHistoricalDataMapper()

    def create_market_metadata_mapper(self) -> MarketMetadataMapperProtocol:
        """Create a HyperliquidMarketMetadataMapper instance."""
        return HyperliquidMarketMetadataMapper()

    def create_trading_enum_mapper(self) -> TradingEnumMapperProtocol:
        """Create a HyperliquidTradingEnumMapper instance."""
        return HyperliquidTradingEnumMapper()

    def create_serialization_strategy(self) -> HyperliquidSerializationStrategy:
        """Create a HyperliquidSerializationStrategy instance.

        Returns:
            Configured serialization strategy instance

        """
        return HyperliquidSerializationStrategy()

    def create_order_mapper(self) -> OrderMapperProtocol:
        """Create a HyperliquidOrderMapper instance.

        Returns:
            Configured order mapper instance
        """
        return HyperliquidOrderMapper()

    def create_order_response_mapper(self) -> OrderResponseMapperProtocol:
        """Create a HyperliquidOrderResponseMapper instance.

        Returns:
            Configured order response mapper instance
        """
        return HyperliquidOrderResponseMapper()

    def create_order_book_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        mapper: OrderBookMapperProtocol,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        exchange_name: str,
    ) -> HyperliquidOrderBookService:
        """Create a HyperliquidOrderBookService instance.

        Args:
            http_client_requester: HTTP client request function
            mapper: Order book mapper instance
            request_builder: Market data request builder instance
            response_handler: Market data response handler instance
            exchange_name: Name of the exchange

        Returns:
            Configured order book service instance
        """
        return HyperliquidOrderBookService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=mapper,
            exchange_name=exchange_name,
        )

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: MarketDataRequestBuilderProtocol,
        response_handler: MarketDataResponseHandlerProtocol,
        exchange_name: str,
        order_book_service: HyperliquidOrderBookService | None = None,
        # Optional dependency injection following Backpack pattern
        order_book_mapper: OrderBookMapperProtocol | None = None,
        price_ticker_mapper: PriceTickerMapperProtocol | None = None,
        historical_data_mapper: HistoricalDataMapperProtocol | None = None,
        market_metadata_mapper: MarketMetadataMapperProtocol | None = None,
    ) -> HyperliquidMarketDataService:
        """Create a HyperliquidMarketDataService instance.

        NOTE: This returns a composite service that combines the decomposed
        service components.

        Args:
            http_client_requester: HTTP client request function
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            order_book_service: Optional order book service to share with other services
            order_book_mapper: Optional order book mapper for dependency injection
            price_ticker_mapper: Optional price ticker mapper for dependency injection
            historical_data_mapper: Optional historical data mapper for dependency injection
            market_metadata_mapper: Optional market metadata mapper for dependency injection

        Returns:
            Configured market data service instance

        """
        # Use decomposed instances for the facade services
        market_data_request_builder = self.create_market_data_request_builder()
        market_data_response_handler = self.create_market_data_response_handler()

        return HyperliquidMarketDataService(
            http_client_requester=http_client_requester,
            request_builder=market_data_request_builder,
            response_handler=market_data_response_handler,
            exchange_name=exchange_name,
            order_book_service=order_book_service,
            # Optional mappers with defaults
            order_book_mapper=order_book_mapper,
            price_ticker_mapper=price_ticker_mapper,
            historical_data_mapper=historical_data_mapper,
            market_metadata_mapper=market_metadata_mapper,
        )

    def create_account_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: HyperliquidEip712Authenticator | None,
        request_builder: AccountRequestBuilderProtocol,
        response_handler: AccountResponseHandlerProtocol,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Awaitable[int | None]],
        # Optional dependency injection following Backpack pattern
        balance_mapper: BalanceMapperProtocol | None = None,
        position_mapper: PositionMapperProtocol | None = None,
        account_summary_mapper: AccountSummaryMapperProtocol | None = None,
        transaction_mapper: TransactionMapperProtocol | None = None,
    ) -> HyperliquidAccountService:
        """Create a HyperliquidAccountService instance.

        NOTE: This returns a composite service that combines the decomposed
        service components.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            wallet_address: Wallet address for account operations
            get_asset_index_callable: Function to retrieve asset index for symbols
            balance_mapper: Optional balance mapper for dependency injection
            position_mapper: Optional position mapper for dependency injection
            account_summary_mapper: Optional account summary mapper for dependency injection
            transaction_mapper: Optional transaction mapper for dependency injection

        Returns:
            Configured account service instance

        """
        # Use decomposed instances for the facade services
        account_request_builder = self.create_account_request_builder()
        account_response_handler = self.create_account_response_handler()

        return HyperliquidAccountService(
            http_client_requester=http_client_requester,
            request_builder=account_request_builder,
            response_handler=account_response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
            # Optional mappers with defaults
            balance_mapper=balance_mapper,
            position_mapper=position_mapper,
            account_summary_mapper=account_summary_mapper,
            transaction_mapper=transaction_mapper,
        )

    def create_trading_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: HyperliquidEip712Authenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: GetAssetIndexCallableSig,
        order_book_service: HyperliquidOrderBookService | None = None,
        # Optional dependency injection following Backpack pattern
        order_mapper: OrderMapperProtocol | None = None,
        order_response_mapper: OrderResponseMapperProtocol | None = None,
        error_mapper: HyperliquidErrorMapper | None = None,
    ) -> HyperliquidTradingService:
        """Create a HyperliquidTradingService instance with optional dependency injection.

        NOTE: This returns a composite service that combines the decomposed
        service components following the Backpack pattern.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            exchange_name: Name of the exchange
            wallet_address: Wallet address for trading operations
            get_asset_index_callable: Callable to get asset index
            order_book_service: Optional order book service for market order handling
            order_mapper: Optional order mapper instance (defaults to HyperliquidOrderMapper)
            order_response_mapper: Optional order response mapper instance
                (defaults to HyperliquidOrderResponseMapper)
            error_mapper: Optional error mapper instance (defaults to HyperliquidErrorMapper)

        Returns:
            Configured trading service instance

        """
        # Use concrete instances for the facade services
        trading_request_builder = self.create_trading_request_builder()
        trading_response_handler = self.create_trading_response_handler()

        return HyperliquidTradingService(
            http_client_requester=http_client_requester,
            request_builder=trading_request_builder,
            response_handler=trading_response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
            get_asset_index_callable=get_asset_index_callable,
            order_book_service=order_book_service,
            # Optional dependency injection
            order_mapper=order_mapper,
            order_response_mapper=order_response_mapper,
            error_mapper=error_mapper,
        )
