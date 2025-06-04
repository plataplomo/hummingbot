"""BackpackAPIComponentsFactory: Centralized factory for Backpack exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Backpack exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from cyberdelta.apis.backpack.bp_auth import BackpackEd25519Authenticator
from cyberdelta.apis.backpack.bp_error_mapper import BackpackErrorMapper
from cyberdelta.apis.backpack.bp_request_builder import BackpackRequestBuilder
from cyberdelta.apis.backpack.bp_response_handler import BackpackResponseHandler
from cyberdelta.apis.backpack.mappers.bp_account_data_mapper import BackpackAccountDataMapper
from cyberdelta.apis.backpack.mappers.bp_market_data_mapper import BackpackMarketDataMapper
from cyberdelta.apis.backpack.mappers.bp_trading_data_mapper import BackpackTradingDataMapper
from cyberdelta.apis.backpack.services.bp_account_service import BackpackAccountService
from cyberdelta.apis.backpack.services.bp_market_data_service import BackpackMarketDataService
from cyberdelta.apis.backpack.services.bp_trading_service import BackpackTradingService
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import AnyExchangeSecrets, ApiKeyAuthSecrets

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the factory will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
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
    ) -> None:
        """Initialize the factory with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model
            exchange_secrets: Exchange secrets configuration model (discriminated union)

        """
        self.exchange_config = exchange_config
        self.exchange_secrets = exchange_secrets

        # Log error if Backpack receives wrong auth type
        if not isinstance(exchange_secrets, ApiKeyAuthSecrets):
            logger.error(
                f"Backpack expects auth_type 'api_key' but received "
                f"'{exchange_secrets.auth_type}'. ED25519 authentication will not work.",
            )

    def create_authenticator(self) -> BackpackEd25519Authenticator | None:
        """Create a BackpackEd25519Authenticator instance.

        The authenticator will perform its own cryptographic validation.

        Returns:
            Configured ED25519 authenticator instance or None if credentials are missing or invalid

        """
        # Check if we have the correct secrets type for Backpack
        if not isinstance(self.exchange_secrets, ApiKeyAuthSecrets):
            logger.error(
                f"Cannot create Backpack authenticator: expected auth_type 'api_key' "
                f"but received '{self.exchange_secrets.auth_type}'. "
                f"Signed operations will fail.",
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
                logger.error(f"Failed to initialize BackpackEd25519Authenticator: {e}")
                return None
        else:
            logger.warning(
                "Backpack secrets (api_key or api_secret as SecretStr) not fully provided. "
                "Cannot create ED25519 authenticator.",
            )
            return None

    def create_error_mapper(self) -> BackpackErrorMapper:
        """Create a BackpackErrorMapper instance.

        Returns:
            Configured error mapper instance

        """
        return BackpackErrorMapper()

    def create_request_builder(self) -> BackpackRequestBuilder:
        """Create a BackpackRequestBuilder instance.

        Returns:
            Configured request builder instance

        """
        return BackpackRequestBuilder(self.exchange_config)

    def create_response_handler(self) -> BackpackResponseHandler:
        """Create a BackpackResponseHandler instance.

        Returns:
            Configured response handler instance

        """
        return BackpackResponseHandler()

    def create_market_data_mapper(self) -> BackpackMarketDataMapper:
        """Create a BackpackMarketDataMapper instance.

        Returns:
            Configured market data mapper instance

        """
        return BackpackMarketDataMapper()

    def create_account_data_mapper(self) -> BackpackAccountDataMapper:
        """Create a BackpackAccountDataMapper instance.

        Returns:
            Configured account data mapper instance

        """
        return BackpackAccountDataMapper()

    def create_trading_data_mapper(self) -> BackpackTradingDataMapper:
        """Create a BackpackTradingDataMapper instance.

        Returns:
            Configured trading data mapper instance

        """
        return BackpackTradingDataMapper()

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        market_data_mapper: BackpackMarketDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackMarketDataService:
        """Create a BackpackMarketDataService instance.

        Args:
            http_client_requester: HTTP client request function
            market_data_mapper: Market data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange

        Returns:
            Configured market data service instance

        """
        return BackpackMarketDataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            exchange_name=exchange_name,
            mapper=market_data_mapper,
        )

    def create_account_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: BackpackEd25519Authenticator | None,
        account_data_mapper: BackpackAccountDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackAccountService:
        """Create a BackpackAccountService instance.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            account_data_mapper: Account data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange

        Returns:
            Configured account service instance

        """
        return BackpackAccountService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            mapper=account_data_mapper,
        )

    def create_trading_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: BackpackEd25519Authenticator | None,
        trading_data_mapper: BackpackTradingDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackTradingService:
        """Create a BackpackTradingService instance.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            trading_data_mapper: Trading data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange

        Returns:
            Configured trading service instance

        """
        return BackpackTradingService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            mapper=trading_data_mapper,
        )
