"""
BackpackAPIComponentsFactory: Centralized factory for Backpack exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Backpack exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import TYPE_CHECKING, Any

from cyberdelta.apis.backpack.bp_auth import BackpackHmacAuthenticator
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
from cyberdelta.apis.connectivity.rate_limiter_service import RateLimiterService
from cyberdelta.utils.logging_config import get_logger

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the factory will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]


class BackpackAPIComponentsFactory:
    """
    Factory class for creating Backpack exchange API components and services.

    This factory centralizes the instantiation logic for all Backpack-specific
    components, making it easier to manage dependencies and simplify the main
    API client initialization.
    """

    def __init__(self, api_config: dict[str, Any], secrets: dict[str, str | None]) -> None:
        """
        Initialize the factory with configuration and secrets.

        Args:
            api_config: Configuration dictionary with API settings
            secrets: Dictionary containing API credentials
        """
        self.api_config = api_config
        self.secrets = secrets
        self._api_key = secrets.get("BACKPACK_API_KEY")
        self._api_secret = secrets.get("BACKPACK_API_SECRET")

    def create_authenticator(self) -> BackpackHmacAuthenticator | None:
        """
        Create a BackpackHmacAuthenticator instance.

        Returns:
            Configured authenticator instance or None if credentials are missing
        """
        if self._api_key and self._api_secret:
            return BackpackHmacAuthenticator(api_key=self._api_key, api_secret=self._api_secret)
        else:
            logger.warning(
                "Backpack API key/secret not provided. Signed operations will fail. "
                "Authenticator not initialized."
            )
            return None

    def create_error_mapper(self) -> BackpackErrorMapper:
        """
        Create a BackpackErrorMapper instance.

        Returns:
            Configured error mapper instance
        """
        return BackpackErrorMapper()

    def create_request_builder(self) -> BackpackRequestBuilder:
        """
        Create a BackpackRequestBuilder instance.

        Returns:
            Configured request builder instance
        """
        return BackpackRequestBuilder(self.api_config)

    def create_response_handler(self) -> BackpackResponseHandler:
        """
        Create a BackpackResponseHandler instance.

        Returns:
            Configured response handler instance
        """
        return BackpackResponseHandler()

    def create_market_data_mapper(self) -> BackpackMarketDataMapper:
        """
        Create a BackpackMarketDataMapper instance.

        Returns:
            Configured market data mapper instance
        """
        return BackpackMarketDataMapper()

    def create_account_data_mapper(self) -> BackpackAccountDataMapper:
        """
        Create a BackpackAccountDataMapper instance.

        Returns:
            Configured account data mapper instance
        """
        return BackpackAccountDataMapper()

    def create_trading_data_mapper(self) -> BackpackTradingDataMapper:
        """
        Create a BackpackTradingDataMapper instance.

        Returns:
            Configured trading data mapper instance
        """
        return BackpackTradingDataMapper()

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        rate_limiter_service: RateLimiterService,
        market_data_mapper: BackpackMarketDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackMarketDataService:
        """
        Create a BackpackMarketDataService instance.

        Args:
            http_client_requester: HTTP client request function
            rate_limiter_service: Rate limiter service instance
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
            rate_limiter_service=rate_limiter_service,
            mapper=market_data_mapper,
        )

    def create_account_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        rate_limiter_service: RateLimiterService,
        authenticator: BackpackHmacAuthenticator | None,
        account_data_mapper: BackpackAccountDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackAccountService:
        """
        Create a BackpackAccountService instance.

        Args:
            http_client_requester: HTTP client request function
            rate_limiter_service: Rate limiter service instance
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
            rate_limiter_service=rate_limiter_service,
            mapper=account_data_mapper,
        )

    def create_trading_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        rate_limiter_service: RateLimiterService,
        authenticator: BackpackHmacAuthenticator | None,
        trading_data_mapper: BackpackTradingDataMapper,
        request_builder: BackpackRequestBuilder,
        response_handler: BackpackResponseHandler,
        exchange_name: str,
    ) -> BackpackTradingService:
        """
        Create a BackpackTradingService instance.

        Args:
            http_client_requester: HTTP client request function
            rate_limiter_service: Rate limiter service instance
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
            rate_limiter_service=rate_limiter_service,
            mapper=trading_data_mapper,
        )
