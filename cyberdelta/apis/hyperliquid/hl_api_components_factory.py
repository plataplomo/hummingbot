"""HyperliquidAPIComponentsFactory: Centralized factory for Hyperliquid exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Hyperliquid exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any

from pydantic import SecretStr

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_payload_serialization_strategy import (
    HyperliquidSerializationStrategy,
)
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper import HyperliquidMarketDataMapper
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.services.hl_account_service import HyperliquidAccountService
from cyberdelta.apis.hyperliquid.services.hl_market_data_service import HyperliquidMarketDataService
from cyberdelta.apis.hyperliquid.services.hl_trading_service import HyperliquidTradingService
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import AnyExchangeSecrets, PrivateKeyAuthSecrets

if TYPE_CHECKING:
    pass

logger = get_logger(__name__)

# Type alias for the HTTP client requester callable that the factory will use.
# This should match the signature of ExchangeAPI._request
HttpClientRequesterSig = Callable[
    ...,
    Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]

# Type alias for the get_asset_index callable
GetAssetIndexCallableSig = Callable[[str], Coroutine[Any, Any, int]]


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
    ) -> None:
        """Initialize the factory with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model
            exchange_secrets: Exchange secrets configuration model (discriminated union)
            chain_id: The blockchain chain ID for EIP-712 signing

        """
        self.exchange_config = exchange_config
        self.exchange_secrets = exchange_secrets
        self.chain_id = chain_id

        # Log error if Hyperliquid receives wrong auth type
        if not isinstance(exchange_secrets, PrivateKeyAuthSecrets):
            logger.error(
                f"Hyperliquid expects auth_type 'private_key' but received "
                f"'{exchange_secrets.auth_type}'. EIP-712 authentication will not work.",
            )

    def create_authenticator(self) -> HyperliquidEip712Authenticator | None:
        """Create a HyperliquidEip712Authenticator instance.

        The authenticator will perform its own cryptographic validation.

        Returns:
            Configured authenticator instance or None if credentials are missing or invalid

        """
        # Check if we have the correct secrets type for Hyperliquid
        if not isinstance(self.exchange_secrets, PrivateKeyAuthSecrets):
            logger.error(
                f"Cannot create Hyperliquid authenticator: expected auth_type 'private_key' "
                f"but received '{self.exchange_secrets.auth_type}'. "
                f"Signed operations will fail.",
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
                logger.error(f"Failed to initialize HyperliquidEip712Authenticator: {e}")
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

    def create_request_builder(self) -> HyperliquidRequestBuilder:
        """Create a HyperliquidRequestBuilder instance.

        Returns:
            Configured request builder instance

        """
        return HyperliquidRequestBuilder()

    def create_response_handler(self) -> HyperliquidResponseHandler:
        """Create a HyperliquidResponseHandler instance.

        Returns:
            Configured response handler instance

        """
        return HyperliquidResponseHandler()

    def create_market_data_mapper(self) -> HyperliquidMarketDataMapper:
        """Create a HyperliquidMarketDataMapper instance.

        Returns:
            Configured market data mapper instance

        """
        return HyperliquidMarketDataMapper()

    def create_account_data_mapper(self) -> HyperliquidAccountDataMapper:
        """Create a HyperliquidAccountDataMapper instance.

        Returns:
            Configured account data mapper instance

        """
        return HyperliquidAccountDataMapper()

    def create_trading_data_mapper(self) -> HyperliquidTradingDataMapper:
        """Create a HyperliquidTradingDataMapper instance.

        Returns:
            Configured trading data mapper instance

        """
        return HyperliquidTradingDataMapper()

    def create_serialization_strategy(self) -> HyperliquidSerializationStrategy:
        """Create a HyperliquidSerializationStrategy instance.

        Returns:
            Configured serialization strategy instance

        """
        return HyperliquidSerializationStrategy()

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        market_data_mapper: HyperliquidMarketDataMapper,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        exchange_name: str,
    ) -> HyperliquidMarketDataService:
        """Create a HyperliquidMarketDataService instance.

        Args:
            http_client_requester: HTTP client request function
            market_data_mapper: Market data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange

        Returns:
            Configured market data service instance

        """
        return HyperliquidMarketDataService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            mapper=market_data_mapper,
            exchange_name=exchange_name,
        )

    def create_account_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: HyperliquidEip712Authenticator | None,
        account_data_mapper: HyperliquidAccountDataMapper,
        trading_data_mapper: HyperliquidTradingDataMapper,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Awaitable[int]],
    ) -> HyperliquidAccountService:
        """Create a HyperliquidAccountService instance.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            account_data_mapper: Account data mapper instance
            trading_data_mapper: Trading data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            wallet_address: Wallet address for account operations
            get_asset_index_callable: Function to retrieve asset index for symbols

        Returns:
            Configured account service instance

        """
        return HyperliquidAccountService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
            account_mapper=account_data_mapper,
            trading_mapper=trading_data_mapper,
            get_asset_index_callable=get_asset_index_callable,
        )

    def create_trading_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        authenticator: HyperliquidEip712Authenticator | None,
        trading_data_mapper: HyperliquidTradingDataMapper,
        error_mapper: HyperliquidErrorMapper,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: GetAssetIndexCallableSig,
    ) -> HyperliquidTradingService:
        """Create a HyperliquidTradingService instance.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            trading_data_mapper: Trading data mapper instance
            error_mapper: Error mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            wallet_address: Wallet address for trading operations
            get_asset_index_callable: Callable to get asset index

        Returns:
            Configured trading service instance

        """
        return HyperliquidTradingService(
            http_client_requester=http_client_requester,
            request_builder=request_builder,
            response_handler=response_handler,
            authenticator=authenticator,
            exchange_name=exchange_name,
            wallet_address=wallet_address,
            get_asset_index_callable=get_asset_index_callable,
            trading_mapper=trading_data_mapper,
            error_mapper=error_mapper,
        )
