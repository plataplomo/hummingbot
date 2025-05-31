"""
HyperliquidAPIComponentsFactory: Centralized factory for Hyperliquid exchange components.

This factory class is responsible for creating and configuring all the necessary
components for the Hyperliquid exchange API, including authenticators, error mappers,
request builders, response handlers, domain data mappers, and service classes.
"""

from __future__ import annotations

from collections.abc import Callable, Coroutine, Mapping
from typing import TYPE_CHECKING, Any

from eth_account.account import Account
from mnemonic import Mnemonic

from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_auth import HyperliquidEip712Authenticator
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
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
    ..., Coroutine[Any, Any, tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]

# Type alias for the get_asset_index callable
GetAssetIndexCallableSig = Callable[[str], Coroutine[Any, Any, int]]


class HyperliquidAPIComponentsFactory:
    """
    Factory class for creating Hyperliquid exchange API components and services.

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
        """
        Initialize the factory with configuration and secrets.

        Args:
            exchange_config: Exchange-specific configuration model
            exchange_secrets: Exchange secrets configuration model (discriminated union)
            chain_id: The blockchain chain ID for EIP-712 signing
        """
        self.exchange_config = exchange_config
        self.exchange_secrets = exchange_secrets
        self.chain_id = chain_id
        
        # Initialize private key credentials based on secret type
        if isinstance(exchange_secrets, PrivateKeyAuthSecrets):
            self._private_key: str | None = exchange_secrets.private_key.get_secret_value()
            self._passphrase: str | None = (
                exchange_secrets.passphrase.get_secret_value() 
                if exchange_secrets.passphrase 
                else None
            )
        else:
            # Log error if Hyperliquid receives wrong auth type
            logger.error(
                f"Hyperliquid expects auth_type 'private_key' but received "
                f"'{exchange_secrets.auth_type}'. EIP-712 authentication will not work."
            )
            self._private_key = None
            self._passphrase = None

    def create_authenticator(self) -> HyperliquidEip712Authenticator | None:
        """
        Create a HyperliquidEip712Authenticator instance.

        Performs cryptographic validation of the private key or passphrase before
        creating the authenticator instance.

        Returns:
            Configured authenticator instance or None if credentials are missing or invalid
        """
        # Check if we have the correct secrets type for Hyperliquid
        if not isinstance(self.exchange_secrets, PrivateKeyAuthSecrets):
            logger.error(
                f"Cannot create Hyperliquid authenticator: expected auth_type 'private_key' "
                f"but received '{self.exchange_secrets.auth_type}'. "
                f"Signed operations will fail."
            )
            return None

        if self._private_key:
            try:
                # Cryptographic validation of private key
                pk_str = self._private_key.strip()
                
                # Strip "0x" prefix if present
                processed_pk_str = pk_str[2:] if pk_str.startswith("0x") else pk_str

                # Validate format (64-character hex string)
                if not (
                    len(processed_pk_str) == 64
                    and all(c in "0123456789abcdefABCDEF" for c in processed_pk_str)
                ):
                    raise ValueError(
                        "Hyperliquid private_key must be a 64-character hex string "
                        "(with or without '0x' prefix)."
                    )

                # Cryptographic validation using eth_account
                try:
                    Account.from_key(processed_pk_str)
                except Exception as e:
                    raise ValueError(
                        f"Hyperliquid private_key is not cryptographically valid: {e}"
                    ) from e

                # Create authenticator if validation passes
                return HyperliquidEip712Authenticator(
                    wallet_private_key=self._private_key,
                    chain_id=self.chain_id,
                )
            except ValueError as e:
                logger.error(f"Failed to create HL authenticator: {e}. Signed endpoints will fail.")
                return None
        elif self._passphrase:
            try:
                # Passphrase cryptographic validation
                phrase_str = self._passphrase.strip()

                # Word count check (12 or 24 words)
                num_words = len(phrase_str.split())
                if num_words not in (12, 24):
                    raise ValueError(
                        f"Hyperliquid passphrase must consist of 12 or 24 words, "
                        f"got {num_words} words."
                    )

                # BIP-39 mnemonic validation
                try:
                    mnemonic_validator = Mnemonic("english")
                    if not mnemonic_validator.check(phrase_str):
                        raise ValueError(
                            "Hyperliquid passphrase is not a valid BIP-39 mnemonic "
                            "(checksum or wordlist error)."
                        )
                except Exception as e:
                    # Handle any other exceptions from mnemonic validation
                    if "not a valid BIP-39 mnemonic" not in str(e):
                        raise ValueError(
                            f"Error validating Hyperliquid passphrase with mnemonic library: {e}"
                        ) from e
                    raise

                # TODO: Implement passphrase-based authentication if supported
                logger.error(
                    "Passphrase-based authentication for Hyperliquid not yet fully implemented."
                )
                return None
            except ValueError as e:
                logger.error(f"Failed to validate HL passphrase: {e}. Signed endpoints will fail.")
                return None
        else:
            logger.warning(
                "Hyperliquid: Neither private_key nor passphrase provided. Signed ops will fail."
            )
            return None

    def create_error_mapper(self) -> HyperliquidErrorMapper:
        """
        Create a HyperliquidErrorMapper instance.

        Returns:
            Configured error mapper instance
        """
        return HyperliquidErrorMapper()

    def create_request_builder(self) -> HyperliquidRequestBuilder:
        """
        Create a HyperliquidRequestBuilder instance.

        Returns:
            Configured request builder instance
        """
        return HyperliquidRequestBuilder()

    def create_response_handler(self) -> HyperliquidResponseHandler:
        """
        Create a HyperliquidResponseHandler instance.

        Returns:
            Configured response handler instance
        """
        return HyperliquidResponseHandler()

    def create_market_data_mapper(self) -> HyperliquidMarketDataMapper:
        """
        Create a HyperliquidMarketDataMapper instance.

        Returns:
            Configured market data mapper instance
        """
        return HyperliquidMarketDataMapper()

    def create_account_data_mapper(self) -> HyperliquidAccountDataMapper:
        """
        Create a HyperliquidAccountDataMapper instance.

        Returns:
            Configured account data mapper instance
        """
        return HyperliquidAccountDataMapper()

    def create_trading_data_mapper(self) -> HyperliquidTradingDataMapper:
        """
        Create a HyperliquidTradingDataMapper instance.

        Returns:
            Configured trading data mapper instance
        """
        return HyperliquidTradingDataMapper()

    def create_market_data_service(
        self,
        http_client_requester: HttpClientRequesterSig,
        market_data_mapper: HyperliquidMarketDataMapper,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        exchange_name: str,
    ) -> HyperliquidMarketDataService:
        """
        Create a HyperliquidMarketDataService instance.

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
    ) -> HyperliquidAccountService:
        """
        Create a HyperliquidAccountService instance.

        Args:
            http_client_requester: HTTP client request function
            authenticator: Authenticator instance
            account_data_mapper: Account data mapper instance
            trading_data_mapper: Trading data mapper instance
            request_builder: Request builder instance
            response_handler: Response handler instance
            exchange_name: Name of the exchange
            wallet_address: Wallet address for account operations

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
        """
        Create a HyperliquidTradingService instance.

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
