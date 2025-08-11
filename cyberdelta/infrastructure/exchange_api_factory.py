"""Exchange API Factory for dynamic API client creation.

This module provides a truly exchange-agnostic factory for creating
exchange API clients based on configuration. It eliminates hardcoded
exchange names and imports, making it scalable to support 20+ exchanges.

Architecture:
- Configuration-driven API client creation
- Dynamic import system for exchange API classes
- Type-safe factory pattern with proper error handling
- Follows CODING_STANDARDS.md - no hardcoding, no assumptions
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, ClassVar, cast

from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.config.models.exchange_config import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import AnyExchangeSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.exceptions.base import ConfigurationError


if TYPE_CHECKING:
    from types import ModuleType

logger = get_logger(__name__)


class ExchangeAPIFactory:
    """Factory for creating exchange API clients dynamically.

    This factory creates API clients for any supported exchange without
    hardcoded imports or exchange-specific logic. It scales to 20+ exchanges
    by using dynamic imports and configuration-driven creation.

    IMPORTANT: Following CODING_STANDARDS.md:
    - NO hardcoded exchange names or imports
    - ALL exchange information from configuration
    - Explicit error handling with context
    - NO assumptions about exchange availability
    """

    # Exchange-to-module mapping for dynamic imports
    # This is the ONLY place exchange-specific information is stored
    _EXCHANGE_MODULE_MAPPING: ClassVar[dict[ExchangeName, dict[str, str]]] = {
        ExchangeName.HYPERLIQUID: {
            "module": "cyberdelta.apis.hyperliquid.hl_api",
            "class": "HyperliquidAPI",
        },
        ExchangeName.BACKPACK: {
            "module": "cyberdelta.apis.backpack.bp_api",
            "class": "BackpackAPI",
        },
    }

    @classmethod
    def create_api_client(
        cls,
        exchange_name: ExchangeName,
        exchange_config: ExchangeSpecificConfig,
        exchange_secrets: AnyExchangeSecrets,
    ) -> ExchangeAPI:
        """Create API client for specified exchange.

        Args:
            exchange_name: Exchange enum identifying the exchange
            exchange_config: Exchange-specific configuration
            exchange_secrets: Exchange-specific secrets

        Returns:
            ExchangeAPI: Initialized API client instance

        Raises:
            ConfigurationError: If exchange is not supported or creation fails
        """
        logger.debug(
            "creating_exchange_api_client",
            exchange_name=exchange_name.value,
            exchange_enabled=exchange_config.enabled,
        )

        # Get module information for this exchange
        module_info = cls._EXCHANGE_MODULE_MAPPING.get(exchange_name)
        if not module_info:
            msg = (
                f"Exchange '{exchange_name.value}' is defined but not implemented. "
                f"Missing module mapping in ExchangeAPIFactory."
            )
            raise ConfigurationError(msg)

        # Dynamic import and instantiation
        try:
            # Import the exchange module
            api_module = cls._import_exchange_module(module_info["module"], exchange_name.value)

            # Get the API class from the module
            api_class = cls._get_api_class(api_module, module_info["class"], exchange_name.value)

            # Create API instance
            api_instance = api_class(
                exchange_config=exchange_config,
                exchange_secrets=exchange_secrets,
            )

            logger.info(
                "exchange_api_client_created",
                exchange_name=exchange_name.value,
                api_class=module_info["class"],
            )

        except (ImportError, AttributeError, TypeError) as e:
            msg = f"Failed to create API client for '{exchange_name.value}': {e}"
            raise ConfigurationError(msg) from e
        else:
            return cast(ExchangeAPI, api_instance)

    @classmethod
    def get_supported_exchanges(cls) -> list[ExchangeName]:
        """Get list of all supported exchanges.

        Returns:
            List of supported exchange enums
        """
        return list(cls._EXCHANGE_MODULE_MAPPING.keys())

    @classmethod
    def is_exchange_supported(cls, exchange_name: ExchangeName) -> bool:
        """Check if exchange is supported by the factory.

        Args:
            exchange_name: Exchange enum to check

        Returns:
            True if exchange is supported, False otherwise
        """
        return exchange_name in cls._EXCHANGE_MODULE_MAPPING

    @classmethod
    def _import_exchange_module(cls, module_path: str, exchange_name: str) -> ModuleType:
        """Import exchange module dynamically.

        Args:
            module_path: Python module path to import
            exchange_name: Exchange name for error context

        Returns:
            Imported module

        Raises:
            ConfigurationError: If module import fails
        """
        try:
            # Dynamic import using __import__
            module_parts = module_path.split(".")
            module = __import__(module_path)

            # Navigate to the actual module (handle nested imports)
            for part in module_parts[1:]:
                module = getattr(module, part)

        except (ImportError, AttributeError) as e:
            msg = (
                f"Failed to import module '{module_path}' for exchange '{exchange_name}'. "
                f"Ensure the exchange API module exists and is properly structured."
            )
            raise ConfigurationError(msg) from e
        else:
            return module

    @classmethod
    def _get_api_class(cls, module: ModuleType, class_name: str, exchange_name: str) -> type[Any]:
        """Get API class from imported module.

        Args:
            module: Imported module containing the API class
            class_name: Name of API class to extract
            exchange_name: Exchange name for error context

        Returns:
            API class type

        Raises:
            ConfigurationError: If class extraction fails
        """
        try:
            api_class = getattr(module, class_name)

            # Validate it's a proper ExchangeAPI subclass
            if not issubclass(api_class, ExchangeAPI):
                msg = (
                    f"Class '{class_name}' from module for exchange '{exchange_name}' "
                    f"is not a subclass of ExchangeAPI"
                )
                raise ConfigurationError(msg)

        except (AttributeError, TypeError) as e:
            msg = (
                f"Failed to get class '{class_name}' from module for exchange '{exchange_name}'. "
                f"Ensure the class exists and is properly defined."
            )
            raise ConfigurationError(msg) from e
        else:
            return cast(type[Any], api_class)
