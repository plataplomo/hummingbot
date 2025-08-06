"""Component registry for Backpack API components.

This module implements the registry pattern for managing Backpack components,
enabling dependency injection, runtime component replacement, and better testability.
"""

from typing import Any, Protocol, runtime_checkable

from cyberdelta.apis.backpack.mappers import (
    BackpackAccountSummaryMapper,
    BackpackBalanceMapper,
    BackpackCandleMapper,
    BackpackCommonMappers,
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
from cyberdelta.apis.backpack.request_builders import (
    BackpackAccountRequestBuilder,
    BackpackMarketDataRequestBuilder,
    BackpackQuoteRequestBuilder,
    BackpackTradingRequestBuilder,
)
from cyberdelta.apis.backpack.response_handlers import (
    BackpackAccountResponseHandler,
    BackpackMarketDataResponseHandler,
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.base.infrastructure_config_domain import RegistrationConfiguration
from cyberdelta.apis.base.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
)
from cyberdelta.apis.base.registry_interface import BaseComponentRegistry
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


@runtime_checkable
class IMapper(MapperProtocol, Protocol):
    """Base interface for all mappers.

    All Backpack mappers should conform to this protocol.
    """


@runtime_checkable
class IRequestBuilder(RequestBuilderProtocol, Protocol):
    """Base interface for all request builders.

    All Backpack request builders should conform to this protocol.
    """


@runtime_checkable
class IResponseHandler(ResponseHandlerProtocol, Protocol):
    """Base interface for all response handlers.

    All Backpack response handlers should conform to this protocol.
    """


class BackpackMapperRegistry(BaseComponentRegistry[IMapper]):
    """Registry for Backpack mappers.

    Manages registration and retrieval of mapper components,
    enabling dynamic mapper replacement for testing and customization.
    """

    def register_default_mappers(self) -> None:
        """Register all default mapper implementations.

        This method registers all standard Backpack mappers with their
        conventional names for easy retrieval.
        """
        # Account mappers
        self.register("account.balance", BackpackBalanceMapper())
        self.register("account.position", BackpackPositionMapper())
        self.register("account.summary", BackpackAccountSummaryMapper())
        self.register("account.transaction", BackpackTransactionMapper())
        self.register("account.transfer", BackpackTransferMapper())

        # Trading mappers
        self.register("trading.order", BackpackOrderMapper())

        # Market data mappers
        self.register("market.ticker", BackpackTickerMapper())
        self.register("market.order_book", BackpackOrderBookMapper())
        self.register("market.candle", BackpackCandleMapper())
        self.register("market.trade", BackpackFillMapper())
        self.register("market.market", BackpackMarketMapper())
        self.register("market.funding_rate", BackpackFundingRateMapper())

        # Utility mappers
        self.register("utils.common", BackpackCommonMappers())

        logger.info(
            "default_mappers_registered",
            count=len(self._components),
            message="Registered default Backpack mappers",
        )


class BackpackRequestBuilderRegistry(BaseComponentRegistry[IRequestBuilder]):
    """Registry for Backpack request builders.

    Manages registration and retrieval of request builder components.
    """

    def register_default_builders(self) -> None:
        """Register all default request builder implementations."""
        self.register("account", BackpackAccountRequestBuilder())
        self.register("trading", BackpackTradingRequestBuilder())
        self.register("market_data", BackpackMarketDataRequestBuilder())
        self.register("quote", BackpackQuoteRequestBuilder())

        logger.info(
            "default_builders_registered",
            count=len(self._components),
            message="Registered default Backpack request builders",
        )


class BackpackResponseHandlerRegistry(BaseComponentRegistry[IResponseHandler]):
    """Registry for Backpack response handlers.

    Manages registration and retrieval of response handler components.
    """

    def register_default_handlers(self) -> None:
        """Register all default response handler implementations."""
        self.register("account", BackpackAccountResponseHandler())
        self.register("trading", BackpackTradingResponseHandler())
        self.register("market_data", BackpackMarketDataResponseHandler())

        logger.info(
            "default_handlers_registered",
            count=len(self._components),
            message="Registered default Backpack response handlers",
        )


class BackpackComponentRegistry:
    """Main registry managing all Backpack components.

    This is the central registry that coordinates all component registries
    and provides a unified interface for component management.
    """

    def __init__(self, registration_config: RegistrationConfiguration | None = None) -> None:
        """Initialize the component registry.

        Args:
            registration_config: Registration configuration for component management
        """
        self.mappers = BackpackMapperRegistry()
        self.request_builders = BackpackRequestBuilderRegistry()
        self.response_handlers = BackpackResponseHandlerRegistry()

        self.registration_config = registration_config or RegistrationConfiguration()

        if self.registration_config.should_register_defaults():
            self._register_defaults()

    def _register_defaults(self) -> None:
        """Register all default component implementations."""
        self.mappers.register_default_mappers()
        self.request_builders.register_default_builders()
        self.response_handlers.register_default_handlers()

        logger.info(
            "component_registry_initialized",
            mapper_count=len(self.mappers.list_registered()),
            builder_count=len(self.request_builders.list_registered()),
            handler_count=len(self.response_handlers.list_registered()),
            message="Backpack component registry initialized with defaults",
        )

    def validate_required_components(self) -> bool:
        """Validate that all required components are registered.

        Returns:
            True if all required components are present, False otherwise
        """
        required_mappers = [
            "account.balance",
            "account.position",
            "trading.order",
            "market.ticker",
        ]

        required_builders = [
            "account",
            "trading",
            "market_data",
        ]

        required_handlers = [
            "account",
            "trading",
            "market_data",
        ]

        # Check mappers
        for mapper_name in required_mappers:
            if not self.mappers.is_registered(mapper_name):
                logger.error(
                    "missing_required_mapper",
                    mapper=mapper_name,
                    message=f"Required mapper '{mapper_name}' not registered",
                )
                return False

        # Check builders
        for builder_name in required_builders:
            if not self.request_builders.is_registered(builder_name):
                logger.error(
                    "missing_required_builder",
                    builder=builder_name,
                    message=f"Required builder '{builder_name}' not registered",
                )
                return False

        # Check handlers
        for handler_name in required_handlers:
            if not self.response_handlers.is_registered(handler_name):
                logger.error(
                    "missing_required_handler",
                    handler=handler_name,
                    message=f"Required handler '{handler_name}' not registered",
                )
                return False

        return True

    def get_component_stats(self) -> dict[str, Any]:
        """Get statistics about registered components.

        Returns:
            Dictionary with component counts and names
        """
        return {
            "mappers": {
                "count": len(self.mappers.list_registered()),
                "names": self.mappers.list_registered(),
            },
            "request_builders": {
                "count": len(self.request_builders.list_registered()),
                "names": self.request_builders.list_registered(),
            },
            "response_handlers": {
                "count": len(self.response_handlers.list_registered()),
                "names": self.response_handlers.list_registered(),
            },
            "total_components": (
                len(self.mappers.list_registered())
                + len(self.request_builders.list_registered())
                + len(self.response_handlers.list_registered())
            ),
        }


def replace_mapper(registry: BackpackComponentRegistry, name: str, mapper: IMapper) -> None:
    """Replace a mapper in the registry at runtime.

    This utility function makes it easy to replace mappers for testing
    or customization purposes.

    Args:
        registry: The component registry to modify
        name: The name of the mapper to replace
        mapper: The new mapper instance

    Example:
        registry = BackpackComponentRegistry()
        custom_mapper = MyCustomBalanceMapper()
        replace_mapper(registry, "account.balance", custom_mapper)
    """
    if registry.mappers.is_registered(name):
        registry.mappers.unregister(name)
    registry.mappers.register(name, mapper)

    logger.info(
        "mapper_replaced",
        name=name,
        mapper_type=type(mapper).__name__,
        message=f"Replaced mapper '{name}' with {type(mapper).__name__}",
    )
