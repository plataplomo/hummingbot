"""Component registry for Hyperliquid API components.

This module implements the registry pattern for managing Hyperliquid components,
enabling dependency injection, runtime component replacement, and better testability.
Following the Backpack pattern for consistency across exchange implementations.
"""

from typing import Any, Protocol, runtime_checkable

from cyberdelta.apis.base.registry_interface import BaseComponentRegistry
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
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.protocols.base_protocols import (
    MapperProtocol,
    RequestBuilderProtocol,
    ResponseHandlerProtocol,
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
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


@runtime_checkable
class IMapper(MapperProtocol, Protocol):
    """Base interface for all mappers.

    All Hyperliquid mappers should conform to this protocol.
    """


@runtime_checkable
class IRequestBuilder(RequestBuilderProtocol, Protocol):
    """Base interface for all request builders.

    All Hyperliquid request builders should conform to this protocol.
    """


@runtime_checkable
class IResponseHandler(ResponseHandlerProtocol, Protocol):
    """Base interface for all response handlers.

    All Hyperliquid response handlers should conform to this protocol.
    """


class HyperliquidMapperRegistry(BaseComponentRegistry[IMapper]):
    """Registry for Hyperliquid mappers.

    Manages registration and retrieval of mapper components,
    enabling dynamic mapper replacement for testing and customization.
    """

    def register_default_mappers(self) -> None:
        """Register all default mapper implementations.

        This method registers all standard Hyperliquid mappers with their
        conventional names for easy retrieval.
        """
        # Account mappers
        self.register("account.balance", HyperliquidBalanceMapper())
        self.register("account.position", HyperliquidPositionMapper())
        self.register("account.summary", HyperliquidAccountSummaryMapper())
        self.register("account.transaction", HyperliquidTransactionMapper())

        # Trading mappers
        self.register("trading.order", HyperliquidOrderMapper())
        self.register("trading.order_response", HyperliquidOrderResponseMapper())
        self.register("trading.enum", HyperliquidTradingEnumMapper())

        # Market data mappers
        self.register("market.ticker", HyperliquidPriceTickerMapper())
        self.register("market.order_book", HyperliquidOrderBookMapper())
        self.register("market.historical", HyperliquidHistoricalDataMapper())
        self.register("market.metadata", HyperliquidMarketMetadataMapper())

        # Utility mappers
        self.register("utils.common", HyperliquidCommonMappers())

        logger.info(
            "default_mappers_registered",
            count=len(self._components),
            message="Registered default Hyperliquid mappers",
        )


class HyperliquidRequestBuilderRegistry(BaseComponentRegistry[IRequestBuilder]):
    """Registry for Hyperliquid request builders.

    Manages registration and retrieval of request builder components.
    """

    def register_default_builders(self) -> None:
        """Register all default request builder implementations."""
        self.register("account", HyperliquidAccountRequestBuilder())
        self.register("trading", HyperliquidTradingRequestBuilder())
        self.register("market_data", HyperliquidMarketDataRequestBuilder())

        logger.info(
            "default_builders_registered",
            count=len(self._components),
            message="Registered default Hyperliquid request builders",
        )


class HyperliquidResponseHandlerRegistry(BaseComponentRegistry[IResponseHandler]):
    """Registry for Hyperliquid response handlers.

    Manages registration and retrieval of response handler components.
    """

    def register_default_handlers(self) -> None:
        """Register all default response handler implementations."""
        self.register("account", HyperliquidAccountResponseHandler())
        self.register("trading", HyperliquidTradingResponseHandler())
        self.register("market_data", HyperliquidMarketDataResponseHandler())

        logger.info(
            "default_handlers_registered",
            count=len(self._components),
            message="Registered default Hyperliquid response handlers",
        )


class HyperliquidComponentRegistry:
    """Main registry managing all Hyperliquid components.

    This is the central registry that coordinates all component registries
    and provides a unified interface for component management.
    """

    def __init__(self, auto_register: bool = True) -> None:
        """Initialize the component registry.

        Args:
            auto_register: Whether to automatically register default components
        """
        self.mappers = HyperliquidMapperRegistry()
        self.request_builders = HyperliquidRequestBuilderRegistry()
        self.response_handlers = HyperliquidResponseHandlerRegistry()

        if auto_register:
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
            message="Hyperliquid component registry initialized with defaults",
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
            "market.order_book",
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


def replace_mapper(registry: HyperliquidComponentRegistry, name: str, mapper: IMapper) -> None:
    """Replace a mapper in the registry at runtime.

    This utility function makes it easy to replace mappers for testing
    or customization purposes.

    Args:
        registry: The component registry to modify
        name: The name of the mapper to replace
        mapper: The new mapper instance

    Example:
        registry = HyperliquidComponentRegistry()
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
