"""Request builder registry for Hyperliquid API components.

This module provides a registry for managing domain-specific request builders,
enabling loose coupling and easy testing of service components.
"""

from typing import Protocol

from cyberdelta.apis.base.registry_interface import BaseComponentRegistry
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


class IRequestBuilder(Protocol):
    """Protocol for request builder components.

    All request builders should implement this interface
    to ensure compatibility with the registry system.
    """

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build a request payload.

        Returns:
            Dictionary containing the request payload
        """
        ...


class HyperliquidRequestBuilderRegistry(BaseComponentRegistry[IRequestBuilder]):
    """Registry for Hyperliquid-specific request builders.

    Manages request builders organized by domain (trading, account, market_data)
    and provides convenient access methods for service components.
    """

    def __init__(self) -> None:
        """Initialize the request builder registry."""
        super().__init__()
        self._initialized = False

    def initialize_default_builders(self) -> None:
        """Initialize registry with default request builders.

        This method should be called after all builders are available
        to set up the standard configuration.
        """
        if self._initialized:
            logger.warning("request_builder_registry_already_initialized")
            return

        # Trading domain builders
        self._register_trading_builders()

        # Account domain builders
        self._register_account_builders()

        # Market data domain builders
        self._register_market_data_builders()

        self._initialized = True

        logger.info(
            "request_builder_registry_initialized",
            total_builders=len(self),
            domains=["trading", "account", "market_data"],
        )

    def get_trading_builder(self, operation: str) -> IRequestBuilder:
        """Get a trading-specific request builder.

        Args:
            operation: Trading operation name (e.g., "place_order", "cancel_order")

        Returns:
            Request builder for the specified trading operation
        """
        builder_name = f"trading.{operation}"
        return self.get(builder_name)

    def get_account_builder(self, operation: str) -> IRequestBuilder:
        """Get an account-specific request builder.

        Args:
            operation: Account operation name (e.g., "get_balance", "get_positions")

        Returns:
            Request builder for the specified account operation
        """
        builder_name = f"account.{operation}"
        return self.get(builder_name)

    def get_market_data_builder(self, operation: str) -> IRequestBuilder:
        """Get a market data request builder.

        Args:
            operation: Market data operation (e.g., "get_ticker", "get_orderbook")

        Returns:
            Request builder for the specified market data operation
        """
        builder_name = f"market_data.{operation}"
        return self.get(builder_name)

    def register_trading_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register a trading request builder.

        Args:
            operation: Trading operation name
            builder: Request builder instance
        """
        self.register(f"trading.{operation}", builder)

    def register_account_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register an account request builder.

        Args:
            operation: Account operation name
            builder: Request builder instance
        """
        self.register(f"account.{operation}", builder)

    def register_market_data_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register a market data request builder.

        Args:
            operation: Market data operation name
            builder: Request builder instance
        """
        self.register(f"market_data.{operation}", builder)

    def _register_trading_builders(self) -> None:
        """Register default trading request builders.

        Note: This is a placeholder. In the actual implementation,
        these would be imported and registered from their respective modules.
        """
        # TODO: Import and register actual builder implementations

        logger.debug("trading_builders_placeholder_registered")

    def _register_account_builders(self) -> None:
        """Register default account request builders."""
        # TODO: Import and register actual builder implementations
        logger.debug("account_builders_placeholder_registered")

    def _register_market_data_builders(self) -> None:
        """Register default market data request builders."""
        # TODO: Import and register actual builder implementations
        logger.debug("market_data_builders_placeholder_registered")

    def get_builders_by_domain(self, domain: str) -> dict[str, IRequestBuilder]:
        """Get all builders for a specific domain.

        Args:
            domain: Domain name ("trading", "account", "market_data")

        Returns:
            Dictionary mapping operation names to builders
        """
        domain_prefix = f"{domain}."
        return {
            name[len(domain_prefix) :]: builder
            for name, builder in self._components.items()
            if name.startswith(domain_prefix)
        }

    def validate_registry(self) -> list[str]:
        """Validate that all expected builders are registered.

        Returns:
            List of missing or invalid builder names
        """
        issues: list[str] = []

        # Define expected builders for each domain
        expected_trading = ["place_order", "cancel_order", "get_order", "batch_orders"]
        expected_account = ["get_balance", "get_positions", "get_account_summary"]
        expected_market_data = ["get_ticker", "get_orderbook", "get_trades"]

        # Check each domain
        for domain, expected_ops in [
            ("trading", expected_trading),
            ("account", expected_account),
            ("market_data", expected_market_data),
        ]:
            for operation in expected_ops:
                builder_name = f"{domain}.{operation}"
                if not self.is_registered(builder_name):
                    issues.append(f"Missing builder: {builder_name}")

        if issues:
            logger.warning(
                "request_builder_registry_validation_issues",
                issues=issues,
                total_registered=len(self),
            )
        else:
            logger.info("request_builder_registry_validation_passed", total_registered=len(self))

        return issues
