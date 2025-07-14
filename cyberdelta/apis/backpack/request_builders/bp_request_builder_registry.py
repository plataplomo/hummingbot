"""Request builder registry for Backpack API components.

This module provides a registry for managing domain-specific request builders,
following the same pattern as Hyperliquid for consistency across exchanges.
"""

from typing import Protocol

from cyberdelta.apis.backpack.request_builders.bp_account_request_builder import (
    BackpackAccountRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_market_data_request_builder import (
    BackpackMarketDataRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_quote_request_builder import (
    BackpackQuoteRequestBuilder,
)
from cyberdelta.apis.backpack.request_builders.bp_trading_request_builder import (
    BackpackTradingRequestBuilder,
)
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


class BackpackRequestBuilderRegistry(BaseComponentRegistry[IRequestBuilder]):
    """Registry for Backpack-specific request builders.

    Manages request builders organized by domain (trading, account, market_data)
    and provides convenient access methods for service components.
    """

    def __init__(self) -> None:
        """Initialize the request builder registry."""
        super().__init__()
        self._initialized = False

    def initialize_default_builders(self) -> None:
        """Initialize registry with default request builders."""
        if self._initialized:
            logger.warning("bp_request_builder_registry_already_initialized")
            return

        # Trading domain builders
        self._register_trading_builders()

        # Account domain builders
        self._register_account_builders()

        # Market data domain builders
        self._register_market_data_builders()

        # Quote domain builders
        self._register_quote_builders()

        self._initialized = True

        logger.info(
            "bp_request_builder_registry_initialized",
            total_builders=len(self),
            domains=["trading", "account", "market_data", "quote"],
        )

    def get_trading_builder(self, operation: str) -> IRequestBuilder:
        """Get a trading-specific request builder."""
        builder_name = f"trading.{operation}"
        return self.get(builder_name)

    def get_account_builder(self, operation: str) -> IRequestBuilder:
        """Get an account-specific request builder."""
        builder_name = f"account.{operation}"
        return self.get(builder_name)

    def get_market_data_builder(self, operation: str) -> IRequestBuilder:
        """Get a market data request builder."""
        builder_name = f"market_data.{operation}"
        return self.get(builder_name)

    def get_quote_builder(self, operation: str) -> IRequestBuilder:
        """Get a quote-specific request builder."""
        builder_name = f"quote.{operation}"
        return self.get(builder_name)

    def register_trading_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register a trading request builder."""
        self.register(f"trading.{operation}", builder)

    def register_account_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register an account request builder."""
        self.register(f"account.{operation}", builder)

    def register_market_data_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register a market data request builder."""
        self.register(f"market_data.{operation}", builder)

    def register_quote_builder(self, operation: str, builder: IRequestBuilder) -> None:
        """Register a quote request builder."""
        self.register(f"quote.{operation}", builder)

    def _register_trading_builders(self) -> None:
        """Register default trading request builders."""
        # Register the actual BackpackTradingRequestBuilder instance
        trading_builder = BackpackTradingRequestBuilder()

        # Register all trading operations
        self.register_trading_builder("place_order", trading_builder)
        self.register_trading_builder("cancel_order", trading_builder)
        self.register_trading_builder("get_order", trading_builder)
        self.register_trading_builder("get_fills", trading_builder)
        self.register_trading_builder("get_open_orders", trading_builder)
        self.register_trading_builder("get_order_history", trading_builder)
        self.register_trading_builder("get_trade_history", trading_builder)
        self.register_trading_builder("cancel_all_orders", trading_builder)

        logger.debug("bp_trading_builders_registered", count=8)

    def _register_account_builders(self) -> None:
        """Register default account request builders."""
        # Register the actual BackpackAccountRequestBuilder instance
        account_builder = BackpackAccountRequestBuilder()

        # Register all account operations
        self.register_account_builder("get_balance", account_builder)
        self.register_account_builder("get_positions", account_builder)
        self.register_account_builder("get_transfers", account_builder)
        self.register_account_builder("get_deposits", account_builder)
        self.register_account_builder("withdraw", account_builder)
        self.register_account_builder("transfer", account_builder)
        self.register_account_builder("get_account_info", account_builder)
        self.register_account_builder("get_collateral", account_builder)

        logger.debug("bp_account_builders_registered", count=8)

    def _register_market_data_builders(self) -> None:
        """Register default market data request builders."""
        # Register the actual BackpackMarketDataRequestBuilder instance
        market_data_builder = BackpackMarketDataRequestBuilder()

        # Register all market data operations
        self.register_market_data_builder("get_ticker", market_data_builder)
        self.register_market_data_builder("get_order_book", market_data_builder)
        self.register_market_data_builder("get_klines", market_data_builder)
        self.register_market_data_builder("get_markets", market_data_builder)
        self.register_market_data_builder("get_recent_trades", market_data_builder)
        self.register_market_data_builder("get_funding_rate", market_data_builder)
        self.register_market_data_builder("get_market_data", market_data_builder)

        logger.debug("bp_market_data_builders_registered", count=7)

    def _register_quote_builders(self) -> None:
        """Register default quote request builders."""
        # Register the actual BackpackQuoteRequestBuilder instance
        quote_builder = BackpackQuoteRequestBuilder()

        # Register all quote operations
        self.register_quote_builder("request_for_quote", quote_builder)
        self.register_quote_builder("submit_quote", quote_builder)
        self.register_quote_builder("accept_quote", quote_builder)
        self.register_quote_builder("cancel_rfq", quote_builder)
        self.register_quote_builder("refresh_rfq", quote_builder)

        logger.debug("bp_quote_builders_registered", count=5)

    def get_builders_by_domain(self, domain: str) -> dict[str, IRequestBuilder]:
        """Get all builders for a specific domain.

        Args:
            domain: Domain name ("trading", "account", "market_data", "quote")

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

        # Backpack-specific expected operations
        expected_trading = ["place_order", "cancel_order", "get_order", "get_fills"]
        expected_account = ["get_balance", "get_positions", "get_transfers", "get_deposits"]
        expected_market_data = ["get_ticker", "get_order_book", "get_klines", "get_markets"]
        expected_quote = [
            "request_for_quote",
            "submit_quote",
            "accept_quote",
            "cancel_rfq",
            "refresh_rfq",
        ]

        # Check each domain
        for domain, expected_ops in [
            ("trading", expected_trading),
            ("account", expected_account),
            ("market_data", expected_market_data),
            ("quote", expected_quote),
        ]:
            for operation in expected_ops:
                builder_name = f"{domain}.{operation}"
                if not self.is_registered(builder_name):
                    issues.append(f"Missing BP builder: {builder_name}")

        if issues:
            logger.warning(
                "bp_request_builder_registry_validation_issues",
                issues=issues,
                total_registered=len(self),
            )
        else:
            logger.info("bp_request_builder_registry_validation_passed", total_registered=len(self))

        return issues
