"""Response handler registry for Backpack API components.

This module provides a registry for managing domain-specific response handlers,
following the same pattern as Hyperliquid for consistency across exchanges.
"""

from typing import Protocol

from cyberdelta.apis.backpack.response_handlers.bp_account_response_handler import (
    BackpackAccountResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_market_data_response_handler import (
    BackpackMarketDataResponseHandler,
)
from cyberdelta.apis.backpack.response_handlers.bp_trading_response_handler import (
    BackpackTradingResponseHandler,
)
from cyberdelta.apis.base.registry_interface import BaseComponentRegistry
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.utils.typing import ParsedJsonResponse


logger = get_logger(__name__)


class IResponseHandler(Protocol):
    """Protocol for response handler components.

    All response handlers should implement this interface
    to ensure compatibility with the registry system.
    """

    def handle_response(
        self,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str,
    ) -> object:
        """Handle and validate an API response.

        Args:
            response: Parsed JSON response data
            status_code: HTTP status code
            headers: Response headers
            context: Context description for error messages

        Returns:
            Processed and validated response data
        """
        ...


class BackpackResponseHandlerRegistry(BaseComponentRegistry[IResponseHandler]):
    """Registry for Backpack-specific response handlers.

    Manages response handlers organized by domain (trading, account, market_data)
    and provides convenient access methods for service components.
    """

    def __init__(self) -> None:
        """Initialize the Backpack response handler registry."""
        super().__init__()
        self._initialized = False

    def initialize_default_handlers(self) -> None:
        """Initialize registry with default response handlers."""
        if self._initialized:
            logger.warning("bp_response_handler_registry_already_initialized")
            return

        # Trading domain handlers
        self._register_trading_handlers()

        # Account domain handlers
        self._register_account_handlers()

        # Market data domain handlers
        self._register_market_data_handlers()

        self._initialized = True

        logger.info(
            "bp_response_handler_registry_initialized",
            total_handlers=len(self),
            domains=["trading", "account", "market_data"],
        )

    def get_trading_handler(self, operation: str) -> IResponseHandler:
        """Get a trading-specific response handler.

        Returns:
            IResponseHandler: The trading response handler for the specified operation.
        """
        handler_name = f"trading.{operation}"
        return self.get(handler_name)

    def get_account_handler(self, operation: str) -> IResponseHandler:
        """Get an account-specific response handler.

        Returns:
            IResponseHandler: The account response handler for the specified operation.
        """
        handler_name = f"account.{operation}"
        return self.get(handler_name)

    def get_market_data_handler(self, operation: str) -> IResponseHandler:
        """Get a market data response handler.

        Returns:
            IResponseHandler: The market data response handler for the specified operation.
        """
        handler_name = f"market_data.{operation}"
        return self.get(handler_name)

    def register_trading_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register a trading response handler."""
        self.register(f"trading.{operation}", handler)

    def register_account_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register an account response handler."""
        self.register(f"account.{operation}", handler)

    def register_market_data_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register a market data response handler."""
        self.register(f"market_data.{operation}", handler)

    def handle_response_by_operation(
        self,
        domain: str,
        operation: str,
        response: ParsedJsonResponse,
        status_code: int,
        headers: dict[str, str],
        context: str | None = None,
    ) -> object:
        """Handle a response using the appropriate domain handler.

        Returns:
            object: The processed response data.
        """
        if context is None:
            context = f"{domain}.{operation}"

        handler_name = f"{domain}.{operation}"
        handler = self.get(handler_name)

        return handler.handle_response(response, status_code, headers, context)

    def _register_trading_handlers(self) -> None:
        """Register default trading response handlers."""
        # Register the actual BackpackTradingResponseHandler instance
        trading_handler = BackpackTradingResponseHandler()

        # Register all trading operations
        self.register_trading_handler("place_order", trading_handler)
        self.register_trading_handler("cancel_order", trading_handler)
        self.register_trading_handler("get_open_orders", trading_handler)
        self.register_trading_handler("get_order_history", trading_handler)
        self.register_trading_handler("get_order_status", trading_handler)
        self.register_trading_handler("get_fills", trading_handler)
        self.register_trading_handler("get_trade_history", trading_handler)
        self.register_trading_handler("cancel_all_orders", trading_handler)

        logger.debug("bp_trading_handlers_registered", count=8)

    def _register_account_handlers(self) -> None:
        """Register default account response handlers."""
        # Register the actual BackpackAccountResponseHandler instance
        account_handler = BackpackAccountResponseHandler()

        # Register all account operations
        self.register_account_handler("get_balances", account_handler)
        self.register_account_handler("get_positions", account_handler)
        self.register_account_handler("get_account_info", account_handler)
        self.register_account_handler("withdraw", account_handler)
        self.register_account_handler("transfer", account_handler)
        self.register_account_handler("get_collateral", account_handler)
        self.register_account_handler("max_borrow_quantity", account_handler)
        self.register_account_handler("max_order_quantity", account_handler)
        self.register_account_handler("max_withdrawal_quantity", account_handler)

        logger.debug("bp_account_handlers_registered", count=9)

    def _register_market_data_handlers(self) -> None:
        """Register default market data response handlers."""
        # Register the actual BackpackMarketDataResponseHandler instance
        market_data_handler = BackpackMarketDataResponseHandler()

        # Register all market data operations
        self.register_market_data_handler("get_ticker", market_data_handler)
        self.register_market_data_handler("get_order_book", market_data_handler)
        self.register_market_data_handler("get_recent_trades", market_data_handler)
        self.register_market_data_handler("get_markets", market_data_handler)
        self.register_market_data_handler("get_market", market_data_handler)
        self.register_market_data_handler("get_funding_rate", market_data_handler)
        self.register_market_data_handler("get_current_funding_rate", market_data_handler)
        self.register_market_data_handler("get_historical_funding_rates", market_data_handler)
        self.register_market_data_handler("get_market_data", market_data_handler)
        self.register_market_data_handler("get_historical_trades", market_data_handler)

        logger.debug("bp_market_data_handlers_registered", count=10)

    def validate_registry(self) -> list[str]:
        """Validate that all expected handlers are registered.

        Returns:
            list[str]: List of validation issues, empty if no issues found.
        """
        issues: list[str] = []

        # Backpack-specific expected operations
        expected_trading = [
            "place_order",
            "cancel_order",
            "get_open_orders",
            "get_order_history",
            "get_order_status",
            "get_fills",
            "get_trade_history",
            "cancel_all_orders",
        ]
        expected_account = [
            "get_balances",
            "get_positions",
            "get_account_info",
            "withdraw",
            "transfer",
            "get_collateral",
            "max_borrow_quantity",
            "max_order_quantity",
            "max_withdrawal_quantity",
        ]
        expected_market_data = [
            "get_ticker",
            "get_order_book",
            "get_recent_trades",
            "get_markets",
            "get_market",
            "get_funding_rate",
            "get_current_funding_rate",
            "get_historical_funding_rates",
            "get_market_data",
            "get_historical_trades",
        ]

        # Check each domain
        for domain, expected_ops in [
            ("trading", expected_trading),
            ("account", expected_account),
            ("market_data", expected_market_data),
        ]:
            for operation in expected_ops:
                handler_name = f"{domain}.{operation}"
                if not self.is_registered(handler_name):
                    issues.append(f"Missing BP handler: {handler_name}")

        if issues:
            logger.warning(
                "bp_response_handler_registry_validation_issues",
                issues=issues,
                total_registered=len(self),
            )

        return issues
