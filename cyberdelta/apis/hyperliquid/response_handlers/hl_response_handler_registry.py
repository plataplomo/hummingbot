"""Response handler registry for Hyperliquid API components.

This module provides a registry for managing domain-specific response handlers,
enabling loose coupling and consistent response processing across services.
"""

from typing import Protocol

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


class HyperliquidResponseHandlerRegistry(BaseComponentRegistry[IResponseHandler]):
    """Registry for Hyperliquid-specific response handlers.

    Manages response handlers organized by domain (trading, account, market_data)
    and provides convenient access methods for service components.
    """

    def __init__(self) -> None:
        """Initialize the response handler registry."""
        super().__init__()
        self._initialized = False

    def initialize_default_handlers(self) -> None:
        """Initialize registry with default response handlers.

        This method should be called after all handlers are available
        to set up the standard configuration.
        """
        if self._initialized:
            logger.warning("response_handler_registry_already_initialized")
            return

        # Trading domain handlers
        self._register_trading_handlers()

        # Account domain handlers
        self._register_account_handlers()

        # Market data domain handlers
        self._register_market_data_handlers()

        self._initialized = True

        logger.info(
            "response_handler_registry_initialized",
            total_handlers=len(self),
            domains=["trading", "account", "market_data"],
        )

    def get_trading_handler(self, operation: str) -> IResponseHandler:
        """Get a trading-specific response handler.

        Args:
            operation: Trading operation name (e.g., "place_order", "cancel_order")

        Returns:
            Response handler for the specified trading operation
        """
        handler_name = f"trading.{operation}"
        return self.get(handler_name)

    def get_account_handler(self, operation: str) -> IResponseHandler:
        """Get an account-specific response handler.

        Args:
            operation: Account operation name (e.g., "get_balance", "get_positions")

        Returns:
            Response handler for the specified account operation
        """
        handler_name = f"account.{operation}"
        return self.get(handler_name)

    def get_market_data_handler(self, operation: str) -> IResponseHandler:
        """Get a market data response handler.

        Args:
            operation: Market data operation (e.g., "get_ticker", "get_orderbook")

        Returns:
            Response handler for the specified market data operation
        """
        handler_name = f"market_data.{operation}"
        return self.get(handler_name)

    def register_trading_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register a trading response handler.

        Args:
            operation: Trading operation name
            handler: Response handler instance
        """
        self.register(f"trading.{operation}", handler)

    def register_account_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register an account response handler.

        Args:
            operation: Account operation name
            handler: Response handler instance
        """
        self.register(f"account.{operation}", handler)

    def register_market_data_handler(self, operation: str, handler: IResponseHandler) -> None:
        """Register a market data response handler.

        Args:
            operation: Market data operation name
            handler: Response handler instance
        """
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

        Args:
            domain: Domain name ("trading", "account", "market_data")
            operation: Operation name within the domain
            response: Parsed JSON response data
            status_code: HTTP status code
            headers: Response headers
            context: Optional context description

        Returns:
            Processed response data
        """
        if context is None:
            context = f"{domain}.{operation}"

        handler_name = f"{domain}.{operation}"
        handler = self.get(handler_name)

        return handler.handle_response(response, status_code, headers, context)

    def _register_trading_handlers(self) -> None:
        """Register default trading response handlers.

        Note: This is a placeholder. In the actual implementation,
        these would be imported and registered from their respective modules.
        """
        # TODO: Import and register actual handler implementations

        logger.debug("trading_handlers_placeholder_registered")

    def _register_account_handlers(self) -> None:
        """Register default account response handlers."""
        # TODO: Import and register actual handler implementations
        logger.debug("account_handlers_placeholder_registered")

    def _register_market_data_handlers(self) -> None:
        """Register default market data response handlers."""
        # TODO: Import and register actual handler implementations
        logger.debug("market_data_handlers_placeholder_registered")

    def get_handlers_by_domain(self, domain: str) -> dict[str, IResponseHandler]:
        """Get all handlers for a specific domain.

        Args:
            domain: Domain name ("trading", "account", "market_data")

        Returns:
            Dictionary mapping operation names to handlers
        """
        domain_prefix = f"{domain}."
        return {
            name[len(domain_prefix) :]: handler
            for name, handler in self._components.items()
            if name.startswith(domain_prefix)
        }

    def validate_registry(self) -> list[str]:
        """Validate that all expected handlers are registered.

        Returns:
            List of missing or invalid handler names
        """
        issues: list[str] = []

        # Define expected handlers for each domain
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
                handler_name = f"{domain}.{operation}"
                if not self.is_registered(handler_name):
                    issues.append(f"Missing handler: {handler_name}")

        if issues:
            logger.warning(
                "response_handler_registry_validation_issues",
                issues=issues,
                total_registered=len(self),
            )
        else:
            logger.info("response_handler_registry_validation_passed", total_registered=len(self))

        return issues

    def create_error_handler_wrapper(
        self,
        base_handler: IResponseHandler,
        error_mapper: object,  # Specific error mapper type
    ) -> IResponseHandler:
        """Create a wrapper that adds error mapping to any handler.

        Args:
            base_handler: Base response handler to wrap
            error_mapper: Error mapper for handling exchange-specific errors

        Returns:
            Wrapped handler with error mapping capability
        """

        class ErrorMappingWrapper:
            def __init__(self, handler: IResponseHandler, mapper: object) -> None:
                self._handler = handler
                self._error_mapper = mapper

            def handle_response(
                self,
                response: ParsedJsonResponse,
                status_code: int,
                headers: dict[str, str],
                context: str,
            ) -> object:
                try:
                    return self._handler.handle_response(response, status_code, headers, context)
                except Exception as e:
                    # Try to map the error using the exchange-specific mapper
                    map_response_error = getattr(self._error_mapper, "map_response_error", None)
                    if map_response_error is not None:
                        mapped_error = map_response_error(e, status_code, context)
                        raise mapped_error from e
                    # If no mapping available, re-raise original error
                    raise

        return ErrorMappingWrapper(base_handler, error_mapper)
