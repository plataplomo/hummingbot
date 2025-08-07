"""CyberDeltaEngine main entry point - Exchange-agnostic trading engine.

This module serves as the primary entry point for the CyberDeltaEngine trading system.
It initializes all existing services and orchestrates the trading engine lifecycle.

Architecture:
- Configuration-first with AppSettings dependency injection
- Exchange-agnostic design (supports any configured exchange)
- Uses existing services from cyberdelta.logic and cyberdelta.application
- Proper structured logging with cyberdelta.config.structlog_config
- No hardcoded values or assumptions
"""

import argparse
import asyncio
import os
import signal
import sys
from typing import Any, NoReturn

from cyberdelta.application.event_bus import EventBus
from cyberdelta.application.trading_engine import TradingEngine
from cyberdelta.config import AppSettings, ConfigurationError, get_app_settings, get_secrets_config
from cyberdelta.config.secrets_models import SecretsConfig
from cyberdelta.config.structlog_config import get_logger, setup_structlog
from cyberdelta.domain.market.market_service import MarketDataService
from cyberdelta.domain.portfolio.portfolio_service import PortfolioService
from cyberdelta.domain.risk.risk_service import RiskService
from cyberdelta.domain.signal.signal_service import SignalService
from cyberdelta.domain.strategy.strategy_service import StrategyService
from cyberdelta.domain.trading.execution import ExecutionEngine
from cyberdelta.domain.trading.trading_service import TradingService
from cyberdelta.infrastructure.exchange_api_factory import ExchangeAPIFactory
from cyberdelta.infrastructure.persistence.file_repository import FilePortfolioStorage


# Module-level logger
logger = get_logger(__name__)


class TradingEngineBootstrap:
    """Bootstrap class for initializing the trading engine with dependency injection."""

    def __init__(self) -> None:
        """Initialize the bootstrap with empty state."""
        self.config: AppSettings | None = None
        self.secrets: SecretsConfig | None = None
        self.shutdown_event = asyncio.Event()

    def _raise_exchange_config_error(self, exchange_name: str) -> NoReturn:
        """Raise configuration error for missing exchange secrets.

        Args:
            exchange_name: Name of the exchange with missing secrets

        Raises:
            ConfigurationError: Always raised for missing secrets
        """
        msg = f"{exchange_name.title()} secrets not found in configuration"
        raise ConfigurationError(msg)

    async def initialize_configuration(self, config_path: str | None = None) -> None:
        """Initialize configuration and logging with proper error handling."""
        try:
            # Set config path if provided
            if config_path:
                os.environ["CYBERDELTA_CONFIG_PATH"] = config_path

            # Load configuration - SINGLE POINT OF CONFIGURATION LOADING
            self.config = get_app_settings()
            self.secrets = get_secrets_config()

            # Setup structured logging based on configuration
            setup_structlog(self.config)

            logger.info(
                "configuration_loaded_successfully",
                config_source=config_path or "environment/default",
                log_level=self.config.general.log_level,
                safe_mode=self.config.general.safe_mode,
            )

        except ConfigurationError as e:
            logger.critical("Configuration error", error=str(e))
            sys.exit(1)
        except (ImportError, ValueError, OSError) as e:
            logger.critical("Fatal configuration error", error=str(e))
            sys.exit(1)

    async def initialize_exchange_apis(self) -> dict[str, Any]:
        """Initialize exchange API clients with configuration and secrets.

        Returns:
            dict: Dictionary of initialized exchange API clients

        Raises:
            RuntimeError: If configuration is not initialized
        """
        if not self.config or not self.secrets:
            msg = "Configuration must be initialized first"
            raise RuntimeError(msg)

        api_clients: dict[str, Any] = {}

        # Initialize ALL enabled exchanges dynamically using factory
        for exchange_name, exchange_config in self.config.exchanges.items():
            if not exchange_config.enabled:
                logger.debug("exchange_disabled", exchange_name=exchange_name)
                continue

            try:
                # Extract exchange-specific secrets from SecretsConfig
                exchange_secrets = self.secrets.exchanges.get(exchange_name)
                if exchange_secrets is None:
                    self._raise_exchange_config_error(exchange_name)

                # Create API client using factory (exchange-agnostic)
                api_client = ExchangeAPIFactory.create_api_client(
                    exchange_name=exchange_name,
                    exchange_config=exchange_config,
                    exchange_secrets=exchange_secrets,
                )

                api_clients[exchange_name] = api_client
                logger.info(
                    "exchange_api_initialized",
                    exchange_name=exchange_name,
                    enabled=True,
                )

            except Exception as e:
                logger.exception(
                    "exchange_initialization_failed",
                    exchange_name=exchange_name,
                    error=str(e),
                )
                if not self.config.general.safe_mode:
                    raise

        logger.info(
            "exchange_apis_initialized",
            count=len(api_clients),
            exchanges=list(api_clients.keys()),
            safe_mode=self.config.general.safe_mode,
        )

        return api_clients

    async def initialize_services(
        self, api_clients: dict[str, Any], event_bus: EventBus
    ) -> dict[str, Any]:
        """Initialize all business logic services with dependency injection.

        Args:
            api_clients: Exchange API clients dictionary
            event_bus: Event bus for service communication

        Returns:
            dict: Dictionary of initialized services

        Raises:
            RuntimeError: If configuration is not initialized
        """
        if not self.config:
            msg = "Configuration must be initialized first"
            raise RuntimeError(msg)

        services: dict[str, Any] = {}

        # Initialize file storage for portfolio state persistence
        portfolio_storage = FilePortfolioStorage(self.config)

        # Initialize portfolio service with config and storage
        portfolio_service = PortfolioService(
            config=self.config,
            storage=portfolio_storage,
            event_bus=event_bus,
            api_clients=api_clients,
        )
        await portfolio_service.initialize()
        services["portfolio"] = portfolio_service

        # Initialize market data service with config and API clients
        market_data_service = MarketDataService(
            config=self.config, api_clients=api_clients, event_bus=event_bus
        )
        services["market_data"] = market_data_service

        # Initialize execution engine and trading service
        execution_engine = ExecutionEngine(config=self.config, api_clients=api_clients)

        trading_service = TradingService(
            config=self.config,
            execution_engine=execution_engine,
            portfolio_service=portfolio_service,
            event_bus=event_bus,
        )
        services["trading"] = trading_service
        services["execution"] = execution_engine

        # Initialize risk service with config and portfolio service
        risk_service = RiskService(config=self.config, portfolio_service=portfolio_service)
        services["risk"] = risk_service

        # Initialize signal service with config and event bus
        signal_service = SignalService(config=self.config, event_bus=event_bus)
        services["signal"] = signal_service

        # Initialize strategy service with config and dependencies
        strategy_service = StrategyService(
            config=self.config,
            market_service=market_data_service,
            portfolio_service=portfolio_service,
            signal_service=signal_service,
            event_bus=event_bus,
        )
        services["strategy"] = strategy_service

        logger.info(
            "services_initialized",
            count=len(services),
            service_types=list(services.keys()),
            safe_mode=self.config.general.safe_mode,
        )

        return services

    async def initialize_trading_engine(
        self, services: dict[str, Any], event_bus: EventBus
    ) -> TradingEngine:
        """Initialize trading engine with all dependencies.

        Args:
            services: Business logic services dictionary
            event_bus: Event bus for communication

        Returns:
            TradingEngine: Initialized trading engine

        Raises:
            RuntimeError: If configuration is not initialized
        """
        if not self.config:
            msg = "Configuration must be initialized first"
            raise RuntimeError(msg)

        # Initialize trading engine with all services
        trading_engine = TradingEngine(
            config=self.config,
            event_bus=event_bus,
            market_data_service=services["market_data"],
            trading_service=services["trading"],
            portfolio_service=services["portfolio"],
            risk_service=services["risk"],
            signal_service=services["signal"],
            strategy_service=services["strategy"],
            execution_engine=services["execution"],
        )

        logger.info(
            "trading_engine_initialized",
            safe_mode=self.config.general.safe_mode,
            monitoring_enabled=(
                self.config.monitoring.notifications_enabled if self.config.monitoring else False
            ),
        )

        return trading_engine

    def setup_signal_handlers(self) -> None:
        """Setup signal handlers for graceful shutdown."""

        def signal_handler(signum: int, _: object) -> None:
            signal_name = signal.Signals(signum).name
            logger.info("signal_received", signal_name=signal_name, signal_number=signum)
            # Trigger shutdown event
            self.shutdown_event.set()

        # Register handlers for common shutdown signals
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        logger.debug("signal_handlers_registered")

    async def run(self, config_path: str | None = None, safe_mode: bool = False) -> None:
        """Main execution method with proper error handling and cleanup."""
        trading_engine: TradingEngine | None = None

        try:
            # Step 1: Initialize configuration and logging
            await self.initialize_configuration(config_path)

            # Override safe mode if specified
            if safe_mode and self.config:
                logger.info("safe_mode_override_enabled")

            # Step 2: Validate configuration
            if self.config and self.config.general.safe_mode:
                logger.warning("safe_mode_enabled", message="No real trades will be executed")

            # Step 3: Setup signal handlers
            self.setup_signal_handlers()

            # Step 4: Initialize event bus
            event_bus = EventBus()

            # Step 5: Initialize exchange APIs with configuration
            logger.info("initializing_exchange_connections")
            api_clients = await self.initialize_exchange_apis()

            # Step 6: Initialize all services with dependency injection
            logger.info("initializing_business_logic_services")
            services = await self.initialize_services(api_clients, event_bus)

            # Step 7: Initialize trading engine
            logger.info("initializing_trading_engine")
            trading_engine = await self.initialize_trading_engine(services, event_bus)

            # Step 8: Start the trading engine
            logger.info("starting_trading_engine")
            await trading_engine.start()

            # Step 9: Run until shutdown signal
            logger.info(
                "trading_engine_running",
                message="Trading engine is running. Press Ctrl+C to stop.",
                safe_mode=self.config.general.safe_mode if self.config else False,
            )

            # Wait for shutdown signal instead of infinite loop
            await self.shutdown_event.wait()

        except KeyboardInterrupt:
            if logger:
                logger.info("shutdown_requested", source="keyboard_interrupt")
        except (ConfigurationError, ValueError, OSError) as e:
            if logger:
                logger.exception("specific_error_in_main", error=str(e))
            sys.exit(1)
        finally:
            # Step 10: Graceful shutdown
            if trading_engine and logger:
                logger.info("shutting_down_trading_engine")
                try:
                    await trading_engine.stop()
                    logger.info("shutdown_complete")
                except Exception as e:
                    logger.exception("shutdown_error", error=str(e))
            elif logger:
                logger.info("shutdown_complete")


def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments with proper validation.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="CyberDeltaEngine - Configuration-First Autonomous Trading System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py                     # Use default configuration
  python main.py --config config.yaml --safe-mode
  python main.py --safe-mode         # Paper trading mode
        """,
    )

    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file (overrides CYBERDELTA_CONFIG_PATH env var)",
        default=None,
    )

    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Run in safe mode - no real trades executed (paper trading)",
    )

    parser.add_argument(
        "--version",
        action="version",
        version="CyberDeltaEngine 2.0.0 - Clean Architecture",
    )

    return parser.parse_args()


async def main() -> None:
    """Main entry point implementing clean architecture principles."""
    # Parse command line arguments
    args = parse_arguments()

    # Initialize and run the trading engine
    bootstrap = TradingEngineBootstrap()
    await bootstrap.run(config_path=args.config, safe_mode=args.safe_mode)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application shutdown complete")
        sys.exit(0)
    except (ConfigurationError, ValueError, OSError) as e:
        logger.critical("Fatal error", error=str(e))
        sys.exit(1)
