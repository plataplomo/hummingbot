#!/usr/bin/env python3
"""CyberDeltaEngine main entry point module.

This module serves as the primary entry point for the CyberDeltaEngine trading system.
It provides command-line interface for running trading strategies, data collection,
backtesting, and other engine operations with proper configuration management.
"""
# cyberdelta/main.py - Application Entry Point

import argparse
import asyncio
import os  # Added for path manipulation
import signal
import sys
from collections.abc import Callable
from typing import Any

import structlog

# Corrected imports for API clients
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config import AppSettings, ConfigurationError, get_app_settings, get_secrets_config
from cyberdelta.config.logging_config import setup_logging
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.engine import Engine
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.core.symbol_mapper import SymbolMapper
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.utils.state_manager import StateManager
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

logger = structlog.get_logger(__name__)

# Global cancellation token
cancellation_token = asyncio.Event()


async def _stop_components(app_state: dict[str, Any]) -> None:
    """Stop application components in proper order."""
    logger.info("Stopping Engine...")
    if "engine" in app_state:
        app_state["engine"].stop()

    logger.info("Stopping Signal Queue...")
    if "signal_queue" in app_state:
        await app_state["signal_queue"].stop()

    logger.info("Stopping Data Handler...")
    if "data_handler" in app_state:
        await app_state["data_handler"].stop()


async def _close_api_connections(app_state: dict[str, Any]) -> None:
    """Close all API connections with timeout."""
    logger.info("Closing API connections...")
    tasks: list[asyncio.Task[None]] = []
    for api_name, api in app_state.get("api_clients", {}).items():
        logger.debug(f"Adding close task for {api_name} API.")
        tasks.append(asyncio.create_task(api.close(), name=f"close_{api_name}"))

    # Wait for API close tasks
    if tasks:
        _: set[asyncio.Task[None]]
        pending: set[asyncio.Task[None]]
        _, pending = await asyncio.wait(tasks, timeout=10.0)
        if pending:
            logger.warning(f"{len(pending)} API close tasks timed out or failed.")
            for task in pending:
                task.cancel()


async def _save_application_state(app_state: dict[str, Any]) -> None:
    """Save application state to persistent storage."""
    logger.info("Saving final state...")
    if "portfolio_tracker" in app_state:
        try:
            await app_state["portfolio_tracker"].save_state()
        except Exception as e:
            logger.error(f"Error saving portfolio state: {e}", exc_info=True)

    if "state_manager" in app_state:
        try:
            await app_state["state_manager"].save_state()
        except Exception as e:
            logger.error(f"Error saving general state: {e}", exc_info=True)


async def shutdown(app_state: dict[str, Any]) -> None:
    """Perform graceful shutdown using cancellation token."""
    global cancellation_token
    if cancellation_token.is_set():
        logger.warning("Shutdown already in progress.")
        return

    logger.info("Initiating graceful shutdown sequence...")
    cancellation_token.set()  # Signal tasks to stop

    # Give tasks a moment to react
    await asyncio.sleep(1)

    # Stop components in proper order
    await _stop_components(app_state)

    # Close API connections
    await _close_api_connections(app_state)

    # Save state (should happen after components are stopped)
    await _save_application_state(app_state)

    logger.info("Shutdown sequence complete.")


def _parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="CyberDeltaEngine - Funding Rate Arbitrage Bot")
    parser.add_argument(
        "--config",
        type=str,
        help="Path to config file (e.g., config/config.yaml)",
        default=None,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run without executing trades",
    )
    return parser.parse_args()


def _load_configuration(args: argparse.Namespace) -> AppSettings:
    """Load application configuration."""
    # Set config path if provided
    if args.config:
        os.environ["CYBERDELTA_CONFIG_PATH"] = args.config

    # Load configuration using new Pydantic system
    try:
        config = get_app_settings()
        logger.info(
            "Configuration loaded successfully",
            source=args.config or "Default",
        )
        return config
    except (ConfigurationError, RuntimeError) as e:
        logger.error("Configuration error", error=str(e), exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error loading configuration", error=str(e), exc_info=True)
        sys.exit(1)


def _initialize_core_components(config: AppSettings) -> dict[str, Any]:
    """Initialize all core application components."""
    app_state: dict[str, Any] = {}

    try:
        logger.info("Initializing core components...")
        # StateManager expects Config
        state_manager = StateManager(config)
        app_state["state_manager"] = state_manager

        # SymbolMapper is required by PortfolioTracker and ExecutionHandler
        exchanges_conf = config.exchanges
        # Convert to dict[str, Any] for SymbolMapper
        exchanges_conf_dict: dict[str, Any] = {}
        for exchange_name, exchange_config in exchanges_conf.items():
            exchanges_conf_dict[exchange_name] = exchange_config.model_dump()

        symbol_mapper = SymbolMapper(exchanges_conf_dict)
        app_state["symbol_mapper"] = symbol_mapper

        # PortfolioTracker expects Config, PortfolioTrackerConfig, and SymbolMapper
        portfolio_tracker: PortfolioTracker = PortfolioTracker(
            config,
            config.portfolio_tracker,
            symbol_mapper=symbol_mapper,
        )
        app_state["portfolio_tracker"] = portfolio_tracker

        # CircuitBreakerSystem expects Config
        circuit_breaker = CircuitBreakerSystem(config)
        app_state["circuit_breaker"] = circuit_breaker

        # ExecutionHandler expects:
        #   (Config, PortfolioTracker, SymbolMapper, CircuitBreakerSystem|None)
        execution_handler = ExecutionHandler(
            config,
            portfolio_tracker,
            symbol_mapper,
            circuit_breaker,
        )
        app_state["execution_handler"] = execution_handler

        # RiskManager expects:
        #   (Config, PortfolioTrackerProtocol, CircuitBreakerSystemProtocol|None,
        #    FundingRateValidatorProtocol|None)
        risk_manager = RiskManager(
            config,
            portfolio_tracker,  # PortfolioTracker instance
            circuit_breaker,
            None,  # No funding rate validator for now
        )
        app_state["risk_manager"] = risk_manager

        # PrioritySignalQueue expects handler and config (assume import and correct signature)
        signal_queue = PrioritySignalQueue(
            config,
            circuit_breaker,
        )
        app_state["signal_queue"] = signal_queue

        # Engine expects name (optional)
        engine = Engine(name="CyberDeltaEngine_Core")
        app_state["engine"] = engine

        # DataHandler expects app_settings, api_clients, portfolio_tracker, symbol_mapper
        # We'll initialize api_clients empty here and register them later
        data_handler = DataHandler(
            app_settings=config,
            api_clients={},
            portfolio_tracker=portfolio_tracker,
            symbol_mapper=symbol_mapper,
        )
        app_state["data_handler"] = data_handler

        # Initialize StrategyManager
        strategy_manager = StrategyManager(
            config=config,
            execution_handler=execution_handler,  # Pass execution_handler
            portfolio_tracker=portfolio_tracker,
            risk_manager=risk_manager,
            signal_queue=signal_queue,  # Pass the initialized signal_queue
        )
        app_state["strategy_manager"] = strategy_manager

        logger.info("Core components initialized.")
        return app_state

    except Exception as e:
        logger.error("Fatal error during component initialization", error=str(e), exc_info=True)
        sys.exit(1)


async def _initialize_api_clients(
    config: AppSettings, app_state: dict[str, Any]
) -> dict[str, ExchangeAPI]:
    """Initialize and connect API clients."""
    api_clients: dict[str, ExchangeAPI] = {}
    client: ExchangeAPI

    try:
        logger.info("Initializing API clients...")
        # Get secrets configuration
        secrets_config = get_secrets_config()

        exchanges = config.exchanges
        for exchange_name, exchange_config in exchanges.items():
            if not exchange_config.enabled:
                logger.info("Skipping disabled exchange", exchange=exchange_name)
                continue

            # Get exchange-specific secrets
            if exchange_name not in secrets_config.exchanges:
                logger.error(f"No secrets found for exchange: {exchange_name}")
                continue

            exchange_secrets = secrets_config.exchanges[exchange_name]

            logger.debug(f"Attempting to initialize API for {exchange_name}...")
            if exchange_name == ExchangeName.HYPERLIQUID:
                client = HyperliquidAPI(exchange_config, exchange_secrets)
            elif exchange_name == ExchangeName.BACKPACK:
                client = BackpackAPI(exchange_config, exchange_secrets)
            else:
                logger.warning(f"Unsupported exchange: {exchange_name}")
                continue
            await client.connect_websocket()  # CORRECTED: Call connect_websocket()
            api_clients[exchange_name] = client
            # Register API client with relevant components
            app_state["data_handler"].register_api_client(exchange_name, client)
            app_state["execution_handler"].register_api_client(exchange_name, client)
            app_state["portfolio_tracker"].register_api_client(exchange_name, client)
            logger.info("Initialized and connected API client", exchange=exchange_name)

        # Update data_handler with the initialized API clients
        app_state["data_handler"].api_clients = api_clients

        if not api_clients:
            logger.error("No enabled API clients found. Exiting.")
            sys.exit(1)
        logger.info("API clients initialized and registered.")
        return api_clients

    except Exception as e:
        logger.error(
            "Fatal error during API client initialization or connection",
            error=str(e),
            exc_info=True,
        )
        # Attempt graceful shutdown of already connected clients
        await shutdown(app_state)
        sys.exit(1)


def _initialize_strategies(config: AppSettings, app_state: dict[str, Any]) -> list[Strategy]:
    """Initialize trading strategies."""
    strategies: list[Strategy] = []

    try:
        logger.info("Initializing strategies...")
        # For now, we're using the HyperLiquid Perp vs Backpack Spot strategy
        # from the config model
        strategy_config = config.strategies.hl_perp_bp_spot
        if strategy_config.enabled:
            # Create the funding rate arbitrage strategy
            # The strategy expects specific symbols from each exchange
            strategy_params: dict[str, Any] = {
                "type": "FundingRateArbitrage",
                "name": "HL-BP-FundingArbitrage",
                "symbol": strategy_config.symbol_long,  # Primary symbol
                "enabled": True,
                "long_exchange": strategy_config.long_exchange,
                "short_exchange": strategy_config.short_exchange,
                "symbol_long": strategy_config.symbol_long,
                "symbol_short": strategy_config.symbol_short,
                "funding_threshold": float(strategy_config.params.funding_threshold),
                "max_price_spread_pct": float(strategy_config.params.max_price_spread_pct),
                "min_profit_usd": float(strategy_config.params.min_profit_usd),
            }

            strategy: Strategy = FundingRateArbitrageStrategy(
                name=strategy_params["name"],
                symbol=strategy_params["symbol"],
                data_handler=app_state["data_handler"],
                portfolio_tracker=app_state["portfolio_tracker"],
                risk_manager=app_state["risk_manager"],
                params=strategy_params,
            )
            strategies.append(strategy)
            logger.info("Initialized strategy", name=strategy.name)
        else:
            logger.info("HyperLiquid-Backpack funding arbitrage strategy is disabled")

        logger.info("Strategies initialized", count=len(strategies))
        return strategies

    except Exception as e:
        logger.error("Fatal error during strategy initialization", error=str(e), exc_info=True)
        raise


async def _setup_signal_handlers(app_state: dict[str, Any]) -> None:
    """Set up signal handling for graceful shutdown."""
    loop = asyncio.get_running_loop()

    # Define the coroutine that will be scheduled by the signal handler
    async def _shutdown_coro_for_signal() -> None:
        logger.info("Signal received, initiating shutdown via app_state.")
        await shutdown(app_state)

    for sig_name_enum in (signal.SIGINT, signal.SIGTERM):
        # loop.add_signal_handler expects a regular callable.
        # asyncio.create_task will be called when the signal is received.
        # Create a closure to properly capture the signal
        def create_signal_handler(sig: signal.Signals) -> Callable[[], None]:
            def handler() -> None:
                asyncio.create_task(
                    _shutdown_coro_for_signal(),
                    name=f"ShutdownHandler_{signal.Signals(sig).name}",
                )

            return handler

        loop.add_signal_handler(
            sig_name_enum,
            create_signal_handler(sig_name_enum),
        )
    logger.debug("Signal handlers registered.")


async def _wire_components(app_state: dict[str, Any]) -> None:
    """Wire components according to the new architecture."""
    logger.info("Wiring components...")
    # DataHandler pushes MarketData -> Engine
    app_state["data_handler"].register_observer(app_state["engine"].process_market_data)
    logger.debug("Registered Engine.process_market_data with DataHandler")

    # Engine pushes TradeSignals -> SignalQueue
    app_state["engine"].set_signal_handler(app_state["signal_queue"].enqueue_signal)
    logger.debug("Set SignalQueue.enqueue_signal as Engine's signal handler")

    # SignalQueue pushes validated/prioritized signals -> RiskManager (done in queue init)
    logger.debug("SignalQueue configured to call RiskManager.process_signal")

    # RiskManager pushes ExecutionOrders -> ExecutionHandler (done in RM init)
    logger.debug("RiskManager configured to send orders to ExecutionHandler")

    # ExecutionHandler updates PortfolioTracker (assumed internal or via events)
    # TODO: Verify how ExecutionHandler reports fills/updates to PortfolioTracker.
    logger.debug("Component wiring complete.")


async def _start_background_tasks(app_state: dict[str, Any]) -> list[asyncio.Task[Any]]:
    """Start background component tasks."""
    main_tasks: list[asyncio.Task[Any]] = []

    logger.info("Loading initial state...")
    await app_state["portfolio_tracker"].load_state()
    # Fetch initial balances/positions AFTER loading state
    await app_state["portfolio_tracker"].initialize_portfolio()

    logger.info("Starting background component tasks...")
    # Start data streams and processing
    main_tasks.append(
        asyncio.create_task(
            app_state["data_handler"].start_connections(),
            name="DataHandler_start_connections",
        ),
    )
    # Start signal queue processing
    main_tasks.append(
        asyncio.create_task(
            app_state["signal_queue"].run(cancellation_token), name="SignalQueue_run"
        ),
    )

    return main_tasks


async def _start_engine(app_state: dict[str, Any]) -> None:
    """Start the trading engine."""
    logger.info("Starting Trading Engine...")
    app_state["engine"].start()  # Call the synchronous start method
    logger.info("Engine started. Entering main monitoring loop.")


async def _run_main_loop() -> None:
    """Run the main monitoring loop."""
    # Keep main running - tasks run in background. Wait for cancellation.
    await cancellation_token.wait()


async def _cleanup_tasks(main_tasks: list[asyncio.Task[Any]]) -> None:
    """Clean up background tasks."""
    # Wait for component tasks launched by main to finish
    if main_tasks:
        logger.info(f"Waiting for {len(main_tasks)} main component tasks to complete...")
        _, pending = await asyncio.wait(main_tasks, timeout=15.0)
        if pending:
            logger.warning(
                f"{len(pending)} main tasks did not finish gracefully, cancelling...",
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug("Task cancelled successfully", task_name=task.get_name())
                except Exception as task_exc:
                    logger.error(
                        "Error during forced cancellation of task",
                        task_name=task.get_name(),
                        error=str(task_exc),
                        exc_info=False,
                    )


async def _handle_shutdown_and_cleanup(
    app_state: dict[str, Any], main_tasks: list[asyncio.Task[Any]]
) -> None:
    """Handle shutdown sequence and cleanup background tasks."""
    global cancellation_token

    logger.info("Main loop terminated or error occurred. Ensuring shutdown...")
    if not cancellation_token.is_set():
        logger.warning("Shutdown not initiated by signal handler, triggering now.")
        # Ensure shutdown runs even if wait() exited unexpectedly
        await shutdown(app_state)

    # Wait for component tasks launched by main to finish
    await _cleanup_tasks(main_tasks)

    if main_tasks:
        # Check if all tasks completed successfully
        _, pending = await asyncio.wait(main_tasks, timeout=0)
        if not pending:
            logger.info("All main component tasks completed.")

    logger.info("CyberDeltaEngine main function finished.")


async def main() -> None:
    """Main application entry point."""
    global cancellation_token
    app_state: dict[str, Any] = {}
    main_tasks: list[asyncio.Task[Any]] = []  # Initialize to avoid unbound variable

    # Parse arguments and load configuration
    args = _parse_arguments()
    config = _load_configuration(args)
    app_state["config"] = config

    # Setup logging (must be after config is loaded)
    setup_logging(config)

    # Initialize core components
    app_state.update(_initialize_core_components(config))

    # Initialize API clients
    api_clients = await _initialize_api_clients(config, app_state)
    app_state["api_clients"] = api_clients

    # Initialize strategies
    try:
        strategies = _initialize_strategies(config, app_state)
    except Exception:
        await shutdown(app_state)
        sys.exit(1)

    # Wire components
    await _wire_components(app_state)

    # Add and enable strategies in the Engine
    for strategy in strategies:
        app_state["engine"].add_strategy(strategy)
        app_state["engine"].enable_strategy(strategy.name)
    logger.info("Added and enabled strategies in Engine", count=len(strategies))

    # Start components and main loop
    try:
        main_tasks = await _start_background_tasks(app_state)
        await _start_engine(app_state)

        await _setup_signal_handlers(app_state)
        await _run_main_loop()

    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
        # Shutdown is handled in the finally block
    except Exception as e:
        logger.critical("CRITICAL UNHANDLED ERROR in main execution", error=str(e), exc_info=True)
        # Trigger emergency shutdown
        if not cancellation_token.is_set():
            await shutdown(app_state)  # Attempt graceful shutdown
    finally:
        await _handle_shutdown_and_cleanup(app_state, main_tasks)


if __name__ == "__main__":
    # Check if running as main script (e.g., python -m cyberdelta.main)
    # or directly (python cyberdelta/main.py) - latter might have import issues
    main_module = sys.modules.get("__main__")
    is_direct_run = False
    if main_module and hasattr(main_module, "__file__") and main_module.__file__:
        # Use os.path.abspath for robustness
        main_file_path = os.path.abspath(main_module.__file__)
        if main_file_path.endswith(os.path.join("cyberdelta", "main.py")):
            is_direct_run = True

    if is_direct_run:
        logger.warning("Running main.py directly might cause import issues.")
        logger.warning("Consider running using 'python -m cyberdelta.main' from the project root.")

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, exiting.")
        # Shutdown is handled within main's finally block
        sys.exit(0)
    except Exception as e:
        # Catch any final unexpected errors
        logger.critical("Unhandled exception at top level", error=str(e), exc_info=True)
        sys.exit(1)
