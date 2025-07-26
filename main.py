"""CyberDeltaEngine main entry point module.

This module serves as the primary entry point for the CyberDeltaEngine trading system.
It provides command-line interface for running trading strategies, data collection,
backtesting, and other engine operations with proper configuration management.
"""
# cyberdelta/main.py - Application Entry Point

import argparse
import asyncio
import contextlib
import os  # Added for path manipulation
import signal
import sys
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# Corrected imports for API clients
from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.base.exchange_api import ExchangeAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.config import AppSettings, ConfigurationError, get_app_settings, get_secrets_config
from cyberdelta.config.structlog_config import get_logger as get_structlog, setup_structlog
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.engine import Engine
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.portfolio_orchestrator import PortfolioOrchestrator
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.portfolio_tracker_async_save import patch_portfolio_tracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.services import PriceDataService
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.core.strategy_manager import StrategyManager
from cyberdelta.core.symbol_service import initialize_symbol_service
from cyberdelta.core.symbols.config_loader import load_symbols_from_config
from cyberdelta.core.symbols.exceptions import SymbolRegistryError
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.strategies.factory import StrategyCreationError, StrategyFactory
from cyberdelta.utils.async_state_manager import AsyncStateManager
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem


logger = get_structlog(__name__)

# Global cancellation token
cancellation_token = asyncio.Event()

# Global task store for signal handlers
_signal_handler_tasks: set[asyncio.Task[Any]] = set()


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
        logger.debug("api_close_task_added: Adding close task for API", api_name=api_name)
        tasks.append(asyncio.create_task(api.close(), name=f"close_{api_name}"))

    # Wait for API close tasks
    if tasks:
        _: set[asyncio.Task[None]]
        pending: set[asyncio.Task[None]]
        _, pending = await asyncio.wait(tasks, timeout=10.0)
        if pending:
            logger.warning(
                "api_close_timeout: API close tasks timed out or failed",
                pending_count=len(pending),
            )
            for task in pending:
                task.cancel()
                # Try to await the cancelled task to clean up properly
                with contextlib.suppress(asyncio.CancelledError):
                    await task


async def _save_application_state(app_state: dict[str, Any]) -> None:
    """Save application state to persistent storage."""
    logger.info("Saving final state...")

    # Save portfolio state using async save_state
    if "portfolio_tracker" in app_state:
        try:
            await app_state["portfolio_tracker"].save_state()
            logger.info("Portfolio state saved successfully")
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("portfolio_save_error: Error saving portfolio state", error=str(e))

    # Save general application state using AsyncStateManager
    if "state_manager" in app_state:
        try:
            # Prepare comprehensive application state
            engine = app_state.get("engine") if "engine" in app_state else None
            data_handler = app_state.get("data_handler") if "data_handler" in app_state else None

            general_state = {
                "shutdown_time": datetime.now(UTC).isoformat(),
                "version": "1.0",
                "components": {
                    "engine": {
                        "name": engine.name if engine else None,
                        "is_running": engine.is_running if engine else False,
                    },
                    "data_handler": {
                        "running": data_handler.is_running if data_handler else False,
                        "connected_exchanges": (
                            list(data_handler.api_clients.keys()) if data_handler else []
                        ),
                    },
                },
                "statistics": {
                    "total_strategies": len(engine.strategies) if engine else 0,
                    "enabled_strategies": len(engine.enabled_strategies) if engine else 0,
                },
            }

            # Use async save_state with the state data
            success = await app_state["state_manager"].save_state(general_state)
            if success:
                logger.info("General application state saved successfully")
            else:
                logger.error("Failed to save general application state")
        except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
            logger.exception("general_state_save_error: Error saving general state", error=str(e))


async def shutdown(app_state: dict[str, Any]) -> None:
    """Perform graceful shutdown using cancellation token."""
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
    except (ConfigurationError, RuntimeError) as e:
        logger.exception("configuration_error: Configuration error", error=str(e))
        sys.exit(1)
    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
        logger.exception("config_load_error: Unexpected error loading configuration", error=str(e))
        sys.exit(1)
    else:
        return config


def _initialize_core_components(config: AppSettings) -> dict[str, Any]:
    """Initialize all core application components."""
    app_state: dict[str, Any] = {}

    try:
        logger.info("Initializing core components...")
        # Patch PortfolioTracker with async save/load methods
        patch_portfolio_tracker()
        logger.info("PortfolioTracker patched with async methods")

        # Use AsyncStateManager instead of regular StateManager
        state_manager = AsyncStateManager(config)
        app_state["state_manager"] = state_manager

        # Initialize the new unified symbol registry from configuration
        def _raise_no_symbols_error() -> None:
            """Raise error when no symbols are loaded."""
            logger.error("No symbols loaded from configuration")
            raise SymbolRegistryError("initialization", "No symbols found in configuration")

        logger.info("Initializing unified symbol registry...")
        try:
            loaded_count = load_symbols_from_config()
            if loaded_count == 0:
                _raise_no_symbols_error()
            logger.info("Loaded symbols into registry", loaded_count=loaded_count)
        except Exception as e:
            logger.exception("Failed to load symbols from configuration")
            raise SymbolRegistryError("initialization", f"Symbol loading failed: {e}") from e

        # Initialize the unified symbol service
        unified_symbol_service = initialize_symbol_service()
        app_state["symbol_service"] = unified_symbol_service

        # Get the underlying SymbolService for components that need it
        symbol_service = unified_symbol_service.service
        app_state["symbol_mapper"] = symbol_service

        # PortfolioTracker now accepts SymbolService directly
        portfolio_tracker: PortfolioTracker = PortfolioTracker(
            config,
            config.portfolio_tracker,
            symbol_mapper=symbol_service,
        )
        app_state["portfolio_tracker"] = portfolio_tracker

        # Create PriceDataService for ticker management
        price_data_service = PriceDataService(
            app_settings=config,
            api_clients={},  # Will be populated later
            cache_expiry_seconds=30,  # 30 second cache
        )
        app_state["price_data_service"] = price_data_service

        # Create PortfolioOrchestrator to handle API interactions
        portfolio_orchestrator = PortfolioOrchestrator(
            app_settings=config,
            portfolio_tracker=portfolio_tracker,
            api_clients={},  # Will be populated later
        )
        app_state["portfolio_orchestrator"] = portfolio_orchestrator

        # CircuitBreakerSystem expects Config
        circuit_breaker = CircuitBreakerSystem(config)
        app_state["circuit_breaker"] = circuit_breaker

        # ExecutionHandler expects:
        execution_handler = ExecutionHandler(
            config,
            portfolio_tracker,
            symbol_service,
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
            symbol_mapper=symbol_service,
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

    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
        logger.exception(
            "component_init_error: Fatal error during component initialization",
            error=str(e),
        )
        sys.exit(1)
    else:
        return app_state


async def _initialize_api_clients(
    config: AppSettings,
    app_state: dict[str, Any],
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
                logger.error(
                    "exchange_secrets_missing: No secrets found for exchange",
                    exchange_name=exchange_name,
                )
                continue

            exchange_secrets = secrets_config.exchanges[exchange_name]

            logger.debug(
                "api_initialization_attempt: Attempting to initialize API",
                exchange_name=exchange_name,
            )
            if exchange_name == ExchangeName.HYPERLIQUID:
                client = HyperliquidAPI(exchange_config, exchange_secrets)
            elif exchange_name == ExchangeName.BACKPACK:
                client = BackpackAPI(exchange_config, exchange_secrets)
            else:
                logger.warning(
                    "unsupported_exchange: Unsupported exchange",
                    exchange_name=exchange_name,
                )
                continue
            await client.connect_websocket()  # CORRECTED: Call connect_websocket()
            api_clients[exchange_name] = client
            # Register API client with relevant components
            app_state["data_handler"].register_api_client(exchange_name, client)
            app_state["execution_handler"].register_api_client(exchange_name, client)
            # Register with PortfolioOrchestrator and PriceDataService instead of PortfolioTracker
            app_state["portfolio_orchestrator"].register_api_client(exchange_name, client)
            app_state["price_data_service"].register_api_client(exchange_name, client)
            logger.info("Initialized and connected API client", exchange=exchange_name)

        # Update data_handler with the initialized API clients
        app_state["data_handler"].api_clients = api_clients

        if not api_clients:
            logger.error("No enabled API clients found. Exiting.")
            sys.exit(1)
        logger.info("API clients initialized and registered.")

    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
        logger.exception(
            "api_client_init_error: Fatal error during API client initialization or connection",
            error=str(e),
        )
        # Attempt graceful shutdown of already connected clients
        await shutdown(app_state)
        sys.exit(1)
    else:
        return api_clients


def _initialize_strategies(config: AppSettings, app_state: dict[str, Any]) -> list[Strategy]:
    """Initialize trading strategies using the factory pattern."""
    strategies: list[Strategy] = []

    try:
        logger.info("Initializing strategies via factory...")

        # Create strategy factory with validated configuration
        strategy_factory = StrategyFactory(config)

        # Initialize HyperLiquid Perp vs Backpack Spot strategy
        strategy_config = config.strategies.hl_perp_bp_spot
        if strategy_config.enabled:
            try:
                strategy = strategy_factory.create_hl_perp_bp_spot_strategy(
                    name="HL-BP-FundingArbitrage",
                    symbol=strategy_config.symbol_long,  # Primary symbol for strategy
                    data_handler=app_state["data_handler"],
                    portfolio_tracker=app_state["portfolio_tracker"],
                    risk_manager=app_state["risk_manager"],
                )
                strategies.append(strategy)
                logger.info(
                    "strategy_created_successfully",
                    strategy_name=strategy.name,
                    strategy_type="hl_perp_bp_spot",
                    symbol=strategy.symbol,
                    action="strategy_initialization_complete",
                    message="Successfully created strategy via factory",
                )
            except StrategyCreationError as e:
                logger.exception(
                    "strategy_creation_failed",
                    strategy_type="hl_perp_bp_spot",
                    error=str(e),
                    action="strategy_initialization_error",
                    message="Failed to create HL Perp BP Spot strategy",
                )
                raise
        else:
            logger.info("HyperLiquid-Backpack funding arbitrage strategy is disabled")

        logger.info(
            "strategies_initialization_complete",
            count=len(strategies),
            enabled_strategies=[s.name for s in strategies],
            action="all_strategies_initialized",
            message="Successfully initialized strategies via factory",
        )

    except (ValueError, TypeError, KeyError, AttributeError, ArithmeticError) as e:
        logger.exception(
            "strategy_init_error: Fatal error during strategy initialization",
            error=str(e),
        )
        raise
    else:
        return strategies


def _setup_signal_handlers(app_state: dict[str, Any]) -> None:
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
                task = asyncio.create_task(
                    _shutdown_coro_for_signal(),
                    name=f"ShutdownHandler_{signal.Signals(sig).name}",
                )
                _signal_handler_tasks.add(task)
                task.add_done_callback(_signal_handler_tasks.discard)

            return handler

        loop.add_signal_handler(
            sig_name_enum,
            create_signal_handler(sig_name_enum),
        )
    logger.debug("Signal handlers registered.")


def _wire_components(app_state: dict[str, Any]) -> None:
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
    # Fetch initial balances/positions AFTER loading state using PortfolioOrchestrator
    logger.info("Performing initial portfolio reconciliation...")
    await app_state["portfolio_orchestrator"].orchestrate_full_reconciliation()

    logger.info("Starting background component tasks...")
    # Start data streams and processing
    main_tasks.extend([
        asyncio.create_task(
            app_state["data_handler"].start_connections(),
            name="DataHandler_start_connections",
        ),
        asyncio.create_task(
            app_state["signal_queue"].run(cancellation_token),
            name="SignalQueue_run",
        ),
    ])

    return main_tasks


def _start_engine(app_state: dict[str, Any]) -> None:
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
        logger.info(
            "waiting_for_tasks: Waiting for main component tasks to complete",
            task_count=len(main_tasks),
        )
        _, pending = await asyncio.wait(main_tasks, timeout=15.0)
        if pending:
            logger.info(
                "cancelling_pending_tasks: Cancelling main tasks that timed out",
                action="task_cleanup",
                pending_count=len(pending),
                timeout_seconds=15.0,
            )
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    logger.debug("Task cancelled successfully", task_name=task.get_name())
                except (RuntimeError, OSError, ValueError, AttributeError) as task_exc:
                    logger.exception(
                        "Error during forced cancellation of task",
                        task_name=task.get_name(),
                        error=str(task_exc),
                    )


async def _handle_shutdown_and_cleanup(
    app_state: dict[str, Any],
    main_tasks: list[asyncio.Task[Any]],
) -> None:
    """Handle shutdown sequence and cleanup background tasks."""
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

    # Final cleanup: ensure any remaining tasks are awaited
    # This helps aiohttp sessions close properly before the event loop exits
    current_task = asyncio.current_task()
    all_tasks = [t for t in asyncio.all_tasks() if t != current_task and not t.done()]
    if all_tasks:
        logger.debug(
            "waiting_for_remaining_tasks: Waiting for remaining tasks",
            task_count=len(all_tasks),
        )
        _, pending = await asyncio.wait(all_tasks, timeout=1.0)
        if pending:
            logger.debug(
                "tasks_still_pending: Tasks still pending after final wait",
                pending_count=len(pending),
            )
            for task in pending:
                task.cancel()

    logger.info("CyberDeltaEngine main function finished.")


async def main() -> None:
    """Main application entry point."""
    app_state: dict[str, Any] = {}
    main_tasks: list[asyncio.Task[Any]] = []  # Initialize to avoid unbound variable

    # Parse arguments and load configuration
    args = _parse_arguments()
    config = _load_configuration(args)
    app_state["config"] = config

    # Setup logging (must be after config is loaded)
    setup_structlog(config)

    # Initialize core components
    app_state.update(_initialize_core_components(config))

    # Initialize API clients
    api_clients = await _initialize_api_clients(config, app_state)
    app_state["api_clients"] = api_clients

    # Initialize strategies
    try:
        strategies = _initialize_strategies(config, app_state)
    except (StrategyCreationError, ValueError, ImportError, AttributeError, RuntimeError):
        await shutdown(app_state)
        sys.exit(1)

    # Wire components
    _wire_components(app_state)

    # Add and enable strategies in the Engine
    for strategy in strategies:
        app_state["engine"].add_strategy(strategy)
        app_state["engine"].enable_strategy(strategy.name)
    logger.info("Added and enabled strategies in Engine", count=len(strategies))

    # Start components and main loop
    try:
        main_tasks = await _start_background_tasks(app_state)
        _start_engine(app_state)

        _setup_signal_handlers(app_state)
        await _run_main_loop()

    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
        # Shutdown is handled in the finally block
    except (RuntimeError, OSError, ConnectionError, ValueError, AttributeError, KeyError) as e:
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
        # Use Path.resolve() for robustness
        main_file_path = str(Path(main_module.__file__).resolve())
        if main_file_path.endswith(str(Path("cyberdelta") / "main.py")):
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
    except (RuntimeError, OSError, ValueError, AttributeError, ImportError) as e:
        # Catch any final unexpected errors
        logger.critical("Unhandled exception at top level", error=str(e), exc_info=True)
        sys.exit(1)
