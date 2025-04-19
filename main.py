# cyberdelta/main.py - Application Entry Point

import argparse
import asyncio
import os  # Added for path manipulation
import signal
import sys
from typing import Any

import structlog

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.base import ExchangeAPI
from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.engine import Engine
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import ConfigError, RiskManager
from cyberdelta.core.strategy import Strategy
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.utils.config import load_config
from cyberdelta.utils.logging_config import setup_logging
from cyberdelta.utils.state_manager import StateManager
from cyberdelta.validation.circuit_breaker import CircuitBreakerSystem

logger = structlog.get_logger(__name__)

# Global cancellation token
cancellation_token = asyncio.Event()


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

    # Stop components in a reasonable order (reverse of startup/dependency)
    logger.info("Stopping Engine...")
    if "engine" in app_state:
        app_state["engine"].stop()

    logger.info("Stopping Signal Queue...")
    if "signal_queue" in app_state:
        await app_state["signal_queue"].stop()

    logger.info("Stopping Data Handler...")
    if "data_handler" in app_state:
        await app_state["data_handler"].stop()

    # Potentially stop ExecutionHandler poller if exists
    # logger.info("Stopping Execution Handler components...")
    # if "execution_handler" in app_state:
    #     await app_state["execution_handler"].stop()

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

    # Save state (should happen after components are stopped)
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

    logger.info("Shutdown sequence complete.")


async def main() -> None:
    """Main application entry point."""
    global cancellation_token
    app_state: dict[str, Any] = {}

    # Argument parsing
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
    args = parser.parse_args()

    # Load configuration using correct utility
    try:
        config = load_config(args.config)  # Only config_path is accepted
        logger.info(
            "Configuration loaded successfully",
            source=args.config or "Default",
        )
        app_state["config"] = config
    except ConfigError as e:
        logger.error("Configuration error", error=str(e), exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error("Unexpected error loading configuration", error=str(e), exc_info=True)
        sys.exit(1)

    # Setup logging (must be after config is loaded)
    setup_logging(config)

    # 1. Initialize Core Components
    try:
        logger.info("Initializing core components...")
        # StateManager expects Config
        state_manager = StateManager(config)
        app_state["state_manager"] = state_manager

        # PortfolioTracker expects Config
        portfolio_tracker = PortfolioTracker(config)
        app_state["portfolio_tracker"] = portfolio_tracker

        # CircuitBreakerSystem expects Config
        circuit_breaker = CircuitBreakerSystem(config)
        app_state["circuit_breaker"] = circuit_breaker

        # SymbolMapper is required for ExecutionHandler (assume import and instantiation)
        from cyberdelta.core.symbol_mapper import SymbolMapper

        # SymbolMapper expects a raw dict, not a Config object
        symbol_mapper = SymbolMapper(config.as_dict())
        app_state["symbol_mapper"] = symbol_mapper

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
            portfolio_tracker,  # PortfolioTracker implements the protocol
            circuit_breaker,
            None,  # No funding rate validator for now
        )
        app_state["risk_manager"] = risk_manager

        # PrioritySignalQueue expects handler and config (assume import and correct signature)
        from cyberdelta.core.signal_queue import PrioritySignalQueue

        signal_queue = PrioritySignalQueue(
            config,
            circuit_breaker,
        )
        app_state["signal_queue"] = signal_queue

        # Engine expects name (optional)
        engine = Engine(name="CyberDeltaEngine_Core")
        app_state["engine"] = engine

        # DataHandler expects Config
        data_handler = DataHandler(config)
        app_state["data_handler"] = data_handler

        logger.info("Core components initialized.")

    except Exception as e:
        logger.error("Fatal error during component initialization", error=str(e), exc_info=True)
        sys.exit(1)

    # 2. Initialize API Clients and link to components
    api_clients: dict[str, ExchangeAPI] = {}
    try:
        logger.info("Initializing API clients...")
        secrets: dict[str, dict[str, str | None]] = {
            "hyperliquid": {
                "wallet_address": os.environ.get("HL_WALLET_ADDRESS"),
                "private_key": os.environ.get("HL_PRIVATE_KEY"),
            },
            "backpack": {
                "BACKPACK_API_KEY": os.environ.get("BP_API_KEY"),
                "BACKPACK_API_SECRET": os.environ.get("BP_API_SECRET"),
            },
        }
        _exchanges = config.get("exchanges", {})
        if not isinstance(_exchanges, dict):
            logger.error("Config 'exchanges' must be a dictionary.")
            sys.exit(1)
        exchanges: dict[str, dict[str, Any]] = _exchanges
        for (
            exchange_name,
            api_config,
        ) in exchanges.items():  # exchange_name: str, api_config: dict[str, Any]
            if not api_config.get("enabled", False):
                logger.info("Skipping disabled exchange", exchange=exchange_name)
                continue
            logger.debug(f"Attempting to initialize API for {exchange_name}...")
            if exchange_name == "hyperliquid":
                client = HyperliquidAPI(api_config, secrets["hyperliquid"])
            elif exchange_name == "backpack":
                client = BackpackAPI(api_config, secrets["backpack"])
            else:
                logger.warning(f"Unsupported exchange: {exchange_name}")
                continue
            await client.connect()  # Connect during setup
            api_clients[exchange_name] = client
            # Register API client with relevant components
            data_handler.register_api_client(exchange_name, client)
            execution_handler.register_api_client(exchange_name, client)
            portfolio_tracker.register_api_client(exchange_name, client)
            logger.info("Initialized and connected API client", exchange=exchange_name)
        app_state["api_clients"] = api_clients

        if not api_clients:
            logger.error("No enabled API clients found. Exiting.")
            sys.exit(1)
        logger.info("API clients initialized and registered.")

    except Exception as e:
        logger.error(
            "Fatal error during API client initialization or connection",
            error=str(e),
            exc_info=True,
        )
        # Attempt graceful shutdown of already connected clients
        await shutdown(app_state)
        sys.exit(1)

    # 3. Initialize Strategies
    strategies: list[Strategy] = []
    try:
        logger.info("Initializing strategies...")
        _strategies_config = config.get("strategies", [])
        if not isinstance(_strategies_config, list):
            logger.error("Config 'strategies' must be a list.")
            _strategies_config = []
        strategies_config: list[dict[str, Any]] = _strategies_config
        for strategy_config in strategies_config:
            if strategy_config.get("enabled", False):
                strategy_type = strategy_config.get("type")
                strategy_name = strategy_config.get("name", "UnnamedStrategy")
                if strategy_type == "FundingRateArbitrage":
                    symbol = strategy_config.get("symbol")
                    if not symbol:
                        logger.error(
                            f"Strategy '{strategy_name}' missing required 'symbol' in config. Skipping."
                        )
                        continue
                    strategy = FundingRateArbitrageStrategy(
                        name=strategy_name,
                        symbol=symbol,
                        data_handler=data_handler,
                        portfolio_tracker=portfolio_tracker,
                        risk_manager=risk_manager,
                        params=strategy_config,
                    )
                    strategies.append(strategy)
                    logger.info("Initialized strategy", name=strategy.name)
                else:
                    logger.warning(
                        "Unsupported strategy type in config",
                        type=strategy_type,
                        name=strategy_name,
                    )
            else:
                logger.info(
                    "Skipping disabled strategy",
                    name=strategy_config.get("name", "Unnamed"),
                )
        logger.info("Strategies initialized", count=len(strategies))

    except Exception as e:
        logger.error("Fatal error during strategy initialization", error=str(e), exc_info=True)
        await shutdown(app_state)
        sys.exit(1)

    # 4. Wire Components according to the new architecture
    logger.info("Wiring components...")
    # DataHandler pushes MarketData -> Engine
    data_handler.register_observer(engine.process_market_data)
    logger.debug("Registered Engine.process_market_data with DataHandler")

    # Engine pushes TradeSignals -> SignalQueue
    engine.set_signal_handler(signal_queue.enqueue_signal)
    logger.debug("Set SignalQueue.enqueue_signal as Engine's signal handler")

    # SignalQueue pushes validated/prioritized signals -> RiskManager (done in queue init)
    logger.debug("SignalQueue configured to call RiskManager.process_signal")

    # RiskManager pushes ExecutionOrders -> ExecutionHandler (done in RM init)
    logger.debug("RiskManager configured to send orders to ExecutionHandler")

    # ExecutionHandler updates PortfolioTracker (assumed internal or via events)
    # TODO: Verify how ExecutionHandler reports fills/updates to PortfolioTracker.
    logger.debug("Component wiring complete.")

    # Add and enable strategies in the Engine
    for strategy in strategies:
        engine.add_strategy(strategy)
        engine.enable_strategy(strategy.name)
    logger.info("Added and enabled strategies in Engine", count=len(strategies))

    # 5. Start Components and Main Loop
    main_tasks = []
    try:
        logger.info("Loading initial state...")
        await portfolio_tracker.load_state()
        # Fetch initial balances/positions AFTER loading state
        await portfolio_tracker.initialize_portfolio()

        logger.info("Starting background component tasks...")
        # Start data streams and processing
        main_tasks.append(
            asyncio.create_task(data_handler.run(cancellation_token), name="DataHandler_run")
        )
        # Start signal queue processing
        main_tasks.append(
            asyncio.create_task(signal_queue.run(cancellation_token), name="SignalQueue_run")
        )
        # Add other component run loops if needed

        # Start the engine (now ready to receive data and forward signals)
        engine.start()
        logger.info("Engine started. Entering main monitoring loop.")

        # Set up signal handling for graceful shutdown
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                lambda s=sig: asyncio.create_task(shutdown(app_state), name=f"ShutdownHandler_{s}"),
            )
        logger.debug("Signal handlers registered.")

        # Keep main running - tasks run in background. Wait for cancellation.
        await cancellation_token.wait()

    except asyncio.CancelledError:
        logger.info("Main task cancelled, initiating shutdown.")
        # Shutdown is handled in the finally block
    except Exception as e:
        logger.critical("CRITICAL UNHANDLED ERROR in main execution", error=str(e), exc_info=True)
        # Trigger emergency shutdown
        if not cancellation_token.is_set():
            await shutdown(app_state)  # Attempt graceful shutdown
    finally:
        logger.info("Main loop terminated or error occurred. Ensuring shutdown...")
        if not cancellation_token.is_set():
            logger.warning("Shutdown not initiated by signal handler, triggering now.")
            # Ensure shutdown runs even if wait() exited unexpectedly
            await shutdown(app_state)

        # Wait for component tasks launched by main to finish
        if main_tasks:
            logger.info(f"Waiting for {len(main_tasks)} main component tasks to complete...")
            _, pending = await asyncio.wait(main_tasks, timeout=15.0)
            if pending:
                logger.warning(
                    f"{len(pending)} main tasks did not finish gracefully, cancelling..."
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
            else:
                logger.info("All main component tasks completed.")

        logger.info("CyberDeltaEngine main function finished.")


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
        print(
            "Warning: Running main.py directly might cause import issues.",
            file=sys.stderr,
        )
        print(
            "Consider running using 'python -m cyberdelta.main' from the project root.",
            file=sys.stderr,
        )

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
