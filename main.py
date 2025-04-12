# cyberdelta/main.py - Application Entry Point

import argparse
import asyncio
import os  # Added for path manipulation
import signal
import sys
from typing import Any

import structlog

from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.engine import Engine
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.core.signal_queue import PrioritySignalQueue
from cyberdelta.core.strategy import Strategy
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.utils.config import ConfigError, load_config
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
    tasks = []
    for api_name, api in app_state.get("api_clients", {}).items():
        logger.debug(f"Adding close task for {api_name} API.")
        tasks.append(asyncio.create_task(api.close(), name=f"close_{api_name}"))

    # Wait for API close tasks
    if tasks:
        _, pending = await asyncio.wait(tasks, timeout=10.0)
        if pending:
            logger.warning(
                f"{len(pending)} API close tasks timed out or failed."
            )
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


# API Client Factory
def get_api_client(
    exchange_name: str,
    api_config: dict[str, Any],
    state_manager: StateManager,
) -> HyperliquidAPI | BackpackAPI:
    """Factory function to create API clients."""
    # Assuming API clients need config dict and state manager
    if exchange_name == "hyperliquid":
        # Pass only relevant sub-config if available
        hl_config = api_config.get("config", api_config)
        return HyperliquidAPI(config=hl_config, state_manager=state_manager)
    elif exchange_name == "backpack":
        bp_config = api_config.get("config", api_config)
        return BackpackAPI(config=bp_config, state_manager=state_manager)
    else:
        raise ValueError(f"Unsupported exchange: {exchange_name}")


async def main() -> None:
    """Main application entry point."""
    global cancellation_token
    app_state: dict[str, Any] = {}

    # Setup logging first
    setup_logging()

    # Argument parsing
    parser = argparse.ArgumentParser(
        description="CyberDeltaEngine - Funding Rate Arbitrage Bot"
    )
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

    # Load configuration using updated utility
    try:
        config = load_config(config_path_or_data=args.config)
        logger.info(
            "Configuration loaded successfully",
            source=config.get("_source_file", "Default"),
        )
        app_state["config"] = config
    except ConfigError as e:
        logger.error("Configuration error", error=str(e), exc_info=True)
        sys.exit(1)
    except Exception as e:
        logger.error(
            "Unexpected error loading configuration", error=str(e), exc_info=True
        )
        sys.exit(1)

    # 1. Initialize Core Components
    try:
        logger.info("Initializing core components...")
        state_manager = StateManager(
            config.get("state_manager.state_file", "engine_state.json")
        )
        app_state["state_manager"] = state_manager

        portfolio_tracker = PortfolioTracker(
            config.get("portfolio_tracker"), state_manager
        )
        app_state["portfolio_tracker"] = portfolio_tracker

        circuit_breaker = CircuitBreakerSystem(
            config.get("circuit_breaker"), portfolio_tracker
        )
        app_state["circuit_breaker"] = circuit_breaker

        execution_handler = ExecutionHandler(
            config.get("execution_handler"),
            portfolio_tracker,
            circuit_breaker,
            state_manager,
            dry_run=args.dry_run,
        )
        app_state["execution_handler"] = execution_handler

        risk_manager = RiskManager(
            config.get("risk_manager"),
            portfolio_tracker,
            execution_handler,  # RiskManager sends orders to ExecutionHandler
            circuit_breaker,
        )
        app_state["risk_manager"] = risk_manager

        # Signal Queue acts as buffer between Engine and Risk Manager
        signal_queue = PrioritySignalQueue(
            handler=risk_manager.process_signal,
            config=config.get("signal_queue"),
            circuit_breaker=circuit_breaker,
        )
        app_state["signal_queue"] = signal_queue

        # Initialize the refactored Engine
        engine = Engine(name="CyberDeltaEngine_Core")
        app_state["engine"] = engine

        data_handler = DataHandler(config.get("data_handler"), state_manager)
        app_state["data_handler"] = data_handler

        logger.info("Core components initialized.")

    except Exception as e:
        logger.error(
            "Fatal error during component initialization", error=str(e), exc_info=True
        )
        sys.exit(1)

    # 2. Initialize API Clients and link to components
    api_clients = {}
    try:
        logger.info("Initializing API clients...")
        for exchange_name, api_config in config.get("exchanges", {}).items():
            if api_config.get("enabled", False):
                logger.debug(f"Attempting to initialize API for {exchange_name}...")
                client = get_api_client(exchange_name, api_config, state_manager)
                await client.connect()  # Connect during setup
                api_clients[exchange_name] = client
                # Register API client with relevant components
                data_handler.add_api_client(exchange_name, client)
                execution_handler.add_api_client(exchange_name, client)
                portfolio_tracker.add_api_client(exchange_name, client)
                logger.info(
                    "Initialized and connected API client", exchange=exchange_name
                )
            else:
                logger.info("Skipping disabled exchange", exchange=exchange_name)
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
        for strategy_config in config.get("strategies", []):
            if strategy_config.get("enabled", False):
                strategy_type = strategy_config.get("type")
                strategy_name = strategy_config.get("name", "UnnamedStrategy")
                if strategy_type == "FundingRateArbitrage":
                    strategy = FundingRateArbitrageStrategy(
                        name=strategy_name,
                        config=strategy_config,
                        portfolio_tracker=portfolio_tracker,
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
        logger.error(
            "Fatal error during strategy initialization", error=str(e), exc_info=True
        )
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
    logger.info(
        "Added and enabled strategies in Engine", count=len(strategies)
    )

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
            asyncio.create_task(
                data_handler.run(cancellation_token), name="DataHandler_run"
            )
        )
        # Start signal queue processing
        main_tasks.append(
            asyncio.create_task(
                signal_queue.run(cancellation_token), name="SignalQueue_run"
            )
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
                lambda s=sig: asyncio.create_task(
                    shutdown(app_state), name=f"ShutdownHandler_{s}"
                ),
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
            logger.info(
                f"Waiting for {len(main_tasks)} main component tasks to complete..."
            )
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
                        logger.debug(
                            "Task cancelled successfully", task_name=task.get_name()
                        )
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
            "Consider running using 'python -m cyberdelta.main' "
            "from the project root.",
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
