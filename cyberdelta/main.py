import asyncio
import logging
import signal
from typing import Dict, List, Optional

# Assuming structure: strategy_math/coding_strategy/src/
from config.settings import settings, ConfigError
from utils.logging_config import setup_logging
from apis.base import ExchangeAPI
from apis.hyperliquid import HyperliquidAPI
# Import other API clients as they are created
# from apis.backpack import BackpackAPI
# from apis.paradex import ParadexAPI

# Import core components
from core.data_handler import DataHandler
from core.signal_generator import SignalGenerator
from core.portfolio_tracker import PortfolioTracker
from core.risk_manager import RiskManager
from core.execution_handler import ExecutionHandler
from core.adaptation_loop import AdaptationLoop

# Configure logging as early as possible
setup_logging()
logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self):
        if settings is None:
            logger.critical("Settings could not be loaded. Exiting.")
            raise ConfigError("Failed to load settings.")

        self.settings = settings
        self.api_clients: Dict[str, ExchangeAPI] = {}
        self.data_handler: Optional[DataHandler] = None
        self.portfolio_tracker: Optional[PortfolioTracker] = None
        self.signal_generator: Optional[SignalGenerator] = None
        self.risk_manager: Optional[RiskManager] = None
        self.execution_handler: Optional[ExecutionHandler] = None
        self.adaptation_loop: Optional[AdaptationLoop] = None
        self.running = False
        self._tasks: List[asyncio.Task] = []

    async def initialize(self):
        """Initialize API clients and core components."""
        logger.info("Initializing Trading Bot...")
        api_classes = {
            "hyperliquid": HyperliquidAPI,
            # "backpack": BackpackAPI,
            # "paradex": ParadexAPI,
        }

        for exchange_name in self.settings.active_exchanges:
            if exchange_name in api_classes:
                try:
                    config = self.settings.get_exchange_config(exchange_name)
                    secrets = {
                        "HYPERLIQUID_WALLET_PRIVATE_KEY": self.settings.get_secret("HYPERLIQUID_WALLET_PRIVATE_KEY"),
                        "BACKPACK_API_KEY": self.settings.get_secret("BACKPACK_API_KEY"),
                        "BACKPACK_API_SECRET": self.settings.get_secret("BACKPACK_API_SECRET"),
                        "PARADEX_WALLET_PRIVATE_KEY": self.settings.get_secret("PARADEX_WALLET_PRIVATE_KEY"),
                    }
                    client = api_classes[exchange_name](api_config=config, secrets=secrets)
                    await client.connect() # Establish connections (e.g., session, WS)
                    self.api_clients[exchange_name] = client
                    logger.info(f"Initialized and connected API client for {exchange_name}")
                except Exception as e:
                    logger.error(f"Failed to initialize API client for {exchange_name}: {e}", exc_info=True)
                    # Decide if failure for one exchange is fatal
            else:
                logger.warning(f"API client class not found for configured exchange: {exchange_name}")

        if not self.api_clients:
            logger.critical("No API clients were successfully initialized. Exiting.")
            raise RuntimeError("Failed to initialize any active exchange clients.")

        # --- Initialize Core Components Here ---
        self.portfolio_tracker = PortfolioTracker(self.api_clients)
        await self.portfolio_tracker.load_initial_state()

        self.data_handler = DataHandler(self.api_clients)
        self.signal_generator = SignalGenerator(self.data_handler, self.api_clients)
        self.risk_manager = RiskManager(self.portfolio_tracker)
        self.execution_handler = ExecutionHandler(self.api_clients, self.portfolio_tracker)
        self.adaptation_loop = AdaptationLoop()
        # self.signal_generator = SignalGenerator(...)
        # self.risk_manager = RiskManager(...)
        # ... etc
        logger.info("Trading Bot Initialization complete.")

    async def _strategy_loop(self):
        """The main loop connecting signal -> risk -> execution."""
        if not self.signal_generator or not self.risk_manager or not self.execution_handler:
            logger.error("Strategy loop cannot run, core components missing.")
            return

        logger.info("Starting main strategy loop...")
        async for opportunities in self.signal_generator.run():
            if not self.running:
                break # Exit loop if bot is stopping

            if opportunities:
                viable_opportunities = await self.risk_manager.assess_and_filter_opportunities(opportunities)
                if viable_opportunities:
                    # Execute the top opportunity (or potentially more based on logic)
                    top_opportunity = viable_opportunities[0]
                    logger.info(f"Top viable opportunity: {top_opportunity.opportunity_id}, Size: {top_opportunity.recommended_size:.6f}")
                    await self.execution_handler.execute_opportunity(top_opportunity)
                    # TODO: Add logic to prevent re-executing immediately, manage concurrency
                    await asyncio.sleep(5) # Simple cooldown after execution attempt
                else:
                     logger.info("No opportunities passed risk assessment.")
            else:
                 pass # No opportunities generated in this cycle
                 # Optional sleep if generator yields empty list frequently
                 # await asyncio.sleep(1)

        logger.info("Main strategy loop finished.")

    async def start(self):
        """Start the main trading loop and background tasks."""
        if not all([self.api_clients, self.data_handler, self.portfolio_tracker,
                    self.signal_generator, self.risk_manager, self.execution_handler,
                    self.adaptation_loop]):
            logger.error("Cannot start bot, one or more components failed to initialize.")
            return

        self.running = True
        logger.info("Starting Trading Bot Core Tasks...")

        # --- Start Core Component Tasks ---
        self._tasks.append(asyncio.create_task(self.data_handler.run(), name="DataHandler"))
        self._tasks.append(asyncio.create_task(self.portfolio_tracker.run(), name="PortfolioTracker"))
        # The strategy loop itself runs as a task
        self._tasks.append(asyncio.create_task(self._strategy_loop(), name="StrategyLoop"))
        # Risk manager run loop is optional, start if needed
        # self._tasks.append(asyncio.create_task(self.risk_manager.run(), name="RiskManagerLoop"))
        if self.adaptation_loop.adaptation_params.get("enabled", False):
             self._tasks.append(asyncio.create_task(self.adaptation_loop.run(), name="AdaptationLoop"))

        logger.info("Trading Bot Running.")
        # Wait for tasks to complete
        done, pending = await asyncio.wait(self._tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            try:
                # Check result to raise exceptions from completed tasks
                await task
                logger.warning(f"Task {task.get_name()} finished unexpectedly without error.")
            except asyncio.CancelledError:
                 logger.info(f"Task {task.get_name()} was cancelled.") # Expected on shutdown
            except Exception as e:
                logger.critical(f"Task {task.get_name()} failed: {e}", exc_info=True)
                # Optionally trigger stop for other tasks on critical failure
                # self.running = False # Signal other loops to stop
                # stop_event.set() # If stop_event is accessible globally or passed

    async def stop(self):
        """Gracefully shut down the bot."""
        if not self.running:
            return
        self.running = False
        logger.info("Stopping Trading Bot...")

        # Signal core components to stop
        if self.data_handler:
            self.data_handler.stop()
        if self.portfolio_tracker:
            self.portfolio_tracker.stop()
        if self.signal_generator:
            self.signal_generator.stop()
        if self.adaptation_loop:
            self.adaptation_loop.stop()
        # Signal other components (SignalGenerator.stop(), etc.)

        # Cancel background tasks (redundant if tasks exit cleanly on stop signal)
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*[t for t in self._tasks if not t.done()], return_exceptions=True)
        logger.info("Core tasks stopped/cancelled.")

        # Close API connections
        for name, client in self.api_clients.items():
            try:
                await client.close()
                logger.info(f"Closed API client for {name}")
            except Exception as e:
                logger.error(f"Error closing API client for {name}: {e}", exc_info=True)

        logger.info("Trading Bot Stopped.")

async def main():
    bot: Optional[TradingBot] = None
    try:
        bot = TradingBot()
    except ConfigError:
        logger.critical("Exiting: Bot initialization failed due to configuration error.")
        return # Exit early if settings failed

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Shutdown signal received.")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler
            # Fallback or specific Windows handling might be needed
            logger.warning(f"Signal handler for {sig} not supported on this platform.")

    start_task = None
    try:
        await bot.initialize()
        start_task = asyncio.create_task(bot.start(), name="BotStartTask")

        # Wait until stop signal is received OR the main start task finishes (error)
        logger.info("Waiting for stop signal or task completion...")
        done, pending = await asyncio.wait([start_task, stop_event.wait()], return_when=asyncio.FIRST_COMPLETED)

        if stop_event.is_set():
             logger.info("Stop event received.")
        else: # start_task must have completed first (likely an error)
            logger.warning("Bot start task completed unexpectedly. Initiating shutdown.")
            # Check the result of start_task if needed
            for task in done:
                 if task is start_task:
                      try:
                          await task # Raise exception if start_task failed
                      except Exception as e:
                           logger.error(f"Bot start task failed: {e}", exc_info=True)

        # Trigger shutdown
        logger.info("Initiating graceful shutdown...")
        await bot.stop()

        # Ensure start task completes or cancels cleanly after stop is called
        if start_task and not start_task.done():
             logger.info("Cancelling main start task...")
             start_task.cancel()
             # Wait for cancellation
             await asyncio.gather(start_task, return_exceptions=True)

    except ConfigError as e:
        logger.critical(f"Exiting due to configuration errors during initialization: {e}")
    except RuntimeError as e:
        logger.critical(f"Exiting due to runtime error during initialization: {e}")
    except asyncio.CancelledError:
        logger.info("Main execution cancelled.")
        # Ensure bot stop is called if cancelled externally before graceful shutdown
        if bot and bot.running:
            logger.warning("Main task cancelled externally, attempting emergency stop...")
            await bot.stop()
    except Exception as e:
        logger.critical(f"Unhandled exception in main: {e}", exc_info=True)
        # Attempt emergency stop
        if bot and bot.running:
             logger.warning("Unhandled exception occurred, attempting emergency stop...")
             await bot.stop()
    finally:
        logger.info("Application shutdown sequence complete.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Application terminated by user (KeyboardInterrupt).") 