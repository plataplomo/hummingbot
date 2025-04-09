#!/usr/bin/env python3
import argparse
import asyncio
import logging
import signal
import sys
from typing import Dict, List, Optional, Any

from cyberdelta.core.engine import Engine
from cyberdelta.core.data_handler import DataHandler
from cyberdelta.core.portfolio_tracker import PortfolioTracker
from cyberdelta.core.execution_handler import ExecutionHandler
from cyberdelta.core.balance_monitor import BalanceMonitor
from cyberdelta.core.risk_manager import RiskManager
from cyberdelta.strategies.funding_rate_arbitrage import FundingRateArbitrageStrategy
from cyberdelta.apis.hyperliquid import HyperliquidAPI
from cyberdelta.apis.backpack import BackpackAPI
from cyberdelta.config import config, secrets, ConfigManager, SecretsManager
from cyberdelta.utils.logging_config import setup_logging
from cyberdelta.utils.state_manager import StateManager

logger = logging.getLogger(__name__)

async def shutdown(app_state: Dict[str, Any]) -> None:
    """
    Perform graceful shutdown
    
    Args:
        app_state: Dictionary containing application state
    """
    logger.info("Shutting down...")
    
    # Stop the engine
    if 'engine' in app_state:
        app_state['engine'].stop()
    
    # Close API connections
    for api_name, api in app_state.get('apis', {}).items():
        logger.info(f"Closing {api_name} connection...")
        try:
            await api.close()
        except Exception as e:
            logger.error(f"Error closing {api_name} connection: {e}")
    
    # Save state
    if 'state_manager' in app_state and 'portfolio_tracker' in app_state:
        logger.info("Saving state...")
        try:
            state_data = app_state['portfolio_tracker'].as_dict()
            app_state['state_manager'].save_state(state_data)
        except Exception as e:
            logger.error(f"Error saving state: {e}")
    
    logger.info("Shutdown complete")

async def main() -> None:
    """Main application entry point"""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="CyberDeltaEngine - Funding Rate Arbitrage Bot")
    parser.add_argument("--config", type=str, help="Path to config file")
    parser.add_argument("--secrets", type=str, help="Path to secrets file")
    parser.add_argument("--log-level", type=str, default=None, help="Override log level")
    parser.add_argument("--dry-run", action="store_true", help="Run without executing trades")
    args = parser.parse_args()
    
    # Load configuration
    if args.config:
        custom_config = ConfigManager(args.config)
        custom_config.load()
        # Override the global config
        config = custom_config
    
    # Load secrets if specified
    if args.secrets:
        # Set environment variable for SecretsManager
        import os
        os.environ['CYBERDELTA_SECRETS_PATH'] = args.secrets
        # Force reload of secrets
        secrets.load_secrets()
    
    # Set up logging
    log_level = args.log_level or config.get("general.log_level", "INFO")
    setup_logging(level=log_level)
    
    logger.info("Starting CyberDeltaEngine...")
    logger.info(f"Using config from: {config.config_path}")
    
    # Initialize application state
    app_state: Dict[str, Any] = {}
    
    try:
        # Set up state manager
        state_file = config.get("general.state_file", "state.json")
        backup_dir = config.get("general.state_backup_directory", "state_backups")
        backup_count = config.get("general.state_backup_count", 5)
        
        state_manager = StateManager(state_file, backup_dir, backup_count)
        app_state['state_manager'] = state_manager
        
        # Restore state if available
        saved_state = state_manager.load_state()
        
        # Initialize API clients
        api_clients = {}
        
        # HyperliquidAPI
        if config.get("exchanges.hyperliquid.enabled", True):
            logger.info("Initializing Hyperliquid API...")
            hl_api = HyperliquidAPI(config, secrets)
            await hl_api.connect()
            api_clients["hyperliquid"] = hl_api
            logger.info("Connected to Hyperliquid API")
        
        # BackpackAPI
        if config.get("exchanges.backpack.enabled", True):
            logger.info("Initializing Backpack API...")
            bp_api = BackpackAPI(config, secrets)
            await bp_api.connect()
            api_clients["backpack"] = bp_api
            logger.info("Connected to Backpack API")
        
        app_state['apis'] = api_clients
        
        # Initialize core components
        data_handler = DataHandler(config, api_clients)
        app_state['data_handler'] = data_handler
        
        portfolio_tracker = PortfolioTracker(config, api_clients)
        if saved_state:
            portfolio_tracker.from_dict(saved_state)
        app_state['portfolio_tracker'] = portfolio_tracker
        
        execution_handler = ExecutionHandler(config, api_clients, portfolio_tracker)
        app_state['execution_handler'] = execution_handler
        
        balance_monitor = BalanceMonitor(config, portfolio_tracker)
        app_state['balance_monitor'] = balance_monitor
        
        risk_manager = RiskManager(config, portfolio_tracker)
        app_state['risk_manager'] = risk_manager
        
        # Initialize engine
        engine = Engine()
        app_state['engine'] = engine
        
        # Check if we're running in safe mode
        safe_mode = config.get("general.safe_mode", True)
        if safe_mode:
            logger.warning("Running in SAFE MODE - trades will not be executed")
        
        # Initialize funding rate arbitrage strategies for HL-Perp vs BP-Spot
        if config.get("strategies.hl_perp_bp_spot.enabled", True):
            logger.info("Initializing Hyperliquid-Perp vs Backpack-Spot strategy...")
            
            # Get strategy configuration
            hl_bp_spot_config = config.get("strategies.hl_perp_bp_spot", {})
            symbols = hl_bp_spot_config.get("symbols", {})
            params = hl_bp_spot_config.get("params", {})
            
            # Get the symbols for each exchange
            hl_symbol = symbols.get("hl_symbol", "BTC")
            bp_symbol = symbols.get("bp_symbol", "BTC_USDC")
            
            # Initialize strategy
            strategy = FundingRateArbitrageStrategy(
                name="hl_perp_bp_spot",
                symbol=hl_symbol,
                data_handler=data_handler,
                portfolio_tracker=portfolio_tracker,
                params={
                    "min_funding_differential": params.get("funding_threshold", 0.0001),
                    "min_profit_threshold": params.get("min_profit_usd", 1.0),
                    "min_spread": params.get("min_spread", 0.0002),
                    "perp_exchange": "hyperliquid",
                    "spot_exchange": "backpack",
                    "symbol_mapping": {hl_symbol: bp_symbol}
                }
            )
            
            # Enable the strategy (unless in safe mode)
            if not safe_mode:
                strategy.enable()
            else:
                strategy.disable()
            
            # Add strategy to engine
            engine.add_strategy(strategy)
            logger.info(f"Added HL-Perp vs BP-Spot strategy for {hl_symbol}")
        
        # Initialize dual-perp strategy (if enabled)
        if config.get("strategies.hl_perp_bp_perp.enabled", False):
            logger.info("Initializing Hyperliquid-Perp vs Backpack-Perp strategy...")
            
            # Get strategy configuration
            hl_bp_perp_config = config.get("strategies.hl_perp_bp_perp", {})
            symbols = hl_bp_perp_config.get("symbols", {})
            params = hl_bp_perp_config.get("params", {})
            
            # Get the symbols for each exchange
            hl_symbol = symbols.get("hl_symbol", "BTC")
            bp_symbol = symbols.get("bp_symbol", "BTC-PERP")
            
            # Initialize strategy
            strategy = FundingRateArbitrageStrategy(
                name="hl_perp_bp_perp",
                symbol=hl_symbol,
                data_handler=data_handler,
                portfolio_tracker=portfolio_tracker,
                params={
                    "min_funding_differential": params.get("min_funding_diff", 0.0002),
                    "min_profit_threshold": params.get("min_profit_usd", 2.0),
                    "max_basis_spread": params.get("max_basis_spread", 0.005),
                    "perp_exchange": "hyperliquid",
                    "perp_exchange2": "backpack",  # Second perp exchange
                    "symbol_mapping": {hl_symbol: bp_symbol}
                }
            )
            
            # Enable the strategy (unless in safe mode)
            if not safe_mode:
                strategy.enable()
            else:
                strategy.disable()
            
            # Add strategy to engine
            engine.add_strategy(strategy)
            logger.info(f"Added HL-Perp vs BP-Perp strategy for {hl_symbol}")
        
        # Register signal handlers for graceful shutdown
        # Connect execution handler to engine signals
        engine.register_signal_handler(execution_handler.handle_signal)
        
        # Set up SIGINT and SIGTERM handling
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown(app_state)))
        
        # Start the engine
        engine.start()
        
        # Start WebSocket connections for real-time data
        await data_handler.start_market_data_streams()
        
        # Main run loop - monitoring and state management
        check_interval = config.get("general.state_save_interval", 60)
        
        while True:
            # Check balances periodically
            await balance_monitor.check_balances()
            
            # Save state periodically
            state_data = portfolio_tracker.as_dict()
            state_manager.save_state(state_data)
            
            # Sleep for a while
            await asyncio.sleep(check_interval)
    
    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # Ensure proper shutdown
        await shutdown(app_state)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nExiting...")
        sys.exit(0) 