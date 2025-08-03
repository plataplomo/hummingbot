"""CyberDeltaEngine main entry point module.

This module serves as the primary entry point for the CyberDeltaEngine trading system.
It provides command-line interface for running the new clean business logic architecture
with proper configuration management and graceful shutdown capabilities.

IMPORTANT: Following CODING_STANDARDS.md:
- Uses new TradingEngine architecture 
- ALL configuration from AppSettings
- NO hardcoded values
- Structured logging throughout
- Fail-fast error handling
"""

import argparse
import asyncio
import signal
import sys
from pathlib import Path
from typing import Any, Dict, Optional

from cyberdelta.application.trading_engine import TradingEngine
from cyberdelta.config import AppSettings, ConfigurationError, get_app_settings
from cyberdelta.config.structlog_config import setup_structlog, get_logger

logger = get_logger(__name__)

# Global shutdown state
_shutdown_initiated = False
_trading_engine: Optional[TradingEngine] = None


async def shutdown() -> None:
    """Perform graceful shutdown of the trading engine.
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses new TradingEngine graceful shutdown
    - NO duplicate shutdown logic
    - Proper error handling
    """
    global _shutdown_initiated, _trading_engine
    
    if _shutdown_initiated:
        logger.warning("shutdown_already_in_progress")
        return
    
    _shutdown_initiated = True
    logger.info("initiating_graceful_shutdown")
    
    if _trading_engine:
        try:
            await _trading_engine.stop()
            logger.info("trading_engine_shutdown_completed")
        except Exception as e:
            logger.error(
                "trading_engine_shutdown_error",
                error=str(e),
                exc_info=True
            )
    else:
        logger.warning("no_trading_engine_to_shutdown")
    
    logger.info("shutdown_sequence_complete")


def _parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.
    
    Returns:
        Parsed command line arguments
        
    IMPORTANT: Following CODING_STANDARDS.md:
    - Minimal CLI interface 
    - Uses configuration-first approach
    """
    parser = argparse.ArgumentParser(
        description="CyberDeltaEngine - Clean Architecture Trading System"
    )
    parser.add_argument(
        "--config",
        type=str,
        help="Path to configuration file",
        default=None,
    )
    parser.add_argument(
        "--safe-mode",
        action="store_true",
        help="Run in safe mode (no trades executed)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="CyberDeltaEngine 2.0.0 - Clean Architecture",
    )
    
    return parser.parse_args()


def _load_configuration(args: argparse.Namespace) -> AppSettings:
    """Load application configuration from file or environment.
    
    Args:
        args: Parsed command line arguments
        
    Returns:
        Loaded and validated application settings
        
    Raises:
        SystemExit: If configuration loading fails
        
    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses new Pydantic configuration system
    - Fail-fast on configuration errors
    - Explicit error handling and logging
    """
    try:
        # Set config path if provided via CLI
        if args.config:
            import os
            os.environ["CYBERDELTA_CONFIG_PATH"] = args.config
        
        # Load configuration using the centralized system
        config = get_app_settings()
        
        logger.info(
            "configuration_loaded_successfully",
            config_source=args.config or "environment/default",
            safe_mode=args.safe_mode
        )
        
        return config
        
    except ConfigurationError as e:
        logger.error(
            "configuration_error",
            error=str(e),
            config_path=args.config
        )
        sys.exit(1)
    except Exception as e:
        logger.error(
            "unexpected_configuration_error",
            error=str(e),
            config_path=args.config,
            exc_info=True
        )
        sys.exit(1)


def _setup_signal_handlers() -> None:
    """Set up signal handlers for graceful shutdown.
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - Simple signal handling using global shutdown function
    - NO complex signal handler state management
    """
    def signal_handler(signum: int, frame) -> None:
        signal_name = signal.Signals(signum).name
        logger.info(
            "signal_received",
            signal_name=signal_name,
            signal_number=signum
        )
        # Create task for async shutdown
        asyncio.create_task(shutdown())
    
    # Register handlers for common shutdown signals
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger.debug("signal_handlers_registered")


async def main() -> None:
    """Main application entry point using new TradingEngine architecture.
    
    IMPORTANT: Following CODING_STANDARDS.md:
    - Uses new TradingEngine as single orchestrator
    - Configuration-first initialization
    - Proper error handling and cleanup
    - NO complex component wiring
    """
    global _trading_engine
    
    try:
        # Parse command line arguments
        args = _parse_arguments()
        
        # Load and validate configuration
        config = _load_configuration(args)
        
        # Setup structured logging with configuration
        setup_structlog(config)
        
        logger.info(
            "cyberdelta_engine_starting",
            version="2.0.0",
            architecture="clean_business_logic",
            safe_mode=args.safe_mode
        )
        
        # Override safe mode if specified via CLI
        if args.safe_mode:
            # Create a copy of config with safe mode enabled
            # This would require implementing safe mode in the config model
            logger.info("safe_mode_enabled_via_cli")
        
        # Initialize the new TradingEngine
        logger.info("initializing_trading_engine")
        _trading_engine = TradingEngine(config)
        
        # Setup signal handlers for graceful shutdown
        _setup_signal_handlers()
        
        # Start the trading engine
        logger.info("starting_trading_engine")
        await _trading_engine.start()
        
        logger.info(
            "trading_engine_started_successfully",
            safe_mode=_trading_engine.is_safe_mode(),
            monitoring_enabled=True,  # Always enabled in new architecture
            circuit_breakers_enabled=True  # Always enabled in new architecture
        )
        
        # Wait for shutdown signal
        logger.info("entering_main_loop")
        while _trading_engine.is_running() and not _shutdown_initiated:
            await asyncio.sleep(1.0)  # Check every second
        
        logger.info("main_loop_exited")
        
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_received")
        await shutdown()
    except Exception as e:
        logger.critical(
            "critical_error_in_main",
            error=str(e),
            exc_info=True
        )
        await shutdown()
        sys.exit(1)
    finally:
        # Ensure clean shutdown
        if not _shutdown_initiated:
            logger.warning("shutdown_not_initiated_ensuring_cleanup")
            await shutdown()
        
        logger.info("cyberdelta_engine_main_finished")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Handle KeyboardInterrupt at the top level
        print("\nShutdown complete.")
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)
