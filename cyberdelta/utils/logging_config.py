import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

# Assuming settings are loaded in src.config.settings
# Adjust import path if structure changes
from ..config.settings import settings, ConfigError

# Cache the logger configuration status
_logging_configured = False

def setup_logging():
    """Configures logging based on settings from config.yaml."""
    global _logging_configured
    if _logging_configured:
        return # Avoid reconfiguring logging

    if settings is None:
        # Basic config if settings failed to load
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        logging.critical("Logging setup with basic config due to Settings initialization failure.")
        _logging_configured = True
        return

    log_level_str = settings.log_level
    log_format = settings.log_format
    log_file = settings.log_file
    enable_file_logging = settings.enable_file_logging

    log_level = getattr(logging, log_level_str, logging.INFO)

    # Create formatter
    formatter = logging.Formatter(log_format)

    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to avoid duplicates if re-run
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Console Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File Handler (Optional)
    if enable_file_logging and log_file:
        try:
            # Use RotatingFileHandler for better log management
            file_handler = RotatingFileHandler(
                filename=log_file,
                maxBytes=10*1024*1024, # 10 MB
                backupCount=5, # Keep 5 backup logs
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            logging.info(f"File logging enabled. Logging to: {log_file}")
        except Exception as e:
            logging.error(f"Failed to set up file logging to {log_file}: {e}", exc_info=True)
            # Continue without file logging
    elif enable_file_logging:
        logging.warning("File logging enabled but no log_file specified in configuration.")
    else:
        logging.info("File logging is disabled.")

    # Suppress noisy library logs if needed (example: websockets)
    logging.getLogger("websockets").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)

    logging.info(f"Logging configured with level: {log_level_str}")
    _logging_configured = True

# Example usage within another module:
# from src.utils.logging_config import setup_logging
# import logging
# setup_logging()
# logger = logging.getLogger(__name__)
# logger.info("This is an info message.") 