import logging
import logging.handlers
import os
from datetime import datetime
from typing import Dict, Any, Optional

from cyberdelta.utils.config import Config

def setup_logging(config: Config) -> None:
    """
    Set up logging based on configuration.
    
    Args:
        config: Application configuration
    """
    # Get logging parameters from config
    log_level_str = config.get('general.log_level', 'INFO')
    log_file = config.get('general.log_file', None)
    log_dir = config.get('general.log_directory', 'logs')
    log_max_size = config.get('general.log_max_size', 10 * 1024 * 1024)  # 10 MB
    log_backup_count = config.get('general.log_backup_count', 5)
    
    # Map log level string to logging level
    log_level_map = {
        'DEBUG': logging.DEBUG,
        'INFO': logging.INFO,
        'WARNING': logging.WARNING,
        'ERROR': logging.ERROR,
        'CRITICAL': logging.CRITICAL
    }
    log_level = log_level_map.get(log_level_str.upper(), logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Create root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    
    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # Create file handler if log file is specified
    if log_file:
        # Ensure log directory exists
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_max_size,
            backupCount=log_backup_count
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    elif log_dir:
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        
        # Generate log file name based on current date
        date_str = datetime.now().strftime('%Y-%m-%d')
        log_file = os.path.join(log_dir, f"cyberdelta_{date_str}.log")
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=log_max_size,
            backupCount=log_backup_count
        )
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    # Set specific logger levels (if specified in config)
    logger_levels = config.get('general.logger_levels', {})
    for logger_name, level_str in logger_levels.items():
        if logger_name and level_str:
            level = log_level_map.get(level_str.upper(), logging.INFO)
            logging.getLogger(logger_name).setLevel(level)
    
    # Log the initialization
    logging.info(f"Logging initialized at level {log_level_str}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        Logger instance
    """
    return logging.getLogger(name)


class LogCapture:
    """
    Context manager to capture logs.
    
    Usage:
    ```
    with LogCapture() as logs:
        # Code that generates logs
        ...
    
    captured_logs = logs.get_logs()
    ```
    """
    
    def __init__(self, level: int = logging.INFO):
        """
        Initialize log capture.
        
        Args:
            level: Minimum log level to capture
        """
        self.level = level
        self.handler = None
        self.logs = []
    
    def __enter__(self):
        """Enter context."""
        self.handler = logging.handlers.MemoryHandler(capacity=1000)
        self.handler.setLevel(self.level)
        
        # Add custom formatter to handler
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.handler.setFormatter(formatter)
        
        # Add handler to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(self.handler)
        
        # Store reference to logs
        self.handler.logs = self.logs
        
        # Add custom emit method to handler
        original_emit = self.handler.emit
        
        def custom_emit(record):
            self.logs.append(formatter.format(record))
            original_emit(record)
        
        self.handler.emit = custom_emit
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context."""
        # Remove handler from root logger
        root_logger = logging.getLogger()
        if self.handler in root_logger.handlers:
            root_logger.removeHandler(self.handler)
    
    def get_logs(self) -> list:
        """
        Get captured logs.
        
        Returns:
            List of log messages
        """
        return self.logs
