import logging
import logging.handlers
import os
import sys
from types import TracebackType

from cyberdelta.config.config_models import AppSettings

# Standard time formatting for all logs
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
DEFAULT_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def setup_logging(app_settings: AppSettings) -> None:
    """
    Set up logging based on configuration.

    Args:
        app_settings: Application configuration settings
    """
    # Get log level from validated AppSettings (already validated as Literal)
    log_level_str = app_settings.general.log_level

    # Map string log level to logging constants
    log_level_map = {
        "DEBUG": logging.DEBUG,
        "INFO": logging.INFO,
        "WARNING": logging.WARNING,
        "ERROR": logging.ERROR,
        "CRITICAL": logging.CRITICAL,
    }

    # Convert string to log level (guaranteed to be valid due to Pydantic validation)
    log_level = log_level_map[log_level_str]

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler with a higher log level
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Create formatter
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT, DEFAULT_DATE_FORMAT)
    console_handler.setFormatter(formatter)

    # Add console handler to root logger
    root_logger.addHandler(console_handler)

    # Add file handler if log file is configured
    log_file_path_str = app_settings.general.log_file  # Optional[NonEmptyConfigString]
    if log_file_path_str:
        # Create the directory if it doesn't exist
        log_dir: str = os.path.dirname(log_file_path_str)
        if log_dir and not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except Exception as e:
                root_logger.warning(f"Failed to create log directory {log_dir}: {e}")

        try:
            file_handler = logging.FileHandler(log_file_path_str)
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            root_logger.addHandler(file_handler)
            root_logger.info(f"Logging to file: {log_file_path_str}")
        except Exception as e:
            root_logger.warning(f"Failed to create log file {log_file_path_str}: {e}")

    # Apply module-specific log levels if specified
    module_levels_settings = (
        app_settings.general.module_log_levels
    )  # Optional[dict[str, Literal[...]]]
    if module_levels_settings:  # Check if it's not None or empty
        for module_name_str, level_literal in module_levels_settings.items():
            # module_name_str is str (validated by Pydantic)
            # level_literal is already a string from the Literal type
            try:
                module_level = log_level_map[level_literal]  # Guaranteed to be valid
                module_logger = logging.getLogger(module_name_str)
                module_logger.setLevel(module_level)
                root_logger.info(f"Set {module_name_str} log level to {level_literal}")
            except Exception as e:
                root_logger.warning(f"Failed to set log level for {module_name_str}: {e}")

    # Log the configured log level
    root_logger.info(f"Logging initialized with level: {log_level_str}")


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the specified name.

    Args:
        name: Logger name, usually __name__ of the module

    Returns:
        Logger instance
    """
    return logging.getLogger(name)


# Create our custom MemoryHandler subclass for log capturing
class CapturingMemoryHandler(logging.handlers.MemoryHandler):
    """Memory handler that captures logs to a list."""

    def __init__(self, capacity: int, logs_list: list[str]) -> None:
        """Initialize with a capacity and a reference to a logs list."""
        super().__init__(capacity=capacity)
        self.logs_list = logs_list
        # Use a standard formatter
        self.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))

    def emit(self, record: logging.LogRecord) -> None:
        """Emit a record and capture it to the logs list."""
        # Capture the formatted log
        self.logs_list.append(self.format(record))
        # Call parent emit to handle the buffering
        super().emit(record)


class LogCapture:
    """
    Context manager for capturing log messages.

    Use this to capture and inspect log messages for testing or debugging.
    """

    def __init__(self, level: int = logging.INFO) -> None:
        """
        Initialize log capture.

        Args:
            level: Minimum log level to capture
        """
        self.level = level
        self.handler: CapturingMemoryHandler | None = None
        self.logs: list[str] = []

    def __enter__(self) -> "LogCapture":
        """Enter context and start capturing logs."""
        # Create memory handler with reference to our logs list
        self.handler = CapturingMemoryHandler(capacity=1000, logs_list=self.logs)
        self.handler.setLevel(self.level)

        # Add handler to root logger
        root_logger = logging.getLogger()
        root_logger.addHandler(self.handler)

        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit context."""
        # Remove handler from root logger
        if self.handler is not None:
            root_logger = logging.getLogger()
            if self.handler in root_logger.handlers:
                root_logger.removeHandler(self.handler)

    def get_logs(self) -> list[str]:
        """
        Get captured logs.

        Returns:
            List of log messages
        """
        return self.logs
