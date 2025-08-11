"""Constants for the CyberDeltaEngine.

Note: Trading enums (OrderType, OrderSide) are now sourced from cyberdelta.models.enums.
"""

from enum import Enum


# System constants
DEFAULT_CONFIG_PATH = "cyberdelta/config/config.yaml"
DEFAULT_SECRETS_PATH = "cyberdelta/config/secrets.yaml"
DEFAULT_STATE_FILE = "state.json"


# Timeframes
class Timeframe(Enum):
    """Timeframe enumeration."""

    MINUTE_1 = "1m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"
    HOUR_1 = "1h"
    HOUR_4 = "4h"
    HOUR_8 = "8h"
    DAY_1 = "1d"
    WEEK_1 = "1w"


# Default HTTP headers
DEFAULT_HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "CyberDeltaEngine/0.0.1",
}

# Rate limiting constants
RATE_LIMIT_BUFFER = 0.9  # 90% of the rate limit to be safe

# WebSocket constants
WEBSOCKET_RECONNECT_DELAY = 5  # seconds
WEBSOCKET_MAX_RECONNECT_DELAY = 300  # seconds
WEBSOCKET_RECONNECT_FACTOR = 1.5  # exponential backoff factor

# Error handling constants
MAX_RETRIES = 3
RETRY_DELAY_BASE = 1.0  # seconds

# Time constants
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 3600
SECONDS_PER_DAY = 86400

# Asset precision defaults (can be overridden by exchange info)
DEFAULT_PRICE_PRECISION = 6
DEFAULT_AMOUNT_PRECISION = 8
