"""
Constants for Backpack Exchange connector.
Based on Backpack API documentation and CyberDelta implementation insights.
"""

from hummingbot.core.api_throttler.data_types import RateLimit

# Default domain
DEFAULT_DOMAIN = "backpack_main"

# Base URLs
REST_URL = "https://api.backpack.exchange/"
WS_PUBLIC_URL = "wss://ws.backpack.exchange/"
WS_PRIVATE_URL = "wss://ws.backpack.exchange/"

# Public REST API endpoints
PING_URL = "api/v1/ping"
TIME_URL = "api/v1/time"
EXCHANGE_INFO_URL = "api/v1/capital"
TICKER_URL = "api/v1/ticker"
DEPTH_URL = "api/v1/depth"
KLINES_URL = "api/v1/klines"
TRADES_URL = "api/v1/trades"

# Private REST API endpoints
ORDER_URL = "api/v1/order"
CANCEL_ORDER_URL = "api/v1/order"
OPEN_ORDERS_URL = "api/v1/orders"
ORDER_HISTORY_URL = "api/v1/orderHistory"
FILLS_URL = "api/v1/fills"
BALANCES_URL = "api/v1/balances"

# WebSocket channels
WS_DEPTH_CHANNEL = "depth"
WS_TRADES_CHANNEL = "trades"
WS_TICKER_CHANNEL = "ticker"

# Private WebSocket channels
WS_ACCOUNT_ORDERS_CHANNEL = "account.orders"
WS_ACCOUNT_BALANCES_CHANNEL = "account.balances"
WS_ACCOUNT_POSITIONS_CHANNEL = "account.positions"
WS_ACCOUNT_TRANSACTIONS_CHANNEL = "account.transactions"

# Rate limits based on Backpack documentation
# Orders: 10 requests per second
# Cancel: 10 requests per second
# Public endpoints: 20 requests per second
# Private account endpoints: 10 requests per second
RATE_LIMITS = [
    # Order management
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=OPEN_ORDERS_URL, limit=10, time_interval=1),
    RateLimit(limit_id=ORDER_HISTORY_URL, limit=10, time_interval=1),
    
    # Account endpoints
    RateLimit(limit_id=BALANCES_URL, limit=10, time_interval=1),
    RateLimit(limit_id=FILLS_URL, limit=10, time_interval=1),
    
    # Public endpoints
    RateLimit(limit_id=PING_URL, limit=20, time_interval=1),
    RateLimit(limit_id=TIME_URL, limit=20, time_interval=1),
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=20, time_interval=1),
    RateLimit(limit_id=TICKER_URL, limit=20, time_interval=1),
    RateLimit(limit_id=DEPTH_URL, limit=20, time_interval=1),
    RateLimit(limit_id=KLINES_URL, limit=20, time_interval=1),
    RateLimit(limit_id=TRADES_URL, limit=20, time_interval=1),
]

# Connector configuration
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

# Order states mapping from Backpack to Hummingbot
ORDER_STATE_MAP = {
    "New": "OPEN",
    "PartiallyFilled": "PARTIALLY_FILLED", 
    "Filled": "FILLED",
    "Cancelled": "CANCELED",
    "Expired": "CANCELED",
    "Rejected": "FAILED",
}

# Order types
ORDER_TYPE_MAP = {
    "LIMIT": "Limit",
    "MARKET": "Market",
}

# Order sides
ORDER_SIDE_MAP = {
    "BUY": "Buy",
    "SELL": "Sell",
}

# Time in force
TIME_IN_FORCE_MAP = {
    "GTC": "GTC",
    "IOC": "IOC", 
    "FOK": "FOK",
}

# WebSocket message types
WS_MESSAGE_TYPE_ORDER_UPDATE = "orderUpdate"
WS_MESSAGE_TYPE_BALANCE_UPDATE = "balanceUpdate"
WS_MESSAGE_TYPE_TRADE_UPDATE = "tradeUpdate"
WS_MESSAGE_TYPE_DEPTH_UPDATE = "depth"

# Error codes
ERROR_CODE_INSUFFICIENT_BALANCE = "INSUFFICIENT_BALANCE"
ERROR_CODE_ORDER_NOT_FOUND = "ORDER_NOT_FOUND"
ERROR_CODE_INVALID_SYMBOL = "INVALID_SYMBOL"
ERROR_CODE_MIN_NOTIONAL = "MIN_NOTIONAL"
ERROR_CODE_RATE_LIMIT = "RATE_LIMIT"
ERROR_CODE_INVALID_SIGNATURE = "INVALID_SIGNATURE"
ERROR_CODE_EXPIRED_TIMESTAMP = "EXPIRED_TIMESTAMP"

# Request timeouts
REQUEST_TIMEOUT = 10.0
WS_HEARTBEAT_INTERVAL = 30.0

# Authentication window (5 seconds)
AUTH_WINDOW_MS = 5000