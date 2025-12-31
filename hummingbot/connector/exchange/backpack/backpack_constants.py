"""Constants for Backpack Exchange connector.

Based on Backpack API documentation and CyberDelta implementation insights.
"""

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType, TradeType

# Exchange name constant
EXCHANGE_NAME = "backpack"

# Default domain
DEFAULT_DOMAIN = "backpack"

# Client order ID settings
HBOT_ORDER_ID_PREFIX = ""  # No prefix needed since we map to numeric IDs
MAX_ORDER_ID_LEN = 32  # Standard Hummingbot ID length

# Base URLs
# Backpack does not have a testnet, so we only have mainnet configuration
REST_URLS = {
    "backpack": "https://api.backpack.exchange/",
}
WSS_URLS = {
    "backpack": "wss://ws.backpack.exchange/",
}

# Public REST API endpoints
PING_URL = "api/v1/ping"
TIME_URL = "api/v1/time"
SERVER_TIME_PATH_URL = TIME_URL
EXCHANGE_INFO_URL = "api/v1/markets"  # Fixed from "api/v1/capital"
TICKER_URL = "api/v1/ticker"
TICKERS_URL = "api/v1/tickers"
DEPTH_URL = "api/v1/depth"
ORDER_BOOK_DEPTH_LIMIT = 1000
KLINES_URL = "api/v1/klines"
TRADES_URL = "api/v1/trades"

# Private REST API endpoints
ORDER_URL = "api/v1/order"
CANCEL_ORDER_URL = "api/v1/order"
OPEN_ORDERS_URL = "api/v1/orders"
ORDER_HISTORY_URL = "wapi/v1/history/orders"  # Private endpoint for order history
FILLS_URL = "wapi/v1/history/fills"  # Private endpoint for fill history
BALANCES_URL = "api/v1/capital"  # Backpack uses /capital for balance information
COLLATERAL_URL = "api/v1/capital/collateral"  # Collateral endpoint for auto-lent funds
ACCOUNT_URL = "api/v1/account"  # Account info endpoint (may contain fee rates)

# Instruction map for signing authenticated requests (from openapi_backpack.json)
INSTRUCTION_MAP = {
    # Account endpoints
    ("GET", f"/{ACCOUNT_URL}"): "accountQuery",
    ("PATCH", f"/{ACCOUNT_URL}"): "accountUpdate",
    # Capital and balance endpoints
    ("GET", f"/{BALANCES_URL}"): "balanceQuery",
    ("GET", f"/{COLLATERAL_URL}"): "collateralQuery",
    # Order management endpoints
    ("POST", f"/{ORDER_URL}"): "orderExecute",
    ("DELETE", f"/{CANCEL_ORDER_URL}"): "orderCancel",
    ("DELETE", f"/{OPEN_ORDERS_URL}"): "orderCancelAll",
    ("GET", f"/{ORDER_URL}"): "orderQuery",
    ("GET", f"/{OPEN_ORDERS_URL}"): "orderQueryAll",
    # Historical data endpoints
    ("GET", f"/{ORDER_HISTORY_URL}"): "orderHistoryQueryAll",
    ("GET", f"/{FILLS_URL}"): "fillHistoryQueryAll",
}

# WebSocket channels
# Note: Public channels require symbol suffix (e.g., "depth.SOL_USDC")
WS_DEPTH_CHANNEL = "depth"  # Full format: depth.<symbol>
WS_TRADES_CHANNEL = "trade"  # Full format: trade.<symbol> (NOT "trades")
WS_TICKER_CHANNEL = "ticker"  # Full format: ticker.<symbol>
WS_KLINE_CHANNEL = "kline"  # Full format: kline.<interval>.<symbol>
WS_AUTH_INSTRUCTION = "subscribe"
WS_AUTH_MESSAGE_METHOD = "auth"

# Private WebSocket channels
WS_ACCOUNT_ORDERS_CHANNEL = "account.orderUpdate"  # Documented in OpenAPI
# Note: account.balanceUpdate is NOT documented in the OpenAPI - balance updates come through orderUpdate events
WS_ACCOUNT_POSITIONS_CHANNEL = "account.positionUpdate"  # Note: Not applicable for spot
WS_ACCOUNT_TRANSACTIONS_CHANNEL = "account.transactionUpdate"  # May not exist in API

# WebSocket auth (used in signature payloads)

# Rate limits based on official Backpack Discord information:
# - All endpoints: 1000 requests per minute (16.67 requests per second)
# - Historical endpoints: 60 requests per 2 minutes (0.5 requests per second)
# Rate limit pools
PUBLIC_ENDPOINT_LIMIT_ID = "PublicEndpoints"
PRIVATE_ENDPOINT_LIMIT_ID = "PrivateEndpoints"
HISTORICAL_ENDPOINT_LIMIT_ID = "HistoricalEndpoints"

RATE_LIMITS = [
    # Main pool limits - 1000 requests per minute for all endpoints
    RateLimit(limit_id=PUBLIC_ENDPOINT_LIMIT_ID, limit=1000, time_interval=60),
    RateLimit(limit_id=PRIVATE_ENDPOINT_LIMIT_ID, limit=1000, time_interval=60),
    # Historical endpoints - 60 requests per 2 minutes
    RateLimit(limit_id=HISTORICAL_ENDPOINT_LIMIT_ID, limit=60, time_interval=120),
    # Order management endpoints (private) - share the main 1000/min limit
    RateLimit(
        limit_id=ORDER_URL,
        limit=16,  # ~16.67 requests per second (1000/60)
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=CANCEL_ORDER_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=OPEN_ORDERS_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=ORDER_HISTORY_URL,
        limit=1,  # Historical endpoint - 60 requests per 2 min = 0.5/sec
        time_interval=2,
        linked_limits=[LinkedLimitWeightPair(HISTORICAL_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    # Account endpoints (private)
    RateLimit(
        limit_id=BALANCES_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=ACCOUNT_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=FILLS_URL,  # /wapi/v1/history/fills is a historical endpoint
        limit=1,  # Historical endpoint - 60 requests per 2 min = 0.5/sec
        time_interval=2,
        linked_limits=[LinkedLimitWeightPair(HISTORICAL_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    # Public endpoints - share the main 1000/min limit
    RateLimit(
        limit_id=PING_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=TIME_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=EXCHANGE_INFO_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=TICKER_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=DEPTH_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=KLINES_URL,  # /api/v1/klines is a historical endpoint (can query with startTime/endTime)
        limit=1,  # Historical endpoint - 60 requests per 2 min = 0.5/sec
        time_interval=2,
        linked_limits=[LinkedLimitWeightPair(HISTORICAL_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=TRADES_URL,
        limit=16,  # ~16.67 requests per second
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)],
    ),
    RateLimit(
        limit_id=COLLATERAL_URL,
        limit=16,  # ~16.67 requests per second (private endpoint)
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)],
    ),
]

# Connector configuration
BROKER_ID = "HBOT"

# Order states mapping from Backpack to Hummingbot
ORDER_STATE_MAP = {
    "New": "OPEN",
    "PartiallyFilled": "PARTIALLY_FILLED",
    "Filled": "FILLED",
    "Cancelled": "CANCELED",
    "Expired": "CANCELED",
    "TriggerPending": "OPEN",
    "TriggerFailed": "FAILED",
    "Rejected": "FAILED",
}

# Order types
# Note: PostOnly is handled via timeInForce parameter, not orderType
ORDER_TYPE_MAP = {
    OrderType.LIMIT.name: "Limit",
    OrderType.MARKET.name: "Market",
    # OrderType.LIMIT_MAKER is not directly supported - use LIMIT with PostOnly timeInForce
}

# Order sides - Backpack uses Bid/Ask instead of Buy/Sell
ORDER_SIDE_MAP = {
    TradeType.BUY.name: "Bid",
    TradeType.SELL.name: "Ask",
}

# Time in force
TIME_IN_FORCE_MAP = {
    "GTC": "GTC",  # Good Till Cancel
    "IOC": "IOC",  # Immediate or Cancel
    "FOK": "FOK",  # Fill or Kill
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

# Timing constants
HEARTBEAT_TIME_INTERVAL = 30.0  # WebSocket heartbeat interval in seconds
REQUEST_TIMEOUT = 10.0  # Default request timeout in seconds

# Authentication window (5 seconds)
AUTH_WINDOW_MS = 5000
