"""
Constants for Backpack Perpetual Exchange connector.
"""

from decimal import Decimal

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType

# Exchange name
EXCHANGE_NAME = "backpack_perpetual"
DEFAULT_DOMAIN = "backpack_perpetual"

# Base URLs
REST_URL = "https://api.backpack.exchange/"
REST_URL_TESTNET = "https://api.backpack.exchange/"  # Backpack doesn't have separate testnet
WS_PUBLIC_URL = "wss://ws.backpack.exchange/stream"
WS_PRIVATE_URL = "wss://ws.backpack.exchange/stream"

# Public API endpoints
EXCHANGE_INFO_URL = "api/v1/capital"
TICKER_URL = "api/v1/ticker"
TICKER_24H_URL = "api/v1/tickers"
ORDER_BOOK_URL = "api/v1/depth"
TRADES_URL = "api/v1/trades"
CANDLES_URL = "api/v1/klines"
TIME_URL = "api/v1/time"
PING_URL = "api/v1/ping"

# Perpetual-specific public endpoints
FUNDING_RATE_URL = "api/v1/funding"
FUNDING_HISTORY_URL = "api/v1/fundingRates"
MARK_PRICE_URL = "api/v1/markPrice"
INDEX_PRICE_URL = "api/v1/indexPrice"

# Private API endpoints (require authentication)
ORDER_URL = "api/v1/order"
CANCEL_URL = "api/v1/order"
CANCEL_ALL_URL = "api/v1/orders"
OPEN_ORDERS_URL = "api/v1/orders"
ORDER_HISTORY_URL = "api/v1/orderHistory"
FILLS_URL = "api/v1/fills"

# Account endpoints
BALANCE_URL = "api/v1/capital"
ACCOUNT_URL = "api/v1/account"

# Perpetual-specific private endpoints
POSITIONS_URL = "api/v1/positions"
LEVERAGE_URL = "api/v1/leverage"
MARGIN_TYPE_URL = "api/v1/marginType"
POSITION_MARGIN_URL = "api/v1/positionMargin"

# WebSocket public channels
WS_DEPTH_CHANNEL = "depth"
WS_TRADES_CHANNEL = "trades"
WS_TICKER_CHANNEL = "ticker"
WS_KLINE_CHANNEL = "kline"

# WebSocket private channels
WS_ACCOUNT_ORDERS_CHANNEL = "account.orders"
WS_ACCOUNT_BALANCES_CHANNEL = "account.balances"
WS_ACCOUNT_FILLS_CHANNEL = "account.fills"

# Perpetual-specific WebSocket channels
WS_ACCOUNT_POSITIONS_CHANNEL = "account.positions"
WS_FUNDING_RATE_CHANNEL = "funding"
WS_MARK_PRICE_CHANNEL = "markPrice"
WS_LIQUIDATION_CHANNEL = "account.liquidation"

# Order configuration
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

# Rate limiting
RATE_LIMIT = "RATE_LIMIT"
PUBLIC_ENDPOINT_LIMIT_ID = "PublicEndpoints"
PRIVATE_ENDPOINT_LIMIT_ID = "PrivateEndpoints"

RATE_LIMITS = [
    RateLimit(limit_id=PUBLIC_ENDPOINT_LIMIT_ID, limit=1200, time_interval=60),
    RateLimit(limit_id=PRIVATE_ENDPOINT_LIMIT_ID, limit=100, time_interval=60),
    # Specific endpoint limits
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1),
    RateLimit(limit_id=CANCEL_URL, limit=10, time_interval=1),
    RateLimit(limit_id=POSITIONS_URL, limit=100, time_interval=60),
    RateLimit(limit_id=LEVERAGE_URL, limit=10, time_interval=60),
    RateLimit(limit_id=FUNDING_RATE_URL, limit=100, time_interval=60),
    RateLimit(limit_id=BALANCE_URL, limit=100, time_interval=60),
]

# Linked limits for public endpoints
PUBLIC_ENDPOINTS = LinkedLimitWeightPair(
    limit_id=PUBLIC_ENDPOINT_LIMIT_ID,
    weight=1
)

# Linked limits for private endpoints
PRIVATE_ENDPOINTS = LinkedLimitWeightPair(
    limit_id=PRIVATE_ENDPOINT_LIMIT_ID,
    weight=1
)

# Order type mapping
ORDER_TYPE_MAP = {
    OrderType.LIMIT.name: "Limit",
    OrderType.MARKET.name: "Market",
    OrderType.LIMIT_MAKER.name: "PostOnly",
}

# Trade type mapping
ORDER_SIDE_MAP = {
    TradeType.BUY.name: "Buy",
    TradeType.SELL.name: "Sell",
}

# Order status mapping
ORDER_STATE_MAP = {
    "New": "OPEN",
    "PartiallyFilled": "PARTIALLY_FILLED",
    "Filled": "FILLED",
    "Cancelled": "CANCELED",
    "PendingCancel": "PENDING_CANCEL",
    "Rejected": "FAILED",
    "Expired": "EXPIRED",
}

# Time in force mapping
TIME_IN_FORCE_MAP = {
    "GTC": "GTC",  # Good Till Cancel
    "IOC": "IOC",  # Immediate or Cancel
    "FOK": "FOK",  # Fill or Kill
    "GTX": "GTX",  # Good Till Crossing (Post Only)
}

# Position side mapping
POSITION_SIDE_MAP = {
    "Long": "LONG",
    "Short": "SHORT",
}

# Margin type mapping
MARGIN_TYPE_MAP = {
    "Cross": "CROSS",
    "Isolated": "ISOLATED",
}

# Default configuration
DEFAULT_LEVERAGE = 1
MAX_LEVERAGE = 20
DEFAULT_POSITION_MODE = PositionMode.ONEWAY
SUPPORTED_POSITION_MODES = [PositionMode.ONEWAY]  # Backpack only supports one-way mode

# Margin and leverage configuration
INITIAL_MARGIN_RATE = Decimal("0.05")  # 5% for 20x leverage
MAINTENANCE_MARGIN_RATE = Decimal("0.025")  # 2.5%

# Funding rate configuration
FUNDING_INTERVAL_HOURS = 8  # Every 8 hours
FUNDING_SETTLEMENT_TIMES = ["00:00", "08:00", "16:00"]  # UTC

# WebSocket configuration
WS_HEARTBEAT_INTERVAL = 30  # Send ping every 30 seconds
WS_MESSAGE_TIMEOUT = 60  # Timeout for receiving messages

# Error codes
ORDER_NOT_EXIST_ERROR_CODE = -2013
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = -2011
UNKNOWN_ORDER_MESSAGE = "Unknown order sent"

# Collateral token (Backpack uses USDC for all perpetuals)
COLLATERAL_TOKEN = "USDC"

# Trading rules update interval
TRADING_RULES_UPDATE_INTERVAL = 3600  # Update every hour

# Funding info update interval
FUNDING_INFO_UPDATE_INTERVAL = 600  # Update every 10 minutes
