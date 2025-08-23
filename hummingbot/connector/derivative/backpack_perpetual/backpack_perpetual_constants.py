"""Constants for Backpack Perpetual Exchange connector."""

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit
from hummingbot.core.data_type.common import OrderType, PositionMode, TradeType


# Exchange name
EXCHANGE_NAME = "backpack_perpetual"
DEFAULT_DOMAIN = "backpack_perpetual"

# Collateral token (Backpack uses USDC for all perpetuals)
COLLATERAL_TOKEN = "USDC"

# Position mode - Backpack only supports ONEWAY mode
DEFAULT_POSITION_MODE = PositionMode.ONEWAY
SUPPORTED_POSITION_MODES = [PositionMode.ONEWAY]  # Backpack only supports one-way mode

# Base URLs
# Backpack does not have a testnet, so we only have mainnet configuration
REST_URLS = {
    "backpack_perpetual": "https://api.backpack.exchange/",
}
WSS_URLS = {
    "backpack_perpetual": "wss://ws.backpack.exchange/",
}

# Public API endpoints
EXCHANGE_INFO_URL = "api/v1/markets"
TICKER_URL = "api/v1/ticker"
TICKER_24H_URL = "api/v1/tickers"
ORDER_BOOK_URL = "api/v1/depth"
TRADES_URL = "api/v1/trades"
CANDLES_URL = "api/v1/klines"
TIME_URL = "api/v1/time"
PING_URL = "api/v1/ping"

# Perpetual-specific public endpoints
FUNDING_RATE_URL = "api/v1/fundingRates"  # Historical funding rates
FUNDING_HISTORY_URL = "wapi/v1/history/funding"  # Personal funding history
MARK_PRICE_URL = "api/v1/markPrices"  # Current mark prices with funding info
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
# Note: Backpack doesn't have a dedicated leverage endpoint - leverage is account-wide
MARGIN_TYPE_URL = "api/v1/marginType"
POSITION_MARGIN_URL = "api/v1/positionMargin"

# WebSocket public channels
# Note: Public channels require symbol suffix (e.g., "depth.SOL_USDC")
WS_DEPTH_CHANNEL = "depth"  # Full format: depth.<symbol>
WS_TRADES_CHANNEL = "trade"  # Full format: trade.<symbol> (NOT "trades")
WS_TICKER_CHANNEL = "ticker"  # Full format: ticker.<symbol>
WS_KLINE_CHANNEL = "kline"  # Full format: kline.<interval>.<symbol>

# WebSocket private channels
WS_ACCOUNT_ORDERS_CHANNEL = "account.orderUpdate"  # Fixed from "account.orders"
WS_ACCOUNT_BALANCES_CHANNEL = "account.balanceUpdate"  # Fixed from "account.balances"
WS_ACCOUNT_FILLS_CHANNEL = "account.fillUpdate"  # Note: May not exist in Backpack API

# Perpetual-specific WebSocket channels
WS_ACCOUNT_POSITIONS_CHANNEL = "account.positionUpdate"  # Fixed from "account.positions"
WS_FUNDING_RATE_CHANNEL = "markPrice"  # Funding info comes via markPrice channel
WS_MARK_PRICE_CHANNEL = "markPrice"  # Full format: markPrice.<symbol>
WS_LIQUIDATION_CHANNEL = "liquidation"  # Not account-specific
WS_OPEN_INTEREST_CHANNEL = "openInterest"  # Full format: openInterest.<symbol>

# Order configuration
BROKER_ID = "HBOT"
MAX_ORDER_ID_LEN = 32

# Rate limiting
RATE_LIMIT = "RATE_LIMIT"
PUBLIC_ENDPOINT_LIMIT_ID = "PublicEndpoints"
PRIVATE_ENDPOINT_LIMIT_ID = "PrivateEndpoints"

RATE_LIMITS = [
    # Pool limits
    RateLimit(limit_id=PUBLIC_ENDPOINT_LIMIT_ID, limit=1200, time_interval=60),
    RateLimit(limit_id=PRIVATE_ENDPOINT_LIMIT_ID, limit=100, time_interval=60),
    
    # Specific endpoint limits with weighted connections
    RateLimit(limit_id=ORDER_URL, limit=10, time_interval=1,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=CANCEL_URL, limit=10, time_interval=1,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=POSITIONS_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=FUNDING_RATE_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=BALANCE_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PRIVATE_ENDPOINT_LIMIT_ID, weight=1)]),
    
    # Public market data endpoints
    RateLimit(limit_id=EXCHANGE_INFO_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=TICKER_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=ORDER_BOOK_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=2)]),
    RateLimit(limit_id=TRADES_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=CANDLES_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
    RateLimit(limit_id=TIME_URL, limit=100, time_interval=60,
              linked_limits=[LinkedLimitWeightPair(PUBLIC_ENDPOINT_LIMIT_ID, weight=1)]),
]

# Order type mapping
# Note: Backpack only supports Limit and Market order types
# PostOnly is handled via timeInForce parameter, not orderType
ORDER_TYPE_MAP = {
    OrderType.LIMIT.name: "Limit",
    OrderType.MARKET.name: "Market",
    # OrderType.LIMIT_MAKER is not directly supported - use LIMIT with PostOnly timeInForce
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
    "PostOnly": "PostOnly",  # Post Only orders (maker only)
    "GTX": "PostOnly",  # Map GTX to PostOnly for compatibility
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
DEFAULT_LEVERAGE = 1  # Safe default, actual max per market from API

# Market-specific parameters MUST be fetched from /api/v1/markets endpoint per symbol
# API provides: maxLeverage, initialMarginRatio, maintenanceMarginRatio per market
# The connector will raise an error if these values are missing from the API response
# No fallback values are provided as per Hummingbot standards - fail fast on missing data

# Funding rate configuration
# TODO: Verify if funding schedule is configurable per market
# API provides: nextFundingTime, fundingRateLowerBound, fundingRateUpperBound
FUNDING_INTERVAL_HOURS = 8  # Standard for crypto perpetuals
FUNDING_SETTLEMENT_TIMES = ["00:00", "08:00", "16:00"]  # UTC standard

# WebSocket configuration
WS_HEARTBEAT_INTERVAL = 30  # Send ping every 30 seconds
WS_MESSAGE_TIMEOUT = 60  # Timeout for receiving messages

# Error codes - Backpack-specific
# Note: Backpack uses string error codes, not numeric ones like Binance
# These match the actual Backpack API error response codes from the OpenAPI spec
ORDER_NOT_EXIST_ERROR_CODE = "RESOURCE_NOT_FOUND"  # When order doesn't exist
ORDER_NOT_EXIST_MESSAGE = "Order does not exist"
UNKNOWN_ORDER_ERROR_CODE = "INVALID_ORDER"  # When order is invalid
UNKNOWN_ORDER_MESSAGE = "Invalid order"

# Trading rules update interval
TRADING_RULES_UPDATE_INTERVAL = 3600  # Update every hour

# Funding info update interval
FUNDING_INFO_UPDATE_INTERVAL = 600  # Update every 10 minutes
