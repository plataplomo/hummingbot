"""Constants for Backpack Perpetual Candles Feed."""

from bidict import bidict

from hummingbot.core.api_throttler.data_types import LinkedLimitWeightPair, RateLimit

# Backpack API endpoints - perpetual uses same endpoints as spot
REST_URL = "https://api.backpack.exchange"
HEALTH_CHECK_ENDPOINT = "/api/v1/ping"
CANDLES_ENDPOINT = "/api/v1/klines"

# WebSocket URL - same for both spot and perpetual
WSS_URL = "wss://ws.backpack.exchange"

# Interval mapping - same as spot
INTERVALS = bidict({
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "8h": "8h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1M": "1month"  # Note: Backpack uses "1month" not "1M"
})

# Maximum results per REST request
MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST = 1000

# Rate limiting - from Backpack connector constants
# Public endpoints: 20 requests per second
RATE_LIMITS = [
    RateLimit(
        CANDLES_ENDPOINT,
        limit=20,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair("raw", 1)]
    ),
    RateLimit(
        HEALTH_CHECK_ENDPOINT,
        limit=20,
        time_interval=1,
        linked_limits=[LinkedLimitWeightPair("raw", 1)]
    )
]
