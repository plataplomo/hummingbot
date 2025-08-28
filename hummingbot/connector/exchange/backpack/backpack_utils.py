"""Utility functions for Backpack exchange connector.
Handles trading pair conversions between Hummingbot and Backpack formats.
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime
from decimal import Decimal

from pydantic import ConfigDict, Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap
from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS
from hummingbot.core.data_type.common import OrderType, TradeType
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

CENTRALIZED = True
EXAMPLE_PAIR = "BTC-USD"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0008"),  # 0.08% maker fee
    taker_percent_fee_decimal=Decimal("0.0010"),  # 0.10% taker fee
    buy_percent_fee_deducted_from_returns=True,
)


class BackpackConfigMap(BaseConnectorConfigMap):
    connector: str = "backpack"
    backpack_api_key: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Backpack API key",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    backpack_api_secret: SecretStr = Field(
        default=...,
        json_schema_extra={
            "prompt": lambda cm: "Enter your Backpack API secret",
            "is_secure": True,
            "is_connect_key": True,
            "prompt_on_new": True,
        },
    )
    model_config = ConfigDict(title="backpack")


KEYS = BackpackConfigMap.model_construct()


def split_trading_pair(trading_pair: str) -> tuple[str, str]:
    """Split a Hummingbot trading pair into base and quote assets.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Tuple of (base_asset, quote_asset)

    Example:
        split_trading_pair("BTC-USDC") -> ("BTC", "USDC")
    """
    try:
        base, quote = trading_pair.split("-")
        return base, quote
    except ValueError as err:
        raise ValueError(f"Invalid trading pair format: {trading_pair}. Expected format: 'BASE-QUOTE'") from err


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str:
    """Convert trading pair from Backpack exchange format to Hummingbot format.

    Backpack uses underscore format: "BTC_USDC"
    Hummingbot uses dash format: "BTC-USDC"

    Args:
        exchange_trading_pair: Trading pair in Backpack format

    Returns:
        Trading pair in Hummingbot format

    Example:
        convert_from_exchange_trading_pair("BTC_USDC") -> "BTC-USDC"
    """
    return exchange_trading_pair.replace("_", "-")


def backpack_order_type(order_type: OrderType) -> str:
    """Convert Hummingbot order type to Backpack format.

    Args:
        order_type: Hummingbot OrderType

    Returns:
        Backpack order type string
    """
    return CONSTANTS.ORDER_TYPE_MAP.get(order_type.name, order_type.name)


def to_hb_order_type(backpack_type: str) -> OrderType:
    """Convert Backpack order type to Hummingbot format.

    Args:
        backpack_type: Backpack order type string

    Returns:
        Hummingbot OrderType
    """
    type_map = {v: k for k, v in CONSTANTS.ORDER_TYPE_MAP.items()}
    return OrderType[type_map.get(backpack_type, backpack_type)]


def backpack_order_side(trade_type: TradeType) -> str:
    """Convert Hummingbot trade type to Backpack format.

    Args:
        trade_type: Hummingbot TradeType

    Returns:
        Backpack side string (Bid/Ask)
    """
    return CONSTANTS.ORDER_SIDE_MAP.get(trade_type.name, trade_type.name)


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """Convert trading pair from Hummingbot format to Backpack exchange format.

    Hummingbot uses dash format: "BTC-USDC"
    Backpack uses underscore format: "BTC_USDC"

    Args:
        hb_trading_pair: Trading pair in Hummingbot format

    Returns:
        Trading pair in Backpack format

    Example:
        convert_to_exchange_trading_pair("BTC-USDC") -> "BTC_USDC"
    """
    return hb_trading_pair.replace("-", "_")


class BackpackIDMapper:
    """Handles bidirectional mapping between Hummingbot string IDs and Backpack numeric IDs.

    Backpack requires integer clientId (uint32), while Hummingbot uses string IDs.
    This class maintains the mapping between the two systems.
    """

    def __init__(self):
        """Initialize the ID mapper with empty mappings."""
        self._hb_to_numeric: dict[str, int] = {}
        self._numeric_to_hb: dict[int, str] = {}

    def get_numeric_id(self, hb_order_id: str) -> int:
        """Convert Hummingbot string order ID to Backpack numeric ID.

        Args:
            hb_order_id: Hummingbot's string order ID

        Returns:
            Numeric ID for Backpack API
        """
        if hb_order_id not in self._hb_to_numeric:
            # Generate a numeric client ID that fits in uint32 (max value: 4294967295)
            # Use timestamp in milliseconds modulo to fit in range
            # Add random component for uniqueness within the same millisecond
            timestamp_component = int(time.time() * 1000) % 1000000000  # Keep under 1 billion
            random_component = secrets.randbelow(1000)  # 0-999

            # Combine timestamp and random, ensuring we stay under uint32 max
            numeric_id = (timestamp_component * 1000 + random_component) % 4294967296

            # Store bidirectional mapping
            self._hb_to_numeric[hb_order_id] = numeric_id
            self._numeric_to_hb[numeric_id] = hb_order_id

        return self._hb_to_numeric[hb_order_id]

    def get_hb_id(self, numeric_id: int) -> str | None:
        """Convert Backpack numeric ID back to Hummingbot string order ID.

        Args:
            numeric_id: Backpack's numeric client ID

        Returns:
            Hummingbot's string order ID, or None if not found
        """
        return self._numeric_to_hb.get(numeric_id)

    def clear(self):
        """Clear all mappings."""
        self._hb_to_numeric.clear()
        self._numeric_to_hb.clear()


def is_exchange_information_valid(exchange_info: dict) -> bool:
    """Validate exchange information response from Backpack.

    Args:
        exchange_info: Exchange info response from API

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(exchange_info, dict) or "data" not in exchange_info:
        return False

    # Validate symbols structure
    symbols = exchange_info["data"]  # Already checked in earlier validation
    if not isinstance(symbols, list):
        return False

    if len(symbols) == 0:
        return False

    # Check first symbol has required fields (new API structure)
    first_symbol = symbols[0]
    required_symbol_fields = ["symbol", "baseSymbol", "quoteSymbol", "marketType"]
    return all(field in first_symbol for field in required_symbol_fields)


def validate_trading_pair(trading_pair: str) -> bool:
    """Validate that a trading pair is in correct Hummingbot format.

    Args:
        trading_pair: Trading pair to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(trading_pair, str) or "-" not in trading_pair:
        return False

    parts = trading_pair.split("-")
    if len(parts) != 2:
        return False

    base, quote = parts
    if not base or not quote:
        return False

    # Check for reasonable asset name lengths
    return not (len(base) > 10 or len(quote) > 10)


def format_trading_pair_for_display(trading_pair: str) -> str:
    """Format trading pair for display purposes.

    Args:
        trading_pair: Trading pair in Hummingbot format

    Returns:
        Formatted trading pair string
    """
    if not validate_trading_pair(trading_pair):
        return trading_pair

    base, quote = split_trading_pair(trading_pair)
    return f"{base}/{quote}"


def normalize_trading_pair(trading_pair: str) -> str:
    """Normalize a trading pair to standard Hummingbot format.

    Handles various input formats and converts to standard BTC-USDC format.

    Args:
        trading_pair: Trading pair in various formats

    Returns:
        Normalized trading pair in Hummingbot format
    """
    if not isinstance(trading_pair, str):
        raise ValueError("Trading pair must be a string")

    # Remove whitespace
    trading_pair = trading_pair.strip().upper()

    # Handle different separators
    if "_" in trading_pair:
        # Backpack format: BTC_USDC -> BTC-USDC
        trading_pair = trading_pair.replace("_", "-")
    elif "/" in trading_pair:
        # Display format: BTC/USDC -> BTC-USDC
        trading_pair = trading_pair.replace("/", "-")

    # Validate result
    if not validate_trading_pair(trading_pair):
        raise ValueError(f"Invalid trading pair format: {trading_pair}")

    return trading_pair


def parse_fill_timestamp(timestamp: str | float | None) -> float:
    """Parse fill timestamp from various formats.

    Backpack API can return timestamps in different formats:
    - ISO format string: "2025-08-27T21:53:48.442"
    - Microseconds as integer: 1756331628442000
    - Microseconds as string: "1756331628442000"

    Args:
        timestamp: Timestamp in ISO format, microseconds, or None

    Returns:
        Timestamp in seconds as float
    """
    if timestamp is None:
        return time.time()

    # If it's a string
    if isinstance(timestamp, str):
        if "T" in timestamp:  # ISO format like "2025-08-27T21:53:48.442"
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return dt.timestamp()
        # Try to parse as microseconds string
        return int(timestamp) / 1_000_000

    # If it's numeric, assume microseconds
    return float(timestamp) / 1_000_000
