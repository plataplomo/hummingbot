"""Utility functions for Backpack exchange connector.
Handles trading pair conversions between Hummingbot and Backpack formats.
"""

from __future__ import annotations

import secrets
import time


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


def get_new_client_order_id(is_buy: bool, trading_pair: str) -> str:
    """Generate a new client order ID for Backpack.

    Args:
        is_buy: Whether this is a buy order
        trading_pair: Trading pair for the order

    Returns:
        Client order ID string
    """
    side = "B" if is_buy else "S"
    base_asset, _ = split_trading_pair(trading_pair)
    timestamp = int(time.time() * 1000)
    random_suffix = secrets.randbelow(900) + 100  # Random between 100-999

    # Format: HBOT-{SIDE}{BASE}{TIMESTAMP}{RANDOM}
    # Keep within MAX_ORDER_ID_LEN limit from constants
    client_id = f"HBOT-{side}{base_asset}{timestamp}{random_suffix}"

    # Truncate if too long (should not happen with reasonable asset names)
    # MAX_ORDER_ID_LEN is typically 36 characters
    max_order_id_len = 36
    if len(client_id) > max_order_id_len:
        client_id = client_id[:max_order_id_len]

    return client_id


def is_exchange_information_valid(exchange_info: dict) -> bool:
    """Validate exchange information response from Backpack.

    Args:
        exchange_info: Exchange info response from API

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(exchange_info, dict):
        return False

    # Check for required fields
    required_fields = ["symbols"]
    for field in required_fields:
        if field not in exchange_info:
            return False

    # Validate symbols structure
    symbols = exchange_info.get("symbols", [])
    if not isinstance(symbols, list):
        return False
    
    if len(symbols) == 0:
        return False

    # Check first symbol has required fields
    first_symbol = symbols[0]
    required_symbol_fields = ["symbol", "baseAsset", "quoteAsset", "status"]
    return all(field in first_symbol for field in required_symbol_fields)


def validate_trading_pair(trading_pair: str) -> bool:
    """Validate that a trading pair is in correct Hummingbot format.

    Args:
        trading_pair: Trading pair to validate

    Returns:
        True if valid, False otherwise
    """
    if not isinstance(trading_pair, str):
        return False

    if "-" not in trading_pair:
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
