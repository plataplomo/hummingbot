"""
Utility functions for Backpack Perpetual Exchange connector.
"""

from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

from pydantic import Field, SecretStr

from hummingbot.client.config.config_data_types import BaseConnectorConfigMap, ClientFieldData
from hummingbot.core.data_type.common import OrderType, PositionAction, TradeType
from hummingbot.core.data_type.in_flight_order import InFlightOrder
from hummingbot.core.data_type.trade_fee import TradeFeeSchema

from . import backpack_perpetual_constants as CONSTANTS

# Backpack uses underscore format for symbols
TRADING_PAIR_SPLITTER = "_"

DEFAULT_FEES = TradeFeeSchema(
    maker_percent_fee_decimal=Decimal("0.0002"),  # 0.02% maker fee
    taker_percent_fee_decimal=Decimal("0.0005"),  # 0.05% taker fee
)


def split_trading_pair(trading_pair: str) -> Optional[Tuple[str, str]]:
    """
    Split a Hummingbot trading pair into base and quote assets.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Tuple of (base_asset, quote_asset) or None if invalid
    """
    try:
        parts = trading_pair.split("-")
        if len(parts) != 2:
            return None
        return parts[0], parts[1]
    except Exception:
        return None


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    """
    Convert Backpack exchange format to Hummingbot format.

    Backpack uses underscore format: BTC_USDC
    Hummingbot uses dash format: BTC-USDC

    Args:
        exchange_trading_pair: Trading pair from Backpack (e.g., "BTC_USDC")

    Returns:
        Hummingbot formatted trading pair (e.g., "BTC-USDC") or None if invalid
    """
    try:
        if TRADING_PAIR_SPLITTER not in exchange_trading_pair:
            return None
        return exchange_trading_pair.replace(TRADING_PAIR_SPLITTER, "-")
    except Exception:
        return None


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """
    Convert Hummingbot format to Backpack exchange format.

    Hummingbot uses dash format: BTC-USDC
    Backpack uses underscore format: BTC_USDC

    Args:
        hb_trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Backpack formatted trading pair (e.g., "BTC_USDC")
    """
    return hb_trading_pair.replace("-", TRADING_PAIR_SPLITTER)


def get_new_client_order_id(
    is_buy: bool,
    trading_pair: str,
    max_id_len: Optional[int] = None
) -> str:
    """
    Generate a new client order ID for Backpack.

    Format: HBOT-<timestamp>-<B/S>-<pair_abbrev>

    Args:
        is_buy: True for buy orders, False for sell orders
        trading_pair: Trading pair for the order
        max_id_len: Maximum length for the order ID

    Returns:
        New client order ID
    """
    import time

    side = "B" if is_buy else "S"
    base, quote = split_trading_pair(trading_pair)

    # Create abbreviated pair (first 3 chars of each)
    pair_abbrev = f"{base[:3]}{quote[:3]}".upper()

    # Generate timestamp-based ID
    timestamp = int(time.time() * 1000) % 1000000000  # Keep it reasonably short
    order_id = f"{CONSTANTS.BROKER_ID}-{timestamp}-{side}-{pair_abbrev}"

    # Ensure it doesn't exceed max length
    if max_id_len and len(order_id) > max_id_len:
        # Truncate the timestamp portion if needed
        excess = len(order_id) - max_id_len
        timestamp_str = str(timestamp)[excess:]
        order_id = f"{CONSTANTS.BROKER_ID}-{timestamp_str}-{side}-{pair_abbrev}"

    return order_id


def is_exchange_information_valid(exchange_info: Dict[str, Any]) -> bool:
    """
    Check if the exchange information response is valid.

    Args:
        exchange_info: Response from exchange info endpoint

    Returns:
        True if valid, False otherwise
    """
    try:
        # Check for required fields
        required_fields = ["symbols"]
        for field in required_fields:
            if field not in exchange_info:
                return False

        # Check that symbols is a list
        if not isinstance(exchange_info["symbols"], list):
            return False

        # Check that at least one symbol exists
        if len(exchange_info["symbols"]) == 0:
            return False

        return True
    except Exception:
        return False


def decimal_val_or_none(string_value: str) -> Optional[Decimal]:
    """
    Convert a string to Decimal or return None if invalid.

    Args:
        string_value: String representation of a number

    Returns:
        Decimal value or None if conversion fails
    """
    try:
        if string_value is None or string_value == "":
            return None
        return Decimal(str(string_value))
    except Exception:
        return None


def get_position_action(
    in_flight_order: InFlightOrder,
    current_position_amount: Decimal
) -> PositionAction:
    """
    Determine the position action for an order based on current position.

    Args:
        in_flight_order: The order to analyze
        current_position_amount: Current position amount (positive for long, negative for short)

    Returns:
        PositionAction.OPEN or PositionAction.CLOSE
    """
    # If no position, any order opens a position
    if current_position_amount == Decimal("0"):
        return PositionAction.OPEN

    # Long position exists
    if current_position_amount > 0:
        # Buy adds to long position
        if in_flight_order.trade_type == TradeType.BUY:
            return PositionAction.OPEN
        # Sell reduces/closes long position
        else:
            return PositionAction.CLOSE

    # Short position exists
    else:
        # Sell adds to short position
        if in_flight_order.trade_type == TradeType.SELL:
            return PositionAction.OPEN
        # Buy reduces/closes short position
        else:
            return PositionAction.CLOSE


def is_reduce_only_order(
    order_side: TradeType,
    position_side: str,
    position_amount: Decimal
) -> bool:
    """
    Determine if an order should be reduce-only based on position.

    Args:
        order_side: Side of the order (BUY/SELL)
        position_side: Side of the position ("LONG"/"SHORT")
        position_amount: Amount of the position

    Returns:
        True if order should be reduce-only
    """
    if position_amount == 0:
        return False

    # Long position: sell orders are reduce-only
    if position_side == "LONG" and order_side == TradeType.SELL:
        return True

    # Short position: buy orders are reduce-only
    if position_side == "SHORT" and order_side == TradeType.BUY:
        return True

    return False


class BackpackPerpetualConfigMap(BaseConnectorConfigMap):
    """
    Configuration map for Backpack Perpetual connector.
    """

    connector: str = Field(default="backpack_perpetual", const=True, client_data=None)

    backpack_perpetual_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Backpack Perpetual API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    backpack_perpetual_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Backpack Perpetual API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        )
    )

    class Config:
        title = "backpack_perpetual"


# Order type validation
def is_order_type_valid(order_type: OrderType) -> bool:
    """
    Check if the order type is supported by Backpack Perpetual.

    Args:
        order_type: Order type to validate

    Returns:
        True if supported, False otherwise
    """
    return order_type in [
        OrderType.LIMIT,
        OrderType.MARKET,
        OrderType.LIMIT_MAKER
    ]


# Symbol validation for perpetuals
def is_perpetual_symbol(symbol: str) -> bool:
    """
    Check if a symbol is a perpetual contract.

    For Backpack, perpetual symbols typically end with PERP or have specific patterns.
    This is a simplified check - actual implementation should query exchange info.

    Args:
        symbol: Symbol to check

    Returns:
        True if likely a perpetual symbol
    """
    # Backpack perpetuals use USDC as quote currency
    # Common patterns: BTC_USDC, ETH_USDC, SOL_USDC
    return symbol.endswith("_USDC") or symbol.endswith("-USDC")
