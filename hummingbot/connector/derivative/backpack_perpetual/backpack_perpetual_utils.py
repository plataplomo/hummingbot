"""Utility functions for Backpack Perpetual Exchange connector.
"""

from decimal import Decimal
from typing import Any, Literal

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


def split_trading_pair(trading_pair: str) -> tuple[str, str]:
    """Split a Hummingbot trading pair into base and quote assets.

    Args:
        trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Tuple of (base_asset, quote_asset)
    
    Raises:
        ValueError: If the trading pair format is invalid
    """
    if not trading_pair or not isinstance(trading_pair, str):
        raise ValueError(f"Invalid trading pair: {trading_pair}")
    
    parts = trading_pair.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid trading pair format: {trading_pair}. Expected format: 'BASE-QUOTE'")
    
    if not parts[0] or not parts[1]:
        raise ValueError(f"Invalid trading pair format: {trading_pair}. Base and quote must not be empty")
    
    return parts[0], parts[1]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> str | None:
    """Convert Backpack exchange format to Hummingbot format.

    For perpetuals:
    - BTC_PERP -> BTC-USDC
    - SOL_USDC_PERP -> SOL-USDC
    
    For spot (if any):
    - BTC_USDC -> BTC-USDC

    Args:
        exchange_trading_pair: Trading pair from Backpack (e.g., "BTC_PERP", "SOL_USDC_PERP")

    Returns:
        Hummingbot formatted trading pair (e.g., "BTC-USDC") or None if invalid
    """
    try:
        if not exchange_trading_pair:
            return None
            
        # Handle perpetual contracts
        if "_PERP" in exchange_trading_pair:
            # Remove _PERP suffix
            symbol = exchange_trading_pair.replace("_PERP", "")
            
            # If it's just BASE_PERP (e.g., BTC_PERP), add quote currency from constants
            if TRADING_PAIR_SPLITTER not in symbol:
                return f"{symbol}-{CONSTANTS.COLLATERAL_TOKEN}"
            # It's BASE_QUOTE_PERP (e.g., SOL_USDC_PERP)
            return symbol.replace(TRADING_PAIR_SPLITTER, "-")
        
        # Handle regular spot pairs (if any)
        if TRADING_PAIR_SPLITTER in exchange_trading_pair:
            return exchange_trading_pair.replace(TRADING_PAIR_SPLITTER, "-")
            
        return None
    except Exception:
        return None


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    """Convert Hummingbot format to Backpack exchange format.

    For perpetuals with standard quote currency:
    - BTC-USDC -> BTC_PERP
    - SOL-USDC -> SOL_PERP
    - ETH-USDC -> ETH_PERP
    
    For other quote currencies, the full format would be used (e.g., BTC-USD -> BTC_USD_PERP)

    Args:
        hb_trading_pair: Trading pair in Hummingbot format (e.g., "BTC-USDC")

    Returns:
        Backpack formatted trading pair (e.g., "BTC_PERP")
    """
    base, quote = split_trading_pair(hb_trading_pair)
    
    # For standard perpetual quote currency, use simplified format (e.g., BTC_PERP)
    if quote == CONSTANTS.COLLATERAL_TOKEN:
        return f"{base}_PERP"
    # For other quote currencies, use full format
    return f"{base}_{quote}_PERP"


def get_new_client_order_id(
    is_buy: bool,
    trading_pair: str,
    max_id_len: int | None = None,
) -> str:
    """Generate a new client order ID for Backpack.

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


def is_exchange_information_valid(exchange_info: dict[str, Any]) -> bool:
    """Check if the exchange information response is valid.

    Args:
        exchange_info: Response from exchange info endpoint

    Returns:
        True if valid, False otherwise
    """
    if not exchange_info:
        return False
    
    # Must be a dict
    if not isinstance(exchange_info, dict):
        return False
        
    try:
        # Check for required fields based on Backpack's API structure
        # The exchange info should have a list of markets/symbols
        if "symbols" in exchange_info:
            # Check that symbols is a list
            if not isinstance(exchange_info["symbols"], list):
                return False
            # Check that at least one symbol exists
            if len(exchange_info["symbols"]) == 0:
                return False
            
            # Validate each symbol has required fields
            required_symbol_fields = ["symbol", "baseAsset", "quoteAsset", "status"]
            for symbol_info in exchange_info["symbols"]:
                if not isinstance(symbol_info, dict):
                    return False
                for field in required_symbol_fields:
                    if field not in symbol_info:
                        return False
            
            return True
        if "markets" in exchange_info:
            # Alternative field name
            if not isinstance(exchange_info["markets"], list):
                return False
            if len(exchange_info["markets"]) == 0:
                return False
            return True
        return False
    except Exception:
        return False


def decimal_val_or_none(string_value: str) -> Decimal | None:
    """Convert a string to Decimal or return None if invalid.

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
    current_position_amount: Decimal,
) -> PositionAction:
    """Determine the position action for an order based on current position.

    Args:
        in_flight_order: The order to analyze
        current_position_amount: Current position amount (positive for long, negative for short)

    Returns:
        PositionAction.OPEN or PositionAction.CLOSE
    """
    # If no position, any order opens a position
    if current_position_amount == Decimal(0):
        return PositionAction.OPEN

    # Long position exists
    if current_position_amount > 0:
        # Buy adds to long position
        if in_flight_order.trade_type == TradeType.BUY:
            return PositionAction.OPEN
        # Sell reduces/closes long position
        return PositionAction.CLOSE

    # Short position exists
    # Sell adds to short position
    if in_flight_order.trade_type == TradeType.SELL:
        return PositionAction.OPEN
    # Buy reduces/closes short position
    return PositionAction.CLOSE


def is_reduce_only_order(
    order_side: TradeType,
    position_side: str,
    position_amount: Decimal,
) -> bool:
    """Determine if an order should be reduce-only based on position.

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
    """Configuration map for Backpack Perpetual connector.
    """

    connector: Literal["backpack_perpetual"] = Field(default="backpack_perpetual", client_data=None)

    backpack_perpetual_api_key: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Backpack Perpetual API key",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )

    backpack_perpetual_api_secret: SecretStr = Field(
        default=...,
        client_data=ClientFieldData(
            prompt=lambda cm: "Enter your Backpack Perpetual API secret",
            is_secure=True,
            is_connect_key=True,
            prompt_on_new=True,
        ),
    )

    class Config:
        title = "backpack_perpetual"


# Order type validation
def is_order_type_valid(order_type: OrderType) -> bool:
    """Check if the order type is supported by Backpack Perpetual.

    Args:
        order_type: Order type to validate

    Returns:
        True if supported, False otherwise
    """
    return order_type in [
        OrderType.LIMIT,
        OrderType.MARKET,
        OrderType.LIMIT_MAKER,
    ]


# Symbol validation for perpetuals
def is_perpetual_symbol(symbol: str) -> bool:
    """Check if a symbol is a perpetual contract.

    For Backpack, perpetual symbols end with _PERP.

    Args:
        symbol: Symbol to check

    Returns:
        True if it's a perpetual symbol
    """
    if not symbol:
        return False
    # Backpack perpetuals use formats like BTC_PERP, SOL_USDC_PERP
    return symbol.endswith("_PERP")


def get_trading_pair_from_symbol(symbol: str) -> str | None:
    """Extract trading pair from a perpetual symbol.
    
    Args:
        symbol: Exchange symbol (e.g., "BTC_PERP")
    
    Returns:
        Trading pair in Hummingbot format (e.g., "BTC-USDC") or None if not a perpetual
    """
    if not is_perpetual_symbol(symbol):
        return None
    return convert_from_exchange_trading_pair(symbol)


def get_next_funding_timestamp(current_timestamp: float = None) -> int:
    """Calculate the next funding timestamp.
    
    Backpack perpetuals have funding every 8 hours at 00:00, 08:00, and 16:00 UTC.
    
    Args:
        current_timestamp: Current timestamp in seconds (optional)
    
    Returns:
        Next funding timestamp in seconds
    """
    import time
    
    if current_timestamp is None:
        current_timestamp = time.time()
    
    # Funding every 8 hours
    funding_interval = 8 * 60 * 60  # 8 hours in seconds
    
    # Calculate next funding time
    # Funding times are at 00:00, 08:00, 16:00 UTC
    next_funding = ((int(current_timestamp) // funding_interval) + 1) * funding_interval
    
    return next_funding
