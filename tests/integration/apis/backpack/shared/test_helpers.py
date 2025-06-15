"""Shared utilities for Backpack integration tests.

This module provides common helper functions for all Backpack integration tests,
including dynamic pricing, market data retrieval, and order management utilities.
These helpers ensure tests use real market data instead of hardcoded values.
"""

from __future__ import annotations

import hashlib
from decimal import Decimal
from typing import TYPE_CHECKING

from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide

if TYPE_CHECKING:
    from cyberdelta.apis.backpack.bp_api import BackpackAPI
    from cyberdelta.core.models.market.ticker import Ticker

logger = get_logger(__name__)


# =============================================================================
# Market Data Utilities
# =============================================================================


async def get_symbol_tick_size(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the tick size (price precision) for a symbol using public API.

    Args:
        api: Backpack API instance
        symbol: Trading symbol (e.g., "SOL-USDC", "BTC-USDC", "SOL_USDC_PERP")

    Returns:
        Tick size for the symbol (price precision)

    Example:
        >>> tick_size = await get_symbol_tick_size(api, "SOL-USDC")
        >>> # Returns Decimal("0.01") for 2 decimal places
    """
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.tick_size

        logger.warning(f"Symbol {symbol} not found in markets, using default tick size")
        return Decimal("0.01")  # Fallback default

    except Exception as e:
        logger.warning(f"Failed to get tick size for {symbol}: {e}, using default")
        return Decimal("0.01")  # Fallback default


async def get_symbol_step_size(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the step size (quantity precision) for a symbol using public API.

    Args:
        api: Backpack API instance
        symbol: Trading symbol (e.g., "SOL-USDC", "BTC-USDC", "SOL_USDC_PERP")

    Returns:
        Step size for the symbol (quantity precision)
    """
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketsArgs

        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.step_size

        logger.warning(f"Symbol {symbol} not found in markets, using default step size")
        return Decimal("0.01")  # Fallback default

    except Exception as e:
        logger.warning(f"Failed to get step size for {symbol}: {e}, using default")
        return Decimal("0.01")  # Fallback default


async def get_market_constraints(api: BackpackAPI, symbol: str) -> dict[str, Decimal]:
    """Get market constraints for a symbol.

    Args:
        api: Backpack API instance
        symbol: Trading symbol

    Returns:
        Dict containing market constraints:
        - tick_size: Price precision
        - step_size: Quantity precision
        - min_quantity: Minimum order size (if available)
        - max_quantity: Maximum order size (if available)
        - min_price: Minimum price (if available)
        - max_price: Maximum price (if available)
    """
    try:
        from cyberdelta.apis.models.service_args_models import GetMarketArgs

        market = await api.get_market(GetMarketArgs(symbol=symbol))

        constraints = {
            "tick_size": market.tick_size,
            "step_size": market.step_size,
        }

        if market.min_quantity is not None:
            constraints["min_quantity"] = market.min_quantity
        if market.max_quantity is not None:
            constraints["max_quantity"] = market.max_quantity
        if market.min_price is not None:
            constraints["min_price"] = market.min_price
        if market.max_price is not None:
            constraints["max_price"] = market.max_price

        return constraints

    except Exception as e:
        logger.warning(f"Failed to get market constraints for {symbol}: {e}, using defaults")
        return {
            "tick_size": Decimal("0.01"),
            "step_size": Decimal("0.01"),
        }


# =============================================================================
# Dynamic Pricing Utilities
# =============================================================================


async def get_current_market_price(api: BackpackAPI, symbol: str) -> Decimal:
    """Get the current market price for a symbol.

    Args:
        api: Backpack API instance
        symbol: Trading symbol

    Returns:
        Current market price

    Raises:
        ValueError: If no market price can be determined
    """
    ticker: Ticker = await api.get_ticker(symbol)

    if ticker.price is not None:
        return ticker.price
    elif ticker.mid_price is not None:
        return ticker.mid_price
    elif ticker.bid is not None and ticker.ask is not None:
        return (ticker.bid + ticker.ask) / Decimal("2")
    elif ticker.bid is not None:
        return ticker.bid
    elif ticker.ask is not None:
        return ticker.ask
    else:
        raise ValueError(f"Unable to determine market price for {symbol}")


async def get_dynamic_test_price(
    api: BackpackAPI, symbol: str, side: OrderSide, tolerance_percent: Decimal = Decimal("5")
) -> Decimal:
    """Get a dynamic test price based on current market conditions.

    This function fetches the current market price and calculates a test price
    that is far enough from market to avoid accidental fills, but close enough
    to be accepted by the exchange's price validation.

    Args:
        api: Backpack API instance
        symbol: Trading symbol (e.g., "SOL-USDC", "BTC-USDC")
        side: Order side (BUY or SELL)
        tolerance_percent: Percentage away from market price (default 5%)

    Returns:
        Test price quantized to proper tick size

    Example:
        >>> # For SOL-USDC at $150, BUY side with 5% tolerance
        >>> price = await get_dynamic_test_price(api, "SOL-USDC", OrderSide.BUY)
        >>> # Returns ~$142.50 (5% below market, quantized to tick size)
    """
    try:
        ticker: Ticker = await api.get_ticker(symbol)

        market_price = None
        if ticker.price is not None:
            market_price = ticker.price
        elif ticker.mid_price is not None:
            market_price = ticker.mid_price
        elif side == OrderSide.BUY and ticker.ask is not None:
            market_price = ticker.ask
        elif side == OrderSide.SELL and ticker.bid is not None:
            market_price = ticker.bid
        else:
            raise ValueError(f"Unable to determine market price for {symbol}")

        tolerance_factor = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            test_price = market_price * (Decimal("1") - tolerance_factor)
        else:
            test_price = market_price * (Decimal("1") + tolerance_factor)

        tick_size = await get_symbol_tick_size(api, symbol)
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except Exception as e:
        # Fallback prices based on symbol type
        fallback_prices = get_fallback_test_prices(symbol, side)
        logger.warning(
            "Dynamic pricing failed for %s, using fallback price %s: %s",
            symbol,
            fallback_prices,
            e,
        )
        return fallback_prices


def get_fallback_test_prices(symbol: str, side: OrderSide) -> Decimal:
    """Get fallback test prices when dynamic pricing fails.

    Args:
        symbol: Trading symbol
        side: Order side

    Returns:
        Fallback test price
    """
    # Normalize symbol for comparison (handle both - and _ formats)
    normalized_symbol = symbol.upper().replace("-", "_")

    # Spot market fallbacks
    if (
        "SOL" in normalized_symbol
        and "USDC" in normalized_symbol
        and "_PERP" not in normalized_symbol
    ):
        return Decimal("50.00") if side == OrderSide.BUY else Decimal("200.00")
    elif (
        "BTC" in normalized_symbol
        and "USDC" in normalized_symbol
        and "_PERP" not in normalized_symbol
    ):
        return Decimal("40000.00") if side == OrderSide.BUY else Decimal("60000.00")
    elif (
        "ETH" in normalized_symbol
        and "USDC" in normalized_symbol
        and "_PERP" not in normalized_symbol
    ):
        return Decimal("2000.00") if side == OrderSide.BUY else Decimal("3000.00")

    # Perp market fallbacks
    elif normalized_symbol == "SOL_USDC_PERP":
        return Decimal("140.00") if side == OrderSide.BUY else Decimal("160.00")
    elif normalized_symbol == "BTC_USDC_PERP":
        return Decimal("40000.00") if side == OrderSide.BUY else Decimal("45000.00")
    elif normalized_symbol == "ETH_USDC_PERP":
        return Decimal("2000.00") if side == OrderSide.BUY else Decimal("3000.00")

    # Generic fallback
    else:
        return Decimal("90.00") if side == OrderSide.BUY else Decimal("110.00")


# =============================================================================
# Order Management Utilities
# =============================================================================


async def get_minimal_order_size(
    api: BackpackAPI, symbol: str, side: OrderSide, price: Decimal
) -> Decimal:
    """Calculate the minimal order size that fits within available balance.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        side: Order side
        price: Order price

    Returns:
        Minimal order size for testing
    """
    try:
        constraints = await get_market_constraints(api, symbol)
        step_size = constraints["step_size"]
        min_quantity = constraints.get("min_quantity", step_size)

        # Use minimum quantity or a small test amount
        test_quantity = max(min_quantity, Decimal("0.01"))

        # Quantize to step size
        quantized_quantity = test_quantity.quantize(step_size)
        return quantized_quantity.normalize()

    except Exception as e:
        logger.warning(f"Failed to calculate minimal order size for {symbol}: {e}")
        return Decimal("0.01")  # Safe fallback


async def validate_order_constraints(
    api: BackpackAPI, symbol: str, side: OrderSide, quantity: Decimal, price: Decimal
) -> dict[str, bool]:
    """Validate order parameters against market constraints.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        side: Order side
        quantity: Order quantity
        price: Order price

    Returns:
        Dict with validation results:
        - price_valid: Price meets tick size requirements
        - quantity_valid: Quantity meets step size requirements
        - size_valid: Quantity meets min/max requirements
        - price_range_valid: Price within min/max range
    """
    try:
        constraints = await get_market_constraints(api, symbol)

        # Check price precision
        tick_size = constraints["tick_size"]
        price_valid = (price % tick_size) == 0

        # Check quantity precision
        step_size = constraints["step_size"]
        quantity_valid = (quantity % step_size) == 0

        # Check quantity limits
        size_valid = True
        if "min_quantity" in constraints:
            size_valid = size_valid and quantity >= constraints["min_quantity"]
        if "max_quantity" in constraints:
            size_valid = size_valid and quantity <= constraints["max_quantity"]

        # Check price limits
        price_range_valid = True
        if "min_price" in constraints:
            price_range_valid = price_range_valid and price >= constraints["min_price"]
        if "max_price" in constraints:
            price_range_valid = price_range_valid and price <= constraints["max_price"]

        return {
            "price_valid": price_valid,
            "quantity_valid": quantity_valid,
            "size_valid": size_valid,
            "price_range_valid": price_range_valid,
        }

    except Exception as e:
        logger.warning(f"Failed to validate order constraints for {symbol}: {e}")
        return {
            "price_valid": False,
            "quantity_valid": False,
            "size_valid": False,
            "price_range_valid": False,
        }


# =============================================================================
# Symbol Utilities
# =============================================================================


def is_perp_symbol(symbol: str) -> bool:
    """Check if a symbol is a perpetual futures symbol.

    Args:
        symbol: Trading symbol

    Returns:
        True if symbol is a perp market
    """
    return "_PERP" in symbol.upper()


def is_spot_symbol(symbol: str) -> bool:
    """Check if a symbol is a spot trading symbol.

    Args:
        symbol: Trading symbol

    Returns:
        True if symbol is a spot market
    """
    return not is_perp_symbol(symbol)


def get_base_quote_assets(symbol: str) -> tuple[str, str]:
    """Extract base and quote assets from a trading symbol.

    Args:
        symbol: Trading symbol (e.g., "SOL_USDC", "BTC_USDC_PERP")

    Returns:
        Tuple of (base_asset, quote_asset)

    Example:
        >>> get_base_quote_assets("SOL_USDC")
        ("SOL", "USDC")
        >>> get_base_quote_assets("BTC_USDC_PERP")
        ("BTC", "USDC")
    """
    # Handle perp symbols
    if is_perp_symbol(symbol):
        # Remove _PERP suffix and split
        base_symbol = symbol.replace("_PERP", "")
        if "_" in base_symbol:
            parts = base_symbol.split("_")
            return parts[0], parts[1]

    # Handle spot symbols (Backpack uses underscores)
    if "_" in symbol:
        parts = symbol.split("_")
        return parts[0], parts[1]
    elif "-" in symbol:
        # Legacy support for dash format
        parts = symbol.split("-")
        return parts[0], parts[1]

    # Fallback
    return symbol, "USDC"


def generate_deterministic_client_order_id(test_name: str, symbol: str, side: str) -> str:
    """Generate a deterministic client order ID for VCR testing.

    This creates a consistent client order ID based on the test name, symbol, and side
    to ensure VCR cassettes work reliably while avoiding hardcoded values.

    Args:
        test_name: Name of the test function
        symbol: Trading symbol
        side: Order side (BUY/SELL)

    Returns:
        Deterministic client order ID as string convertible to integer
    """
    # Create a hash from test context to ensure deterministic but unique IDs
    context = f"{test_name}_{symbol}_{side}"
    hash_obj = hashlib.md5(context.encode())

    # Convert to integer and ensure it's within reasonable range for Backpack
    # Use first 8 hex chars to create a 7-8 digit integer (Backpack compatible)
    hex_str = hash_obj.hexdigest()[:8]
    client_id = int(hex_str, 16) % 999999999  # Keep it under 10 digits

    # Ensure minimum 6 digits for consistency
    if client_id < 100000:
        client_id += 100000

    return str(client_id)


# =============================================================================
# Common Test Symbols
# =============================================================================


COMMON_SPOT_SYMBOLS = [
    "SOL_USDC",
    "BTC_USDC",
    "ETH_USDC",
    "USDT_USDC",
]

COMMON_PERP_SYMBOLS = [
    "SOL_USDC_PERP",
    "BTC_USDC_PERP",
    "ETH_USDC_PERP",
]

DEFAULT_TEST_SYMBOL_SPOT = "SOL_USDC"
DEFAULT_TEST_SYMBOL_PERP = "SOL_USDC_PERP"


# =============================================================================
# Test Tolerances and Thresholds
# =============================================================================


# Precision tolerances for balance comparisons
BALANCE_PRECISION_TOLERANCE = Decimal("0.0001")  # 0.0001 units
DUST_THRESHOLD = Decimal("0.00001")  # Amounts below this are considered dust

# Percentage tolerances
PRICE_TOLERANCE_PERCENT = Decimal("0.01")  # 0.01% for price comparisons
QUANTITY_TOLERANCE_PERCENT = Decimal("0.01")  # 0.01% for quantity comparisons
EQUITY_TOLERANCE_PERCENT = Decimal("0.1")  # 0.1% for equity calculations

# Fixed value tolerances
SMALL_VALUE_TOLERANCE = Decimal("0.01")  # $0.01 for USD values
LARGE_VALUE_TOLERANCE = Decimal("1.0")  # $1.00 for larger USD calculations

# Margin calculation tolerances
MARGIN_FRACTION_TOLERANCE = Decimal("0.1")  # 10% tolerance for margin fraction differences
COLLATERAL_VALUE_TOLERANCE = Decimal("0.01")  # $0.01 for collateral value calculations

# Auto-lending specific
AUTO_LENDING_DETECTION_THRESHOLD = Decimal(
    "0"
)  # If all spot balances are 0, auto-lending is likely active


# =============================================================================
# Margin and Account Helpers
# =============================================================================


async def get_account_margin_parameters(api: BackpackAPI) -> dict[str, Decimal | None]:
    """Get current margin parameters from the exchange.

    Args:
        api: Backpack API instance

    Returns:
        Dict containing margin parameters:
        - margin_fraction: Current margin utilization
        - initial_margin_factor: IMF from exchange
        - maintenance_margin_factor: MMF from exchange
        - has_positions: Whether account has positions
        - has_open_orders: Whether account has open orders
    """
    try:
        # Get account summary which includes margin info
        account_summary = await api.get_account_summary()

        params = {
            "margin_fraction": None,
            "initial_margin_factor": None,
            "maintenance_margin_factor": None,
            "has_positions": False,
            "has_open_orders": False,
        }

        if account_summary.bp_details:
            params["margin_fraction"] = account_summary.bp_details.margin_fraction

            # Parse IMF/MMF from raw strings if available
            if account_summary.bp_details.imf_raw:
                try:
                    params["initial_margin_factor"] = Decimal(account_summary.bp_details.imf_raw)
                except:
                    pass

            if account_summary.bp_details.mmf_raw:
                try:
                    params["maintenance_margin_factor"] = Decimal(
                        account_summary.bp_details.mmf_raw
                    )
                except:
                    pass

        # Check for positions
        positions = await api.get_positions()
        params["has_positions"] = len(positions) > 0

        # Check for open orders
        open_orders = await api.get_open_orders()
        params["has_open_orders"] = len(open_orders) > 0

        return params

    except Exception as e:
        logger.warning(f"Failed to get margin parameters: {e}")
        return {
            "margin_fraction": None,
            "initial_margin_factor": None,
            "maintenance_margin_factor": None,
            "has_positions": False,
            "has_open_orders": False,
        }


async def validate_margin_consistency(
    api: BackpackAPI, account_summary: MarginAccountSummary
) -> dict[str, bool]:
    """Validate margin calculations are internally consistent.

    Args:
        api: Backpack API instance
        account_summary: Account summary to validate

    Returns:
        Dict with validation results
    """
    validations = {
        "equity_positive": True,
        "available_within_bounds": True,
        "margin_requirements_valid": True,
        "margin_fraction_valid": True,
    }

    # Equity should be non-negative
    validations["equity_positive"] = account_summary.total_equity >= Decimal("0")

    # Available equity should be <= total equity
    validations["available_within_bounds"] = (
        account_summary.available_equity <= account_summary.total_equity
    )

    # Initial margin >= maintenance margin
    if (
        account_summary.total_initial_margin_required is not None
        and account_summary.total_maintenance_margin_required is not None
    ):
        validations["margin_requirements_valid"] = (
            account_summary.total_initial_margin_required
            >= account_summary.total_maintenance_margin_required
        )

    # Margin fraction should be between 0 and 1
    if account_summary.bp_details and account_summary.bp_details.margin_fraction is not None:
        mf = account_summary.bp_details.margin_fraction
        validations["margin_fraction_valid"] = Decimal("0") <= mf <= Decimal("1")

    return validations


def get_balance_tolerance(balance_value: Decimal) -> Decimal:
    """Get appropriate tolerance for balance comparison based on value.

    Args:
        balance_value: The balance amount to compare

    Returns:
        Appropriate tolerance for comparison
    """
    if balance_value < DUST_THRESHOLD:
        return DUST_THRESHOLD
    elif balance_value < Decimal("1"):
        return BALANCE_PRECISION_TOLERANCE
    else:
        # For larger values, use percentage-based tolerance
        return balance_value * QUANTITY_TOLERANCE_PERCENT / Decimal("100")


def get_price_tolerance(price_value: Decimal) -> Decimal:
    """Get appropriate tolerance for price comparison.

    Args:
        price_value: The price to compare

    Returns:
        Appropriate tolerance for comparison
    """
    if price_value < Decimal("1"):
        return Decimal("0.0001")  # Fixed small tolerance for low prices
    else:
        return price_value * PRICE_TOLERANCE_PERCENT / Decimal("100")


def is_within_tolerance(
    actual: Decimal,
    expected: Decimal,
    tolerance: Decimal | None = None,
    tolerance_percent: Decimal | None = None,
) -> bool:
    """Check if actual value is within tolerance of expected value.

    Args:
        actual: Actual value
        expected: Expected value
        tolerance: Fixed tolerance amount (optional)
        tolerance_percent: Percentage tolerance (optional)

    Returns:
        True if within tolerance
    """
    if tolerance is not None:
        return abs(actual - expected) <= tolerance
    elif tolerance_percent is not None:
        if expected == Decimal("0"):
            return actual == Decimal("0")
        percent_diff = abs((actual - expected) / expected) * Decimal("100")
        return percent_diff <= tolerance_percent
    else:
        # Default to exact match
        return actual == expected
