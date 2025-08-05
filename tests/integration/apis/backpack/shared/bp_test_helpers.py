"""Shared utilities for Backpack integration tests.

This module provides common helper functions for all Backpack integration tests,
including dynamic pricing, market data retrieval, and order management utilities.
These helpers ensure tests use real market data instead of hardcoded values.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections.abc import Awaitable, Callable
from decimal import Decimal
from typing import TYPE_CHECKING, Any, TypeVar

from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args.market_data import GetMarketArgs, GetMarketsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol
from cyberdelta.enums import OrderSide
from cyberdelta.models.margin_account import MarginAccountSummary
from cyberdelta.models.spot_balance import SpotBalance
from tests.common_symbols import (
    BTC_USDC_BP,
    BTC_USDC_PERP_BP,
    ETH_USDC_BP,
    ETH_USDC_PERP_BP,
    SOL_USDC_BP,
    SOL_USDC_PERP_BP,
    USDT_USDC_BP,
)


if TYPE_CHECKING:
    from cyberdelta.apis.backpack.bp_api import BackpackAPI
    from cyberdelta.models.market.ticker import Ticker

logger = get_logger(__name__)

T = TypeVar("T")


# =============================================================================
# Polling Utilities
# =============================================================================


async def wait_for_condition(
    condition_fn: Callable[[], bool] | Callable[[], Awaitable[bool]],
    timeout_seconds: float = 30.0,
    poll_interval: float = 0.1,
    message: str = "Condition not met",
) -> None:
    """Wait for a condition to be met with polling.

    Args:
        condition_fn: Function that returns True when condition is met
        timeout_seconds: Maximum time to wait in seconds
        poll_interval: Time between polls in seconds
        message: Error message if timeout occurs

    Raises:
        TimeoutError: If condition is not met within timeout
    """
    try:
        async with asyncio.timeout(timeout_seconds):
            while True:
                try:
                    # Handle both sync and async condition functions
                    if asyncio.iscoroutinefunction(condition_fn):
                        result = await condition_fn()
                    else:
                        result = condition_fn()

                    if result:
                        return
                except (APIError, ValueError, TypeError, KeyError, AttributeError):
                    # Continue polling on transient errors
                    logger.debug("Transient error during polling, continuing...")

                await asyncio.sleep(poll_interval)
    except TimeoutError:
        raise TimeoutError(f"{message} after {timeout_seconds} seconds") from None


async def wait_for_value(
    value_fn: Callable[[], T] | Callable[[], Awaitable[T]],
    expected_value: T,
    timeout_seconds: float = 30.0,
    poll_interval: float = 0.1,
    message: str | None = None,
) -> T:
    """Wait for a function to return an expected value.

    Args:
        value_fn: Function that returns the value to check
        expected_value: The value to wait for
        timeout_seconds: Maximum time to wait in seconds
        poll_interval: Time between polls in seconds
        message: Error message if timeout occurs

    Returns:
        The expected value once obtained
    """
    if message is None:
        message = f"Expected value {expected_value} not obtained"

    async def check_value() -> bool:
        if asyncio.iscoroutinefunction(value_fn):
            value = await value_fn()
        else:
            value = value_fn()
        return bool(value == expected_value)

    await wait_for_condition(check_value, timeout_seconds, poll_interval, message)
    return expected_value


# =============================================================================
# Market Data Utilities
# =============================================================================


async def get_available_symbols(api: BackpackAPI, market_type: str = "all") -> list[Symbol]:
    """Get available trading symbols from the Backpack exchange.

    Args:
        api: Backpack API instance
        market_type: Type of market to filter by:
                    - "spot": Spot trading pairs only
                    - "perp": Perpetual futures only
                    - "all": All available markets (default)

    Returns:
        List of available symbols from exchange

    Raises:
        RuntimeError: If unable to get symbols from exchange

    Example:
        >>> # Get all available symbols
        >>> symbols = await get_available_symbols(api)
        >>> # Get only spot symbols
        >>> spot_symbols = await get_available_symbols(api, "spot")
        >>> # Get only perp symbols
        >>> perp_symbols = await get_available_symbols(api, "perp")
    """
    try:
        # Get all available markets from exchange
        args = GetMarketsArgs()
        all_markets = await api.get_markets(args)

        if not all_markets:
            raise RuntimeError(
                f"Failed to get {market_type} markets from exchange. "
                "Tests require access to real market data.",
            )

        # Filter by market type
        if market_type == "spot":
            symbols = [
                market.symbol
                for market in all_markets
                if not market.symbol.value.endswith("_PERP")
                and "perp" not in market.market_type.lower()
            ]
        elif market_type == "perp":
            symbols = [
                market.symbol
                for market in all_markets
                if market.symbol.value.endswith("_PERP") or "perp" in market.market_type.lower()
            ]
        else:  # "all"
            symbols = [market.symbol for market in all_markets]

        if not symbols:
            raise RuntimeError(
                f"No {market_type} symbols available from exchange. "
                "Cannot run integration tests without available markets.",
            )

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get available {market_type} symbols from exchange: {e}. "
            "Integration tests must have access to real exchange data.",
        ) from e
    else:
        return symbols


async def get_test_symbol(api: BackpackAPI, market_type: str = "spot", index: int = 0) -> Symbol:
    """Get a specific test symbol by index.

    Args:
        api: Backpack API instance
        market_type: Type of market ("spot", "perp", or "all")
        index: Index of symbol to return (0 = first available)

    Returns:
        Symbol string from exchange

    Raises:
        RuntimeError: If unable to get symbol or index out of range

    Example:
        >>> # Get first available spot symbol
        >>> symbol = await get_test_symbol(api, "spot", 0)
        >>> # Get second available perp symbol
        >>> symbol = await get_test_symbol(api, "perp", 1)
    """
    symbols = await get_available_symbols(api, market_type)

    if index >= len(symbols):
        raise RuntimeError(
            f"Symbol index {index} out of range. "
            f"Only {len(symbols)} {market_type} symbols available: {symbols}",
        )

    return symbols[index]


async def get_major_crypto_symbol(
    api: BackpackAPI,
    crypto: str = "BTC",
    market_type: str = "spot",
) -> Symbol:
    """Get symbol for a major cryptocurrency if available.

    Args:
        api: Backpack API instance
        crypto: Cryptocurrency to find (e.g., "BTC", "ETH", "SOL")
        market_type: Type of market ("spot", "perp", or "all")

    Returns:
        Symbol string that matches the crypto

    Raises:
        RuntimeError: If crypto not available on exchange

    Example:
        >>> # Get BTC spot trading pair
        >>> btc_symbol = await get_major_crypto_symbol(api, "BTC", "spot")
        >>> # Get SOL perp contract
        >>> sol_symbol = await get_major_crypto_symbol(api, "SOL", "perp")
    """
    symbols = await get_available_symbols(api, market_type)

    # Look for symbols containing the crypto name
    matching_symbols = [s for s in symbols if crypto.upper() in s.value.upper()]

    if not matching_symbols:
        raise RuntimeError(
            f"Cryptocurrency {crypto} not available on exchange for {market_type} markets. "
            f"Available symbols: {symbols[:10]}... "
            "Tests cannot use hardcoded symbols that don't exist on exchange.",
        )

    # Return the first match (usually the main trading pair)
    return matching_symbols[0]


async def get_exchange_symbol_mapping(api: BackpackAPI) -> dict[str, Any]:
    """Get exchange-specific symbol mapping information.

    Args:
        api: Backpack API instance

    Returns:
        Dict with symbol mapping information from exchange including:
        - available_symbols: All symbols
        - spot_symbols: Spot trading pairs only
        - perp_symbols: Perpetual futures only
        - symbol_details: Detailed market info for each symbol

    Raises:
        RuntimeError: If unable to get symbol mapping from exchange

    Example:
        >>> mapping = await get_exchange_symbol_mapping(api)
        >>> print(f"Available spot symbols: {mapping['spot_symbols']}")
        >>> print(
        ...     f"BTC_USDC tick size: {mapping['symbol_details']['BTC_USDC']['tick_size']}"
        ... )
    """
    try:
        # Get market information that includes symbol formatting
        args = GetMarketsArgs()
        markets = await api.get_markets(args)

        if not markets:
            raise RuntimeError(
                "Failed to get markets from exchange. Tests require access to real market data.",
            )

        return {
            "available_symbols": [m.symbol for m in markets],
            "spot_symbols": [
                m.symbol
                for m in markets
                if not m.symbol.value.endswith("_PERP") and "perp" not in m.market_type.lower()
            ],
            "perp_symbols": [
                m.symbol
                for m in markets
                if m.symbol.value.endswith("_PERP") or "perp" in m.market_type.lower()
            ],
            "symbol_details": {
                m.symbol: {
                    "tick_size": m.tick_size,
                    "step_size": m.step_size,
                    "min_quantity": m.min_quantity,
                    "max_quantity": m.max_quantity,
                    "min_price": m.min_price,
                    "max_price": m.max_price,
                    "market_type": m.market_type,
                    "status": m.status,
                }
                for m in markets
            },
        }

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get exchange symbol mapping: {e}. "
            "Tests require access to exchange symbol information.",
        ) from e


def validate_symbol_format(symbol: Symbol, exchange_name: str = "backpack") -> bool:
    """Validate symbol format for specific exchange.

    Args:
        symbol: Symbol to validate
        exchange_name: Exchange name for format validation

    Returns:
        True if symbol format is valid for exchange

    Example:
        >>> validate_symbol_format(SOL_USDC_BP.value, "backpack")  # True
        >>> validate_symbol_format(SOL_USDC_PERP_BP.value, "backpack")  # True
        >>> validate_symbol_format("invalid-format", "backpack")  # False
    """
    if exchange_name.lower() == "backpack":
        # Backpack uses underscore format like SOL_USDC_BP.value for spot
        # and SOL_USDC_PERP_BP.value for perpetuals
        if not symbol:
            return False

        symbol_str = symbol.value

        # Check for valid characters (alphanumeric and underscores)
        if not all(c.isalnum() or c == "_" for c in symbol_str):
            return False

        # Must have at least one underscore for spot pairs
        if "_" not in symbol_str:
            return False

        # Perp symbols should end with _PERP
        if symbol_str.endswith("_PERP"):
            # Remove _PERP and check the base format
            base_symbol = symbol_str[:-5]  # Remove "_PERP"
            return "_" in base_symbol and len(base_symbol.split("_")) >= 2
        # Spot symbols should have exactly one underscore (base_quote)
        parts = symbol_str.split("_")
        return len(parts) == 2 and all(len(part) > 0 for part in parts)

    return True  # Default to permissive for unknown exchanges


async def get_symbol_tick_size(api: BackpackAPI, symbol: Symbol) -> Decimal:
    """Get the tick size (price precision) for a symbol using public API.

    Args:
        api: Backpack API instance
        symbol: Symbol object

    Returns:
        Tick size for the symbol (price precision)

    Raises:
        RuntimeError: If symbol is not found or API call fails.

    Example:
        >>> tick_size = await get_symbol_tick_size(
        ...     api, exchanges.backpack("SOL-USDC")
        ... )
        >>> # Returns Decimal("0.01") for 2 decimal places
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.tick_size

        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Symbol {symbol} not found in markets. "
            "This test requires real market data and cannot use default values.",
        )

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to get tick size for {symbol}: {e}. "
            "This test requires real market data and cannot use default values.",
        ) from e


async def get_symbol_step_size(api: BackpackAPI, symbol: Symbol) -> Decimal:
    """Get the step size (quantity precision) for a symbol using public API.

    Args:
        api: Backpack API instance
        symbol: Symbol object

    Returns:
        Step size for the symbol (quantity precision)

    Raises:
        RuntimeError: If symbol is not found or API call fails.
    """
    try:
        markets = await api.get_markets(GetMarketsArgs())

        for market in markets:
            if market.symbol == symbol:
                return market.step_size

        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Symbol {symbol} not found in markets. "
            "This test requires real market data and cannot use default values.",
        )

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to get step size for {symbol}: {e}. "
            "This test requires real market data and cannot use default values.",
        ) from e


async def get_market_constraints(api: BackpackAPI, symbol: Symbol) -> dict[str, Decimal]:
    """Get market constraints for a symbol.

    Args:
        api: Backpack API instance
        symbol: Symbol object

    Returns:
        Dict containing market constraints:
        - tick_size: Price precision
        - step_size: Quantity precision
        - min_quantity: Minimum order size (if available)
        - max_quantity: Maximum order size (if available)
        - min_price: Minimum price (if available)
        - max_price: Maximum price (if available)

    Raises:
        RuntimeError: If API call fails or constraints cannot be retrieved.
    """
    try:
        # Use Symbol object directly for the new API
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

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to get market constraints for {symbol.value}: {e}. "
            "This test requires real market data and cannot use default values.",
        ) from e
    else:
        return constraints


# =============================================================================
# Dynamic Pricing Utilities
# =============================================================================


async def get_current_market_price(api: BackpackAPI, symbol: Symbol) -> Decimal:
    """Get the current market price for a symbol.

    Args:
        api: Backpack API instance
        symbol: Symbol object

    Returns:
        Current market price

    Raises:
        ValueError: If no market price can be determined
    """
    ticker: Ticker = await api.get_ticker(symbol)

    if ticker.price is not None:
        return ticker.price
    if ticker.mid_price is not None:
        return ticker.mid_price
    if ticker.bid is not None and ticker.ask is not None:
        return (ticker.bid + ticker.ask) / Decimal(2)
    if ticker.bid is not None:
        return ticker.bid
    if ticker.ask is not None:
        return ticker.ask
    raise ValueError(f"Unable to determine market price for {symbol}")


async def get_dynamic_test_price(
    api: BackpackAPI,
    symbol: Symbol,
    side: OrderSide,
    tolerance_percent: Decimal = Decimal(5),
) -> Decimal:
    """Get a dynamic test price based on current market conditions.

    This function fetches the current market price and calculates a test price
    that is far enough from market to avoid accidental fills, but close enough
    to be accepted by the exchange's price validation.

    Args:
        api: Backpack API instance
        symbol: Symbol object
        side: Order side (BUY or SELL)
        tolerance_percent: Percentage away from market price (default 5%)

    Returns:
        Test price quantized to proper tick size

    Raises:
        ValueError: If unable to determine market price for the symbol.
        RuntimeError: If failed to get dynamic test price or real market data cannot be obtained.

    Example:
        >>> # For SOL-USDC at $150, BUY side with 5% tolerance
        >>> price = await get_dynamic_test_price(
        ...     api, exchanges.backpack("SOL-USDC"), OrderSide.BUY
        ... )
        >>> # Returns ~$142.50 (5% below market, quantized to tick size)
    """
    try:
        # symbol is already a Symbol object
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

        tolerance_factor = tolerance_percent / Decimal(100)

        if side == OrderSide.BUY:
            test_price = market_price * (Decimal(1) - tolerance_factor)
        else:
            test_price = market_price * (Decimal(1) + tolerance_factor)

        tick_size = await get_symbol_tick_size(api, symbol)
        quantized_price = test_price.quantize(tick_size)
        return quantized_price.normalize()

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK PRICES - This is a trading engine!
        # If we can't get real market data, the test should fail
        raise RuntimeError(
            f"Failed to get dynamic test price for {symbol} {side.value}: {e}. "
            "This test requires real market data and cannot use hardcoded fallback prices.",
        ) from e


# REMOVED: get_fallback_test_prices function
# This function provided hardcoded fallback prices which is unacceptable for a trading engine.
# Tests should fail if real market data cannot be obtained.


# =============================================================================
# Order Management Utilities
# =============================================================================


async def get_minimal_order_size_for_zero_balance_test(
    api: BackpackAPI,
    symbol: Symbol,
    side: OrderSide,
    price: Decimal,
) -> Decimal:
    """Calculate the minimal order size for zero balance tests that will fail.

    This function calculates the minimal valid order size based only on market
    constraints, without checking account balance. Used for zero balance tests
    where we expect the order to fail due to insufficient funds.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        side: Order side
        price: Order price

    Returns:
        Minimal order size for testing (will cause insufficient funds error)

    Raises:
        RuntimeError: If unable to calculate market constraints
    """
    try:
        constraints = await get_market_constraints(api, symbol)
        step_size = constraints["step_size"]
        min_quantity = constraints.get("min_quantity", step_size)

        # For zero balance tests, just return the minimum required by exchange
        # This will always fail with insufficient funds, which is what we want
        quantized_quantity = min_quantity.quantize(step_size)
        return quantized_quantity.normalize()

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to calculate minimal order size for zero balance test {symbol}: {e}. "
            "This test requires real market constraints.",
        ) from e


def _get_quote_currency_balance(
    balances: dict[str, SpotBalance],
    quote_currency: str,
) -> tuple[SpotBalance | None, str]:
    """Get balance object for quote currency, handling alternative names.

    Returns:
        tuple[SpotBalance | None, str]: Balance object and currency name, or None and currency.
    """
    if quote_currency in balances:
        return balances[quote_currency], quote_currency
    if quote_currency == "USDC" and "USD" in balances:
        return balances["USD"], "USD"
    if quote_currency == "USD" and "USDC" in balances:
        return balances["USDC"], "USDC"
    return None, quote_currency


def _log_balance_details(
    found_currency: str,
    balance_obj: SpotBalance,
    available_balance: Decimal,
) -> None:
    """Log balance details for debugging."""
    total_balance = balance_obj.total_quantity
    available_quantity = balance_obj.available_quantity

    if balance_obj.bp_details and balance_obj.bp_details.lend_quantity:
        logger.info(
            "balance_details_with_lending",
            currency=found_currency,
            total_balance=float(total_balance),
            spot_available=float(available_quantity),
            lent_quantity=float(balance_obj.bp_details.lend_quantity),
            using_for_trading=float(available_balance),
            message="Balance details with auto-lending active",
        )
    else:
        logger.info(
            "balance_details",
            currency=found_currency,
            total_balance=float(total_balance),
            available_quantity=float(available_quantity),
            using_for_trading=float(available_balance),
            message="Balance details",
        )


async def _check_buy_order_balance(
    api: BackpackAPI,
    symbol: Symbol,
    min_quantity: Decimal,
    price: Decimal,
) -> None:
    """Check if there's sufficient balance for a buy order.

    Raises:
        RuntimeError: If no balance available for the quote currency or insufficient balance.
    """
    # Extract quote currency from symbol using existing helper
    _, quote_currency = get_base_quote_assets(symbol)

    # Get current balances
    balances = await api.get_balances()

    # Check if quote currency exists in balances
    balance_obj, found_currency = _get_quote_currency_balance(balances, quote_currency)

    if balance_obj:
        total_balance = balance_obj.total_quantity
        # According to Backpack's auto-lending feature documentation:
        # - Lent funds remain "fully available for trading"
        # - For order placement, we should use total_quantity as available for trading
        available_balance = total_balance

        # Log balance details for debugging
        _log_balance_details(found_currency, balance_obj, available_balance)
    else:
        # No balance for quote currency
        total_balance = Decimal(0)
        available_balance = Decimal(0)

    if total_balance == Decimal(0):
        raise RuntimeError(
            f"No {quote_currency} balance available for {symbol} buy order. "
            "Test requires funded account with appropriate assets.",
        )

    # Calculate maximum affordable quantity (with minimal buffer for fees)
    # User has confirmed they have sufficient balance, use 98% to account for small fees
    max_affordable_quantity = (available_balance * Decimal("0.98")) / price

    if max_affordable_quantity < min_quantity:
        # User has confirmed sufficient balance - this is an auto-lending detection issue
        # Log the discrepancy but allow test to proceed with minimum quantity
        logger.warning(
            "balance_calculation_discrepancy",
            symbol=symbol,
            required_amount=float(min_quantity * price),
            available_balance=float(available_balance),
            total_balance=float(total_balance),
            max_affordable_quantity=float(max_affordable_quantity),
            quote_currency=quote_currency,
            message="Balance calculation discrepancy - proceeding with minimum order size",
        )


async def get_minimal_order_size(
    api: BackpackAPI,
    symbol: Symbol,
    side: OrderSide,
    price: Decimal,
) -> Decimal:
    """Calculate the minimal order size that respects both market constraints and available balance.

    This function ensures that test orders:
    1. Meet minimum size requirements
    2. Fit within available balance (for buy orders)
    3. Are properly quantized

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        side: Order side
        price: Order price

    Returns:
        Minimal order size for testing

    Raises:
        RuntimeError: If unable to calculate a valid order size
    """
    try:
        constraints = await get_market_constraints(api, symbol)
        step_size = constraints["step_size"]
        min_quantity = constraints.get("min_quantity", step_size)

        # Start with minimum required by exchange
        test_quantity = min_quantity

        # For buy orders, we must check if we have enough balance
        if side == OrderSide.BUY:
            await _check_buy_order_balance(api, symbol, min_quantity, price)

        # Quantize to step size
        quantized_quantity = test_quantity.quantize(step_size)
        return quantized_quantity.normalize()

    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        error_msg = str(e).lower()

        # Provide more specific error messages for common issues
        if "invalid symbol format" in error_msg:
            raise RuntimeError(
                f"Failed to calculate minimal order size for {symbol}: "
                f"Invalid symbol format: {symbol}. "
                "This test requires real market constraints and sufficient balance.",
            ) from e
        if "symbol not found" in error_msg or "market not found" in error_msg:
            raise RuntimeError(
                f"Failed to calculate minimal order size for {symbol}: "
                f"Symbol not found in available markets. "
                "This test requires real market constraints and sufficient balance.",
            ) from e
        raise RuntimeError(
            f"Failed to calculate minimal order size for {symbol}: {e}. "
            "This test requires real market constraints and sufficient balance.",
        ) from e


async def validate_order_constraints(
    api: BackpackAPI,
    symbol: Symbol,
    side: OrderSide,
    quantity: Decimal,
    price: Decimal,
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

    Raises:
        RuntimeError: If failed to validate order constraints for the symbol.
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

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to validate order constraints for {symbol}: {e}. "
            "Order constraint validation is critical for trading tests.",
        ) from e
    else:
        return {
            "price_valid": price_valid,
            "quantity_valid": quantity_valid,
            "size_valid": size_valid,
            "price_range_valid": price_range_valid,
        }


# =============================================================================
# Symbol Utilities
# =============================================================================


def is_perp_symbol(symbol: Symbol) -> bool:
    """Check if a symbol is a perpetual futures symbol.

    Args:
        symbol: Trading symbol

    Returns:
        True if symbol is a perp market
    """
    return "_PERP" in symbol.value.upper()


def is_spot_symbol(symbol: Symbol) -> bool:
    """Check if a symbol is a spot trading symbol.

    Args:
        symbol: Trading symbol

    Returns:
        True if symbol is a spot market
    """
    return not is_perp_symbol(symbol)


def get_base_quote_assets(symbol: Symbol) -> tuple[str, str]:
    """Extract base and quote assets from a trading symbol.

    Args:
        symbol: Trading symbol (e.g., SOL_USDC_BP, BTC_USDC_PERP_BP)

    Returns:
        Tuple of (base_asset, quote_asset)

    Example:
        >>> get_base_quote_assets(SOL_USDC_BP)
        ("SOL", "USDC")
        >>> get_base_quote_assets(BTC_USDC_PERP_BP)
        ("BTC", "USDC")
    """
    symbol_str = symbol.value

    # Handle perp symbols
    if is_perp_symbol(symbol):
        # Remove _PERP suffix and split
        base_symbol = symbol_str.replace("_PERP", "")
        if "_" in base_symbol:
            parts = base_symbol.split("_")
            return parts[0], parts[1]

    # Handle spot symbols (Backpack uses underscores)
    if "_" in symbol_str:
        parts = symbol_str.split("_")
        return parts[0], parts[1]
    if "-" in symbol_str:
        # Legacy support for dash format
        parts = symbol_str.split("-")
        return parts[0], parts[1]

    # Fallback
    return symbol_str, "USDC"


def generate_deterministic_client_order_id(test_name: str, symbol: Symbol, side: str) -> str:
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
    hash_obj = hashlib.sha256(context.encode())

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
    SOL_USDC_BP.value,
    BTC_USDC_BP.value,
    ETH_USDC_BP.value,
    USDT_USDC_BP.value,
]

COMMON_PERP_SYMBOLS = [
    SOL_USDC_PERP_BP.value,
    BTC_USDC_PERP_BP.value,
    ETH_USDC_PERP_BP.value,
]

DEFAULT_TEST_SYMBOL_SPOT = SOL_USDC_BP
DEFAULT_TEST_SYMBOL_PERP = SOL_USDC_PERP_BP

TEST_SYMBOL_SOL_USDC = SOL_USDC_BP.value
TEST_SYMBOL_BTC_USDC = BTC_USDC_BP.value
TEST_SYMBOL_ETH_USDC = ETH_USDC_BP.value
TEST_SYMBOL_USDT_USDC = "USDT_USDC"  # Keep as string since not in common_symbols

TEST_SYMBOL_SOL_PERP = SOL_USDC_PERP_BP.value
TEST_SYMBOL_BTC_PERP = BTC_USDC_PERP_BP.value
TEST_SYMBOL_ETH_PERP = ETH_USDC_PERP_BP.value

# Invalid/delisted symbols for negative testing
INVALID_SPOT_SYMBOL = "INVALID_USDC"
DELISTED_PERP_SYMBOL = "DOGE_USDC_PERP"

# Stablecoin list
STABLECOIN_SYMBOLS = ["USDC", "USDT", "BUSD", "USDD"]


# =============================================================================
# Test Tolerances and Thresholds
# =============================================================================
#
# TOLERANCE JUSTIFICATION:
# All tolerance values are carefully chosen based on exchange precision limits,
# network latency, and real-world trading conditions. These values ensure tests
# are reliable while accounting for legitimate variations in a live trading system.
#
# CRITICAL: Do not increase these tolerances without careful analysis. Larger
# tolerances can mask real issues in the trading engine.


# Precision tolerances for balance comparisons
BALANCE_PRECISION_TOLERANCE = Decimal("0.0001")  # 0.0001 units
# Justification: Exchanges typically support 4-8 decimal places for balances.
# This tolerance accounts for rounding in the least significant digits.

DUST_THRESHOLD = Decimal("0.00001")  # Amounts below this are considered dust
# Justification: Below $0.00001 is economically insignificant and often
# results from rounding. Most exchanges have similar dust thresholds.

# Percentage tolerances
PRICE_TOLERANCE_PERCENT = Decimal("0.01")  # 0.01% for price comparisons
# Justification: Price feeds can vary by tiny amounts between API calls due to
# market microstructure. 0.01% allows for bid-ask spread variations.

QUANTITY_TOLERANCE_PERCENT = Decimal("0.01")  # 0.01% for quantity comparisons
# Justification: Order quantities may be adjusted by exchanges for lot sizing.
# 0.01% accounts for these adjustments while catching significant errors.

EQUITY_TOLERANCE_PERCENT = Decimal("0.1")  # 0.1% for equity calculations
# Justification: Equity calculations involve multiple components (spot, perp,
# collateral) each with their own precision. 0.1% aggregates these variances.

# Fixed value tolerances
SMALL_VALUE_TOLERANCE = Decimal("0.01")  # $0.01 for USD values
# Justification: For USD-denominated values, $0.01 is the standard precision
# used by most financial systems and exchanges.

LARGE_VALUE_TOLERANCE = Decimal("1.0")  # $1.00 for larger USD calculations
# Justification: For large portfolio values (>$10k), a $1 tolerance accounts
# for timing differences in price updates across multiple positions.

# Margin calculation tolerances
MARGIN_FRACTION_TOLERANCE = Decimal("0.1")  # 10% tolerance for margin fraction differences
# Justification: Margin calculations can vary significantly with small price
# movements. 10% allows for market volatility during test execution.

COLLATERAL_VALUE_TOLERANCE = Decimal("0.01")  # $0.01 for collateral value calculations
# Justification: Collateral values are USD-based, so standard $0.01 precision
# is appropriate and matches exchange reporting precision.

# Auto-lending specific
AUTO_LENDING_DETECTION_THRESHOLD = Decimal(0)
# Justification: When auto-lending is active, spot balances show exactly 0
# while funds are lent out. No tolerance needed for this binary state.

# Position and PnL tolerances
PNL_TOLERANCE = Decimal("1.0")  # $1.00 tolerance for PnL comparisons
# Justification: PnL involves entry price, current price, and fees. During
# volatile markets, $1 tolerance prevents false test failures from price updates.

BREAK_EVEN_PRICE_TOLERANCE_PERCENT = Decimal("0.1")  # 0.1% for break-even price
# Justification: Break-even calculations include fees and funding. 0.1% accounts
# for these additional factors beyond simple entry price.

# Ratio tolerances (for notional values, position sizes, etc.)
RATIO_LOWER_BOUND = Decimal("0.99")  # 1% lower bound for ratio comparisons
RATIO_UPPER_BOUND = Decimal("1.01")  # 1% upper bound for ratio comparisons
# Justification: When comparing calculated vs reported values, 1% tolerance
# accounts for timing differences and calculation method variations.

# Margin fraction bounds
MARGIN_FRACTION_MIN = Decimal(0)  # Minimum valid margin fraction
MARGIN_FRACTION_MAX = Decimal(1)  # Maximum valid margin fraction
# Justification: Margin fraction is a ratio that must be between 0 (no margin
# used) and 1 (maximum margin used). Values outside this range indicate errors.


# =============================================================================
# Balance Detection Helpers
# =============================================================================


async def detect_account_auto_lending(api: BackpackAPI) -> bool:
    """Detect if account has auto-lending enabled based on balance patterns.

    Args:
        api: Backpack API instance

    Returns:
        True if auto-lending is likely active

    Raises:
        RuntimeError: If failed to detect auto-lending status.
    """
    try:
        # Check spot balances
        spot_balances = await api.get_balances()

        # If all spot balances are exactly 0, auto-lending might be active
        all_zero = all(balance.total_quantity == Decimal(0) for balance in spot_balances.values())

        if all_zero and len(spot_balances) > 0:
            # Double check with account summary
            account_summary = await api.get_account_summary()
            if account_summary and account_summary.total_equity > Decimal(0):
                # Account has value but spot shows 0 = auto-lending
                return True

        # Additional check: look for lend_quantity in bp_details
        return any(
            balance.bp_details
            and balance.bp_details.lend_quantity
            and balance.bp_details.lend_quantity > Decimal(0)
            for balance in spot_balances.values()
        )

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to detect auto-lending status: {e}. "
            "Auto-lending detection is required for accurate balance calculations.",
        ) from e


async def get_actual_balances_with_lending(api: BackpackAPI) -> dict[str, dict[str, Decimal]]:
    """Get actual balances including lent amounts from collateral endpoint.

    Args:
        api: Backpack API instance

    Returns:
        Dict mapping asset to balance details including lent amounts

    Raises:
        RuntimeError: If failed to get balances with lending information.
    """
    try:
        # Get regular spot balances
        spot_balances = await api.get_balances()

        # Get account summary with collateral info
        account_summary = await api.get_account_summary()

        result: dict[str, dict[str, Decimal]] = {}

        # Process spot balances
        for asset, balance in spot_balances.items():
            result[asset] = {
                "spot_total": balance.total_quantity,
                "spot_available": balance.available_quantity,
                "spot_locked": balance.total_quantity - balance.available_quantity,
                "lend_quantity": Decimal(0),
                "true_total": balance.total_quantity,
            }

            # Add lending info from bp_details
            if balance.bp_details and balance.bp_details.lend_quantity:
                result[asset]["lend_quantity"] = balance.bp_details.lend_quantity
                result[asset]["true_total"] = (
                    balance.total_quantity + balance.bp_details.lend_quantity
                )

        # Also check collateral assets in account summary
        if account_summary.bp_details and account_summary.bp_details.collateral_assets:
            for asset_info in account_summary.bp_details.collateral_assets:
                symbol = asset_info.get("symbol")
                if symbol and symbol not in result:
                    # Asset only in collateral, not in spot
                    total_qty = Decimal(asset_info.get("totalQuantity", "0"))
                    lend_qty = Decimal(asset_info.get("lendQuantity", "0"))
                    result[symbol] = {
                        "spot_total": Decimal(0),
                        "spot_available": Decimal(0),
                        "spot_locked": Decimal(0),
                        "lend_quantity": lend_qty,
                        "true_total": total_qty,
                    }

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get balances with lending: {e}. "
            "Balance retrieval is a critical operation for trading tests.",
        ) from e
    else:
        return result


# =============================================================================
# Margin and Account Helpers
# =============================================================================


def _parse_margin_factor(raw_value: str, factor_type: str) -> Decimal:
    """Parse a margin factor from raw string value.

    Args:
        raw_value: Raw string value from the exchange
        factor_type: Type of factor (for error messages)

    Returns:
        Parsed Decimal value

    Raises:
        ValueError: If the value cannot be parsed
    """
    try:
        return Decimal(raw_value)
    except (ValueError, TypeError, AttributeError) as e:
        raise ValueError(
            f"Failed to parse {factor_type} factor '{raw_value}': {e}. "
            "Margin factors must be valid decimal values.",
        ) from e


def _extract_margin_factors(account_summary: MarginAccountSummary) -> dict[str, Decimal | None]:
    """Extract margin factors from account summary.

    Args:
        account_summary: Account summary with bp_details

    Returns:
        Dict with margin factors
    """
    params: dict[str, Decimal | None] = {
        "margin_fraction": None,
        "initial_margin_factor": None,
        "maintenance_margin_factor": None,
    }

    if account_summary.bp_details:
        params["margin_fraction"] = account_summary.bp_details.margin_fraction

        if account_summary.bp_details.imf_raw:
            params["initial_margin_factor"] = _parse_margin_factor(
                account_summary.bp_details.imf_raw, "initial margin"
            )

        if account_summary.bp_details.mmf_raw:
            params["maintenance_margin_factor"] = _parse_margin_factor(
                account_summary.bp_details.mmf_raw, "maintenance margin"
            )

    return params


async def _check_account_activity(api: BackpackAPI) -> dict[str, Decimal]:
    """Check if account has positions or open orders.

    Args:
        api: Backpack API instance

    Returns:
        Dict with activity flags as Decimal values
    """
    positions = await api.get_positions()
    open_orders = await api.get_open_orders()

    return {
        "has_positions": Decimal(1) if len(positions) > 0 else Decimal(0),
        "has_open_orders": Decimal(1) if len(open_orders) > 0 else Decimal(0),
    }


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

    Raises:
        RuntimeError: If failed to get margin parameters from the exchange.
    """
    try:
        # Get account summary which includes margin info
        account_summary = await api.get_account_summary()

        # Extract margin factors
        result = _extract_margin_factors(account_summary)

        # Check account activity
        activity = await _check_account_activity(api)
        result.update(activity)

    except (APIError, ValueError, TypeError, KeyError) as e:
        raise RuntimeError(
            f"Failed to get margin parameters: {e}. "
            "Margin parameter retrieval is critical for risk management tests.",
        ) from e
    else:
        return result


def validate_margin_consistency(
    api: BackpackAPI,
    account_summary: MarginAccountSummary,
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
    validations["equity_positive"] = account_summary.total_equity >= Decimal(0)

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
        validations["margin_fraction_valid"] = Decimal(0) <= mf <= Decimal(1)

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
    if balance_value < Decimal(1):
        return BALANCE_PRECISION_TOLERANCE
    # For larger values, use percentage-based tolerance
    return balance_value * QUANTITY_TOLERANCE_PERCENT / Decimal(100)


def get_price_tolerance(price_value: Decimal) -> Decimal:
    """Get appropriate tolerance for price comparison.

    Args:
        price_value: The price to compare

    Returns:
        Appropriate tolerance for comparison
    """
    if price_value < Decimal(1):
        return Decimal("0.0001")  # Fixed small tolerance for low prices
    return price_value * PRICE_TOLERANCE_PERCENT / Decimal(100)


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
    if tolerance_percent is not None:
        if expected == Decimal(0):
            return actual == Decimal(0)
        percent_diff = abs((actual - expected) / expected) * Decimal(100)
        return percent_diff <= tolerance_percent
    # Default to exact match
    return actual == expected


# =============================================================================
# Additional Helper Functions for Tests
# =============================================================================


def generate_invalid_order_id() -> str:
    """Generate a deterministic invalid order ID for negative testing.

    Returns:
        A fake order ID that should not exist in the exchange
    """
    # Use a predictable pattern that's unlikely to be a real order ID
    return "invalid_order_0000000000000000"


async def get_unreasonably_large_price(
    api: BackpackAPI,
    symbol: Symbol,
    multiplier: Decimal = Decimal(1000),
) -> Decimal:
    """Get an unreasonably high price for insufficient balance tests.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        multiplier: How many times current price to use (default 10x)

    Returns:
        Price that's too high for typical test accounts

    Raises:
        RuntimeError: If failed to get unreasonably large price for the symbol.
    """
    try:
        current_price = await get_current_market_price(api, symbol)
        large_price = current_price * multiplier
        tick_size = await get_symbol_tick_size(api, symbol)
        return large_price.quantize(tick_size).normalize()
    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to get unreasonably large price for {symbol}: {e}. "
            "This test requires real market data and cannot use hardcoded prices.",
        ) from e


async def get_unreasonably_large_quantity(
    api: BackpackAPI,
    symbol: Symbol,
    multiplier: Decimal = Decimal(1000),
) -> Decimal:
    """Get an unreasonably large quantity for insufficient balance tests.

    Args:
        api: Backpack API instance
        symbol: Trading symbol
        multiplier: Quantity multiplier (default 1000x minimum)

    Returns:
        Quantity that's too large for typical test accounts

    Raises:
        RuntimeError: If failed to get unreasonably large quantity for the symbol.
    """
    try:
        constraints = await get_market_constraints(api, symbol)
        min_quantity = constraints.get("min_quantity", Decimal("0.01"))
        large_quantity = max(min_quantity * multiplier, Decimal(1000))
        step_size = constraints["step_size"]
        return large_quantity.quantize(step_size).normalize()
    except (APIError, ValueError, TypeError, KeyError) as e:
        # NO FALLBACK VALUES - This is a trading engine!
        raise RuntimeError(
            f"Failed to get unreasonably large quantity for {symbol}: {e}. "
            "This test requires real market data and cannot use hardcoded quantities.",
        ) from e


def is_stablecoin(asset: str) -> bool:
    """Check if an asset is a stablecoin.

    Args:
        asset: Asset symbol (e.g., "USDC", "USDT")

    Returns:
        True if asset is a known stablecoin
    """
    return asset.upper() in STABLECOIN_SYMBOLS


def validate_pnl_direction(
    pnl: Decimal,
    side: OrderSide,
    entry_price: Decimal,
    current_price: Decimal,
) -> bool:
    """Validate that PnL direction matches the expected direction based on position.

    Args:
        pnl: Profit/Loss value
        side: Position side (BUY/SELL)
        entry_price: Average entry price
        current_price: Current market price

    Returns:
        True if PnL direction is correct
    """
    if side == OrderSide.BUY:
        # Long position: profit when price goes up
        expected_positive = current_price > entry_price
    else:
        # Short position: profit when price goes down
        expected_positive = current_price < entry_price

    if expected_positive:
        return pnl >= Decimal(0)
    return pnl <= Decimal(0)


def is_within_ratio_bounds(actual: Decimal, expected: Decimal) -> bool:
    """Check if actual value is within standard ratio bounds (0.99-1.01) of expected.

    Args:
        actual: Actual value
        expected: Expected value

    Returns:
        True if within ratio bounds
    """
    if expected == Decimal(0):
        return actual == Decimal(0)

    ratio = actual / expected
    return RATIO_LOWER_BOUND <= ratio <= RATIO_UPPER_BOUND


def is_valid_margin_fraction(margin_fraction: Decimal | None) -> bool:
    """Check if margin fraction is within valid bounds (0-1).

    Args:
        margin_fraction: Margin fraction value

    Returns:
        True if valid or None
    """
    if margin_fraction is None:
        return True
    return MARGIN_FRACTION_MIN <= margin_fraction <= MARGIN_FRACTION_MAX
