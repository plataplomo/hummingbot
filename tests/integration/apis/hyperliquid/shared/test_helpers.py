"""Dynamic test helpers for Hyperliquid integration tests.

This module provides utilities for creating robust, adaptive tests that work
across different account states and market conditions. Inspired by Backpack's
dynamic helper patterns but adapted for Hyperliquid's specific API structure.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetMarketArgs
from cyberdelta.core.models.enums import OrderSide

# Common test symbols for Hyperliquid
COMMON_PERP_SYMBOLS = ["BTC", "ETH", "SOL", "AVAX"]
COMMON_SPOT_SYMBOLS = ["BTC", "ETH", "USDC", "SOL"]

# Default test tolerances
DEFAULT_PRICE_TOLERANCE_PERCENT = Decimal("3.0")  # 3% from market price
DEFAULT_SIZE_TOLERANCE_PERCENT = Decimal("10.0")  # 10% margin for sizing
MIN_ORDER_VALUE_USD = Decimal("1.0")  # Minimum viable order value


class HyperliquidTestHelpers:
    """Collection of dynamic test helpers for Hyperliquid integration tests."""

    # Market Data Utilities

    @staticmethod
    async def get_market_constraints(api: HyperliquidAPI, symbol: str) -> dict[str, Decimal]:
        """Get dynamic market constraints for a symbol.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol (e.g., "BTC", "ETH")

        Returns:
            Dict with tick_size, step_size, min_quantity, max_quantity
        """
        try:
            market = await api.get_market(GetMarketArgs(symbol=symbol))
            if not market:
                # Fallback values for common symbols
                return HyperliquidTestHelpers._get_fallback_constraints(symbol)

            return {
                "tick_size": market.tick_size,
                "step_size": market.step_size,
                "min_quantity": market.min_quantity or Decimal("0.001"),
                "max_quantity": market.max_quantity or Decimal("1000000"),
            }
        except Exception:
            return HyperliquidTestHelpers._get_fallback_constraints(symbol)

    @staticmethod
    def _get_fallback_constraints(symbol: str) -> dict[str, Decimal]:
        """Get conservative fallback constraints when market data unavailable."""
        if symbol in ["BTC"]:
            return {
                "tick_size": Decimal("0.1"),
                "step_size": Decimal("0.0001"),
                "min_quantity": Decimal("0.0001"),
                "max_quantity": Decimal("100"),
            }
        elif symbol in ["ETH"]:
            return {
                "tick_size": Decimal("0.01"),
                "step_size": Decimal("0.001"),
                "min_quantity": Decimal("0.001"),
                "max_quantity": Decimal("1000"),
            }
        else:
            return {
                "tick_size": Decimal("0.0001"),
                "step_size": Decimal("0.001"),
                "min_quantity": Decimal("0.001"),
                "max_quantity": Decimal("10000"),
            }

    @staticmethod
    async def get_current_market_price(api: HyperliquidAPI, symbol: str) -> Decimal:
        """Get current market price with fallback hierarchy.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol

        Returns:
            Current market price as Decimal
        """
        try:
            # Try ticker first (most current)
            ticker = await api.get_ticker(symbol)
            if ticker and ticker.price:
                return ticker.price

            # Fallback to market details with mark price
            market = await api.get_market(GetMarketArgs(symbol=symbol))
            if market and market.hl_details and market.hl_details.mark_price:
                return market.hl_details.mark_price

            # Last resort: hardcoded fallback prices
            return HyperliquidTestHelpers._get_fallback_price(symbol)

        except Exception:
            return HyperliquidTestHelpers._get_fallback_price(symbol)

    @staticmethod
    def _get_fallback_price(symbol: str) -> Decimal:
        """Get conservative fallback price when market data unavailable."""
        fallback_prices = {
            "BTC": Decimal("50000"),
            "ETH": Decimal("3000"),
            "SOL": Decimal("100"),
            "AVAX": Decimal("30"),
            "USDC": Decimal("1.0"),
        }
        return fallback_prices.get(symbol, Decimal("100"))

    # Dynamic Pricing Utilities

    @staticmethod
    async def get_dynamic_test_price(
        api: HyperliquidAPI,
        symbol: str,
        side: OrderSide,
        tolerance_percent: Decimal = DEFAULT_PRICE_TOLERANCE_PERCENT,
    ) -> Decimal:
        """Calculate a safe test price offset from market price.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol
            side: Order side (BUY or SELL)
            tolerance_percent: Percentage offset from market price

        Returns:
            Safe test price that won't immediately execute
        """
        market_price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)
        constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)

        # Calculate offset price
        offset_multiplier = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            # Buy orders: price below market
            test_price = market_price * (Decimal("1") - offset_multiplier)
        else:
            # Sell orders: price above market
            test_price = market_price * (Decimal("1") + offset_multiplier)

        # Round to tick size
        tick_size = constraints["tick_size"]
        return (test_price / tick_size).quantize(Decimal("1")) * tick_size

    @staticmethod
    async def get_unreasonably_large_price(api: HyperliquidAPI, symbol: str) -> Decimal:
        """Get an unreasonably large price for negative testing."""
        market_price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)
        return market_price * Decimal("10")  # 10x market price

    @staticmethod
    async def get_unreasonably_large_quantity(api: HyperliquidAPI, symbol: str) -> Decimal:
        """Get an unreasonably large quantity for negative testing."""
        constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
        return constraints["max_quantity"] * Decimal("10")  # 10x max quantity

    # Dynamic Sizing Utilities

    @staticmethod
    async def get_minimal_order_size(
        api: HyperliquidAPI,
        symbol: str,
        side: OrderSide,
        price: Decimal | None = None,
    ) -> Decimal:
        """Calculate minimal viable order size based on account balance.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol
            side: Order side
            price: Order price (if None, uses current market price)

        Returns:
            Minimal order quantity that account can afford
        """
        if price is None:
            price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)

        constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
        min_quantity = constraints["min_quantity"]

        try:
            # Get account balance
            account_summary = await api.get_account_summary()
            if not account_summary or account_summary.total_equity <= Decimal("0"):
                return min_quantity

            # Calculate affordable quantity based on available balance
            notional_value = MIN_ORDER_VALUE_USD
            if account_summary.total_equity > MIN_ORDER_VALUE_USD:
                # Use small fraction of available balance
                notional_value = min(
                    account_summary.total_equity * Decimal("0.01"),  # 1% of equity
                    Decimal("10"),  # Cap at $10 for safety
                )

            affordable_quantity = notional_value / price

            # Round to step size
            step_size = constraints["step_size"]
            rounded_quantity = (affordable_quantity / step_size).quantize(Decimal("1")) * step_size

            # Ensure it meets minimum requirements
            return max(rounded_quantity, min_quantity)

        except Exception:
            return min_quantity

    @staticmethod
    async def validate_order_constraints(
        api: HyperliquidAPI,
        symbol: str,
        quantity: Decimal,
        price: Decimal,
    ) -> bool:
        """Validate if order meets exchange constraints.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol
            quantity: Order quantity
            price: Order price

        Returns:
            True if order meets all constraints
        """
        try:
            constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)

            # Check quantity constraints
            if quantity < constraints["min_quantity"]:
                return False
            if quantity > constraints["max_quantity"]:
                return False

            # Check notional value
            notional_value = quantity * price
            if notional_value < MIN_ORDER_VALUE_USD:
                return False

            # Check step size alignment
            step_size = constraints["step_size"]
            if (quantity % step_size) != Decimal("0"):
                return False

            # Check tick size alignment
            tick_size = constraints["tick_size"]
            if (price % tick_size) != Decimal("0"):
                return False

            return True

        except Exception:
            return False

    # Account State Detection

    @staticmethod
    async def detect_account_state(api: HyperliquidAPI) -> dict[str, Any]:
        """Detect current account state and capabilities.

        Args:
            api: HyperliquidAPI instance

        Returns:
            Dict containing account state information
        """
        try:
            account_summary = await api.get_account_summary()
            if not account_summary:
                return {"has_balance": False, "can_trade": False}

            has_balance = account_summary.total_equity > Decimal("0")
            can_trade = account_summary.total_equity >= MIN_ORDER_VALUE_USD

            return {
                "has_balance": has_balance,
                "can_trade": can_trade,
                "total_equity": account_summary.total_equity,
                "available_balance": getattr(account_summary, "available_balance", Decimal("0")),
                "margin_used": getattr(account_summary, "margin_used", Decimal("0")),
            }

        except Exception:
            return {"has_balance": False, "can_trade": False}

    @staticmethod
    async def get_account_margin_parameters(api: HyperliquidAPI) -> dict[str, Decimal]:
        """Get dynamic margin parameters from account.

        Args:
            api: HyperliquidAPI instance

        Returns:
            Dict with margin-related parameters
        """
        try:
            account_summary = await api.get_account_summary()
            if not account_summary:
                return HyperliquidTestHelpers._get_fallback_margin_params()

            return {
                "maintenance_margin": getattr(account_summary, "maintenance_margin", Decimal("0")),
                "initial_margin": getattr(account_summary, "initial_margin", Decimal("0")),
                "leverage": getattr(account_summary, "leverage", Decimal("1")),
                "max_leverage": getattr(account_summary, "max_leverage", Decimal("20")),
            }

        except Exception:
            return HyperliquidTestHelpers._get_fallback_margin_params()

    @staticmethod
    def _get_fallback_margin_params() -> dict[str, Decimal]:
        """Get conservative fallback margin parameters."""
        return {
            "maintenance_margin": Decimal("0.05"),  # 5%
            "initial_margin": Decimal("0.1"),  # 10%
            "leverage": Decimal("1"),  # No leverage
            "max_leverage": Decimal("20"),  # Hyperliquid typical max
        }

    # Position Limit Calculations

    @staticmethod
    async def calculate_maximum_position_size(
        api: HyperliquidAPI,
        symbol: str,
    ) -> dict[str, Decimal]:
        """Calculate maximum position size based on account parameters.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol

        Returns:
            Dict with max position calculations
        """
        try:
            account_state = await HyperliquidTestHelpers.detect_account_state(api)
            margin_params = await HyperliquidTestHelpers.get_account_margin_parameters(api)
            market_price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)

            if not account_state["can_trade"]:
                return {"max_quantity": Decimal("0"), "max_notional": Decimal("0")}

            # Calculate based on available balance and leverage
            available_balance = account_state.get("available_balance", Decimal("0"))
            max_leverage = margin_params["max_leverage"]

            # Conservative approach: use only a fraction of available balance
            usable_balance = available_balance * Decimal("0.5")  # 50% of available
            max_notional = usable_balance * max_leverage
            max_quantity = max_notional / market_price

            # Apply constraints
            constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
            max_quantity = min(max_quantity, constraints["max_quantity"])

            return {
                "max_quantity": max_quantity,
                "max_notional": max_quantity * market_price,
                "leverage_used": max_leverage,
            }

        except Exception:
            return {"max_quantity": Decimal("0"), "max_notional": Decimal("0")}

    # Test Cleanup Utilities

    @staticmethod
    async def cleanup_test_orders(api: HyperliquidAPI, symbol: str | None = None) -> None:
        """Clean up any test orders that might be open.

        Args:
            api: HyperliquidAPI instance
            symbol: Specific symbol to clean (if None, cleans all)
        """
        try:
            # Cancel all open orders for the symbol or all symbols
            await api.cancel_all_orders(symbol=symbol)

            # Wait a moment for cancellation to process
            await asyncio.sleep(0.5)

        except Exception as e:
            # Cleanup is best-effort, don't fail tests if it doesn't work
            logging.warning(f"Failed to cleanup test orders: {e}")

    @staticmethod
    async def cleanup_test_positions(api: HyperliquidAPI, symbol: str | None = None) -> None:
        """Clean up any test positions that might be open.

        Args:
            api: HyperliquidAPI instance
            symbol: Specific symbol to clean (if None, attempts all)
        """
        try:
            positions = await api.get_positions()
            if not positions:
                return

            for position in positions:
                if symbol and position.symbol != symbol:
                    continue

                # Close position if it has size
                if abs(position.size) > Decimal("0"):
                    # This would need to be implemented based on your position closing logic
                    # For now, just document that manual cleanup may be needed
                    pass

        except Exception as e:
            # Cleanup is best-effort, don't fail tests if it doesn't work
            logging.warning(f"Failed to cleanup test positions: {e}")


# Convenience functions for common operations


async def get_symbol_tick_size(api: HyperliquidAPI, symbol: str) -> Decimal:
    """Get tick size for a symbol."""
    constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
    return constraints["tick_size"]


async def get_symbol_step_size(api: HyperliquidAPI, symbol: str) -> Decimal:
    """Get step size for a symbol."""
    constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
    return constraints["step_size"]


async def get_minimal_test_quantity(
    api: HyperliquidAPI,
    symbol: str,
    side: OrderSide,
) -> Decimal:
    """Get minimal test quantity for an order."""
    return await HyperliquidTestHelpers.get_minimal_order_size(api, symbol, side)


async def get_safe_test_price(
    api: HyperliquidAPI,
    symbol: str,
    side: OrderSide,
    tolerance: Decimal = DEFAULT_PRICE_TOLERANCE_PERCENT,
) -> Decimal:
    """Get safe test price that won't immediately execute."""
    return await HyperliquidTestHelpers.get_dynamic_test_price(api, symbol, side, tolerance)
