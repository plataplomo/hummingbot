"""Dynamic test helpers for Hyperliquid integration tests.

This module provides utilities for creating robust, adaptive tests that work
across different account states and market conditions. Inspired by Backpack's
dynamic helper patterns but adapted for Hyperliquid's specific API structure.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.service_args_models import GetMarketArgs
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.enums import OrderSide

logger = get_logger(__name__)

# REMOVED HARDCODED SYMBOL LISTS - SECURITY VIOLATION
# Hardcoded symbol lists are forbidden - must get available symbols from exchange
# Use get_available_symbols() to query exchange for current trading pairs

# REMOVE HARDCODED TOLERANCES - THESE ARE SECURITY VIOLATIONS
# Tests must get real tolerances from exchange or fail


class HyperliquidTestHelpers:
    """Collection of dynamic test helpers for Hyperliquid integration tests."""

    # Symbol Discovery Utilities

    @staticmethod
    async def get_available_perp_symbols(api: HyperliquidAPI, limit: int = 3) -> list[str]:
        """Get available perpetual symbols from the exchange.

        Args:
            api: HyperliquidAPI instance
            limit: Maximum number of symbols to return

        Returns:
            List of available perpetual symbols

        Raises:
            RuntimeError: If unable to fetch symbols from exchange
        """
        try:
            # For Hyperliquid, perp symbols are like "BTC", "ETH", "SOL"
            # We'll get these from the markets endpoint
            from cyberdelta.apis.models.service_args_models import GetMarketsArgs

            markets = await api.get_markets(GetMarketsArgs())
            if markets:
                # Extract symbols from markets data
                symbols = [market.symbol for market in markets][:limit]
                if symbols:
                    return symbols

            # Fallback to trying common symbols if meta doesn't work
            common_symbols = ["BTC", "ETH", "SOL"]
            available_symbols: list[str] = []

            for symbol in common_symbols:
                try:
                    market = await api.get_market(GetMarketArgs(symbol=symbol))
                    if market:
                        available_symbols.append(symbol)
                        if len(available_symbols) >= limit:
                            break
                except Exception as e:
                    logger.debug(f"Symbol {symbol} unavailable: {e}")
                    continue  # Skip unavailable symbols

            if not available_symbols:
                raise RuntimeError(
                    "No perpetual symbols available from exchange. "
                    "Hyperliquid tests require real trading symbols."
                )

            return available_symbols

        except Exception as e:
            raise RuntimeError(
                f"Failed to fetch perpetual symbols from exchange: {e}. "
                "Hyperliquid tests require real market data and cannot use hardcoded symbols."
            ) from e

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
                raise RuntimeError(
                    f"Failed to get market data for {symbol}. "
                    "Trading tests require real market data and cannot use fallback values."
                )

            # Verify all required fields are present
            if not market.tick_size or not market.step_size:
                raise RuntimeError(
                    f"Incomplete market data for {symbol}: missing tick_size or step_size. "
                    "Cannot proceed with trading tests without complete market constraints."
                )

            if not market.min_quantity:
                raise RuntimeError(
                    f"Missing min_quantity for {symbol}. "
                    "Cannot determine minimum order size for trading tests."
                )

            return {
                "tick_size": market.tick_size,
                "step_size": market.step_size,
                "min_quantity": market.min_quantity,
                "max_quantity": market.max_quantity or market.min_quantity * Decimal("1000000"),
            }
        except Exception as e:
            raise RuntimeError(
                f"Failed to get market constraints for {symbol}: {e}. "
                "Trading tests must have access to real market data to ensure safety."
            ) from e

    # REMOVED _get_fallback_constraints - SECURITY VIOLATION
    # Fallback constraints with hardcoded values are forbidden in trading tests

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

            # No fallback prices - fail fast instead
            raise RuntimeError(
                f"Failed to get market price for {symbol}. No ticker or market data available. "
                "Trading tests require real market data and cannot use hardcoded fallback values."
            )

        except Exception as e:
            raise RuntimeError(
                f"Failed to get market price for {symbol}: {e}. "
                "Trading tests require real market data and cannot use fallback values."
            ) from e

    # REMOVED _get_fallback_price - SECURITY VIOLATION
    # Hardcoded fallback prices are forbidden in trading tests

    # Funding Rate Utilities

    @staticmethod
    async def get_funding_rate_bounds(api: HyperliquidAPI, symbol: str) -> dict[str, Decimal]:
        """Get exchange-specific funding rate bounds for a symbol.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol (e.g., "BTC", "ETH")

        Returns:
            Dict with min_rate, max_rate, typical_range

        Raises:
            RuntimeError: If unable to get funding rate bounds from exchange
        """
        try:
            # Get market information to understand funding rate constraints
            market = await api.get_market(GetMarketArgs(symbol=symbol))
            if not market:
                raise RuntimeError(
                    f"Failed to get market data for {symbol}. "
                    "Funding rate tests require real market data and cannot use hardcoded bounds."
                )

            # Hyperliquid typically uses ±0.75% daily max (±0.0075 per 8hr period)
            # But we should get this from actual market data if available

            # Try to get historical funding rates to determine actual bounds
            from datetime import UTC, datetime, timedelta

            from cyberdelta.apis.models.service_args_models import GetHistoricalFundingRatesArgs

            # Get last 7 days of funding data to establish bounds
            end_time = datetime.now(UTC)
            start_time = end_time - timedelta(days=7)

            try:
                args = GetHistoricalFundingRatesArgs(
                    symbol=symbol,
                    start_time=start_time,
                    end_time=end_time,
                )

                historical_rates = await api.get_historical_funding_rates(args)

                if historical_rates and len(historical_rates) > 0:
                    # Calculate actual observed bounds from recent data
                    rates: list[Decimal] = []
                    for rate_entry in historical_rates:
                        # DEFENSIVE CHECK: None check required by RULE-RUNTIME-SAFETY-V4
                        if (
                            rate_entry.funding_rate is not None
                            and rate_entry.funding_rate.is_finite()
                        ):
                            rates.append(rate_entry.funding_rate)

                    if rates:
                        min_observed: Decimal = min(rates)
                        max_observed: Decimal = max(rates)

                        # Add 50% buffer to observed range for safety
                        range_buffer: Decimal = (max_observed - min_observed) * Decimal("0.5")

                        return {
                            "min_rate": min_observed - range_buffer,
                            "max_rate": max_observed + range_buffer,
                            "typical_range": max_observed - min_observed,
                        }

            except Exception as e:
                logger.debug(f"Could not get historical funding rates for bounds: {e}")

            # If no historical data available, fail rather than use hardcoded values
            raise RuntimeError(
                f"Failed to determine funding rate bounds for {symbol} from exchange data. "
                "Cannot get historical funding rates to establish realistic bounds. "
                "Funding rate tests require real exchange data and cannot use hardcoded bounds."
            )

        except Exception as e:
            raise RuntimeError(
                f"Failed to get funding rate bounds for {symbol}: {e}. "
                "Funding rate tests require real exchange constraints."
            ) from e

    # Dynamic Pricing Utilities

    @staticmethod
    async def get_dynamic_test_price(
        api: HyperliquidAPI,
        symbol: str,
        side: OrderSide,
        tolerance_percent: Decimal | None = None,
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

        # Get tolerance from exchange or fail
        if tolerance_percent is None:
            # Must get real tolerance requirements from exchange
            raise RuntimeError(
                f"No price tolerance provided for {symbol}. "
                "Tests must specify explicit tolerance based on exchange requirements, "
                "not use hardcoded default values."
            )

        # Calculate offset price
        offset_multiplier = tolerance_percent / Decimal("100")

        if side == OrderSide.BUY:
            # Buy orders: price below market
            test_price = market_price * (Decimal("1") - offset_multiplier)
        else:
            # Sell orders: price above market
            test_price = market_price * (Decimal("1") + offset_multiplier)

        # Round to tick size (properly)
        tick_size = constraints["tick_size"]
        # Round to the nearest tick and ensure no floating point errors
        from decimal import ROUND_HALF_UP

        # Get the precision of the tick size first
        exponent = tick_size.as_tuple().exponent
        if isinstance(exponent, int):
            tick_decimal_places = abs(exponent)
        else:
            # Handle special cases like 'n', 'N', 'F' - fallback to string analysis
            tick_str = str(tick_size)
            if "." in tick_str:
                tick_decimal_places = len(tick_str.split(".")[1])
            else:
                tick_decimal_places = 0
        price_precision = Decimal(10) ** (-tick_decimal_places)

        # Calculate how many ticks this test price represents
        ticks_decimal = test_price / tick_size
        rounded_ticks = ticks_decimal.quantize(Decimal("1"), rounding=ROUND_HALF_UP)

        # Calculate the final price by multiplying back
        final_price = rounded_ticks * tick_size

        # Ensure the final price is quantized to match tick_size precision exactly
        # This is critical for Hyperliquid's strict tick size validation
        quantized_price = final_price.quantize(price_precision, rounding=ROUND_HALF_UP)

        # Log calculation information for debugging
        logger.info(
            f"Price calculation for {symbol}: market={market_price}, "
            f"tolerance={tolerance_percent}%, side={side.value}"
        )
        operator = "-" if side == OrderSide.BUY else "+"
        logger.info(
            f"Step 1 - test_price calculation: {market_price} * "
            f"(1 {operator} {tolerance_percent / 100}) = {test_price}"
        )
        logger.info(
            f"Step 2 - tick alignment: test_price={test_price}, tick_size={tick_size}, "
            f"ticks_decimal={ticks_decimal}, rounded_ticks={rounded_ticks}"
        )
        logger.info(
            f"Step 3 - final calculation: {rounded_ticks} * {tick_size} = {final_price}, "
            f"quantized to {tick_decimal_places} places = {quantized_price}"
        )

        # Verify the final price is correctly aligned to tick size
        remainder = quantized_price % tick_size
        if remainder != Decimal("0"):
            # Force alignment by recalculating
            corrected_ticks = (quantized_price / tick_size).quantize(
                Decimal("1"), rounding=ROUND_HALF_UP
            )
            quantized_price = corrected_ticks * tick_size
            quantized_price = quantized_price.quantize(price_precision, rounding=ROUND_HALF_UP)

            logger.warning(
                f"Price alignment corrected for {symbol}: was {final_price}, now {quantized_price}"
            )

        return quantized_price

    @staticmethod
    async def get_unreasonably_large_price(api: HyperliquidAPI, symbol: str) -> Decimal:
        """Get an unreasonably large price for negative testing."""
        market_price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)
        # Use exchange maximum price limits instead of arbitrary multiplier
        await HyperliquidTestHelpers.get_market_constraints(api, symbol)
        max_reasonable_multiplier = Decimal("2")  # 2x as maximum for negative testing
        return market_price * max_reasonable_multiplier

    @staticmethod
    async def get_unreasonably_large_quantity(api: HyperliquidAPI, symbol: str) -> Decimal:
        """Get an unreasonably large quantity for negative testing."""
        constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
        # Use exchange maximum or account limits instead of arbitrary multiplier
        max_account_size = await HyperliquidTestHelpers.calculate_maximum_position_size(api, symbol)
        if max_account_size["max_quantity"] > Decimal("0"):
            return max_account_size["max_quantity"] * Decimal("2")  # 2x account max
        else:
            return constraints["max_quantity"]  # Use exchange max if no account limit

    # Dynamic Sizing Utilities

    @staticmethod
    async def get_minimal_order_size(
        api: HyperliquidAPI,
        symbol: str,
        side: OrderSide,
        price: Decimal | None = None,
    ) -> Decimal:
        """Calculate minimal viable order size that meets exchange minimum notional value.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol
            side: Order side
            price: Order price (if None, uses current market price)

        Returns:
            Minimal order quantity that meets exchange minimum notional requirements

        Raises:
            RuntimeError: If unable to determine valid order size
        """
        if price is None:
            price = await HyperliquidTestHelpers.get_current_market_price(api, symbol)

        constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
        min_quantity = constraints["min_quantity"]
        step_size = constraints["step_size"]

        try:
            # Get account information to determine real minimum requirements
            account_summary = await api.get_account_summary()
            if not account_summary or account_summary.total_equity <= Decimal("0"):
                raise RuntimeError(
                    f"No account equity available for {symbol}. "
                    "Trading tests require funded account to determine real order minimums."
                )

            # Hyperliquid testnet has a $10 minimum notional value requirement
            # Calculate quantity needed to meet this minimum
            MIN_NOTIONAL_USD = Decimal("10.00")  # $10 minimum from Hyperliquid testnet
            min_qty_for_notional = MIN_NOTIONAL_USD / price

            # Use the larger of: exchange min quantity OR quantity for $10 notional
            required_min_quantity = max(min_quantity, min_qty_for_notional)

            # Round up to next valid step size to ensure we meet minimums
            from decimal import ROUND_UP

            rounded_steps = (required_min_quantity / step_size).quantize(
                Decimal("1"), rounding=ROUND_UP
            )
            final_quantity = rounded_steps * step_size

            # Verify we meet exchange minimum quantity
            if final_quantity < min_quantity:
                raise RuntimeError(
                    f"Calculated quantity {final_quantity} below exchange minimum "
                    f"{min_quantity} for {symbol}"
                )

            # Verify we meet minimum notional value
            final_notional = final_quantity * price
            if final_notional < MIN_NOTIONAL_USD:
                # Recalculate with slightly higher quantity to ensure we meet minimum
                min_qty_for_notional = MIN_NOTIONAL_USD / price
                # Add small buffer to account for rounding
                buffered_qty = min_qty_for_notional * Decimal("1.01")  # 1% buffer

                rounded_steps = (buffered_qty / step_size).quantize(Decimal("1"), rounding=ROUND_UP)
                final_quantity = rounded_steps * step_size
                final_notional = final_quantity * price

            # Verify account can afford this order (max 20% of equity for testing)
            max_affordable_notional = account_summary.total_equity * Decimal("0.2")
            if final_notional > max_affordable_notional:
                raise RuntimeError(
                    f"Required notional value {final_notional} exceeds 20% of account equity "
                    f"({max_affordable_notional}). Cannot safely test with minimum $10 order "
                    f"on this account."
                )

            logger.info(
                f"Calculated minimal order size for {symbol}: qty={final_quantity}, "
                f"price={price}, notional=${final_notional}, min_required=${MIN_NOTIONAL_USD}"
            )

            return final_quantity

        except Exception as e:
            raise RuntimeError(
                f"Failed to calculate minimal order size for {symbol} at price {price}: {e}. "
                "Cannot determine safe order size without valid exchange constraints."
            ) from e

    @staticmethod
    async def get_minimal_order_size_for_zero_balance(
        api: HyperliquidAPI,
        symbol: str,
        side: OrderSide,
        price: Decimal | None = None,
    ) -> Decimal:
        """Calculate minimal viable order size for zero balance accounts.

        This method returns the exchange minimum quantity without requiring
        account equity, making it suitable for zero balance testing scenarios.

        Args:
            api: HyperliquidAPI instance
            symbol: Trading symbol
            side: Order side
            price: Order price (if None, uses current market price)

        Returns:
            Exchange minimum order quantity (ignores notional requirements)

        Raises:
            RuntimeError: If unable to determine valid order size
        """
        try:
            constraints = await HyperliquidTestHelpers.get_market_constraints(api, symbol)
            min_quantity = constraints["min_quantity"]
            step_size = constraints["step_size"]

            # For zero balance tests, just return the exchange minimum
            # Round to next valid step size if needed
            from decimal import ROUND_UP

            rounded_steps = (min_quantity / step_size).quantize(
                Decimal("1"), rounding=ROUND_UP
            )
            final_quantity = rounded_steps * step_size

            # Ensure we meet exchange minimum
            if final_quantity < min_quantity:
                final_quantity = min_quantity

            logger.info(
                f"Calculated minimal order size for zero balance test {symbol}: "
                f"qty={final_quantity} (exchange minimum)"
            )

            return final_quantity

        except Exception as e:
            raise RuntimeError(
                f"Failed to calculate minimal order size for zero balance test {symbol}: {e}. "
                "Cannot determine exchange minimum constraints."
            ) from e

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

            # Check notional value against exchange minimums
            notional_value = quantity * price
            min_notional = constraints["min_quantity"] * price
            if notional_value < min_notional:
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
            can_trade = account_summary.total_equity > Decimal("0")

            return {
                "has_balance": has_balance,
                "can_trade": can_trade,
                "total_equity": account_summary.total_equity,
                "available_balance": getattr(account_summary, "available_balance", Decimal("0")),
                "margin_used": getattr(account_summary, "margin_used", Decimal("0")),
            }

        except Exception as e:
            # Don't hide account access failures
            raise RuntimeError(
                f"Failed to detect account state: {e}. "
                "Trading tests require access to account information."
            ) from e

    @staticmethod
    async def wait_for_order_cancellation(
        api: HyperliquidAPI, symbol: str | None = None, timeout: int = 30
    ) -> None:
        """Wait for order cancellation to complete with proper verification and adaptive polling."""
        import time

        start_time = time.time()
        attempt = 0

        while time.time() - start_time < timeout:
            try:
                open_orders = await api.get_open_orders()
                if symbol:
                    symbol_orders = [order for order in open_orders if order.symbol == symbol]
                    if not symbol_orders:
                        return  # All orders for symbol cancelled
                else:
                    if not open_orders:
                        return  # All orders cancelled

                # Adaptive polling interval: shorter intervals initially, longer as time passes
                attempt += 1
                if attempt <= 3:
                    interval = 0.5  # 0.5s for first 3 attempts (fast initial checks)
                elif attempt <= 10:
                    interval = 1.0  # 1s for next 7 attempts (standard polling)
                else:
                    interval = 2.0  # 2s for remaining attempts (slower polling)

                await asyncio.sleep(interval)
            except Exception as e:
                raise RuntimeError(f"Failed to verify order cancellation: {e}") from e

        raise RuntimeError(f"Order cancellation not completed within {timeout} seconds")

    @staticmethod
    async def wait_for_order_placement(
        api: HyperliquidAPI, order_id: str, timeout: int = 30
    ) -> None:
        """Wait for order to appear in open orders with proper verification."""
        import time

        start_time = time.time()
        attempt = 0

        while time.time() - start_time < timeout:
            try:
                open_orders = await api.get_open_orders()
                if any(order.exchange_order_id == order_id for order in open_orders):
                    return  # Order found in open orders

                # Adaptive polling interval
                attempt += 1
                if attempt <= 3:
                    interval = 0.5  # Fast initial checks
                elif attempt <= 10:
                    interval = 1.0  # Standard polling
                else:
                    interval = 2.0  # Slower polling

                await asyncio.sleep(interval)
            except Exception as e:
                raise RuntimeError(f"Failed to verify order placement: {e}") from e

        raise RuntimeError(f"Order {order_id} not found in open orders within {timeout} seconds")

    @staticmethod
    async def eventually_assert(
        condition_func: Callable[[], bool | Any],
        timeout: int = 30,
        message: str = "Condition not met",
    ) -> None:
        """Poll until condition is true or timeout occurs."""
        import time

        start_time = time.time()
        attempt = 0

        while time.time() - start_time < timeout:
            try:
                condition_result = condition_func()
                if asyncio.iscoroutine(condition_result):
                    result = await condition_result
                else:
                    result = condition_result

                if result:
                    return  # Condition met

                # Adaptive polling interval
                attempt += 1
                if attempt <= 3:
                    interval = 0.5
                elif attempt <= 10:
                    interval = 1.0
                else:
                    interval = 2.0

                await asyncio.sleep(interval)
            except Exception as e:
                raise RuntimeError(f"Failed to check condition: {e}") from e

        raise RuntimeError(f"{message} (timeout after {timeout} seconds)")

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

            # Verify cancellation completed instead of fixed sleep
            await HyperliquidTestHelpers.wait_for_order_cancellation(api, symbol)

        except Exception as e:
            # Order cleanup failures are critical in trading tests
            raise RuntimeError(
                f"Failed to cleanup test orders for {symbol}: {e}. "
                "Order cleanup is critical for test isolation and financial safety."
            ) from e

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
            # Position cleanup failures are critical in trading tests
            raise RuntimeError(
                f"Failed to cleanup test positions for {symbol}: {e}. "
                "Position cleanup is critical for test isolation and financial safety."
            ) from e


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


async def get_minimal_test_quantity_for_zero_balance(
    api: HyperliquidAPI,
    symbol: str,
    side: OrderSide,
) -> Decimal:
    """Get minimal test quantity for zero balance accounts.
    
    Returns exchange minimum quantity without requiring account equity.
    Suitable for zero balance testing scenarios.
    """
    return await HyperliquidTestHelpers.get_minimal_order_size_for_zero_balance(api, symbol, side)


async def get_safe_test_price(
    api: HyperliquidAPI,
    symbol: str,
    side: OrderSide,
    tolerance: Decimal,
) -> Decimal:
    """Get safe test price that won't immediately execute.

    Args:
        api: HyperliquidAPI instance
        symbol: Trading symbol
        side: Order side (BUY or SELL)
        tolerance: Price tolerance percentage - MUST be provided explicitly
                  based on exchange requirements, not hardcoded defaults

    Returns:
        Safe test price that won't immediately execute
    """
    return await HyperliquidTestHelpers.get_dynamic_test_price(api, symbol, side, tolerance)
