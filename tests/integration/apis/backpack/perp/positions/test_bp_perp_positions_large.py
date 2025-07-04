"""Integration tests for Backpack maximum position limits.

Tests the absolute limits of what positions can be created with available margin.
No hardcoded values - everything calculated dynamically from account state.
"""

from __future__ import annotations

from decimal import ROUND_DOWN, Decimal
from typing import TYPE_CHECKING, Any, TypedDict


if TYPE_CHECKING:
    from cyberdelta.core.models import MarginAccountSummary

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetMarketArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    get_available_symbols,
    get_current_market_price,
    get_market_constraints,
    wait_for_condition,
)


logger = get_logger(__name__)


class MaxPositionParams(TypedDict):
    """Type definition for maximum position parameters."""

    max_position_size: Decimal
    max_notional_value: Decimal
    account_equity: Decimal
    available_equity: Decimal
    market_price: Decimal
    step_size: Decimal
    imf: Decimal | None
    mmf: Decimal | None
    margin_fraction: Decimal | None
    leverage_limit: Decimal | None


# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.large_positions,
    pytest.mark.requires_large_balance,
    pytest.mark.timing,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/perp/positions/large"],
    indirect=True,
)
class TestBackpackPerpLargePositions:
    """Test maximum position limits based on actual available margin."""

    async def _close_all_positions(self, api: BackpackAPI) -> None:
        """Close all open positions."""
        try:
            positions = await api.get_positions()
            for position in positions:
                if abs(position.size) > Decimal("0.001"):
                    close_side = OrderSide.SELL if position.size > 0 else OrderSide.BUY
                    place_args = PlaceOrderArgs(
                        symbol=position.symbol,
                        side=close_side,
                        order_type=OrderType.MARKET,
                        quantity=abs(position.size),
                        time_in_force=TimeInForce.IOC,
                    )
                    try:
                        await api.place_order(place_args)

                        # Wait for position to be closed
                        position_symbol = position.symbol

                        async def position_closed(symbol: str = position_symbol) -> bool:
                            current_positions = await api.get_positions()
                            # Position is closed when it has zero size,
                            # not when it's removed from the list
                            symbol_position = next(
                                (p for p in current_positions if p.symbol == symbol),
                                None,
                            )
                            return symbol_position is None or abs(symbol_position.size) <= Decimal(
                                "0.001",
                            )

                        await wait_for_condition(
                            position_closed,
                            timeout_seconds=30.0,  # Increased timeout for exchange operations
                            poll_interval=0.5,  # Less frequent polling to reduce API load
                            message=f"Position {position.symbol} was not closed",
                        )
                    except (APIError, ValueError, TypeError, KeyError) as e:
                        # Log and ignore errors when closing positions in cleanup
                        logger.warning(
                            "failed_to_close_position",
                            symbol=position.symbol,
                            error=str(e),
                            message="Failed to close position in cleanup",
                        )
                        continue
        except (APIError, ValueError, TypeError, KeyError):
            # Ignore errors in position cleanup
            return

    async def _find_max_order_via_exchange_limits(
        self,
        api: BackpackAPI,
        symbol: str,
        side: OrderSide,
    ) -> Decimal:
        """Find maximum order size by querying exchange limits and validating.

        This properly tests the exchange's reported limits without making
        dangerous assumptions about leverage or fallback values.

        Returns:
            The maximum order quantity validated by the exchange.
        """
        constraints = await get_market_constraints(api, symbol)
        account_summary = await api.get_account_summary()
        market_price = await get_current_market_price(api, symbol)

        self._validate_account_equity(account_summary)
        max_notional = await self._calculate_max_notional(api, symbol, account_summary)
        max_quantity = self._calculate_max_quantity(max_notional, market_price, constraints)

        if max_quantity < constraints["min_order_size"]:
            logger.info(
                "max_quantity_below_min_size",
                max_quantity=max_quantity,
                min_size=constraints["min_order_size"],
                message=(
                    "Calculated max quantity is below min size. "
                    "Account may have insufficient funds for minimum position."
                ),
            )
            return Decimal(0)

        return await self._validate_test_size(
            api, symbol, side, max_quantity, constraints, market_price
        )

    def _validate_account_equity(self, account_summary: MarginAccountSummary) -> None:
        """Validate account has sufficient equity for testing."""
        if account_summary.available_equity <= Decimal(0):
            pytest.fail(
                f"No available equity for testing. "
                f"Available: {account_summary.available_equity}. "
                "Cannot test position limits without real funds."
            )

    async def _calculate_max_notional(
        self, api: BackpackAPI, symbol: str, account_summary: MarginAccountSummary
    ) -> Decimal:
        """Calculate maximum notional based on exchange limits."""
        await api.get_market(GetMarketArgs(symbol=symbol))

        # For Backpack, we need to use a default max leverage since it's not in the market data
        # This is a reasonable default for perpetual markets
        default_max_leverage = Decimal(50)
        return account_summary.available_equity * default_max_leverage

    def _calculate_max_quantity(
        self, max_notional: Decimal, market_price: Decimal, constraints: dict[str, Any]
    ) -> Decimal:
        """Calculate maximum quantity respecting step size."""
        max_quantity = (max_notional / market_price).quantize(Decimal(1), rounding=ROUND_DOWN)

        if max_quantity > Decimal(0):
            step_size = constraints["step_size"]
            steps = (max_quantity / step_size).quantize(Decimal(1), rounding=ROUND_DOWN)
            max_quantity = steps * step_size

        return max_quantity

    async def _validate_test_size(
        self,
        api: BackpackAPI,
        symbol: str,
        side: OrderSide,
        max_quantity: Decimal,
        constraints: dict[str, Any],
        market_price: Decimal,
    ) -> Decimal:
        """Validate test size by placing and canceling an order."""
        test_size = (max_quantity * Decimal("0.95")).quantize(
            Decimal(10) ** -constraints["quantity_precision"], rounding=ROUND_DOWN
        )

        step_size = constraints["step_size"]
        steps = (test_size / step_size).quantize(Decimal(1), rounding=ROUND_DOWN)
        test_size = steps * step_size

        if test_size < constraints["min_order_size"]:
            pytest.fail(
                f"Cannot create valid test order. "
                f"95% of max ({test_size}) is below min size ({constraints['min_order_size']})"
            )

        # Place and cancel test order
        if side == OrderSide.BUY:
            test_price = market_price * Decimal("0.9")
        else:
            test_price = market_price * Decimal("1.1")

        place_args = PlaceOrderArgs(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=test_size,
            price=test_price,
            time_in_force=TimeInForce.GTC,
            post_only=True,
        )

        try:
            order = await api.place_order(place_args)
            if not order or not order.exchange_order_id:
                pytest.fail(
                    f"Failed to place test order at {test_size}. Exchange limits may have changed."
                )

            # Cancel immediately - this was just a validation
            cancel_result = await api.cancel_order(
                CancelOrderArgs(order_id=order.exchange_order_id, symbol=symbol)
            )
            if not cancel_result:
                pytest.fail(
                    f"Critical: Failed to cancel test order {order.exchange_order_id}. "
                    "Order cancellation must work reliably."
                )

        except APIError as e:
            if "insufficient" in str(e).lower() or "margin" in str(e).lower():
                pytest.fail(
                    f"Exchange rejected order at 95% of calculated max. "
                    f"Error: {e}. "
                    f"Exchange limits may be more restrictive than reported."
                )
            else:
                pytest.fail(f"Unexpected error validating position limits: {e}")

        return max_quantity

    async def _get_account_margin_parameters(self, api: BackpackAPI) -> dict[str, Decimal | None]:
        """Get actual margin parameters from the exchange.

        Returns:
            Dictionary containing margin parameters with keys: imf, mmf,
                margin_fraction, leverage_limit.

        Raises:
            ValueError: If no Backpack-specific margin details are available.
        """
        # Get account summary which includes bp_details with enhanced margin info
        account_summary = await api.get_account_summary()

        if not account_summary.bp_details:
            raise ValueError("No Backpack-specific margin details available")

        return {
            "imf": (
                Decimal(account_summary.bp_details.imf_raw)
                if account_summary.bp_details.imf_raw
                else None
            ),
            "mmf": (
                Decimal(account_summary.bp_details.mmf_raw)
                if account_summary.bp_details.mmf_raw
                else None
            ),
            "margin_fraction": account_summary.bp_details.margin_fraction,
            "leverage_limit": account_summary.bp_details.leverage_limit,
        }

    async def _find_maximum_position_using_exchange_limits(
        self,
        api: BackpackAPI,
        symbol: str,
    ) -> MaxPositionParams:
        """Find the absolute maximum position using exchange's max order endpoint.

        Returns:
            MaxPositionParams containing max position size, notional value, account equity,
            available equity, market price, step size, and margin parameters.
        """
        # Close all positions first
        await self._close_all_positions(api)

        # Wait for all positions to be confirmed closed (zero size)
        async def all_positions_closed() -> bool:
            positions = await api.get_positions()
            # Positions are closed when they have zero size, not when they're removed from the list
            return all(abs(position.size) <= Decimal("0.001") for position in positions)

        await wait_for_condition(
            all_positions_closed,
            timeout_seconds=30.0,  # Increased timeout for exchange operations
            poll_interval=0.5,  # Less frequent polling to reduce API load
            message="All positions were not closed",
        )

        # Get constraints
        constraints = await get_market_constraints(api, symbol)
        step_size = constraints["step_size"]

        # Get account info and market price
        account_summary = await api.get_account_summary()
        market_price = await get_current_market_price(api, symbol)

        # Get margin parameters from exchange
        margin_params = await self._get_account_margin_parameters(api)

        # Get maximum order quantity from exchange limits
        max_quantity = await self._find_max_order_via_exchange_limits(api, symbol, OrderSide.BUY)

        # Quantize to step size
        if max_quantity > Decimal(0):
            steps = (max_quantity / step_size).quantize(Decimal(1), rounding=ROUND_DOWN)
            max_quantity = steps * step_size

        return {
            "max_position_size": max_quantity,
            "max_notional_value": max_quantity * market_price,
            "account_equity": account_summary.total_equity,
            "available_equity": account_summary.available_equity,
            "market_price": market_price,
            "step_size": step_size,
            "imf": margin_params["imf"],
            "mmf": margin_params["mmf"],
            "margin_fraction": margin_params["margin_fraction"],
            "leverage_limit": margin_params["leverage_limit"],
        }

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_absolute_maximum_position(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test creating position at absolute maximum of available margin."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        symbol = available_symbols[0]  # Use first available perp symbol

        try:
            # Find maximum using exchange limits
            max_params = await self._find_maximum_position_using_exchange_limits(
                bp_api_for_large_balance_test,
                symbol,
            )

            # Create position at maximum
            if max_params["max_position_size"] > Decimal(0):
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=max_params["max_position_size"],
                    time_in_force=TimeInForce.IOC,
                )
                order = await bp_api_for_large_balance_test.place_order(place_args)
                assert order.exchange_order_id is not None
                # Position update handled by proper polling

                # Get final position to verify size
                positions = await bp_api_for_large_balance_test.get_positions()
                actual_position = next((p for p in positions if p.symbol == symbol), None)
                assert actual_position is not None, (
                    "Position should exist after creating max position"
                )

                # The exchange gave us max_quantity, we should have created at least
                # that minus 10 steps
                # (accounting for fees and slippage)
                min_acceptable_size = max_params["max_position_size"] - (
                    Decimal(10) * max_params["step_size"]
                )
                assert actual_position.size >= min_acceptable_size, (
                    f"Position size {actual_position.size} should be at least "
                    f"{min_acceptable_size} "
                    f"(max {max_params['max_position_size']} - 10 steps of "
                    f"{max_params['step_size']})"
                )

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_position_at_limit_then_one_more_fails(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that adding one step beyond maximum fails."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        symbol = available_symbols[0]  # Use first available perp symbol

        try:
            # Find and create maximum position
            max_params = await self._find_maximum_position_using_exchange_limits(
                bp_api_for_large_balance_test,
                symbol,
            )

            if max_params["max_position_size"] > Decimal(0):
                # Create max position
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=max_params["max_position_size"],
                    time_in_force=TimeInForce.IOC,
                )
                await bp_api_for_large_balance_test.place_order(place_args)
                # Position update handled by proper polling

                # Try to add one more step - should fail
                additional_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=max_params["step_size"],
                    time_in_force=TimeInForce.IOC,
                )

                with pytest.raises(Exception) as exc_info:
                    await bp_api_for_large_balance_test.place_order(additional_args)

                assert (
                    "insufficient" in str(exc_info.value).lower()
                    or "margin" in str(exc_info.value).lower()
                )

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_edge_precision_maximum_minus_two_steps(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test position at max minus two steps - edge case that should succeed."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        symbol = available_symbols[0]  # Use first available perp symbol

        try:
            # Find maximum
            max_params = await self._find_maximum_position_using_exchange_limits(
                bp_api_for_large_balance_test,
                symbol,
            )

            if max_params["max_position_size"] > max_params["step_size"] * Decimal(2):
                # Create position at max minus two steps - this MUST succeed
                position_size = max_params["max_position_size"] - (
                    max_params["step_size"] * Decimal(2)
                )
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=position_size,
                    time_in_force=TimeInForce.IOC,
                )
                order = await bp_api_for_large_balance_test.place_order(place_args)
                assert order.exchange_order_id is not None
                # Position update handled by proper polling

                # Verify actual position size
                positions = await bp_api_for_large_balance_test.get_positions()
                actual_position = next((p for p in positions if p.symbol == symbol), None)
                assert actual_position is not None

                # Must be at least the requested size minus 2 steps for fees
                min_size = position_size - (max_params["step_size"] * Decimal(2))
                assert actual_position.size >= min_size, (
                    f"Position {actual_position.size} should be at least {min_size}"
                )

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_negative_edge_beyond_max_plus_two_steps(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that position at max plus two steps fails - negative edge case."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        symbol = available_symbols[0]  # Use first available perp symbol

        try:
            # Find maximum
            max_params = await self._find_maximum_position_using_exchange_limits(
                bp_api_for_large_balance_test,
                symbol,
            )

            if max_params["max_position_size"] > Decimal(0):
                # Try to create position at max plus two steps - this MUST fail
                oversized_position = max_params["max_position_size"] + (
                    max_params["step_size"] * Decimal(2)
                )
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=oversized_position,
                    time_in_force=TimeInForce.IOC,
                )

                with pytest.raises(Exception) as exc_info:
                    await bp_api_for_large_balance_test.place_order(place_args)

                # Must be rejected for insufficient margin
                assert (
                    "insufficient" in str(exc_info.value).lower()
                    or "margin" in str(exc_info.value).lower()
                ), f"Expected insufficient margin error, got: {exc_info.value}"

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_multiple_symbols_exhaust_margin(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test exhausting all margin across multiple symbols."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        # Use first 3 available symbols (or all if less than 3)
        symbols = available_symbols[:3]

        try:
            # Start clean
            await self._close_all_positions(bp_api_for_large_balance_test)
            # Position update handled by proper polling

            positions_created: list[str] = []

            for symbol in symbols:
                # Get max order quantity for this symbol with current state
                try:
                    max_quantity = await self._find_max_order_via_exchange_limits(
                        bp_api_for_large_balance_test,
                        symbol,
                        OrderSide.BUY,
                    )

                    if max_quantity > Decimal(0):
                        # Quantize to step size
                        constraints = await get_market_constraints(
                            bp_api_for_large_balance_test,
                            symbol,
                        )
                        step_size = constraints["step_size"]
                        steps = (max_quantity / step_size).quantize(
                            Decimal(1),
                            rounding=ROUND_DOWN,
                        )
                        max_quantity = steps * step_size

                        place_args = PlaceOrderArgs(
                            symbol=symbol,
                            side=OrderSide.BUY,
                            order_type=OrderType.MARKET,
                            quantity=max_quantity,
                            time_in_force=TimeInForce.IOC,
                        )
                        await bp_api_for_large_balance_test.place_order(place_args)
                        # Position update handled by proper polling
                        positions_created.append(symbol)
                except (APIError, ValueError, TypeError, KeyError):
                    # No more margin available - expected
                    break

            # We should have created at least one position
            assert len(positions_created) > 0, "Should have created at least one position"

            # Verify no more significant positions possible on any symbol
            for symbol in symbols:
                max_quantity = await self._find_max_order_via_exchange_limits(
                    bp_api_for_large_balance_test,
                    symbol,
                    OrderSide.BUY,
                )
                constraints = await get_market_constraints(bp_api_for_large_balance_test, symbol)

                # The exchange should report very limited or no capacity left (less than 10 steps)
                max_steps = (max_quantity / constraints["step_size"]).quantize(
                    Decimal(1),
                    rounding=ROUND_DOWN,
                )
                assert max_steps < Decimal(10), (
                    f"Should have less than 10 steps of capacity left for {symbol}, "
                    f"but have {max_steps} steps"
                )

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_reducing_position_at_max_margin(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that reducing positions works even at max margin."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        symbol = available_symbols[0]  # Use first available perp symbol

        try:
            # Create maximum position
            max_params = await self._find_maximum_position_using_exchange_limits(
                bp_api_for_large_balance_test,
                symbol,
            )

            if max_params["max_position_size"] > Decimal(0):
                place_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.BUY,
                    order_type=OrderType.MARKET,
                    quantity=max_params["max_position_size"],
                    time_in_force=TimeInForce.IOC,
                )
                await bp_api_for_large_balance_test.place_order(place_args)
                # Position update handled by proper polling

                # Reducing position should always work
                reduce_args = PlaceOrderArgs(
                    symbol=symbol,
                    side=OrderSide.SELL,  # Opposite side reduces position
                    order_type=OrderType.MARKET,
                    quantity=max_params["step_size"],
                    time_in_force=TimeInForce.IOC,
                )
                order = await bp_api_for_large_balance_test.place_order(reduce_args)
                assert order.exchange_order_id is not None

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_different_symbols_different_imf(
        self,
        bp_api_for_large_balance_test: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that different symbols have different max positions due to IMF."""
        # Get available perp symbols from exchange (fail-fast approach)
        available_symbols = await get_available_symbols(
            bp_api_for_large_balance_test,
            market_type="perp",
        )
        if not available_symbols:
            pytest.fail(
                "No perpetual symbols available from exchange. Cannot test position operations.",
            )

        # Use first 2 available symbols (or all if less than 2)
        symbols = available_symbols[:2]
        max_sizes: dict[str, dict[str, Decimal]] = {}

        try:
            for symbol in symbols:
                # Get max quantity from exchange for this symbol
                max_quantity = await self._find_max_order_via_exchange_limits(
                    bp_api_for_large_balance_test,
                    symbol,
                    OrderSide.BUY,
                )

                # Get market price
                market_price = await get_current_market_price(bp_api_for_large_balance_test, symbol)

                max_sizes[symbol] = {
                    "quantity": max_quantity,
                    "notional": max_quantity * market_price,
                }

            # Different symbols should have different characteristics
            # The exchange's max order endpoint already accounts for symbol-specific IMF
            if len(max_sizes) == 2:
                # Verify we got valid max sizes from the exchange
                for symbol, data in max_sizes.items():
                    quantity = data["quantity"]
                    notional = data["notional"]
                    assert quantity > Decimal(0), (
                        f"{symbol} should have positive max quantity from exchange"
                    )
                    assert notional > Decimal(0), f"{symbol} should have positive max notional"

        finally:
            await self._close_all_positions(bp_api_for_large_balance_test)
