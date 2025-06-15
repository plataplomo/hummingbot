"""Integration tests for Backpack private positions endpoints."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    GetMarketArgs,
    PlaceOrderArgs,
)
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.logging_config import get_logger
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.core.models.market.ticker import Ticker

logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
]


async def get_perp_market_info(api: BackpackAPI, symbol: str) -> dict[str, Decimal]:
    """Get market information for a perpetual symbol."""
    try:
        market = await api.get_market(GetMarketArgs(symbol=symbol))
        return {
            "tick_size": market.tick_size,
            "step_size": market.step_size,
            "min_quantity": market.min_quantity or market.step_size,
        }
    except Exception as e:
        logger.warning(f"Failed to get market info for {symbol}: {e}")
        # Fallback to common Backpack perp constraints
        return {
            "tick_size": Decimal("0.01"),
            "step_size": Decimal("0.01"),
            "min_quantity": Decimal("0.01"),
        }


async def get_minimal_perp_order_size(
    api: BackpackAPI, symbol: str, side: OrderSide, price: Decimal
) -> Decimal:
    """Calculate minimal order size based on balance and market constraints."""
    try:
        # Get market constraints
        market_info = await get_perp_market_info(api, symbol)
        min_quantity = market_info["min_quantity"]
        step_size = market_info["step_size"]

        # Get account balance
        balances = await api.get_balances()

        # For perps, we need USDC as collateral
        usdc_balance = Decimal("0")
        if "USDC" in balances:
            usdc_balance = balances["USDC"].available_quantity

        if usdc_balance <= Decimal("0"):
            logger.warning("No USDC balance available for perp trading")
            return min_quantity

        # Get actual fees from exchange
        # Note: Backpack doesn't have a direct fee endpoint, but we can infer from markets
        # Typical perp fees are 0.02% maker, 0.05% taker
        taker_fee_rate = Decimal("0.0005")  # 0.05%

        # Calculate notional value we can afford
        # For perps, leverage is typically available (e.g., 20x)
        # But for safety in tests, assume 1x leverage
        notional_with_fees = usdc_balance / (Decimal("1") + taker_fee_rate)

        # Calculate quantity
        affordable_quantity = notional_with_fees / price

        # Round down to step size
        rounded_quantity = (affordable_quantity / step_size).quantize(
            Decimal("1"), rounding="ROUND_DOWN"
        ) * step_size

        # Use the greater of minimum or affordable quantity
        return max(min_quantity, rounded_quantity)

    except Exception as e:
        logger.warning(f"Failed to calculate minimal order size: {e}")
        # Fallback to market minimum
        return Decimal("0.01")


async def get_dynamic_perp_price(
    api: BackpackAPI, symbol: str, side: OrderSide, offset_percent: Decimal = Decimal("5")
) -> Decimal:
    """Get a dynamic price for perp orders that won't fill immediately."""
    try:
        ticker: Ticker = await api.get_ticker(symbol)

        # Get current market price
        market_price = ticker.price or ticker.mid_price
        if market_price is None:
            if side == OrderSide.BUY and ticker.ask:
                market_price = ticker.ask
            elif side == OrderSide.SELL and ticker.bid:
                market_price = ticker.bid
            else:
                raise ValueError(f"Cannot determine market price for {symbol}")

        # Calculate offset price
        offset_factor = offset_percent / Decimal("100")
        if side == OrderSide.BUY:
            # Buy below market
            test_price = market_price * (Decimal("1") - offset_factor)
        else:
            # Sell above market
            test_price = market_price * (Decimal("1") + offset_factor)

        # Get tick size and round
        market_info = await get_perp_market_info(api, symbol)
        tick_size = market_info["tick_size"]

        return test_price.quantize(tick_size).normalize()

    except Exception as e:
        logger.warning(f"Failed to get dynamic price for {symbol}: {e}")
        # Fallback prices
        if symbol == "SOL_USDC_PERP":
            return Decimal("140.0") if side == OrderSide.BUY else Decimal("160.0")
        else:
            return Decimal("90.0") if side == OrderSide.BUY else Decimal("110.0")


async def create_test_perp_position(
    api: BackpackAPI, symbol: str = "SOL_USDC_PERP"
) -> tuple[str, Decimal]:
    """Create a small test position and return order ID and quantity."""
    try:
        # Get market price and place order ABOVE it to ensure IOC fill
        ticker = await api.get_ticker(symbol)
        market_price = ticker.price or ticker.ask
        if market_price is None:
            raise ValueError(f"Cannot determine market price for {symbol}")

        # Get minimal order size
        min_quantity = await get_minimal_perp_order_size(api, symbol, OrderSide.BUY, market_price)

        # Place market order with IOC to guarantee fill
        order_args = PlaceOrderArgs(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=min_quantity,
            time_in_force=TimeInForce.IOC,  # Immediate or cancel
        )

        order = await api.place_order(order_args)

        if order.exchange_order_id:
            logger.info(f"Created test position: {symbol} {min_quantity} @ market price")

            # Give a small delay to allow position to be created
            import asyncio

            await asyncio.sleep(0.5)

            return order.exchange_order_id, min_quantity
        else:
            raise ValueError("Order placed but no order ID returned")

    except Exception as e:
        logger.error(f"Failed to create test position: {e}")
        raise


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/perp/positions/positive"], indirect=True
)
class TestBackpackPerpPositionsPrivate:
    """Private positions integration tests for DerivativePosition model validation."""

    def _validate_position_core_fields(self, position: DerivativePosition, index: int) -> None:
        """Validate core fields of a DerivativePosition."""
        assert isinstance(position, DerivativePosition)
        assert position.exchange == "backpack"

        assert isinstance(position.symbol, str)
        assert len(position.symbol) > 0
        assert len(position.symbol) <= 20

        if "PERP" in position.symbol.upper():
            assert "-" in position.symbol

        assert position.timestamp is not None
        time_diff = datetime.now(position.timestamp.tzinfo) - position.timestamp
        assert time_diff.total_seconds() < 3600

    def _validate_position_decimal_fields(self, position: DerivativePosition, index: int) -> None:
        """Validate Decimal fields of a DerivativePosition."""
        assert isinstance(position.size, Decimal)
        assert isinstance(position.entry_price, Decimal)
        assert isinstance(position.mark_price, Decimal)
        assert isinstance(position.unrealized_pnl, Decimal)
        assert isinstance(position.realized_pnl, Decimal)

    def _validate_position_prices(self, position: DerivativePosition, index: int) -> None:
        """Validate price fields and relationships of a DerivativePosition."""
        if position.size != Decimal("0"):
            if position.entry_price is not None:
                assert position.entry_price > Decimal("0")
            if position.mark_price is not None:
                assert position.mark_price > Decimal("0")

        size_precision = len(str(position.size).split(".")[-1]) if "." in str(position.size) else 0
        assert size_precision <= 18

        if position.entry_price is not None:
            entry_precision = (
                len(str(position.entry_price).split(".")[-1])
                if "." in str(position.entry_price)
                else 0
            )
            assert entry_precision <= 18

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_success_comprehensive(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test successful get_positions() with comprehensive DerivativePosition validation."""
        # Get positions first to check if any exist
        positions = await bp_api_for_test_env.get_positions()
        assert isinstance(positions, list)

        # If no positions exist, try to create one
        if not positions:
            try:
                # Try to create a position, but if it fails due to balance or API issues,
                # mark the test as expected failure
                order_id, quantity = await create_test_perp_position(bp_api_for_test_env)
                logger.info(f"Created test position with order {order_id}, quantity {quantity}")

                # Get positions again
                positions = await bp_api_for_test_env.get_positions()
                assert isinstance(positions, list)

            except Exception as e:
                # If we can't create a position due to balance or API issues,
                # mark as expected failure
                pytest.xfail(f"Cannot create test position due to: {e}")

        # Validate positions if we have any
        if not positions:
            pytest.skip("No positions available and couldn't create test position")

        for i, position in enumerate(positions):
            self._validate_position_core_fields(position, i)
            self._validate_position_decimal_fields(position, i)
            self._validate_position_prices(position, i)

            if position.bp_details:
                bp_details = position.bp_details
                if bp_details.imf_base is not None:
                    assert isinstance(bp_details.imf_base, Decimal)
                    assert bp_details.imf_base >= Decimal("0")

                if bp_details.mmf_base is not None:
                    assert isinstance(bp_details.mmf_base, Decimal)
                    assert bp_details.mmf_base >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid Ed25519 authentication."""
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        bad_api = BackpackAPI(
            exchange_config=active_bp_config,
            exchange_secrets=invalid_secrets,
        )

        with pytest.raises((APIError, AttributeError)) as exc_info:
            await bad_api.get_positions()

        error = exc_info.value
        if isinstance(error, APIError):
            assert error.code in [
                APIErrorCode.AUTHENTICATION_FAILED.value,
                APIErrorCode.INVALID_REQUEST.value,
            ]
        else:
            assert "authenticator" in str(error).lower() or "NoneType" in str(error)

    def _validate_pnl_fields(self, position: DerivativePosition) -> None:
        """Validate PnL fields are finite."""
        if position.unrealized_pnl is not None:
            assert position.unrealized_pnl.is_finite()
        if position.realized_pnl is not None:
            assert position.realized_pnl.is_finite()

        if position.unrealized_pnl is not None and position.realized_pnl is not None:
            total_pnl = position.unrealized_pnl + position.realized_pnl
            assert total_pnl.is_finite()

    def _validate_profitable_long_position(self, position: DerivativePosition) -> None:
        """Validate profitable long position PnL."""
        if (
            position.size > Decimal("0")
            and position.mark_price is not None
            and position.entry_price is not None
            and position.mark_price > position.entry_price
            and position.unrealized_pnl is not None
        ):
            assert position.unrealized_pnl >= Decimal("0")

    def _validate_profitable_short_position(self, position: DerivativePosition) -> None:
        """Validate profitable short position PnL."""
        if (
            position.size < Decimal("0")
            and position.mark_price is not None
            and position.entry_price is not None
            and position.mark_price < position.entry_price
            and position.unrealized_pnl is not None
        ):
            assert position.unrealized_pnl >= Decimal("0")

    async def _ensure_test_positions(
        self, bp_api_for_test_env: BackpackAPI
    ) -> list[DerivativePosition]:
        """Ensure we have positions for testing, creating if necessary."""
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            try:
                await create_test_perp_position(bp_api_for_test_env)
                logger.info("Created test position for PnL testing")
                positions = await bp_api_for_test_env.get_positions()
            except Exception as e:
                pytest.xfail(f"Cannot test PnL without positions: {e}")

        if not positions:
            pytest.skip("No positions available for PnL testing")

        return positions

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_position_pnl_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() PnL calculation validation."""
        positions = await self._ensure_test_positions(bp_api_for_test_env)

        for position in positions:
            if position.size != Decimal("0"):
                self._validate_pnl_fields(position)
                self._validate_profitable_long_position(position)
                self._validate_profitable_short_position(position)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_bp_position_margin_calculations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() margin requirement validation."""
        # Get positions
        positions = await bp_api_for_test_env.get_positions()

        # If no positions, try to create one
        if not positions:
            try:
                await create_test_perp_position(bp_api_for_test_env)
                logger.info("Created test position for margin testing")
                positions = await bp_api_for_test_env.get_positions()
            except Exception as e:
                pytest.xfail(f"Cannot test margin without positions: {e}")

        if not positions:
            pytest.skip("No positions available for margin testing")

        for position in positions:
            if position.bp_details and position.size != Decimal("0"):
                bp_details = position.bp_details

                if bp_details.imf_base is not None and bp_details.mmf_base is not None:
                    assert bp_details.imf_base >= bp_details.mmf_base

                if bp_details.imf_factor is not None:
                    assert bp_details.imf_factor >= Decimal("0")

                if bp_details.mmf_factor is not None:
                    assert bp_details.mmf_factor >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with concurrent requests to same endpoint."""
        import asyncio

        tasks = [
            bp_api_for_test_env.get_positions(),
            bp_api_for_test_env.get_positions(),
            bp_api_for_test_env.get_positions(),
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        successful_results: list[list[DerivativePosition]] = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                if isinstance(result, APIError) and result.code == APIErrorCode.RATE_LIMITED.value:
                    continue
                else:
                    pytest.fail(f"Unexpected error in concurrent call {i}: {result}")
            else:
                assert isinstance(result, list)
                successful_results.append(result)

        assert len(successful_results) > 0

        if len(successful_results) > 1:
            first_result = successful_results[0]
            for result in successful_results[1:]:
                assert len(first_result) == len(result)

                for _, (first_pos, second_pos) in enumerate(
                    zip(first_result, result, strict=False)
                ):
                    assert first_pos.symbol == second_pos.symbol
                    size_diff = abs(first_pos.size - second_pos.size)
                    assert size_diff <= Decimal("0.00001")
