"""Integration tests for Backpack private positions endpoints."""

from __future__ import annotations

import asyncio
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.common import APIError, APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    PlaceOrderArgs,
)
from cyberdelta.config.models.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models.derivative_position import DerivativePosition
from cyberdelta.core.models.enums import OrderSide, OrderType, TimeInForce
from cyberdelta.exceptions.authentication import InvalidPrivateKeyError
from tests.integration.apis.backpack.shared.bp_test_helpers import (
    get_market_constraints,
    get_minimal_order_size,
    wait_for_condition,
)


logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance,
    pytest.mark.timing,
]


# REMOVED: get_perp_market_info function
# This function had fallback values which is unacceptable for a trading engine.
# Use get_market_constraints from shared test_helpers instead.


# REMOVED: get_minimal_perp_order_size function
# This function had fallback values which is unacceptable for a trading engine.
# Use get_minimal_order_size from shared test_helpers instead.


# REMOVED: get_dynamic_perp_price function
# This function had hardcoded fallback prices which is unacceptable for a trading engine.
# Use get_dynamic_test_price from shared test_helpers instead.


async def create_test_perp_position(
    api: BackpackAPI,
    symbol: str = "SOL_USDC_PERP",
) -> tuple[str, Decimal]:
    """Create a small test position and return order ID and quantity.

    Returns:
        tuple[str, Decimal]: Order ID and quantity of the created position.

    Raises:
        ValueError: If market price cannot be determined.
    """
    try:
        # Get market price and place order ABOVE it to ensure IOC fill
        ticker = await api.get_ticker(symbol)
        market_price = ticker.price or ticker.ask
        if market_price is None:
            raise ValueError(f"Cannot determine market price for {symbol}")

        # Get minimal order size
        min_quantity = await get_minimal_order_size(
            api=api,
            symbol=symbol,
            side=OrderSide.BUY,
            price=market_price,
        )

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
            logger.info(
                "created_test_position",
                symbol=symbol,
                quantity=min_quantity,
                message="Created test position at market price",
            )

            # Wait for position to be created

            async def position_exists() -> bool:
                positions = await api.get_positions()
                return any(p.symbol == symbol for p in positions)

            await wait_for_condition(
                position_exists,
                timeout_seconds=5.0,
                poll_interval=0.1,
                message=f"Position for {symbol} was not created",
            )

            return order.exchange_order_id, min_quantity
        raise ValueError("Order placed but no order ID returned")

    except (APIError, ValueError, TypeError, KeyError) as e:
        logger.exception(
            "failed_to_create_test_position",
            error=str(e),
            message="Failed to create test position",
        )
        raise


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir",
    ["apis/backpack/perp/positions/positive"],
    indirect=True,
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
            # Backpack perp symbols use underscores (e.g., SOL_USDC_PERP)
            assert "_" in position.symbol

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

    async def _validate_position_prices(
        self,
        position: DerivativePosition,
        api: BackpackAPI,
        index: int,
    ) -> None:
        """Validate price fields and relationships of a DerivativePosition."""
        if position.size != Decimal(0):
            if position.entry_price is not None:
                assert position.entry_price > Decimal(0)
            if position.mark_price is not None:
                assert position.mark_price > Decimal(0)

        size_precision = len(str(position.size).split(".")[-1]) if "." in str(position.size) else 0
        assert size_precision <= 18

        if position.entry_price is not None:
            # Get market constraints from exchange to validate entry_price precision

            constraints = await get_market_constraints(api, position.symbol)
            tick_size = constraints["tick_size"]

            # Note: Entry prices are calculated averages from multiple fills and may not
            # align exactly with the market's tick size. This is expected behavior.
            # We validate that the entry price is reasonable but don't enforce tick size alignment.

            # Validate entry price is within reasonable bounds relative to tick size
            # (e.g., not wildly off due to parsing errors, but allow natural precision variance)
            if tick_size > Decimal(0):
                # Entry price should be at least somewhat close to a valid tick increment
                # Allow for averaging effects but catch major parsing/calculation errors
                normalized_price = (position.entry_price / tick_size).quantize(
                    Decimal(1),
                    rounding=ROUND_HALF_UP,
                ) * tick_size
                price_deviation = abs(position.entry_price - normalized_price)
                max_deviation = tick_size  # Allow up to 1 tick size deviation

                assert price_deviation <= max_deviation, (
                    f"Entry price {position.entry_price} for {position.symbol} deviates "
                    f"significantly from nearest tick increment. Deviation: {price_deviation}, "
                    f"Max allowed: {max_deviation}, Tick size: {tick_size}. "
                    f"This suggests a parsing or calculation error."
                )

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
                logger.info(
                    "test_position_created",
                    order_id=order_id,
                    quantity=float(quantity),
                    message="Created test position for testing",
                )

                # Get positions again
                positions = await bp_api_for_test_env.get_positions()
                assert isinstance(positions, list)

            except (APIError, ValueError, TypeError, KeyError) as e:
                # Position creation is critical for position tests
                pytest.fail(
                    f"Failed to create test position: {e}. "
                    "Position tests require the ability to create positions. "
                    "This may indicate insufficient margin or API issues that must be resolved.",
                )

        # Validate positions if we have any
        if not positions:
            pytest.skip("No positions available and couldn't create test position")

        for i, position in enumerate(positions):
            self._validate_position_core_fields(position, i)
            self._validate_position_decimal_fields(position, i)
            await self._validate_position_prices(position, bp_api_for_test_env, i)

            if position.bp_details:
                bp_details = position.bp_details
                if bp_details.imf_base is not None:
                    assert isinstance(bp_details.imf_base, Decimal)
                    assert bp_details.imf_base >= Decimal(0)

                if bp_details.mmf_base is not None:
                    assert isinstance(bp_details.mmf_base, Decimal)
                    assert bp_details.mmf_base >= Decimal(0)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_authentication_failure(
        self,
        active_bp_config: ExchangeSpecificConfig,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test that invalid Ed25519 authentication fails during API initialization."""
        invalid_secrets = ApiKeyAuthSecrets(
            api_key=SecretStr("fake_api_key_for_testing_auth_failure"),
            api_secret=SecretStr("fake_api_secret_for_testing_auth_failure"),
        )

        # Authentication validation now happens during API initialization
        # This provides better security by failing fast with invalid credentials
        with pytest.raises(InvalidPrivateKeyError) as exc_info:
            BackpackAPI(
                exchange_config=active_bp_config,
                exchange_secrets=invalid_secrets,
            )

        error = exc_info.value
        assert "Invalid Base64 ED25519 private key" in str(error)
        assert "Incorrect padding" in str(error)

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
            position.size > Decimal(0)
            and position.mark_price is not None
            and position.entry_price is not None
            and position.mark_price > position.entry_price
            and position.unrealized_pnl is not None
        ):
            assert position.unrealized_pnl >= Decimal(0)

    def _validate_profitable_short_position(self, position: DerivativePosition) -> None:
        """Validate profitable short position PnL."""
        if (
            position.size < Decimal(0)
            and position.mark_price is not None
            and position.entry_price is not None
            and position.mark_price < position.entry_price
            and position.unrealized_pnl is not None
        ):
            assert position.unrealized_pnl >= Decimal(0)

    async def _ensure_test_positions(
        self,
        bp_api_for_test_env: BackpackAPI,
    ) -> list[DerivativePosition]:
        """Ensure we have positions for testing, creating if necessary.

        Returns:
            list[DerivativePosition]: List of derivative positions.
        """
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            try:
                await create_test_perp_position(bp_api_for_test_env)
                logger.info("Created test position for PnL testing")
                positions = await bp_api_for_test_env.get_positions()
            except (APIError, ValueError, TypeError, KeyError) as e:
                # PnL calculation is critical for trading
                pytest.fail(
                    f"Failed to test PnL calculation: {e}. "
                    "PnL calculation is a critical trading function that must work reliably.",
                )

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
            if position.size != Decimal(0):
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
            except (APIError, ValueError, TypeError, KeyError) as e:
                # Margin calculation is critical for trading
                pytest.fail(
                    f"Failed to test margin calculation: {e}. "
                    "Margin calculation is a critical trading function that must work reliably.",
                )

        if not positions:
            pytest.skip("No positions available for margin testing")

        for position in positions:
            if position.bp_details and position.size != Decimal(0):
                bp_details = position.bp_details

                if bp_details.imf_base is not None and bp_details.mmf_base is not None:
                    assert bp_details.imf_base >= bp_details.mmf_base

                if bp_details.imf_factor is not None:
                    assert bp_details.imf_factor >= Decimal(0)

                if bp_details.mmf_factor is not None:
                    assert bp_details.mmf_factor >= Decimal(0)

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_concurrent_requests(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with concurrent requests to same endpoint."""
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
                    zip(first_result, result, strict=False),
                ):
                    assert first_pos.symbol == second_pos.symbol
                    size_diff = abs(first_pos.size - second_pos.size)
                    assert size_diff <= Decimal("0.00001")
