"""Cross-exchange derivative position consistency tests."""

from __future__ import annotations

from decimal import Decimal

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from tests.integration.apis.shared.validation_helpers import assert_valid_derivative_position

# Mark all tests in this file
pytestmark = [pytest.mark.integration, pytest.mark.cross_exchange, pytest.mark.perp]


class TestCrossExchangeDerivativePositionConsistency:
    """Cross-exchange derivative position consistency tests."""

    @pytest.mark.asyncio
    async def test_position_model_consistency(
        self, exchange_client: BackpackAPI | HyperliquidAPI
    ) -> None:
        """Validate DerivativePosition model across exchanges."""
        try:
            positions = await exchange_client.get_positions()

            for position in positions:
                assert_valid_derivative_position(position)

                # Cross-exchange validation
                assert position.exchange in ["backpack", "hyperliquid"]
                assert isinstance(position.symbol, str)
                assert len(position.symbol) > 0

                # Size validation
                assert isinstance(position.size, Decimal)
                assert position.size.is_finite()

                # Price validation
                if position.entry_price is not None:
                    assert isinstance(position.entry_price, Decimal)
                    assert position.entry_price > Decimal("0")

                if position.mark_price is not None:
                    assert isinstance(position.mark_price, Decimal)
                    assert position.mark_price > Decimal("0")

        except NotImplementedError:
            pytest.skip(f"get_positions not implemented for {exchange_client.exchange}")

    @pytest.mark.parametrize("precision", [8, 10, 12])
    @pytest.mark.asyncio
    async def test_position_precision_handling(
        self, exchange_client: BackpackAPI | HyperliquidAPI, precision: int
    ) -> None:
        """Test decimal precision handling across exchanges."""
        try:
            positions = await exchange_client.get_positions()

            for position in positions:
                # Test size precision
                size_str = str(position.size)
                if "." in size_str:
                    decimal_places = len(size_str.split(".")[1].rstrip("0"))
                    assert decimal_places <= precision

                # Test price precision
                if position.entry_price:
                    price_str = str(position.entry_price)
                    if "." in price_str:
                        decimal_places = len(price_str.split(".")[1].rstrip("0"))
                        assert decimal_places <= precision

        except NotImplementedError:
            pytest.skip(f"get_positions not implemented for {exchange_client.exchange}")
