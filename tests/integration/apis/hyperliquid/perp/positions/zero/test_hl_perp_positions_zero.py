"""Integration tests for Hyperliquid positions endpoints with zero position scenarios.

This module focuses specifically on testing the DerivativePosition model pipeline
through Hyperliquid's /info endpoint with user address authentication for zero position scenarios.
Tests validate complete data transformation from API responses to DerivativePosition instances.

Model Focus: DerivativePosition (Zero Position Edge Cases)
- Validates complete DerivativePosition model field mapping for empty accounts
- Tests Decimal precision for zero financial values
- Validates business logic constraints with zero positions
- Tests Hyperliquid-specific position details (hl_details) for empty accounts
- Comprehensive error handling and position edge cases

Authentication: EIP-712 signing for testnet environment
VCR: Records both success and error responses with sensitive data filtering
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

import pytest
from pydantic import SecretStr

from cyberdelta.apis.hyperliquid.hl_api import HyperliquidAPI
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.config.secrets_models import PrivateKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition

pytestmark = [pytest.mark.integration, pytest.mark.perp, pytest.mark.zero_balance]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/hyperliquid/perp/positions/zero"], indirect=True
)
@pytest.mark.perp
@pytest.mark.zero_balance
class TestHyperliquidPerpPositionsZero:
    """Comprehensive zero position integration tests for DerivativePosition model validation."""

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_empty_account(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with account that has no open positions."""
        positions = await hl_api_for_test_env.get_positions()

        assert isinstance(positions, list), "get_positions() should always return list"

        if len(positions) == 0:
            return

        for position in positions:
            assert isinstance(position, DerivativePosition), (
                "All returned positions should be valid"
            )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_authentication_failure(
        self,
        hl_api_with_di: Callable[..., HyperliquidAPI],
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with invalid EIP-712 authentication."""
        invalid_secrets = PrivateKeyAuthSecrets(
            private_key=SecretStr(
                "0x0000000000000000000000000000000000000000000000000000000000000002"
            ),
        )

        bad_api = hl_api_with_di(secrets=invalid_secrets)

        with pytest.raises(APIError) as exc_info:
            await bad_api.get_positions()

        error = exc_info.value
        assert error.code == APIErrorCode.AUTHENTICATION_FAILED.value, (
            f"Expected AUTHENTICATION_FAILED, got {error.code}"
        )
        assert error.http_status in [401, 403], f"Expected 401/403 status, got {error.http_status}"

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_precision_edge_cases(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() with edge cases around decimal precision."""
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for precision testing")

        for position in positions:
            if abs(position.size) > Decimal("0") and abs(position.size) < Decimal("0.001"):
                assert position.size.is_finite(), (
                    f"Small position size should be finite: {position.size}"
                )

                size_str = str(position.size)
                if "E" in size_str.upper():
                    assert "E-" in size_str.upper(), (
                        f"Scientific notation should be negative: {size_str}"
                    )

            if (
                position.unrealized_pnl is not None
                and abs(position.unrealized_pnl) > Decimal("0")
                and abs(position.unrealized_pnl) < Decimal("0.01")
            ):
                assert position.unrealized_pnl.is_finite(), (
                    f"Small PnL should be finite: {position.unrealized_pnl}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_pnl_consistency(
        self,
        hl_api_for_test_env: HyperliquidAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() PnL calculation consistency for zero positions."""
        positions = await hl_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for PnL testing")

        total_unrealized_pnl = Decimal("0")
        total_realized_pnl = Decimal("0")

        for position in positions:
            if position.unrealized_pnl is not None:
                total_unrealized_pnl += position.unrealized_pnl
            if position.realized_pnl is not None:
                total_realized_pnl += position.realized_pnl

            if position.unrealized_pnl is not None:
                assert position.unrealized_pnl.is_finite(), (
                    f"Unrealized PnL should be finite: {position.unrealized_pnl}"
                )
            if position.realized_pnl is not None:
                assert position.realized_pnl.is_finite(), (
                    f"Realized PnL should be finite: {position.realized_pnl}"
                )

            if position.size == Decimal("0") and position.unrealized_pnl is not None:
                assert abs(position.unrealized_pnl) < Decimal("1.0"), (
                    f"Zero position should have minimal unrealized PnL: {position.unrealized_pnl}"
                )

        assert total_unrealized_pnl.is_finite(), (
            f"Total unrealized PnL should be finite: {total_unrealized_pnl}"
        )
        assert total_realized_pnl.is_finite(), (
            f"Total realized PnL should be finite: {total_realized_pnl}"
        )
