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
from cyberdelta.config.config_models import ExchangeSpecificConfig
from cyberdelta.config.secrets_models import ApiKeyAuthSecrets
from cyberdelta.core.models.derivative_position import DerivativePosition

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.requires_balance,
    pytest.mark.positive_balance
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/private/positions"], indirect=True
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

        size_precision = (
            len(str(position.size).split(".")[-1])
            if "." in str(position.size)
            else 0
        )
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
        positions = await bp_api_for_test_env.get_positions()

        assert isinstance(positions, list)

        if not positions:
            pytest.skip("No positions available in test environment for validation")

        for i, position in enumerate(positions):
            self._validate_position_core_fields(position, i)
            self._validate_position_decimal_fields(position, i)
            self._validate_position_prices(position, i)

            if position.bp_details:
                bp_details = position.bp_details
                if bp_details.initial_margin is not None:
                    assert isinstance(bp_details.initial_margin, Decimal)
                    assert bp_details.initial_margin >= Decimal("0")

                if bp_details.maintenance_margin is not None:
                    assert isinstance(bp_details.maintenance_margin, Decimal)
                    assert bp_details.maintenance_margin >= Decimal("0")

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

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_pnl_calculations(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() PnL calculation validation."""
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for PnL testing")

        for position in positions:
            if position.size != Decimal("0"):
                assert position.unrealized_pnl.is_finite()
                assert position.realized_pnl.is_finite()

                total_pnl = position.unrealized_pnl + position.realized_pnl
                assert total_pnl.is_finite()

                if position.size > Decimal("0") and position.mark_price > position.entry_price:
                    assert position.unrealized_pnl >= Decimal("0")
                elif position.size < Decimal("0") and position.mark_price < position.entry_price:
                    assert position.unrealized_pnl >= Decimal("0")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_margin_validation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() margin requirement validation."""
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for margin testing")

        for position in positions:
            if position.bp_details and position.size != Decimal("0"):
                bp_details = position.bp_details

                if bp_details.initial_margin is not None and bp_details.maintenance_margin is not None:
                    assert bp_details.initial_margin >= bp_details.maintenance_margin

                if bp_details.initial_margin is not None:
                    assert bp_details.initial_margin > Decimal("0")

                if bp_details.maintenance_margin is not None:
                    assert bp_details.maintenance_margin >= Decimal("0")

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

                for j, (first_pos, second_pos) in enumerate(zip(first_result, result)):
                    assert first_pos.symbol == second_pos.symbol
                    size_diff = abs(first_pos.size - second_pos.size)
                    assert size_diff <= Decimal("0.00001")

    @pytest.mark.vcr
    @pytest.mark.asyncio
    async def test_get_positions_large_positions(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() handling of large position sizes."""
        positions = await bp_api_for_test_env.get_positions()

        if not positions:
            pytest.skip("No positions for large size testing")

        for position in positions:
            if abs(position.size) > Decimal("1000"):
                assert position.size.is_finite()
                assert position.entry_price > Decimal("0")
                assert position.mark_price > Decimal("0")

                notional_value = abs(position.size) * position.mark_price
                assert notional_value.is_finite()

                if position.bp_details and position.bp_details.initial_margin:
                    margin_ratio = position.bp_details.initial_margin / notional_value
                    assert margin_ratio > Decimal("0")
                    assert margin_ratio <= Decimal("1")