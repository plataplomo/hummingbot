"""Integration tests for Backpack large position handling.

This module contains tests specifically for validating the handling of large
derivative positions, including precision, margin calculations, and notional
value computations for positions exceeding 1000 units.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import pytest

from cyberdelta.apis.backpack.bp_api import BackpackAPI
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models.derivative_position import DerivativePosition

logger = get_logger(__name__)

# Mark all tests in this file
pytestmark = [
    pytest.mark.integration,
    pytest.mark.perp,
    pytest.mark.large_positions,
    pytest.mark.requires_large_balance,
]


@pytest.mark.parametrize(
    "custom_vcr_cassette_dir", ["apis/backpack/perp/positions/large"], indirect=True
)
class TestBackpackPerpLargePositions:
    """Test suite for validating large position handling in Backpack perpetuals.
    
    These tests validate that the system correctly handles positions with sizes
    exceeding 1000 units, ensuring proper precision, finite calculations, and
    valid margin ratios for large notional values.
    """

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires account with large positions - to be enabled for specific test environments")
    async def test_bp_position_large_size_handling(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test get_positions() handling of large position sizes.
        
        This test validates:
        1. Large position sizes (> 1000) are represented as finite Decimal values
        2. Entry and mark prices remain valid for large positions
        3. Notional value calculations don't overflow
        4. Margin ratios remain within valid bounds (0 < ratio <= 1)
        
        Note: This test requires an account with existing large positions,
        as creating them programmatically would require significant capital.
        """
        positions = await bp_api_for_test_env.get_positions()
        
        large_positions = [p for p in positions if abs(p.size) > Decimal("1000")]
        
        if not large_positions:
            pytest.skip("No large positions (> 1000 units) found in account")
        
        for position in large_positions:
            # Validate size is finite (no overflow/infinity)
            assert position.size.is_finite(), (
                f"Position size {position.size} is not finite for {position.symbol}"
            )
            
            # Validate prices are positive if present
            if position.entry_price is not None:
                assert position.entry_price > Decimal("0"), (
                    f"Entry price {position.entry_price} must be positive for {position.symbol}"
                )
                assert position.entry_price.is_finite(), (
                    f"Entry price {position.entry_price} is not finite for {position.symbol}"
                )
            
            if position.mark_price is not None:
                assert position.mark_price > Decimal("0"), (
                    f"Mark price {position.mark_price} must be positive for {position.symbol}"
                )
                assert position.mark_price.is_finite(), (
                    f"Mark price {position.mark_price} is not finite for {position.symbol}"
                )
            
            # Validate notional value calculation
            if position.mark_price is not None:
                notional_value = abs(position.size) * position.mark_price
                assert notional_value.is_finite(), (
                    f"Notional value {notional_value} is not finite for {position.symbol} "
                    f"(size: {position.size}, mark: {position.mark_price})"
                )
                
                logger.info(
                    f"Large position {position.symbol}: "
                    f"size={position.size}, mark_price={position.mark_price}, "
                    f"notional={notional_value}"
                )
                
                # Validate margin calculations if available
                if position.bp_details and position.bp_details.imf_base:
                    margin_ratio = position.bp_details.imf_base / notional_value
                    assert margin_ratio > Decimal("0"), (
                        f"Margin ratio {margin_ratio} must be positive for {position.symbol}"
                    )
                    assert margin_ratio <= Decimal("1"), (
                        f"Margin ratio {margin_ratio} exceeds 100% for {position.symbol}"
                    )
                    assert margin_ratio.is_finite(), (
                        f"Margin ratio {margin_ratio} is not finite for {position.symbol}"
                    )
                    
                    logger.info(
                        f"  Margin: imf_base={position.bp_details.imf_base}, "
                        f"ratio={margin_ratio:.4%}"
                    )

    @pytest.mark.vcr
    @pytest.mark.asyncio
    @pytest.mark.skip(reason="Requires account with extremely large positions")
    async def test_bp_position_extreme_size_precision(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test precision handling for extremely large positions (> 100,000 units).
        
        This test validates that the system maintains proper decimal precision
        even for positions with extreme sizes that could challenge floating-point
        arithmetic.
        """
        positions = await bp_api_for_test_env.get_positions()
        
        extreme_positions = [p for p in positions if abs(p.size) > Decimal("100000")]
        
        if not extreme_positions:
            pytest.skip("No extreme positions (> 100,000 units) found in account")
        
        for position in extreme_positions:
            # Check that string representation doesn't use scientific notation
            size_str = str(position.size)
            assert 'E' not in size_str.upper(), (
                f"Position size {size_str} uses scientific notation for {position.symbol}"
            )
            
            # Validate decimal places are reasonable
            if '.' in size_str:
                decimal_places = len(size_str.split('.')[-1])
                assert decimal_places <= 8, (
                    f"Excessive decimal places ({decimal_places}) in size for {position.symbol}"
                )
            
            # Validate PnL calculations remain precise
            if position.unrealized_pnl is not None:
                assert position.unrealized_pnl.is_finite()
                pnl_str = str(position.unrealized_pnl)
                assert 'E' not in pnl_str.upper(), (
                    f"PnL {pnl_str} uses scientific notation for {position.symbol}"
                )

    @pytest.mark.vcr
    @pytest.mark.asyncio 
    @pytest.mark.skip(reason="Requires account with large positions across multiple symbols")
    async def test_bp_multiple_large_positions_aggregation(
        self,
        bp_api_for_test_env: BackpackAPI,
        custom_vcr_config: dict[str, Any],
    ) -> None:
        """Test aggregation of multiple large positions.
        
        This test validates that when an account has multiple large positions,
        the total notional value and margin calculations remain accurate.
        """
        positions = await bp_api_for_test_env.get_positions()
        
        large_positions = [p for p in positions if abs(p.size) > Decimal("1000")]
        
        if len(large_positions) < 2:
            pytest.skip("Need at least 2 large positions for aggregation testing")
        
        total_notional = Decimal("0")
        total_margin_required = Decimal("0")
        
        for position in large_positions:
            if position.mark_price is not None:
                notional = abs(position.size) * position.mark_price
                total_notional += notional
                
                if position.bp_details and position.bp_details.imf_base:
                    total_margin_required += position.bp_details.imf_base
        
        # Validate aggregated values
        assert total_notional.is_finite(), "Total notional value is not finite"
        assert total_notional > Decimal("0"), "Total notional must be positive"
        
        if total_margin_required > Decimal("0"):
            assert total_margin_required.is_finite(), "Total margin required is not finite"
            
            # Overall margin ratio should still be reasonable
            overall_margin_ratio = total_margin_required / total_notional
            assert overall_margin_ratio > Decimal("0"), "Overall margin ratio must be positive"
            assert overall_margin_ratio <= Decimal("1"), "Overall margin ratio exceeds 100%"
            
            logger.info(
                f"Large positions aggregation: "
                f"count={len(large_positions)}, "
                f"total_notional={total_notional}, "
                f"total_margin={total_margin_required}, "
                f"overall_ratio={overall_margin_ratio:.4%}"
            )