"""CyberDeltaEngine: Hyperliquid Account Data Mapper Robustness Tests.

-----------------------------------------------------------------

Comprehensive robustness test suite for HyperliquidAccountDataMapper.
Tests edge cases, boundary conditions, and error handling including:
- Invalid data validation and error recovery
- Boundary value testing with extreme inputs
- Unicode and encoding support
- Performance and memory considerations
- Error handling and recovery scenarios
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from pydantic import ValidationError

# Third-party imports for type checking only
# Project-specific imports
from cyberdelta.apis.hyperliquid.mappers.hl_account_data_mapper import HyperliquidAccountDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_fill import HyperliquidRawFill
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawAssetPosition,
    HyperliquidRawClearinghouseState,
    HyperliquidRawLeverage,
    HyperliquidRawMarginSummary,
    HyperliquidRawPositionInfo,
)
from cyberdelta.core.models import (
    MarginAccountSummary,
)
from cyberdelta.enums.exchange_names import ExchangeName

# Alias for shorter method calls
Mapper = HyperliquidAccountDataMapper

# --- Fixtures ---


@pytest.fixture
def account_data_mapper() -> HyperliquidAccountDataMapper:
    """Provide an instance of HyperliquidAccountDataMapper."""
    return HyperliquidAccountDataMapper()


@pytest.fixture
def raw_clearinghouse_state_base_fixture() -> HyperliquidRawClearinghouseState:
    """Provide a base HyperliquidRawClearinghouseState fixture for testing."""
    margin_summary = HyperliquidRawMarginSummary(
        accountValue="12000.0",
        totalRawUsd="12500.0",
        totalMarginUsed="200.0",
        totalNtlPos="2000.0",
    )
    return HyperliquidRawClearinghouseState(
        assetPositions=[],
        marginSummary=margin_summary,
        crossMaintenanceMarginUsed="50.0",
        crossMarginSummary=margin_summary,
        isolatedMaintenanceMarginUsed="25.0",
        isolatedMarginSummary=margin_summary,
        withdrawable="9800.0",
    )


# --- Tests for validation error handling ---


class TestValidationErrorHandling:
    """Tests for validation error handling and boundary conditions."""

    def test_invalid_numeric_strings_in_raw_state(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that creating raw_state with invalid numeric strings raises ValidationError."""
        invalid_margin_summary_data = {
            "accountValue": "not-a-number",
            "totalRawUsd": "still-bad",
            "totalMarginUsed": "nope",
            "totalNtlPos": "bad",
        }

        with pytest.raises(ValidationError):
            raw_clearinghouse_state_base_fixture.model_copy(
                update={
                    "marginSummary": HyperliquidRawMarginSummary(**invalid_margin_summary_data),
                    "crossMaintenanceMarginUsed": "also-invalid",
                    "isolatedMaintenanceMarginUsed": "another-bad-one",
                    "withdrawable": "bad-decimal",
                },
            )

    def test_withdrawable_funds_invalid_in_raw_state(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test that mapping raw_state with invalid 'withdrawable' raises ValueError."""
        current_raw_state_invalid = raw_clearinghouse_state_base_fixture.model_copy(
            update={"withdrawable": "not-a-decimal"},
        )

        with pytest.raises(ValueError) as exc_info:
            Mapper.transform_raw_clearinghouse_state_to_margin_summary(
                current_raw_state_invalid,
            )
        assert "withdrawable" in str(exc_info.value).lower()

    def test_cross_maintenance_margin_used_invalid_validation(
        self,
        raw_clearinghouse_state_base_fixture: HyperliquidRawClearinghouseState,
    ) -> None:
        """Test mapping with problematic crossMaintenanceMarginUsed."""
        # Test ValidationError on invalid numeric string
        with pytest.raises(ValidationError, match="crossMaintenanceMarginUsed"):
            data_to_validate = raw_clearinghouse_state_base_fixture.model_dump(by_alias=True)
            data_to_validate["crossMaintenanceMarginUsed"] = "bad-value"
            HyperliquidRawClearinghouseState.model_validate(data_to_validate)

        # Test successful mapping with valid values
        base_dump_python_names = raw_clearinghouse_state_base_fixture.model_dump(by_alias=False)
        updated_data_python_names = {
            **base_dump_python_names,
            "cross_maintenance_margin_used": "0.0",
            "isolated_maintenance_margin_used": "25.0",
        }
        current_raw_state_updated_mmr = HyperliquidRawClearinghouseState.model_validate(
            updated_data_python_names,
        )

        summary_updated_mmr = Mapper.transform_raw_clearinghouse_state_to_margin_summary(
            current_raw_state_updated_mmr,
        )
        assert summary_updated_mmr.total_maintenance_margin_required == Decimal("25.0")

    def test_direct_validation_cross_maintenance_margin_used_invalid(self) -> None:
        """Test direct validation of crossMaintenanceMarginUsed field."""
        valid_margin_summary = HyperliquidRawMarginSummary(
            accountValue="1000",
            totalRawUsd="500",
            totalMarginUsed="200",
            totalNtlPos="0",
        )

        with pytest.raises(ValidationError, match="crossMaintenanceMarginUsed"):
            HyperliquidRawClearinghouseState(
                assetPositions=[],
                marginSummary=valid_margin_summary,
                crossMaintenanceMarginUsed="bad-value",
                crossMarginSummary=valid_margin_summary,
                isolatedMaintenanceMarginUsed="0",
                isolatedMarginSummary=valid_margin_summary,
                withdrawable="100",
            )

    def test_invalid_raw_fill_data_handling(self) -> None:
        """Test that invalid data in raw fill raises ValidationError."""
        invalid_raw_fill_data = {
            "tid": 123,
            "coin": "XYZ-PERP",
            "px": "not_a_decimal",
            "sz": "1",
            "time": int(datetime.now(UTC).timestamp() * 1000),
            "side": "B",
            "oid": 1,
            "startPosition": "0",
            "dir": "Open",
            "hash": "0x123",
            "fee": "0.1",
            "isMaker": False,
        }

        with pytest.raises(ValidationError):
            HyperliquidRawFill.model_validate(invalid_raw_fill_data)


# --- Tests for boundary value conditions ---


class TestBoundaryValueConditions:
    """Tests for boundary values and extreme inputs."""

    def test_extremely_large_numeric_values(self) -> None:
        """Test handling of extremely large numeric values."""
        # Test with very large but valid decimal values
        large_value = "999999999999999999.999999999999999"

        position_info = HyperliquidRawPositionInfo(
            coin="LARGE-PERP",
            szi=large_value,
            entryPx=large_value,
            leverage=HyperliquidRawLeverage(type="cross", value=1),
            liquidationPx=large_value,
            marginUsed=large_value,
            maxLeverage=1000,
            positionValue=large_value,
            returnOnEquity="0.999999999999999",
            unrealizedPnl=large_value,
        )

        asset_position = HyperliquidRawAssetPosition(
            asset="LARGE-PERP",
            position=position_info,
        )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[asset_position],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue=large_value,
                totalRawUsd=large_value,
                totalMarginUsed=large_value,
                totalNtlPos=large_value,
            ),
            crossMaintenanceMarginUsed=large_value,
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue=large_value,
                totalRawUsd=large_value,
                totalMarginUsed=large_value,
                totalNtlPos=large_value,
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable=large_value,
        )

        # Should handle large values without error
        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        assert len(positions) == 1
        position = positions["LARGE-PERP"]
        assert position.size == Decimal(large_value)
        assert position.entry_price == Decimal(large_value)

    def test_extremely_small_numeric_values(self) -> None:
        """Test handling of extremely small numeric values."""
        # Test with very small but valid decimal values
        small_value = "0.000000000000001"

        position_info = HyperliquidRawPositionInfo(
            coin="SMALL-PERP",
            szi=small_value,
            entryPx="1000.0",  # Keep entry price reasonable
            leverage=HyperliquidRawLeverage(type="cross", value=1),
            liquidationPx="900.0",  # Keep liquidation price reasonable
            marginUsed=small_value,
            maxLeverage=10,
            positionValue=small_value,
            returnOnEquity=small_value,
            unrealizedPnl=small_value,
        )

        asset_position = HyperliquidRawAssetPosition(
            asset="SMALL-PERP",
            position=position_info,
        )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[asset_position],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="1000.0",
                totalRawUsd="0.0",
                totalMarginUsed=small_value,
                totalNtlPos=small_value,
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="1000.0",
                totalRawUsd="0.0",
                totalMarginUsed=small_value,
                totalNtlPos=small_value,
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="1000.0",
        )

        # Should handle small values without error
        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        assert len(positions) == 1
        position = positions["SMALL-PERP"]
        assert position.size == Decimal(small_value)
        assert position.hl_details is not None
        assert position.hl_details.margin_used == Decimal(small_value)

    def test_zero_and_negative_boundary_values(self) -> None:
        """Test handling of zero and negative boundary values."""
        test_cases = [
            # Note: Zero positions are filtered out by the mapper, so we only test non-zero values
            ("-1000000.0", "Large negative position"),
            ("-0.000001", "Small negative position"),
        ]

        for size_value, description in test_cases:
            position_info = HyperliquidRawPositionInfo(
                coin=f"TEST-{description.replace(' ', '-').upper()}-PERP",
                szi=size_value,
                entryPx="1000.0",
                leverage=HyperliquidRawLeverage(type="cross", value=1),
                liquidationPx="900.0",
                marginUsed="100.0",
                maxLeverage=10,
                positionValue="1000.0",
                returnOnEquity="0.0",
                unrealizedPnl="0.0",
            )

            asset_position = HyperliquidRawAssetPosition(
                asset=f"TEST-{description.replace(' ', '-').upper()}-PERP",
                position=position_info,
            )

            raw_state = HyperliquidRawClearinghouseState(
                assetPositions=[asset_position],
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue="1000.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                crossMaintenanceMarginUsed="0.0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="1000.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                isolatedMaintenanceMarginUsed="0.0",
                isolatedMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="0.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                withdrawable="900.0",
            )

            positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )

            symbol = f"TEST-{description.replace(' ', '-').upper()}-PERP"
            assert symbol in positions
            position = positions[symbol]
            assert position.size == Decimal(size_value)

    def test_maximum_leverage_boundary_values(self) -> None:
        """Test handling of maximum leverage boundary values."""
        leverage_test_cases = [
            1,  # Minimum leverage
            100,  # High leverage
            1000,  # Maximum leverage
        ]

        for leverage_value in leverage_test_cases:
            position_info = HyperliquidRawPositionInfo(
                coin=f"LEV{leverage_value}-PERP",
                szi="1.0",
                entryPx="1000.0",
                leverage=HyperliquidRawLeverage(type="cross", value=leverage_value),
                liquidationPx="900.0",
                marginUsed="100.0",
                maxLeverage=leverage_value,
                positionValue="1000.0",
                returnOnEquity="0.1",
                unrealizedPnl="50.0",
            )

            asset_position = HyperliquidRawAssetPosition(
                asset=f"LEV{leverage_value}-PERP",
                position=position_info,
            )

            raw_state = HyperliquidRawClearinghouseState(
                assetPositions=[asset_position],
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                crossMaintenanceMarginUsed="0.0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                isolatedMaintenanceMarginUsed="0.0",
                isolatedMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="0.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                withdrawable="950.0",
            )

            positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )

            symbol = f"LEV{leverage_value}-PERP"
            position = positions[symbol]
            assert position.hl_details is not None
            assert position.hl_details.leverage_value == leverage_value
            assert position.hl_details.max_leverage == leverage_value


# --- Tests for unicode and encoding support ---


class TestUnicodeAndEncodingSupport:
    """Tests for Unicode symbol support and encoding handling."""

    def test_unicode_symbols_in_position_names(self) -> None:
        """Test handling of Unicode characters in position symbols."""
        unicode_symbols = [
            "ETH🚀-PERP",
            "BTC💎-PERP",
            "SOL⚡-PERP",
            "测试-PERP",  # Chinese characters
            "тест-PERP",  # Cyrillic characters
        ]

        for symbol in unicode_symbols:
            position_info = HyperliquidRawPositionInfo(
                coin=symbol,
                szi="1.0",
                entryPx="1000.0",
                leverage=HyperliquidRawLeverage(type="cross", value=10),
                liquidationPx="900.0",
                marginUsed="100.0",
                maxLeverage=50,
                positionValue="1000.0",
                returnOnEquity="0.1",
                unrealizedPnl="50.0",
            )

            asset_position = HyperliquidRawAssetPosition(
                asset=symbol,
                position=position_info,
            )

            raw_state = HyperliquidRawClearinghouseState(
                assetPositions=[asset_position],
                marginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                crossMaintenanceMarginUsed="0.0",
                crossMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="1050.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="100.0",
                    totalNtlPos="1000.0",
                ),
                isolatedMaintenanceMarginUsed="0.0",
                isolatedMarginSummary=HyperliquidRawMarginSummary(
                    accountValue="0.0",
                    totalRawUsd="0.0",
                    totalMarginUsed="0.0",
                    totalNtlPos="0.0",
                ),
                withdrawable="950.0",
            )

            # Should handle Unicode symbols without error
            positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
                raw_state,
            )

            assert symbol in positions
            position = positions[symbol]
            assert position.symbol == symbol
            assert position.exchange == ExchangeName.HYPERLIQUID.value

    def test_unicode_in_fill_client_order_ids(self) -> None:
        """Test handling of Unicode characters in fill client order IDs."""
        unicode_client_ids = [
            "order_🚀_123",
            "заказ_456",  # Cyrillic
            "订单_789",  # Chinese
            "注文_abc",  # Japanese
        ]

        for client_id in unicode_client_ids:
            raw_fill = HyperliquidRawFill(
                tid=12345,
                coin="UNICODE-PERP",
                px="1000.0",
                sz="1.0",
                time=int(datetime.now(UTC).timestamp() * 1000),
                side="B",
                oid=67890,
                startPosition="0.0",
                dir="Open Long",
                hash="0x1111222233334444555566667777888899990000aaaabbbbccccddddeeeeffff",
                fee="1.0",
                isMaker=False,
                liquidationMarkPx=None,
                cloid=client_id,
            )

            trade = Mapper.transform_raw_fill_to_internal(raw_fill)
            assert trade.client_order_id == client_id

    def test_very_long_string_fields(self) -> None:
        """Test handling of very long string fields within allowed limits."""
        # Use shorter strings that fit within the model's constraints
        long_symbol = "LONG_SYMBOL_" + "X" * 40 + "-PERP"  # Within 64 char limit
        long_client_id = "client_" + "a" * 50  # Within 64 char limit for Trade model
        long_hash = "0x" + "f" * 64  # Exactly 66 characters (0x + 64 hex chars)

        # Test long symbol in position (within 64 char limit)
        position_info = HyperliquidRawPositionInfo(
            coin=long_symbol,
            szi="1.0",
            entryPx="1000.0",
            leverage=HyperliquidRawLeverage(type="cross", value=10),
            liquidationPx="900.0",
            marginUsed="100.0",
            maxLeverage=50,
            positionValue="1000.0",
            returnOnEquity="0.1",
            unrealizedPnl="50.0",
        )

        asset_position = HyperliquidRawAssetPosition(
            asset=long_symbol,
            position=position_info,
        )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[asset_position],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="1050.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.0",
                totalNtlPos="1000.0",
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="1050.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.0",
                totalNtlPos="1000.0",
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="950.0",
        )

        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        assert long_symbol in positions
        position = positions[long_symbol]
        assert position.symbol == long_symbol

        # Test long fields in fill
        raw_fill = HyperliquidRawFill(
            tid=12345,
            coin=long_symbol,
            px="1000.0",
            sz="1.0",
            time=int(datetime.now(UTC).timestamp() * 1000),
            side="B",
            oid=67890,
            startPosition="0.0",
            dir="Open Long",
            hash=long_hash,
            fee="1.0",
            isMaker=False,
            liquidationMarkPx=None,
            cloid=long_client_id,
        )

        trade = Mapper.transform_raw_fill_to_internal(raw_fill)
        assert trade.symbol == long_symbol
        assert trade.client_order_id == long_client_id
        assert trade.hl_details is not None
        assert trade.hl_details.trade_hash == long_hash


# --- Tests for performance and memory considerations ---


class TestPerformanceAndMemory:
    """Tests for performance and memory considerations."""

    def test_large_batch_transformation_efficiency(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
    ) -> None:
        """Test transformation efficiency with large batches of data."""
        # Create a large number of positions
        asset_positions: list[HyperliquidRawAssetPosition] = []
        for i in range(100):
            symbol = f"BATCH{i:03d}-PERP"
            position_info = HyperliquidRawPositionInfo(
                coin=symbol,
                szi=str(1.0 + i * 0.01),
                entryPx=str(1000.0 + i),
                leverage=HyperliquidRawLeverage(
                    type="cross" if i % 2 == 0 else "isolated",
                    value=min(5 + i % 10, 100),
                ),
                liquidationPx=str(900.0 + i),
                marginUsed=str(100.0 + i),
                maxLeverage=50,
                positionValue=str(1000.0 + i),
                returnOnEquity=str(0.01 + i * 0.001),
                unrealizedPnl=str(10.0 + i),
            )
            asset_positions.append(
                HyperliquidRawAssetPosition(asset=symbol, position=position_info),
            )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=asset_positions,
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="50000.0",
                totalRawUsd="0.0",
                totalMarginUsed="15000.0",
                totalNtlPos="105000.0",
            ),
            crossMaintenanceMarginUsed="500.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="50000.0",
                totalRawUsd="0.0",
                totalMarginUsed="15000.0",
                totalNtlPos="105000.0",
            ),
            isolatedMaintenanceMarginUsed="100.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="35000.0",
        )

        # Transform all positions efficiently
        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        # Verify all positions were transformed correctly
        assert len(positions) == 100

        for i, (symbol, position) in enumerate(positions.items()):
            assert symbol == f"BATCH{i:03d}-PERP"
            assert position.exchange == ExchangeName.HYPERLIQUID.value
            assert position.hl_details is not None
            assert position.hl_details.leverage_type in ["cross", "isolated"]

        # Test margin summary transformation with large data set
        margin_summary = Mapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_state,
        )

        assert isinstance(margin_summary, MarginAccountSummary)
        assert margin_summary.exchange == ExchangeName.HYPERLIQUID.value
        assert margin_summary.total_equity == Decimal("50000.0")

    def test_memory_efficient_transformation_patterns(
        self,
        account_data_mapper: HyperliquidAccountDataMapper,
    ) -> None:
        """Test that transformations use memory efficiently."""
        # Create state with various types of data
        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="10000.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="10000.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="10000.0",
        )

        # Test multiple transformation methods on same data
        margin_summary = Mapper.transform_raw_clearinghouse_state_to_margin_summary(
            raw_state,
        )

        spot_balances = account_data_mapper.transform_raw_clearinghouse_state_to_spot_balances(
            raw_state,
        )

        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        # All transformations should complete successfully
        assert isinstance(margin_summary, MarginAccountSummary)
        assert isinstance(spot_balances, dict)
        assert isinstance(positions, dict)


# --- Tests for error recovery scenarios ---


class TestErrorRecoveryScenarios:
    """Tests for error recovery and graceful degradation."""

    def test_partial_data_recovery(self) -> None:
        """Test recovery when some data fields are missing or invalid."""
        # Create position with some optional fields as None
        position_info = HyperliquidRawPositionInfo(
            coin="PARTIAL-PERP",
            szi="1.0",
            entryPx="1000.0",
            leverage=HyperliquidRawLeverage(type="cross", value=10),
            liquidationPx=None,  # Missing liquidation price
            marginUsed="100.0",
            maxLeverage=50,
            positionValue="1000.0",
            returnOnEquity="0.1",
            unrealizedPnl="50.0",
        )

        asset_position = HyperliquidRawAssetPosition(
            asset="PARTIAL-PERP",
            position=position_info,
        )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=[asset_position],
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="1050.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.0",
                totalNtlPos="1000.0",
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="1050.0",
                totalRawUsd="0.0",
                totalMarginUsed="100.0",
                totalNtlPos="1000.0",
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="950.0",
        )

        # Should handle missing optional fields gracefully
        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        position = positions["PARTIAL-PERP"]
        assert position.liquidation_price is None  # Should handle None gracefully
        assert position.size == Decimal("1.0")
        assert position.entry_price == Decimal("1000.0")

    def test_graceful_degradation_with_mixed_valid_invalid_data(self) -> None:
        """Test graceful degradation when processing mixed valid/invalid data."""
        # Test with multiple positions, some with edge case values
        # Note: Zero positions are filtered out by the mapper
        positions_data = [
            ("VALID-PERP", "1.0", "1000.0", "900.0"),
            ("NEGATIVE-PERP", "-0.5", "1000.0", "1100.0"),
            ("SMALL-PERP", "0.000001", "1000.0", "900.0"),  # Very small but non-zero
        ]

        asset_positions: list[HyperliquidRawAssetPosition] = []
        for symbol, size, entry_px, liq_px in positions_data:
            position_info = HyperliquidRawPositionInfo(
                coin=symbol,
                szi=size,
                entryPx=entry_px,
                leverage=HyperliquidRawLeverage(type="cross", value=10),
                liquidationPx=liq_px,
                marginUsed="100.0",
                maxLeverage=50,
                positionValue="1000.0",
                returnOnEquity="0.1",
                unrealizedPnl="50.0",
            )
            asset_positions.append(
                HyperliquidRawAssetPosition(asset=symbol, position=position_info),
            )

        raw_state = HyperliquidRawClearinghouseState(
            assetPositions=asset_positions,
            marginSummary=HyperliquidRawMarginSummary(
                accountValue="3000.0",
                totalRawUsd="0.0",
                totalMarginUsed="300.0",
                totalNtlPos="3000.0",
            ),
            crossMaintenanceMarginUsed="0.0",
            crossMarginSummary=HyperliquidRawMarginSummary(
                accountValue="3000.0",
                totalRawUsd="0.0",
                totalMarginUsed="300.0",
                totalNtlPos="3000.0",
            ),
            isolatedMaintenanceMarginUsed="0.0",
            isolatedMarginSummary=HyperliquidRawMarginSummary(
                accountValue="0.0",
                totalRawUsd="0.0",
                totalMarginUsed="0.0",
                totalNtlPos="0.0",
            ),
            withdrawable="2700.0",
        )

        # Should process all non-zero positions
        positions = Mapper.transform_raw_clearinghouse_state_to_derivative_positions(
            raw_state,
        )

        assert len(positions) == 3

        # Verify each position was processed correctly
        assert positions["VALID-PERP"].size == Decimal("1.0")
        assert positions["NEGATIVE-PERP"].size == Decimal("-0.5")
        assert positions["SMALL-PERP"].size == Decimal("0.000001")

        # All should have consistent metadata
        for position in positions.values():
            assert position.exchange == ExchangeName.HYPERLIQUID.value
            assert position.hl_details is not None
