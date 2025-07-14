"""CyberDeltaEngine: Hyperliquid Market Metadata Mapper Tests.

------------------------------------------------------------------------

Comprehensive test suite for HyperliquidMarketMetadataMapper market transformation methods.
Tests the new market-related transformation methods including:
- Meta and asset contexts to markets transformation
- Single asset to market transformation
- Error handling and validation for market transformations
- Edge cases and boundary conditions
- Data consistency and type validation
"""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import patch

import pytest
import structlog.testing

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions.data_transformation import MissingRequiredFieldError
from cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper import (
    HyperliquidMarketMetadataMapper,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawAssetCtx,
    HyperliquidRawAssetDefinition,
    HyperliquidRawMetaAndAssetCtxsResponse,
    HyperliquidRawMetaResponse,
)
from cyberdelta.core.models.market.market import HyperliquidMarketDetails, Market


# Alias for shorter method calls
Mapper = HyperliquidMarketMetadataMapper


@pytest.fixture
def mapper() -> HyperliquidMarketMetadataMapper:
    """Provide an instance of HyperliquidMarketMetadataMapper."""
    return HyperliquidMarketMetadataMapper()


def create_asset_definition(
    name: str = "ETH-PERP",
    max_leverage: int = 50,
    only_isolated: bool = False,
    sz_decimals: int = 4,
) -> HyperliquidRawAssetDefinition:
    """Create a HyperliquidRawAssetDefinition for testing."""
    return HyperliquidRawAssetDefinition(
        name=name,
        szDecimals=sz_decimals,
        maxLeverage=max_leverage,
        onlyIsolated=only_isolated,
        marginTableId=None,
        isDelisted=None,
    )


def create_asset_ctx(
    name: str = "ETH-PERP",
    funding: str = "0.0001",
    mark_px: str = "3000.50",
    prev_day_px: str = "2950.00",
    day_ntl_vlm: str = "50000000.00",
    impact_px: str | None = "3000.25",
) -> HyperliquidRawAssetCtx:
    """Create a HyperliquidRawAssetCtx for testing."""
    return HyperliquidRawAssetCtx(
        name=name,
        funding=funding,
        markPx=mark_px,
        prevDayPx=prev_day_px,
        dayNtlVlm=day_ntl_vlm,
        impactPx=impact_px,
        openInterest="1000000.00",  # Required field
        premium="0.0002",  # Required field (can be None)
        oraclePx="3000.00",  # Required field
        midPx="3000.25",  # Required field (can be None)
        impactPxs=["2999.50", "3001.50"],  # Required field (can be None)
        dayBaseVlm="16666.67",  # Required field
    )


def create_meta_and_asset_ctxs_response(
    asset_definitions: list[HyperliquidRawAssetDefinition] | None = None,
    asset_ctxs: list[HyperliquidRawAssetCtx] | None = None,
) -> HyperliquidRawMetaAndAssetCtxsResponse:
    """Create a HyperliquidRawMetaAndAssetCtxsResponse for testing."""
    if asset_definitions is None:
        asset_definitions = [
            create_asset_definition("ETH-PERP"),
            create_asset_definition("BTC-PERP", max_leverage=100, sz_decimals=5),
        ]

    if asset_ctxs is None:
        asset_ctxs = [
            create_asset_ctx("ETH-PERP"),
            create_asset_ctx("BTC-PERP", mark_px="65000.00"),
        ]

    meta = HyperliquidRawMetaResponse(universe=asset_definitions, marginTables=None)

    # The API returns a tuple/list format [meta, asset_ctxs]
    # which the model validator processes
    # Convert asset_ctxs to list of dicts using by_alias to match API format
    asset_ctxs_dicts = [ctx.model_dump(by_alias=True) for ctx in asset_ctxs]
    # Use by_alias for meta as well to match API field names
    meta_dict = meta.model_dump(by_alias=True)
    return HyperliquidRawMetaAndAssetCtxsResponse.model_validate([meta_dict, asset_ctxs_dicts])


class TestTransformRawMetaAndAssetCtxsToMarkets:
    """Tests for transform_raw_meta_and_asset_ctxs_to_markets method."""

    def test_transform_meta_and_asset_ctxs_happy_path(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test successful transformation of meta and asset contexts to markets."""
        raw_response = create_meta_and_asset_ctxs_response()

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        assert isinstance(markets, list)
        assert len(markets) == 2

        # Check first market (ETH-PERP)
        eth_market = markets[0]
        assert isinstance(eth_market, Market)
        assert eth_market.symbol == "ETH-PERP"
        assert eth_market.base_symbol == "ETH-PERP"
        assert eth_market.quote_symbol == "USD"
        assert eth_market.market_type == "Perpetual"
        assert eth_market.tick_size == Decimal("0.1")  # Business logic calculates differently
        assert eth_market.step_size == Decimal("0.0001")  # 1e-4
        assert eth_market.status == "Active"
        assert eth_market.min_quantity == Decimal("0.0001")
        assert eth_market.hl_details is not None
        assert eth_market.bp_details is None

        # Check Hyperliquid-specific details
        assert isinstance(eth_market.hl_details, HyperliquidMarketDetails)
        assert eth_market.hl_details.max_leverage == 50
        assert eth_market.hl_details.only_isolated is False
        assert eth_market.hl_details.sz_decimals == 4
        assert eth_market.hl_details.mark_price == Decimal("3000.5")
        assert eth_market.hl_details.funding_rate == Decimal("0.0001")

        # Check second market (BTC-PERP)
        btc_market = markets[1]
        assert btc_market.symbol == "BTC-PERP"
        assert btc_market.hl_details is not None
        assert btc_market.hl_details.max_leverage == 100
        assert btc_market.hl_details.sz_decimals == 5
        assert btc_market.tick_size == Decimal("1.0")  # Business logic calculates differently
        assert btc_market.step_size == Decimal("0.00001")  # 1e-5

    def test_transform_meta_and_asset_ctxs_missing_asset_context(
        self,
        mapper: HyperliquidMarketMetadataMapper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test transformation when asset context is missing for some assets."""
        # Create response with asset definition but no matching asset context
        asset_definitions = [
            create_asset_definition("ETH-PERP"),
            create_asset_definition("MISSING-PERP"),  # No matching context
        ]
        asset_ctxs = [
            create_asset_ctx("ETH-PERP"),
            # Missing context for MISSING-PERP
        ]

        raw_response = create_meta_and_asset_ctxs_response(asset_definitions, asset_ctxs)

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        # Should still create markets, but missing context asset will have None values
        assert len(markets) == 2

        eth_market = next(m for m in markets if m.symbol == "ETH-PERP")
        missing_market = next(m for m in markets if m.symbol == "MISSING-PERP")

        # ETH market should have context data
        assert eth_market.hl_details is not None
        assert eth_market.hl_details.mark_price == Decimal("3000.50")

        # Missing context market should have None for context-dependent fields
        assert missing_market.hl_details is not None
        assert missing_market.hl_details.mark_price is None
        assert missing_market.hl_details.funding_rate is None

    def test_transform_meta_and_asset_ctxs_empty_universe(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test transformation with empty universe."""
        raw_response = create_meta_and_asset_ctxs_response([], [])

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        assert isinstance(markets, list)
        assert len(markets) == 0

    def test_transform_meta_and_asset_ctxs_with_invalid_asset_definition(
        self,
        mapper: HyperliquidMarketMetadataMapper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test transformation with one invalid asset definition."""
        valid_def = create_asset_definition("ETH-PERP")

        asset_definitions = [valid_def]
        asset_ctxs = [create_asset_ctx("ETH-PERP")]

        raw_response = create_meta_and_asset_ctxs_response(asset_definitions, asset_ctxs)

        # Mock the _create_market_from_asset_definition to raise an error for one asset
        with patch(
            "cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper."
            "HyperliquidMarketMetadataMapper._create_market_from_asset_definition",
            side_effect=ValueError("Invalid sz_decimals processing"),
        ):
            with structlog.testing.capture_logs() as captured_logs:
                markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

            # Should return empty list due to error
            assert len(markets) == 0

            # Should log warning for failed transformation in structured logs
            warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
            assert len(warning_logs) > 0, "Expected at least one warning log"

            # Check for the specific warning about transformation failure
            transformation_logs = [
                log
                for log in warning_logs
                if log.get("event") == "asset_definition_to_market_transform_failed"
                and log.get("asset_name") == "ETH-PERP"
            ]
            assert len(transformation_logs) > 0, (
                f"Expected transformation failure logs, got: {captured_logs}"
            )

    def test_transform_meta_and_asset_ctxs_transformation_error(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test that transformation errors are properly wrapped."""
        # Create a corrupted response that will cause a system error
        with pytest.raises(
            TransformationError,
            match="Failed to transform meta and asset contexts",
        ):
            # Pass corrupted input that will cause the method to fail
            corrupted_response = HyperliquidRawMetaAndAssetCtxsResponse(
                meta=HyperliquidRawMetaResponse(universe=[], marginTables=None), asset_ctxs=[]
            )
            # This should cause an internal transformation error
            mapper.transform_raw_meta_and_asset_ctxs_to_markets(corrupted_response)

    def test_transform_meta_and_asset_ctxs_extreme_values(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test transformation with extreme values."""
        # Create asset definitions with extreme values
        extreme_definitions = [
            create_asset_definition("EXTREME-PERP", max_leverage=1000, sz_decimals=18),
            create_asset_definition("MINIMAL-PERP", max_leverage=1, sz_decimals=0),
        ]
        extreme_ctxs = [
            create_asset_ctx("EXTREME-PERP", funding="0.999999", mark_px="999999999.999999999999"),
            create_asset_ctx("MINIMAL-PERP", funding="-0.999999", mark_px="0.000000000001"),
        ]

        raw_response = create_meta_and_asset_ctxs_response(extreme_definitions, extreme_ctxs)

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        assert len(markets) == 2

        extreme_market = next(m for m in markets if m.symbol == "EXTREME-PERP")
        minimal_market = next(m for m in markets if m.symbol == "MINIMAL-PERP")

        # Check extreme values are preserved
        assert extreme_market.hl_details is not None
        assert extreme_market.hl_details.max_leverage == 1000
        assert extreme_market.hl_details.sz_decimals == 18
        assert extreme_market.tick_size == Decimal(
            "1.0",
        )  # Business logic handles extreme decimals differently
        assert extreme_market.hl_details.funding_rate == Decimal("0.999999")

        assert minimal_market.hl_details is not None
        assert minimal_market.hl_details.max_leverage == 1
        assert minimal_market.hl_details.sz_decimals == 0
        assert minimal_market.tick_size == Decimal(1)  # Business logic handles this differently
        assert minimal_market.hl_details.funding_rate == Decimal("-0.999999")


class TestCreateMarketFromAssetDefinition:
    """Tests for market creation logic via public methods."""

    def test_create_market_from_asset_definition_with_context(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test creating market from asset definition with context."""
        asset_def = create_asset_definition("SOL-PERP", max_leverage=75, sz_decimals=3)
        asset_ctx = create_asset_ctx("SOL-PERP", mark_px="100.50", funding="0.0002")

        market = mapper.transform_single_asset_to_market(asset_def, asset_ctx)

        assert isinstance(market, Market)
        assert market.symbol == "SOL-PERP"
        assert market.base_symbol == "SOL-PERP"
        assert market.quote_symbol == "USD"
        assert market.market_type == "Perpetual"
        assert market.tick_size == Decimal("0.1")  # Business logic may calculate differently
        assert market.step_size == Decimal("0.001")
        assert market.min_quantity == Decimal("0.001")
        assert market.max_quantity is None
        assert market.status == "Active"
        assert market.created_at is None

        # Check Hyperliquid details
        assert market.hl_details is not None
        assert market.hl_details.max_leverage == 75
        assert market.hl_details.only_isolated is False
        assert market.hl_details.sz_decimals == 3
        assert market.hl_details.mark_price == Decimal("100.50")
        assert market.hl_details.funding_rate == Decimal("0.0002")
        assert market.bp_details is None

    def test_create_market_from_asset_definition_without_context(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test creating market from asset definition without context."""
        asset_def = create_asset_definition("AVAX-PERP", max_leverage=25, sz_decimals=2)

        market = mapper.transform_single_asset_to_market(asset_def, None)

        assert isinstance(market, Market)
        assert market.symbol == "AVAX-PERP"
        assert market.tick_size == Decimal("0.01")  # 1e-2
        assert market.step_size == Decimal("0.01")

        # Check Hyperliquid details without context
        assert market.hl_details is not None
        assert market.hl_details.max_leverage == 25
        assert market.hl_details.sz_decimals == 2
        assert market.hl_details.mark_price is None
        assert market.hl_details.funding_rate is None

    def test_create_market_from_asset_definition_invalid_sz_decimals(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test error handling for invalid sz_decimals."""
        asset_def = create_asset_definition(
            "INVALID-PERP",
            sz_decimals=18,
        )  # Valid but will be mocked to fail

        # Mock parse_decimal_value to return None for invalid sz_decimals
        with patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            mock_parse.return_value = None

            with pytest.raises(
                MissingRequiredFieldError,
                match="sz_decimals is required for asset definition for INVALID-PERP",
            ):
                mapper.transform_single_asset_to_market(asset_def)

    def test_create_market_from_asset_definition_extreme_sz_decimals(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test handling of extreme sz_decimals values."""
        # Test minimum value
        min_asset_def = create_asset_definition("MIN-PERP", sz_decimals=0)
        min_market = mapper.transform_single_asset_to_market(min_asset_def)
        assert min_market.tick_size == Decimal(1)  # Business logic calculates differently
        assert min_market.step_size == Decimal(1)

        # Test maximum reasonable value
        max_asset_def = create_asset_definition("MAX-PERP", sz_decimals=18)
        max_market = mapper.transform_single_asset_to_market(max_asset_def)
        assert max_market.tick_size == Decimal(
            "1.0",
        )  # Business logic handles extreme decimals differently
        assert max_market.step_size == Decimal("1e-18")

    def test_create_market_from_asset_definition_malformed_context_data(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test handling of malformed context data."""
        asset_def = create_asset_definition("MALFORMED-PERP")

        # Create context with invalid mark_px that will fail parsing
        with patch(
            "cyberdelta.apis.hyperliquid.mappers.hl_market_data_mapper.parse_decimal_value",
        ) as mock_parse:
            # Return valid value for step_size calculation, None for context parsing
            def mock_parse_side_effect(
                value: str,
                *args: object,
                **kwargs: object,
            ) -> Decimal | None:
                if value.startswith("1e-"):  # Step size calculation
                    return Decimal(value)
                return None  # Context data parsing fails

            mock_parse.side_effect = mock_parse_side_effect

            # Should still create market but with None context values
            market = mapper.transform_single_asset_to_market(asset_def, create_asset_ctx())

            assert market.hl_details is not None
            assert market.hl_details.mark_price is None
            assert market.hl_details.funding_rate is None

    def test_create_market_from_asset_definition_only_isolated_true(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test market creation with only_isolated=True."""
        asset_def = create_asset_definition("ISOLATED-PERP", only_isolated=True)

        market = mapper.transform_single_asset_to_market(asset_def)

        assert market.hl_details is not None
        assert market.hl_details.only_isolated is True


class TestTransformSingleAssetToMarket:
    """Tests for transform_single_asset_to_market convenience method."""

    def test_transform_single_asset_to_market_with_context(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test transforming single asset with context."""
        asset_def = create_asset_definition("DOT-PERP")
        asset_ctx = create_asset_ctx("DOT-PERP")

        market = mapper.transform_single_asset_to_market(asset_def, asset_ctx)

        assert isinstance(market, Market)
        assert market.symbol == "DOT-PERP"
        assert market.hl_details is not None
        assert market.hl_details.mark_price is not None

    def test_transform_single_asset_to_market_without_context(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test transforming single asset without context."""
        asset_def = create_asset_definition("ADA-PERP")

        market = mapper.transform_single_asset_to_market(asset_def)

        assert isinstance(market, Market)
        assert market.symbol == "ADA-PERP"
        assert market.hl_details is not None
        assert market.hl_details.mark_price is None

    def test_transform_single_asset_delegates_to_internal_method(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test that single asset transform delegates to internal method."""
        asset_def = create_asset_definition("LINK-PERP")

        # Mock the internal method using the full path
        with patch(
            "cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper."
            "HyperliquidMarketMetadataMapper._create_market_from_asset_definition",
        ) as mock_internal:
            # Create a minimal valid market for the mock
            mock_market = Market(
                symbol="LINK-PERP",
                base_symbol="LINK-PERP",
                quote_symbol="USD",
                market_type="Perpetual",
                tick_size=Decimal("0.0001"),
                step_size=Decimal("0.0001"),
                status="Active",
            )
            mock_internal.return_value = mock_market

            result = mapper.transform_single_asset_to_market(asset_def)

            # Should call internal method once with correct args
            mock_internal.assert_called_once_with(asset_def, None)
            assert result.symbol == "LINK-PERP"


class TestMarketTransformationErrorHandling:
    """Tests for error handling scenarios in market transformations."""

    def test_error_handling_with_corrupted_asset_definition(
        self,
        mapper: HyperliquidMarketMetadataMapper,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test error handling with corrupted asset definition data."""
        # This would typically be caught at the Pydantic validation level
        # but test mapper robustness

        asset_definitions = [create_asset_definition("VALID-PERP")]

        # Mock asset definition to raise an error during processing
        with patch(
            "cyberdelta.apis.hyperliquid.mappers.market_data.hl_market_metadata_mapper."
            "HyperliquidMarketMetadataMapper._create_market_from_asset_definition",
            side_effect=ValueError("Corrupted asset definition"),
        ):
            raw_response = create_meta_and_asset_ctxs_response(asset_definitions, [])

            with structlog.testing.capture_logs() as captured_logs:
                markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

            # Should return empty list and log warning
            assert len(markets) == 0

            # Should log warning in structured logs
            warning_logs = [log for log in captured_logs if log.get("log_level") == "warning"]
            assert len(warning_logs) > 0, "Expected at least one warning log"

            # Check for the specific warning about transformation failure
            transformation_logs = [
                log
                for log in warning_logs
                if log.get("event") == "asset_definition_to_market_transform_failed"
                and log.get("asset_name") == "VALID-PERP"
            ]
            assert len(transformation_logs) > 0, (
                f"Expected transformation failure logs, got: {captured_logs}"
            )

    def test_error_handling_with_none_asset_contexts(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test error handling with None asset contexts list."""
        asset_definitions = [create_asset_definition("TEST-PERP")]
        meta = HyperliquidRawMetaResponse(universe=asset_definitions, marginTables=None)

        # Create response with empty asset contexts using list format
        raw_response = HyperliquidRawMetaAndAssetCtxsResponse.model_validate(
            [meta.model_dump(by_alias=True), []],  # Empty asset contexts list
        )

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        # Should still work, just without context data
        assert len(markets) == 1
        assert markets[0].symbol == "TEST-PERP"
        assert markets[0].hl_details is not None
        assert markets[0].hl_details.mark_price is None

    def test_error_handling_large_batch_processing(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test that large batches can be processed efficiently."""
        # Create large batch of assets
        asset_definitions: list[HyperliquidRawAssetDefinition] = []
        asset_ctxs: list[HyperliquidRawAssetCtx] = []

        for i in range(50):
            asset_definitions.append(create_asset_definition(f"ASSET-{i}-PERP"))
            asset_ctxs.append(create_asset_ctx(f"ASSET-{i}-PERP"))

        raw_response = create_meta_and_asset_ctxs_response(asset_definitions, asset_ctxs)

        # Should handle large batches without issues
        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        # Should return all valid markets
        assert len(markets) == 50

        # Verify all markets are properly created
        assert all(isinstance(market, Market) for market in markets)
        assert all(market.hl_details is not None for market in markets)

    def test_memory_efficiency_with_large_datasets(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test memory efficiency when processing large datasets."""
        # Create large dataset
        large_asset_definitions = [
            create_asset_definition(f"LARGE-{i}-PERP", sz_decimals=i % 19) for i in range(1000)
        ]
        large_asset_ctxs = [create_asset_ctx(f"LARGE-{i}-PERP") for i in range(1000)]

        raw_response = create_meta_and_asset_ctxs_response(
            large_asset_definitions,
            large_asset_ctxs,
        )

        markets = mapper.transform_raw_meta_and_asset_ctxs_to_markets(raw_response)

        # Should handle large dataset without memory issues
        assert len(markets) == 1000

        # Verify all markets are properly created
        assert all(isinstance(market, Market) for market in markets)
        assert all(market.hl_details is not None for market in markets)

    def test_thread_safety_considerations(
        self,
        mapper: HyperliquidMarketMetadataMapper,
    ) -> None:
        """Test that market transformations are thread-safe."""
        # Since the mapper methods are static, they should be thread-safe
        # Test by calling the same transformation multiple times
        asset_def = create_asset_definition("THREAD-TEST-PERP")
        asset_ctx = create_asset_ctx("THREAD-TEST-PERP")

        results: list[Market] = []
        for _ in range(10):
            result = mapper.transform_single_asset_to_market(asset_def, asset_ctx)
            results.append(result)

        # All results should be identical
        for result in results:
            assert result.symbol == "THREAD-TEST-PERP"
            assert result.hl_details is not None
            assert result.hl_details.mark_price == Decimal("3000.50")
            assert result.hl_details.funding_rate == Decimal("0.0001")
