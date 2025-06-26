"""Unit tests for HyperliquidRequestBuilder info and market data request functionality."""

from __future__ import annotations

from datetime import UTC, datetime

from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawHistoricalOrdersRequestPayload,
)
from cyberdelta.apis.models.service_args_models import (
    GetCandleSnapshotArgs,
)


# Import fixtures from the shared conftest
pytest_plugins = ["tests.unit.apis.hyperliquid.conftest_request_builder"]


class TestHyperliquidRequestBuilderInfoMarket:
    """Tests for HyperliquidRequestBuilder info and market data functionality."""

    def test_build_info_request_payload(self) -> None:
        """Test that build_info_request_payload constructs the correct Pydantic model."""
        payload = HyperliquidRequestBuilder.build_info_request_payload()
        assert isinstance(payload, HyperliquidRawMetaAndAssetCtxsRequestPayload), (
            "Payload should be an instance of HyperliquidRawMetaAndAssetCtxsRequestPayload"
        )
        assert payload.type == "metaAndAssetCtxs"
        # Ensure model_dump works as expected for this simple model
        assert payload.model_dump() == {"type": "metaAndAssetCtxs"}

    def test_build_historical_orders_payload(self, valid_wallet_address: str) -> None:
        """Test build_historical_orders_payload with valid inputs."""
        request_model = HyperliquidRequestBuilder.build_historical_orders_payload(
            wallet_address=valid_wallet_address,
        )
        assert isinstance(request_model, HyperliquidRawHistoricalOrdersRequestPayload)
        assert request_model.type == "historicalOrders"
        assert request_model.user == valid_wallet_address

    def test_build_candle_snapshot_payload(self, symbol: str) -> None:
        """Test build_candle_snapshot_payload with valid inputs."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
        args = GetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="1h",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload = HyperliquidRequestBuilder.build_candle_snapshot_payload(
            args=args,
        )
        assert isinstance(payload, HyperliquidRawCandleSnapshotRequestPayload)
        assert payload.type == "candleSnapshot"
        assert isinstance(payload.req, HyperliquidRawCandleRequestDetails)
        assert payload.req.coin == symbol
        assert payload.req.interval == "1h"
        assert payload.req.start_time == start_time_ms
        assert payload.req.end_time == end_time_ms

    def test_build_candle_snapshot_payload_different_timeframes(self, symbol: str) -> None:
        """Test build_candle_snapshot_payload with different timeframe values."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 0, 15, 0, tzinfo=UTC).timestamp() * 1000)

        # Test with 1m timeframe
        args_1m = GetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="1m",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload_1m = HyperliquidRequestBuilder.build_candle_snapshot_payload(
            args=args_1m,
        )
        assert payload_1m.req.interval == "1m"

        # Test with 15m timeframe
        args_15m = GetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="15m",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload_15m = HyperliquidRequestBuilder.build_candle_snapshot_payload(
            args=args_15m,
        )
        assert payload_15m.req.interval == "15m"

    def test_build_historical_orders_payload_multiple_calls(
        self,
        valid_wallet_address: str,
    ) -> None:
        """Test build_historical_orders_payload with multiple calls."""
        # Test that the method works consistently
        request_model1 = HyperliquidRequestBuilder.build_historical_orders_payload(
            wallet_address=valid_wallet_address,
        )
        request_model2 = HyperliquidRequestBuilder.build_historical_orders_payload(
            wallet_address=valid_wallet_address,
        )

        # Both should produce the same result
        assert request_model1.type == request_model2.type
        assert request_model1.user == request_model2.user
