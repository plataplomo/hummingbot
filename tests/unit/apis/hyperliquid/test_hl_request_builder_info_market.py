"""Unit tests for HyperliquidMarketDataRequestBuilder functionality."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from cyberdelta.apis.hyperliquid.models.hl_raw_candles import (
    HyperliquidRawCandleRequestDetails,
    HyperliquidRawCandleSnapshotRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_meta_and_asset_ctxs import (
    HyperliquidRawMetaAndAssetCtxsRequestPayload,
)
from cyberdelta.apis.hyperliquid.request_builders.hl_market_data_request_builder import (
    HyperliquidMarketDataRequestBuilder,
)
from cyberdelta.apis.models.service_args.hyperliquid import (
    HyperliquidGetCandleSnapshotArgs,
)


class TestHyperliquidMarketDataRequestBuilder:
    """Tests for HyperliquidMarketDataRequestBuilder functionality."""

    @pytest.fixture
    def builder(self) -> HyperliquidMarketDataRequestBuilder:
        """Create a HyperliquidMarketDataRequestBuilder instance."""
        return HyperliquidMarketDataRequestBuilder()

    @pytest.fixture
    def symbol(self) -> str:
        """Provide a test symbol."""
        return "BTC-PERP"

    def test_build_info_request_payload(self, builder: HyperliquidMarketDataRequestBuilder) -> None:
        """Test building info request payload."""
        payload = builder.build_info_request_payload()
        assert isinstance(payload, HyperliquidRawMetaAndAssetCtxsRequestPayload)
        assert payload.type == "metaAndAssetCtxs"
        # Ensure model_dump works as expected for this simple model
        assert payload.model_dump() == {"type": "metaAndAssetCtxs"}

    def test_build_candle_snapshot_payload(
        self,
        builder: HyperliquidMarketDataRequestBuilder,
        symbol: str,
    ) -> None:
        """Test build_candle_snapshot_payload with valid inputs."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
        args = HyperliquidGetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="1h",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload = builder.build_candle_snapshot_payload(args)

        assert isinstance(payload, HyperliquidRawCandleSnapshotRequestPayload)
        assert payload.type == "candleSnapshot"
        assert isinstance(payload.req, HyperliquidRawCandleRequestDetails)
        assert payload.req.coin == symbol
        assert payload.req.interval == "1h"
        # The builder calculates the actual start/end times based on lookback_days

    def test_build_candle_snapshot_payload_different_timeframes(
        self,
        builder: HyperliquidMarketDataRequestBuilder,
        symbol: str,
    ) -> None:
        """Test build_candle_snapshot_payload with different timeframe values."""
        # Test with 1m timeframe
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
        args_1m = HyperliquidGetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="1m",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload_1m = builder.build_candle_snapshot_payload(args_1m)
        assert payload_1m.req.interval == "1m"

        # Test with 15m timeframe
        args_15m = HyperliquidGetCandleSnapshotArgs(
            symbol=symbol,
            timeframe="15m",
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )
        payload_15m = builder.build_candle_snapshot_payload(args_15m)
        assert payload_15m.req.interval == "15m"

    def test_multiple_candle_requests_different_args(
        self,
        builder: HyperliquidMarketDataRequestBuilder,
    ) -> None:
        """Test that multiple candle requests with different args produce different payloads."""
        start_time_ms = int(datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
        end_time_ms = int(datetime(2023, 1, 1, 1, 0, 0, tzinfo=UTC).timestamp() * 1000)
        args1 = HyperliquidGetCandleSnapshotArgs(
            symbol="BTC-PERP", timeframe="1h", start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        payload1 = builder.build_candle_snapshot_payload(args1)

        args2 = HyperliquidGetCandleSnapshotArgs(
            symbol="ETH-PERP", timeframe="1h", start_time_ms=start_time_ms, end_time_ms=end_time_ms
        )
        payload2 = builder.build_candle_snapshot_payload(args2)

        assert payload1.req.coin != payload2.req.coin
        assert payload1.req.interval == payload2.req.interval
