"""Test suite for Backpack Perpetual Candles Feed."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.data_feed.candles_feed.backpack_perpetual_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.backpack_perpetual_candles.backpack_perpetual_candles import (
    BackpackPerpetualCandles,
)


class TestBackpackPerpetualCandles(TestCase):
    """Test cases for BackpackPerpetualCandles implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.trading_pair = "BTC-USDC"
        self.interval = "1h"
        self.max_records = 150
        self.candles = BackpackPerpetualCandles(
            trading_pair=self.trading_pair,
            interval=self.interval,
            max_records=self.max_records
        )

    def test_initialization(self):
        """Test proper initialization of BackpackPerpetualCandles."""
        self.assertEqual(self.candles._trading_pair, self.trading_pair)
        self.assertEqual(self.candles.interval, self.interval)
        self.assertEqual(self.candles._max_records, self.max_records)

    def test_name_property(self):
        """Test the name property returns correct format."""
        expected_name = f"backpack_perpetual_{self.trading_pair}"
        self.assertEqual(self.candles.name, expected_name)

    def test_rest_url_property(self):
        """Test REST URL is correctly set."""
        self.assertEqual(self.candles.rest_url, CONSTANTS.REST_URL)

    def test_wss_url_property(self):
        """Test WebSocket URL is correctly set."""
        self.assertEqual(self.candles.wss_url, CONSTANTS.WSS_URL)

    def test_get_exchange_trading_pair(self):
        """Test trading pair format conversion for perpetuals."""
        # Test regular perpetual pair
        hb_pair = "BTC-USDC"
        bp_pair = self.candles.get_exchange_trading_pair(hb_pair)
        self.assertEqual(bp_pair, "BTC_USDC_PERP")

        # Test with multiple dashes
        hb_pair = "WRAPPED-BTC-USDC"
        bp_pair = self.candles.get_exchange_trading_pair(hb_pair)
        self.assertEqual(bp_pair, "WRAPPED_BTC_USDC_PERP")

        # Test ETH pair
        hb_pair = "ETH-USDC"
        bp_pair = self.candles.get_exchange_trading_pair(hb_pair)
        self.assertEqual(bp_pair, "ETH_USDC_PERP")

    def test_get_rest_candles_params(self):
        """Test REST API parameters generation for perpetuals."""
        # Test without start/end time
        params = self.candles._get_rest_candles_params()
        self.assertEqual(params["symbol"], "BTC_USDC_PERP")  # Note _PERP suffix
        self.assertEqual(params["interval"], "1h")
        self.assertNotIn("startTime", params)
        self.assertNotIn("endTime", params)

        # Test with start time only
        start_time = 1704067200  # 2024-01-01 00:00:00 UTC
        params = self.candles._get_rest_candles_params(start_time=start_time)
        self.assertEqual(params["symbol"], "BTC_USDC_PERP")
        self.assertEqual(params["startTime"], start_time)
        self.assertNotIn("endTime", params)

        # Test with both start and end time
        end_time = 1704153600  # 2024-01-02 00:00:00 UTC
        params = self.candles._get_rest_candles_params(
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(params["symbol"], "BTC_USDC_PERP")
        self.assertEqual(params["startTime"], start_time)
        self.assertEqual(params["endTime"], end_time)

    def test_parse_rest_candles(self):
        """Test parsing of REST API klines response for perpetuals."""
        # Sample response from Backpack API (perpetual market)
        mock_data = [
            {
                "start": "1704067200",
                "end": "1704070800",
                "open": "42100.00",  # Perpetual prices might differ from spot
                "high": "42600.00",
                "low": "41900.00",
                "close": "42400.00",
                "volume": "2500.75",  # Higher volume typical for perps
                "quoteVolume": "105500000.00",
                "trades": "5250"
            },
            {
                "start": "1704070800",
                "end": "1704074400",
                "open": "42400.00",
                "high": "42700.00",
                "low": "42200.00",
                "close": "42550.00",
                "volume": "1980.50",
                "quoteVolume": "84200000.00",
                "trades": "4100"
            }
        ]

        parsed = self.candles._parse_rest_candles(mock_data)

        # Verify first candle
        self.assertEqual(len(parsed), 2)
        first_candle = parsed[0]
        self.assertEqual(first_candle[0], 1704067200)    # timestamp
        self.assertEqual(first_candle[1], 42100.00)      # open
        self.assertEqual(first_candle[2], 42600.00)      # high
        self.assertEqual(first_candle[3], 41900.00)      # low
        self.assertEqual(first_candle[4], 42400.00)      # close
        self.assertEqual(first_candle[5], 2500.75)       # volume
        self.assertEqual(first_candle[6], 105500000.00)  # quote volume
        self.assertEqual(first_candle[7], 5250)          # trades
        self.assertEqual(first_candle[8], 0.0)           # taker buy base (not provided)
        self.assertEqual(first_candle[9], 0.0)           # taker buy quote (not provided)

    def test_ws_subscription_payload(self):
        """Test WebSocket subscription payload generation for perpetuals."""
        self.candles._ex_trading_pair = "BTC_USDC_PERP"
        payload = self.candles.ws_subscription_payload()

        expected_stream = "kline.1h.BTC_USDC_PERP"
        self.assertEqual(payload["method"], "SUBSCRIBE")
        self.assertIn(expected_stream, payload["params"])

    def test_parse_websocket_message(self):
        """Test parsing of WebSocket kline message for perpetuals."""
        # Sample WebSocket message from Backpack (perpetual market)
        mock_message = {
            "e": "kline",
            "t": 1704067200,   # Start time in seconds
            "o": "42100.00",   # Open
            "h": "42600.00",   # High
            "l": "41900.00",   # Low
            "c": "42400.00",   # Close
            "v": "2500.75",    # Volume
            "n": 5250          # Number of trades
        }

        parsed = self.candles._parse_websocket_message(mock_message)

        self.assertEqual(parsed["timestamp"], 1704067200)
        self.assertEqual(parsed["open"], 42100.00)
        self.assertEqual(parsed["high"], 42600.00)
        self.assertEqual(parsed["low"], 41900.00)
        self.assertEqual(parsed["close"], 42400.00)
        self.assertEqual(parsed["volume"], 2500.75)
        self.assertEqual(parsed["quote_asset_volume"], 0.0)  # Not provided
        self.assertEqual(parsed["n_trades"], 5250)
        self.assertEqual(parsed["taker_buy_base_volume"], 0.0)  # Not provided
        self.assertEqual(parsed["taker_buy_quote_volume"], 0.0)  # Not provided

    def test_interval_mapping(self):
        """Test interval mapping for special cases in perpetuals."""
        # Test 1 month mapping
        candles = BackpackPerpetualCandles("BTC-USDC", "1M", 150)
        params = candles._get_rest_candles_params()
        self.assertEqual(params["interval"], "1month")
        self.assertEqual(params["symbol"], "BTC_USDC_PERP")

        # Test regular mapping
        candles = BackpackPerpetualCandles("ETH-USDC", "15m", 150)
        params = candles._get_rest_candles_params()
        self.assertEqual(params["interval"], "15m")
        self.assertEqual(params["symbol"], "ETH_USDC_PERP")

    @patch("hummingbot.data_feed.candles_feed.backpack_perpetual_candles.backpack_perpetual_candles.BackpackPerpetualCandles._api_factory")
    async def test_check_network(self):
        """Test network connectivity check for perpetuals."""
        # Mock the API factory and rest assistant
        mock_rest_assistant = AsyncMock()
        mock_rest_assistant.execute_request = AsyncMock(return_value={"status": "ok"})

        self.candles._api_factory = MagicMock()
        self.candles._api_factory.get_rest_assistant = AsyncMock(return_value=mock_rest_assistant)

        # Run the check_network method
        result = await self.candles.check_network()

        # Verify the health check was called
        mock_rest_assistant.execute_request.assert_called_once_with(
            url=self.candles.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT
        )

    def test_different_perpetual_pairs(self):
        """Test various perpetual trading pairs."""
        test_pairs = [
            ("BTC-USDC", "BTC_USDC_PERP"),
            ("ETH-USDC", "ETH_USDC_PERP"),
            ("SOL-USDC", "SOL_USDC_PERP"),
            ("ARB-USDC", "ARB_USDC_PERP"),
            ("MATIC-USDC", "MATIC_USDC_PERP"),
        ]

        for hb_pair, expected_bp_pair in test_pairs:
            candles = BackpackPerpetualCandles(hb_pair, "1h", 150)
            bp_pair = candles.get_exchange_trading_pair(hb_pair)
            self.assertEqual(
                bp_pair,
                expected_bp_pair,
                f"Failed for pair {hb_pair}"
            )

    def test_perpetual_specific_constants(self):
        """Test that perpetual uses same endpoints as spot."""
        # Verify endpoints are the same (perpetual uses same API)
        self.assertEqual(CONSTANTS.REST_URL, "https://api.backpack.exchange")
        self.assertEqual(CONSTANTS.WSS_URL, "wss://ws.backpack.exchange")
        self.assertEqual(CONSTANTS.CANDLES_ENDPOINT, "/api/v1/klines")
        self.assertEqual(CONSTANTS.HEALTH_CHECK_ENDPOINT, "/api/v1/ping")

        # Verify rate limits
        self.assertEqual(len(self.candles.rate_limits), 2)

        # Verify max results
        self.assertEqual(
            CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST,
            1000
        )


def run_tests():
    """Run all tests."""
    import unittest
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBackpackPerpetualCandles)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


if __name__ == "__main__":
    run_tests()
