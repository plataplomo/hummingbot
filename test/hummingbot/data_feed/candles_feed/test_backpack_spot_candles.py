"""Test suite for Backpack Spot Candles Feed."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest import TestCase
from unittest.mock import AsyncMock, MagicMock, patch

from hummingbot.data_feed.candles_feed.backpack_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.backpack_spot_candles.backpack_spot_candles import BackpackSpotCandles


class TestBackpackSpotCandles(TestCase):
    """Test cases for BackpackSpotCandles implementation."""

    def setUp(self):
        """Set up test fixtures."""
        self.trading_pair = "BTC-USDC"
        self.interval = "1h"
        self.max_records = 150
        self.candles = BackpackSpotCandles(
            trading_pair=self.trading_pair,
            interval=self.interval,
            max_records=self.max_records
        )

    def test_initialization(self):
        """Test proper initialization of BackpackSpotCandles."""
        self.assertEqual(self.candles._trading_pair, self.trading_pair)
        self.assertEqual(self.candles.interval, self.interval)
        self.assertEqual(self.candles._max_records, self.max_records)

    def test_name_property(self):
        """Test the name property returns correct format."""
        expected_name = f"backpack_{self.trading_pair}"
        self.assertEqual(self.candles.name, expected_name)

    def test_rest_url_property(self):
        """Test REST URL is correctly set."""
        self.assertEqual(self.candles.rest_url, CONSTANTS.REST_URL)

    def test_wss_url_property(self):
        """Test WebSocket URL is correctly set."""
        self.assertEqual(self.candles.wss_url, CONSTANTS.WSS_URL)

    def test_candles_url_property(self):
        """Test candles endpoint URL is correctly constructed."""
        expected_url = CONSTANTS.REST_URL + CONSTANTS.CANDLES_ENDPOINT
        self.assertEqual(self.candles.candles_url, expected_url)

    def test_get_exchange_trading_pair(self):
        """Test trading pair format conversion."""
        # Test regular spot pair
        hb_pair = "BTC-USDC"
        bp_pair = self.candles.get_exchange_trading_pair(hb_pair)
        self.assertEqual(bp_pair, "BTC_USDC")

        # Test with multiple dashes
        hb_pair = "WRAPPED-BTC-USDC"
        bp_pair = self.candles.get_exchange_trading_pair(hb_pair)
        self.assertEqual(bp_pair, "WRAPPED_BTC_USDC")

    def test_get_rest_candles_params(self):
        """Test REST API parameters generation."""
        # Test without start/end time
        params = self.candles._get_rest_candles_params()
        self.assertEqual(params["symbol"], "BTC_USDC")
        self.assertEqual(params["interval"], "1h")
        self.assertNotIn("startTime", params)
        self.assertNotIn("endTime", params)

        # Test with start time only
        start_time = 1704067200  # 2024-01-01 00:00:00 UTC
        params = self.candles._get_rest_candles_params(start_time=start_time)
        self.assertEqual(params["startTime"], start_time)
        self.assertNotIn("endTime", params)

        # Test with both start and end time
        end_time = 1704153600  # 2024-01-02 00:00:00 UTC
        params = self.candles._get_rest_candles_params(
            start_time=start_time,
            end_time=end_time
        )
        self.assertEqual(params["startTime"], start_time)
        self.assertEqual(params["endTime"], end_time)

    def test_parse_rest_candles(self):
        """Test parsing of REST API klines response."""
        # Sample response from Backpack API
        mock_data = [
            {
                "start": "1704067200",
                "end": "1704070800",
                "open": "42000.50",
                "high": "42500.00",
                "low": "41800.25",
                "close": "42300.75",
                "volume": "125.5",
                "quoteVolume": "5275000.00",
                "trades": "1250"
            },
            {
                "start": "1704070800",
                "end": "1704074400",
                "open": "42300.75",
                "high": "42600.00",
                "low": "42100.00",
                "close": "42450.50",
                "volume": "98.25",
                "quoteVolume": "4165000.00",
                "trades": "980"
            }
        ]

        parsed = self.candles._parse_rest_candles(mock_data)

        # Verify first candle
        self.assertEqual(len(parsed), 2)
        first_candle = parsed[0]
        self.assertEqual(first_candle[0], 1704067200)  # timestamp
        self.assertEqual(first_candle[1], 42000.50)     # open
        self.assertEqual(first_candle[2], 42500.00)     # high
        self.assertEqual(first_candle[3], 41800.25)     # low
        self.assertEqual(first_candle[4], 42300.75)     # close
        self.assertEqual(first_candle[5], 125.5)        # volume
        self.assertEqual(first_candle[6], 5275000.00)   # quote volume
        self.assertEqual(first_candle[7], 1250)         # trades
        self.assertEqual(first_candle[8], 0.0)          # taker buy base (not provided)
        self.assertEqual(first_candle[9], 0.0)          # taker buy quote (not provided)

    def test_ws_subscription_payload(self):
        """Test WebSocket subscription payload generation."""
        self.candles._ex_trading_pair = "BTC_USDC"
        payload = self.candles.ws_subscription_payload()

        expected_stream = "kline.1h.BTC_USDC"
        self.assertEqual(payload["method"], "SUBSCRIBE")
        self.assertIn(expected_stream, payload["params"])

    def test_parse_websocket_message(self):
        """Test parsing of WebSocket kline message."""
        # Sample WebSocket message from Backpack
        mock_message = {
            "e": "kline",
            "t": 1704067200,  # Start time in seconds
            "o": "42000.50",   # Open
            "h": "42500.00",   # High
            "l": "41800.25",   # Low
            "c": "42300.75",   # Close
            "v": "125.5",      # Volume
            "n": 1250          # Number of trades
        }

        parsed = self.candles._parse_websocket_message(mock_message)

        self.assertEqual(parsed["timestamp"], 1704067200)
        self.assertEqual(parsed["open"], 42000.50)
        self.assertEqual(parsed["high"], 42500.00)
        self.assertEqual(parsed["low"], 41800.25)
        self.assertEqual(parsed["close"], 42300.75)
        self.assertEqual(parsed["volume"], 125.5)
        self.assertEqual(parsed["quote_asset_volume"], 0.0)  # Not provided
        self.assertEqual(parsed["n_trades"], 1250)
        self.assertEqual(parsed["taker_buy_base_volume"], 0.0)  # Not provided
        self.assertEqual(parsed["taker_buy_quote_volume"], 0.0)  # Not provided

    def test_parse_websocket_message_non_kline(self):
        """Test parsing returns None for non-kline messages."""
        # Non-kline message
        mock_message = {"e": "trade", "data": "something"}
        result = self.candles._parse_websocket_message(mock_message)
        self.assertIsNone(result)

        # None input
        result = self.candles._parse_websocket_message(None)
        self.assertIsNone(result)

    def test_interval_mapping(self):
        """Test interval mapping for special cases."""
        # Test 1 month mapping
        candles = BackpackSpotCandles("BTC-USDC", "1M", 150)
        params = candles._get_rest_candles_params()
        self.assertEqual(params["interval"], "1month")

        # Test regular mapping
        candles = BackpackSpotCandles("BTC-USDC", "5m", 150)
        params = candles._get_rest_candles_params()
        self.assertEqual(params["interval"], "5m")

    @patch("hummingbot.data_feed.candles_feed.backpack_spot_candles.backpack_spot_candles.BackpackSpotCandles._api_factory")
    async def test_check_network(self):
        """Test network connectivity check."""
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

    def test_rate_limits(self):
        """Test rate limits are properly configured."""
        rate_limits = self.candles.rate_limits

        # Check we have rate limits defined
        self.assertIsNotNone(rate_limits)
        self.assertTrue(len(rate_limits) > 0)

        # Verify candles endpoint limit
        candles_limit = next(
            (rl for rl in rate_limits if rl.limit_id == CONSTANTS.CANDLES_ENDPOINT),
            None
        )
        self.assertIsNotNone(candles_limit)
        self.assertEqual(candles_limit.limit, 20)  # 20 requests per second
        self.assertEqual(candles_limit.time_interval, 1)

    def test_max_results_per_request(self):
        """Test maximum results per REST request is set correctly."""
        self.assertEqual(
            self.candles.candles_max_result_per_rest_request,
            CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST
        )
        self.assertEqual(CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST, 1000)


def run_tests():
    """Run all tests."""
    import unittest
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestBackpackSpotCandles)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


if __name__ == "__main__":
    run_tests()
