"""Backpack Perpetual Candles Feed Implementation."""

import logging
from typing import Optional

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.backpack_perpetual_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger


class BackpackPerpetualCandles(CandlesBase):
    """Candles feed for Backpack perpetual futures markets."""

    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        """Get logger instance."""
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        """Initialize Backpack perpetual candles feed."""
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        """Get feed name."""
        return f"backpack_perpetual_{self._trading_pair}"

    @property
    def rest_url(self):
        """Get REST API base URL."""
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        """Get WebSocket base URL."""
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        """Get health check endpoint URL."""
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        """Get candles endpoint URL."""
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self):
        """Get candles endpoint path."""
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self):
        """Get maximum results per REST request."""
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        """Get rate limits configuration."""
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        """Get interval mappings."""
        return CONSTANTS.INTERVALS

    async def check_network(self) -> NetworkStatus:
        """Check network connectivity."""
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(
            url=self.health_check_url,
            throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT
        )
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair):
        """Convert trading pair format from Hummingbot to Backpack format.

        Hummingbot uses format like "BTC-USDC"
        Backpack perpetual uses format like "BTC_USDC_PERP" for perpetuals
        """
        base_pair = trading_pair.replace("-", "_")
        # Add _PERP suffix for perpetual markets
        return f"{base_pair}_PERP"

    def _get_rest_candles_params(
        self,
        start_time: int | None = None,
        end_time: int | None = None,
        limit: int | None = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST
    ) -> dict:
        """Build parameters for REST API klines request."""
        params = {
            "symbol": self._ex_trading_pair,
            "interval": self.intervals[self.interval],
        }

        if start_time:
            params["startTime"] = start_time  # Already in seconds

        if end_time:
            params["endTime"] = end_time  # Already in seconds

        # Note: limit parameter is not used but kept for interface compatibility
        return params

    def _parse_rest_candles(self, data: dict, end_time: int | None = None) -> list[list[float]]:
        """Parse klines response from REST API."""
        parsed_candles = []
        for kline in data:
            # Parse timestamps - they come as strings in the response
            start_timestamp = self.ensure_timestamp_in_seconds(int(kline["start"]))

            # Build candle row in expected format
            candle_row = [
                start_timestamp,
                float(kline.get("open", 0)),
                float(kline.get("high", 0)),
                float(kline.get("low", 0)),
                float(kline.get("close", 0)),
                float(kline.get("volume", 0)),
                float(kline.get("quoteVolume", 0)),
                int(kline.get("trades", 0)),
                0.0,  # taker_buy_base_volume - not provided by Backpack
                0.0   # taker_buy_quote_volume - not provided by Backpack
            ]
            parsed_candles.append(candle_row)

        return parsed_candles

    def ws_subscription_payload(self):
        """Build WebSocket subscription payload for kline stream."""
        stream_name = f"kline.{self.intervals[self.interval]}.{self._ex_trading_pair}"
        return {
            "method": "SUBSCRIBE",
            "params": [stream_name]
        }

    def _parse_websocket_message(self, data: dict):
        """Parse WebSocket kline message."""
        candles_row_dict = {}

        if data is not None and data.get("e") == "kline":
            # Note: Start time comes in seconds already
            candles_row_dict["timestamp"] = float(data["t"])
            candles_row_dict["open"] = float(data["o"])
            candles_row_dict["high"] = float(data["h"])
            candles_row_dict["low"] = float(data["l"])
            candles_row_dict["close"] = float(data["c"])
            candles_row_dict["volume"] = float(data["v"])
            # Quote volume not provided in WebSocket stream
            candles_row_dict["quote_asset_volume"] = 0.0
            candles_row_dict["n_trades"] = int(data.get("n", 0))
            # Taker buy volumes not provided by Backpack
            candles_row_dict["taker_buy_base_volume"] = 0.0
            candles_row_dict["taker_buy_quote_volume"] = 0.0

            return candles_row_dict
