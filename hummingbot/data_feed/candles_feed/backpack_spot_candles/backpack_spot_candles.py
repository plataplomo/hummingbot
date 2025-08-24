import logging
from typing import List, Optional

from hummingbot.core.network_iterator import NetworkStatus
from hummingbot.data_feed.candles_feed.backpack_spot_candles import constants as CONSTANTS
from hummingbot.data_feed.candles_feed.candles_base import CandlesBase
from hummingbot.logger import HummingbotLogger


class BackpackSpotCandles(CandlesBase):
    _logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._logger is None:
            cls._logger = logging.getLogger(__name__)
        return cls._logger

    def __init__(self, trading_pair: str, interval: str = "1m", max_records: int = 150):
        super().__init__(trading_pair, interval, max_records)

    @property
    def name(self):
        return f"backpack_{self._trading_pair}"

    @property
    def rest_url(self):
        return CONSTANTS.REST_URL

    @property
    def wss_url(self):
        return CONSTANTS.WSS_URL

    @property
    def health_check_url(self):
        return self.rest_url + CONSTANTS.HEALTH_CHECK_ENDPOINT

    @property
    def candles_url(self):
        return self.rest_url + CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_endpoint(self):
        return CONSTANTS.CANDLES_ENDPOINT

    @property
    def candles_max_result_per_rest_request(self):
        return CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST

    @property
    def rate_limits(self):
        return CONSTANTS.RATE_LIMITS

    @property
    def intervals(self):
        return CONSTANTS.INTERVALS

    async def check_network(self) -> NetworkStatus:
        rest_assistant = await self._api_factory.get_rest_assistant()
        await rest_assistant.execute_request(url=self.health_check_url,
                                             throttler_limit_id=CONSTANTS.HEALTH_CHECK_ENDPOINT)
        return NetworkStatus.CONNECTED

    def get_exchange_trading_pair(self, trading_pair):
        """Convert trading pair format from Hummingbot to Backpack format.

        Hummingbot uses format like "BTC-USDC"
        Backpack uses format like "BTC_USDC" (underscore separator)
        """
        return trading_pair.replace("-", "_")

    def _get_rest_candles_params(self,
                                 start_time: Optional[int] = None,
                                 end_time: Optional[int] = None,
                                 limit: Optional[int] = CONSTANTS.MAX_RESULTS_PER_CANDLESTICK_REST_REQUEST) -> dict:
        """Build parameters for REST API klines request.

        According to openapi_backpack.json:
        - symbol: Market symbol (e.g. SOL_USDC)
        - interval: Kline interval
        - startTime: UTC timestamp in seconds (required)
        - endTime: UTC timestamp in seconds (optional, defaults to current time)
        - priceType: Price type (optional, defaults to LastPrice)
        """
        params = {
            "symbol": self._ex_trading_pair,
            "interval": self.intervals[self.interval],  # Map to Backpack's interval format
        }

        # Backpack requires startTime
        if start_time:
            params["startTime"] = start_time  # Already in seconds

        if end_time:
            params["endTime"] = end_time  # Already in seconds

        # Note: Backpack doesn't use 'limit' parameter, it returns data between start and end times
        # We'll handle limiting in the response parsing if needed

        return params

    def _parse_rest_candles(self, data: dict, end_time: Optional[int] = None) -> List[List[float]]:
        """Parse klines response from REST API.

        According to openapi_backpack.json, Kline object has:
        - start: Start time
        - end: End time
        - open: Open price
        - high: High price
        - low: Low price
        - close: Close price
        - volume: Volume
        - quoteVolume: Quote volume
        - trades: Number of trades

        We need to map this to the expected format:
        [timestamp, open, high, low, close, volume, quote_asset_volume, n_trades,
         taker_buy_base_volume, taker_buy_quote_volume]
        """
        parsed_candles = []
        for kline in data:
            # Parse timestamps - they come as strings in the response
            start_timestamp = self.ensure_timestamp_in_seconds(int(kline["start"]))

            # Build candle row in expected format
            # Note: Backpack doesn't provide taker buy volumes, so we'll use 0
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
        """Build WebSocket subscription payload for kline stream.

        According to openapi_backpack.json, WebSocket stream format is:
        kline.<interval>.<symbol>

        Subscription format:
        {
          "method": "SUBSCRIBE",
          "params": ["kline.<interval>.<symbol>"]
        }
        """
        stream_name = f"kline.{self.intervals[self.interval]}.{self._ex_trading_pair}"
        payload = {
            "method": "SUBSCRIBE",
            "params": [stream_name]
        }
        return payload

    def _parse_websocket_message(self, data: dict):
        """Parse WebSocket kline message.

        According to openapi_backpack.json, K-Line stream format:
        {
          "e": "kline",           // Event type
          "E": 1694687692980000,  // Event time in microseconds
          "s": "SOL_USD",         // Symbol
          "t": 123400000,         // K-Line start time in seconds
          "T": 123460000,         // K-Line close time in seconds
          "o": "18.75",           // Open price
          "c": "19.25",           // Close price
          "h": "19.80",           // High price
          "l": "18.50",           // Low price
          "v": "32123",           // Base asset volume
          "n": 93828,             // Number of trades
          "X": false              // Is this k-line closed?
        }

        Note: WebSocket doesn't provide quote volume or taker buy volumes
        """
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
