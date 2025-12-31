import unittest

from hummingbot.connector.exchange.backpack import backpack_utils as utils


class BackpackUtilTestCases(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.hb_trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}"

    def test_is_exchange_information_valid(self):
        invalid_info_1 = {"status": "TRADING"}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_1))

        invalid_info_2 = {"data": [{"symbol": self.ex_trading_pair, "baseSymbol": self.base_asset}]}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_2))

        valid_info_1 = {
            "data": [
                {
                    "symbol": self.ex_trading_pair,
                    "baseSymbol": self.base_asset,
                    "quoteSymbol": self.quote_asset,
                    "marketType": "SPOT",
                },
            ]
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info_1))

        valid_info_2 = {
            "symbols": [
                {
                    "symbol": self.ex_trading_pair,
                    "baseAsset": self.base_asset,
                    "quoteAsset": self.quote_asset,
                    "status": "TRADING",
                },
            ]
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info_2))
