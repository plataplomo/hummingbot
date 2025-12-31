import unittest

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_utils as utils


class BackpackPerpetualUtilsUnitTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.base_asset = "COINALPHA"
        cls.quote_asset = "HBOT"
        cls.trading_pair = f"{cls.base_asset}-{cls.quote_asset}"
        cls.ex_trading_pair = f"{cls.base_asset}_{cls.quote_asset}_PERP"

    def test_convert_to_exchange_trading_pair(self):
        self.assertEqual(self.ex_trading_pair, utils.convert_to_exchange_trading_pair(self.trading_pair))

    def test_convert_from_exchange_trading_pair_perp(self):
        self.assertEqual(self.trading_pair, utils.convert_from_exchange_trading_pair(self.ex_trading_pair))

    def test_convert_from_exchange_trading_pair_base_perp_defaults_quote(self):
        self.assertEqual("BTC-USDC", utils.convert_from_exchange_trading_pair("BTC_PERP"))

    def test_is_exchange_information_valid(self):
        invalid_info_1 = {"status": "TRADING"}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_1))

        invalid_info_2 = {"data": [{"symbol": self.ex_trading_pair, "baseSymbol": self.base_asset}]}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info_2))

        valid_info = {
            "data": [
                {
                    "symbol": self.ex_trading_pair,
                    "baseSymbol": self.base_asset,
                    "quoteSymbol": self.quote_asset,
                    "marketType": "PERP",
                    "filters": {"price": {"tickSize": "0.01"}, "quantity": {"stepSize": "0.001"}},
                },
            ]
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info))

    def test_get_new_client_order_id_respects_max_length(self):
        order_id = utils.get_new_client_order_id(is_buy=True, trading_pair=self.trading_pair, max_id_len=16)
        self.assertLessEqual(len(order_id), 16)

    def test_is_perpetual_symbol(self):
        self.assertTrue(utils.is_perpetual_symbol(self.ex_trading_pair))
        self.assertFalse(utils.is_perpetual_symbol("BTC-USDC"))
