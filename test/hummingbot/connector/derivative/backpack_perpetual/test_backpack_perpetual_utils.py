from unittest import TestCase

from hummingbot.connector.derivative.backpack_perpetual import backpack_perpetual_utils as utils


class BackpackPerpetualUtilsTests(TestCase):

    def test_split_trading_pair(self):
        """Test splitting trading pairs into base and quote."""
        base, quote = utils.split_trading_pair("BTC-USDC")
        self.assertEqual(base, "BTC")
        self.assertEqual(quote, "USDC")

        base, quote = utils.split_trading_pair("ETH-USDC")
        self.assertEqual(base, "ETH")
        self.assertEqual(quote, "USDC")

        base, quote = utils.split_trading_pair("SOL-USDC")
        self.assertEqual(base, "SOL")
        self.assertEqual(quote, "USDC")

    def test_split_trading_pair_invalid(self):
        """Test splitting invalid trading pairs."""
        with self.assertRaises(ValueError):
            utils.split_trading_pair("BTCUSDC")  # No separator

        with self.assertRaises(ValueError):
            utils.split_trading_pair("BTC-USD-C")  # Too many separators

    def test_convert_from_exchange_trading_pair(self):
        """Test converting from Backpack format to Hummingbot format."""
        result = utils.convert_from_exchange_trading_pair("BTC_PERP")
        self.assertEqual(result, "BTC-USDC")

        result = utils.convert_from_exchange_trading_pair("ETH_PERP")
        self.assertEqual(result, "ETH-USDC")

        result = utils.convert_from_exchange_trading_pair("SOL_PERP")
        self.assertEqual(result, "SOL-USDC")

    def test_convert_to_exchange_trading_pair(self):
        """Test converting from Hummingbot format to Backpack format."""
        result = utils.convert_to_exchange_trading_pair("BTC-USDC")
        self.assertEqual(result, "BTC_PERP")

        result = utils.convert_to_exchange_trading_pair("ETH-USDC")
        self.assertEqual(result, "ETH_PERP")

        result = utils.convert_to_exchange_trading_pair("SOL-USDC")
        self.assertEqual(result, "SOL_PERP")

    def test_is_perpetual_symbol(self):
        """Test checking if symbol is a perpetual."""
        # Valid perpetual symbols
        self.assertTrue(utils.is_perpetual_symbol("BTC_PERP"))
        self.assertTrue(utils.is_perpetual_symbol("ETH_PERP"))
        self.assertTrue(utils.is_perpetual_symbol("SOL_PERP"))

        # Invalid symbols (spot format)
        self.assertFalse(utils.is_perpetual_symbol("BTC_USDC"))
        self.assertFalse(utils.is_perpetual_symbol("ETH_USD"))
        self.assertFalse(utils.is_perpetual_symbol("BTC-USDC"))

        # Invalid format
        self.assertFalse(utils.is_perpetual_symbol("BTCPERP"))
        self.assertFalse(utils.is_perpetual_symbol(""))

    def test_get_trading_pair_from_symbol(self):
        """Test extracting trading pair from perpetual symbol."""
        result = utils.get_trading_pair_from_symbol("BTC_PERP")
        self.assertEqual(result, "BTC-USDC")

        result = utils.get_trading_pair_from_symbol("ETH_PERP")
        self.assertEqual(result, "ETH-USDC")

        # Should return None for invalid symbols
        result = utils.get_trading_pair_from_symbol("BTC_USDC")
        self.assertIsNone(result)

        result = utils.get_trading_pair_from_symbol("INVALID")
        self.assertIsNone(result)

    def test_get_next_funding_timestamp(self):
        """Test calculating next funding timestamp."""
        # Test at exactly funding time (00:00 UTC)
        current_timestamp = 1640995200  # 2022-01-01 00:00:00 UTC
        next_funding = utils.get_next_funding_timestamp(current_timestamp)
        expected = 1641024000  # 2022-01-01 08:00:00 UTC
        self.assertEqual(next_funding, expected)

        # Test between funding times
        current_timestamp = 1640998800  # 2022-01-01 01:00:00 UTC
        next_funding = utils.get_next_funding_timestamp(current_timestamp)
        expected = 1641024000  # 2022-01-01 08:00:00 UTC
        self.assertEqual(next_funding, expected)

        # Test just before funding time
        current_timestamp = 1641023999  # 2022-01-01 07:59:59 UTC
        next_funding = utils.get_next_funding_timestamp(current_timestamp)
        expected = 1641024000  # 2022-01-01 08:00:00 UTC
        self.assertEqual(next_funding, expected)

        # Test just after funding time
        current_timestamp = 1641024001  # 2022-01-01 08:00:01 UTC
        next_funding = utils.get_next_funding_timestamp(current_timestamp)
        expected = 1641052800  # 2022-01-01 16:00:00 UTC
        self.assertEqual(next_funding, expected)

    def test_is_exchange_information_valid(self):
        """Test exchange information validation."""
        # Valid exchange info
        valid_info = {
            "symbols": [
                {
                    "symbol": "BTC_PERP",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "status": "TRADING",
                    "contractType": "PERPETUAL"
                }
            ]
        }
        self.assertTrue(utils.is_exchange_information_valid(valid_info))

        # Invalid - not a dict
        self.assertFalse(utils.is_exchange_information_valid("invalid"))

        # Invalid - missing symbols
        invalid_info = {"someOtherField": "value"}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info))

        # Invalid - empty symbols
        invalid_info = {"symbols": []}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info))

        # Invalid - symbols not a list
        invalid_info = {"symbols": "not_a_list"}
        self.assertFalse(utils.is_exchange_information_valid(invalid_info))

        # Invalid - missing required fields in symbol
        invalid_info = {
            "symbols": [
                {
                    "symbol": "BTC_PERP",
                    # Missing baseAsset, quoteAsset, status
                }
            ]
        }
        self.assertFalse(utils.is_exchange_information_valid(invalid_info))

    def test_decimal_val_or_none(self):
        """Test decimal value conversion."""
        from decimal import Decimal

        # Valid decimal strings
        result = utils.decimal_val_or_none("123.456")
        self.assertEqual(result, Decimal("123.456"))

        result = utils.decimal_val_or_none("0.00001")
        self.assertEqual(result, Decimal("0.00001"))

        result = utils.decimal_val_or_none("1000000")
        self.assertEqual(result, Decimal("1000000"))

        # Zero values
        result = utils.decimal_val_or_none("0")
        self.assertEqual(result, Decimal("0"))

        result = utils.decimal_val_or_none("0.0")
        self.assertEqual(result, Decimal("0"))

        # None and empty values
        result = utils.decimal_val_or_none(None)
        self.assertIsNone(result)

        result = utils.decimal_val_or_none("")
        self.assertIsNone(result)

        # Invalid values should return None
        result = utils.decimal_val_or_none("invalid")
        self.assertIsNone(result)

        result = utils.decimal_val_or_none("abc123")
        self.assertIsNone(result)
