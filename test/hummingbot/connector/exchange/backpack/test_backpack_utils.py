"""
Unit tests for Backpack utils module.
"""

import unittest

from hummingbot.connector.exchange.backpack.backpack_utils import (
    split_trading_pair,
    convert_from_exchange_trading_pair,
    convert_to_exchange_trading_pair,
    validate_trading_pair,
    normalize_trading_pair,
    is_exchange_information_valid
)


class TestBackpackUtils(unittest.TestCase):
    """Test cases for Backpack utility functions."""

    def test_split_trading_pair(self):
        """Test splitting trading pairs into base and quote."""
        base, quote = split_trading_pair("BTC-USDC")
        self.assertEqual(base, "BTC")
        self.assertEqual(quote, "USDC")
        
        base, quote = split_trading_pair("ETH-BTC")
        self.assertEqual(base, "ETH")
        self.assertEqual(quote, "BTC")

    def test_split_trading_pair_invalid(self):
        """Test splitting invalid trading pairs."""
        with self.assertRaises(ValueError):
            split_trading_pair("BTCUSDC")  # No separator
            
        with self.assertRaises(ValueError):
            split_trading_pair("BTC-USD-C")  # Too many separators

    def test_convert_from_exchange_trading_pair(self):
        """Test converting from Backpack format to Hummingbot format."""
        result = convert_from_exchange_trading_pair("BTC_USDC")
        self.assertEqual(result, "BTC-USDC")
        
        result = convert_from_exchange_trading_pair("ETH_BTC")
        self.assertEqual(result, "ETH-BTC")

    def test_convert_to_exchange_trading_pair(self):
        """Test converting from Hummingbot format to Backpack format."""
        result = convert_to_exchange_trading_pair("BTC-USDC")
        self.assertEqual(result, "BTC_USDC")
        
        result = convert_to_exchange_trading_pair("ETH-BTC")
        self.assertEqual(result, "ETH_BTC")

    def test_validate_trading_pair(self):
        """Test trading pair validation."""
        # Valid pairs
        self.assertTrue(validate_trading_pair("BTC-USDC"))
        self.assertTrue(validate_trading_pair("ETH-BTC"))
        self.assertTrue(validate_trading_pair("DOGE-USD"))
        
        # Invalid pairs
        self.assertFalse(validate_trading_pair("BTCUSDC"))  # No separator
        self.assertFalse(validate_trading_pair("BTC-"))     # Missing quote
        self.assertFalse(validate_trading_pair("-USDC"))    # Missing base
        self.assertFalse(validate_trading_pair("BTC-USD-C")) # Too many parts
        self.assertFalse(validate_trading_pair(123))        # Not a string
        self.assertFalse(validate_trading_pair(""))         # Empty string
        
        # Very long asset names (should be invalid)
        self.assertFalse(validate_trading_pair("VERYLONGASSETNAME-USDC"))

    def test_normalize_trading_pair(self):
        """Test trading pair normalization."""
        # Already normalized
        result = normalize_trading_pair("BTC-USDC")
        self.assertEqual(result, "BTC-USDC")
        
        # From Backpack format
        result = normalize_trading_pair("BTC_USDC")
        self.assertEqual(result, "BTC-USDC")
        
        # From display format
        result = normalize_trading_pair("BTC/USDC")
        self.assertEqual(result, "BTC-USDC")
        
        # Case normalization
        result = normalize_trading_pair("btc-usdc")
        self.assertEqual(result, "BTC-USDC")
        
        # With whitespace
        result = normalize_trading_pair(" BTC-USDC ")
        self.assertEqual(result, "BTC-USDC")

    def test_normalize_trading_pair_invalid(self):
        """Test normalization of invalid trading pairs."""
        with self.assertRaises(ValueError):
            normalize_trading_pair("BTCUSDC")  # No separator
            
        with self.assertRaises(ValueError):
            normalize_trading_pair(123)  # Not a string

    def test_is_exchange_information_valid(self):
        """Test exchange information validation."""
        # Valid exchange info
        valid_info = {
            "symbols": [
                {
                    "symbol": "BTC_USDC",
                    "baseAsset": "BTC",
                    "quoteAsset": "USDC",
                    "status": "TRADING"
                }
            ]
        }
        self.assertTrue(is_exchange_information_valid(valid_info))
        
        # Invalid - not a dict
        self.assertFalse(is_exchange_information_valid("invalid"))
        
        # Invalid - missing symbols
        invalid_info = {"someOtherField": "value"}
        self.assertFalse(is_exchange_information_valid(invalid_info))
        
        # Invalid - empty symbols
        invalid_info = {"symbols": []}
        self.assertFalse(is_exchange_information_valid(invalid_info))
        
        # Invalid - symbols not a list
        invalid_info = {"symbols": "not_a_list"}
        self.assertFalse(is_exchange_information_valid(invalid_info))
        
        # Invalid - missing required fields in symbol
        invalid_info = {
            "symbols": [
                {
                    "symbol": "BTC_USDC",
                    # Missing baseAsset, quoteAsset, status
                }
            ]
        }
        self.assertFalse(is_exchange_information_valid(invalid_info))


if __name__ == "__main__":
    unittest.main()