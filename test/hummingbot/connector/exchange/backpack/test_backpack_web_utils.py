import unittest

from hummingbot.connector.exchange.backpack import backpack_constants as CONSTANTS, backpack_web_utils as web_utils


class BackpackUtilTestCases(unittest.TestCase):

    def test_public_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = f"{CONSTANTS.REST_URLS[domain]}TEST_PATH"
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url, domain))

    def test_private_rest_url(self):
        path_url = "/TEST_PATH"
        domain = CONSTANTS.DEFAULT_DOMAIN
        expected_url = f"{CONSTANTS.REST_URLS[domain]}TEST_PATH"
        self.assertEqual(expected_url, web_utils.private_rest_url(path_url, domain))
