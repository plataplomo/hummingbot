import asyncio
import unittest
from typing import Awaitable

from hummingbot.connector.derivative.backpack_perpetual import (
    backpack_perpetual_constants as CONSTANTS,
    backpack_perpetual_web_utils as web_utils,
)
from hummingbot.connector.derivative.backpack_perpetual.backpack_perpetual_web_utils import (
    HeadersContentRESTPreProcessor,
)
from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest
from hummingbot.core.web_assistant.web_assistants_factory import WebAssistantsFactory


class BackpackPerpetualWebUtilsUnitTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        cls.ev_loop = asyncio.get_event_loop()

        cls.pre_processor = HeadersContentRESTPreProcessor()

    def async_run_with_timeout(self, coroutine: Awaitable, timeout: float = 1):
        ret = self.ev_loop.run_until_complete(asyncio.wait_for(coroutine, timeout))
        return ret

    def test_rest_pre_processor_sets_json_content_type(self):
        request: RESTRequest = RESTRequest(method=RESTMethod.GET, url="/TEST_URL")

        result_request: RESTRequest = self.async_run_with_timeout(self.pre_processor.pre_process(request))

        self.assertIn("Content-Type", result_request.headers)
        self.assertEqual(result_request.headers["Content-Type"], "application/json")

    def test_rest_url_main_domain(self):
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.REST_URL}TEST_PATH_URL"
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url))

    def test_rest_url_unknown_domain_defaults(self):
        path_url = "/TEST_PATH_URL"

        expected_url = f"{CONSTANTS.REST_URL}TEST_PATH_URL"
        self.assertEqual(expected_url, web_utils.public_rest_url(path_url=path_url, domain="unknown"))

    def test_build_api_factory(self):
        api_factory = web_utils.build_api_factory(
            time_synchronizer=TimeSynchronizer(),
            time_provider=lambda: None,
        )

        self.assertIsInstance(api_factory, WebAssistantsFactory)
        self.assertIsNone(api_factory._auth)

        self.assertEqual(2, len(api_factory._rest_pre_processors))
