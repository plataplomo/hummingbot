import asyncio
import time
from typing import Any
from unittest.mock import MagicMock

import pytest
from pytest import LogCaptureFixture

from cyberdelta.apis.connectivity.rate_limiter_service import (
    SERVICE_FALLBACK_BUCKET_SIZE,
    SERVICE_FALLBACK_RATE,
    RateLimiterService,
)
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


@pytest.fixture
def mock_loop() -> MagicMock:
    return MagicMock(spec=asyncio.AbstractEventLoop)


@pytest.fixture
def basic_config() -> dict[str, Any]:
    return {
        "rate_limits": {
            "default_rate": 10,
            "default_bucket_size": 10,
            "endpoints": {
                "GET:/specific/path": {"rate": 5, "bucket_size": 5},
                "POST:/another/path": {"rate": 1, "bucket_size": 1},
                "/path/only": {"rate": 2, "bucket_size": 2},
            },
        }
    }


@pytest.fixture
def config_no_endpoints() -> dict[str, Any]:
    return {
        "rate_limits": {
            "default_rate": 20,
            "default_bucket_size": 20,
        }
    }


@pytest.fixture
def config_invalid_types() -> dict[str, Any]:
    return {
        "rate_limits": {
            "default_rate": "invalid_float",
            "default_bucket_size": "invalid_int",
            "endpoints": {
                "GET:/specific/path": {"rate": "bad_rate", "bucket_size": "bad_bucket"},
            },
        }
    }


@pytest.fixture
def config_missing_rate_limits_key() -> dict[str, Any]:
    return {}


class TestRateLimiterService:
    def test_initialization_with_basic_config(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        assert isinstance(service.default_limiter, TokenBucketRateLimiterRuntime)
        assert service.default_limiter.rate == 10
        assert service.default_limiter.bucket_size == 10
        assert len(service.endpoint_limiters) == 3
        assert "GET:/specific/path" in service.endpoint_limiters
        assert service.endpoint_limiters["GET:/specific/path"].rate == 5
        assert "/path/only" in service.endpoint_limiters
        assert service.endpoint_limiters["/path/only"].rate == 2

    def test_initialization_no_endpoints(
        self, config_no_endpoints: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_no_endpoints, loop=mock_loop
        )
        assert service.default_limiter.rate == 20
        assert service.default_limiter.bucket_size == 20
        assert len(service.endpoint_limiters) == 0

    def test_initialization_invalid_types_uses_defaults(
        self, config_invalid_types: dict[str, Any], mock_loop: MagicMock, caplog: LogCaptureFixture
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_invalid_types, loop=mock_loop
        )
        # Default values should be used when parsing fails
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        # Endpoint with invalid types should also use defaults for that endpoint
        # because RateLimiterConfig validation will fail, leading to full fallback config.
        # Therefore, endpoint_limiters will be empty.
        assert not service.endpoint_limiters

        # Check for the generic fallback log message due to overall validation failure
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )
        # Specific field errors should also be present in the detailed Pydantic error message
        assert "default_rate" in caplog.text and "invalid_float" in caplog.text
        assert "default_bucket_size" in caplog.text and "invalid_int" in caplog.text
        assert "endpoints.GET:/specific/path.rate" in caplog.text and "bad_rate" in caplog.text
        assert (
            "endpoints.GET:/specific/path.bucket_size" in caplog.text
            and "bad_bucket" in caplog.text
        )

    def test_initialization_missing_rate_limits_key_uses_defaults(
        self, config_missing_rate_limits_key: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_missing_rate_limits_key, loop=mock_loop
        )
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0

    def test_initialization_invalid_rate_limits_structure(
        self, mock_loop: MagicMock, caplog: LogCaptureFixture
    ) -> None:
        config_bad_structure = {"rate_limits": "not_a_dict"}
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_bad_structure, loop=mock_loop
        )
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0
        # Check for the warning about rate_limits not being a dict, and the subsequent fallback log
        assert "'rate_limits' in config is not a dictionary" in caplog.text
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    def test_initialization_invalid_endpoints_structure(
        self, mock_loop: MagicMock, caplog: LogCaptureFixture
    ) -> None:
        config_bad_endpoints = {"rate_limits": {"endpoints": "not_a_dict"}}
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_bad_endpoints, loop=mock_loop
        )
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0
        # Check for the Pydantic validation error concerning 'endpoints' not being a dict
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert "Input should be a valid dictionary" in caplog.text and "endpoints" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    def test_initialization_invalid_endpoint_config_item_type(
        self, mock_loop: MagicMock, caplog: LogCaptureFixture
    ) -> None:
        config_bad_item = {"rate_limits": {"endpoints": {"GET:/foo": "not_a_dict"}}}
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_bad_item, loop=mock_loop
        )
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE  # Service falls back fully
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert (
            len(service.endpoint_limiters) == 0
        )  # Malformed endpoint config leads to full fallback

        # Check for the Pydantic validation error concerning the specific endpoint item
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert "endpoints.GET:/foo" in caplog.text
        assert "Input should be a valid dictionary or instance of EndpointRateConfig" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    def test_get_limiter_specific_method_path(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        limiter = service.get_limiter("GET", "/specific/path")
        assert limiter.rate == 5
        assert limiter.bucket_size == 5

    def test_get_limiter_specific_path_only(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        limiter = service.get_limiter(
            "PUT", "/path/only"
        )  # Method doesn't match specific, but path does
        assert limiter.rate == 2
        assert limiter.bucket_size == 2

    def test_get_limiter_default_no_match(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        limiter = service.get_limiter("DELETE", "/unknown/path")
        assert limiter.rate == 10
        assert limiter.bucket_size == 10

    def test_get_limiter_case_insensitivity_for_method(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        limiter = service.get_limiter("gEt", "/specific/path")
        assert limiter.rate == 5
        assert limiter.bucket_size == 5

    @pytest.mark.asyncio
    async def test_limiter_acquires_token(
        self, basic_config: dict[str, Any], mock_loop: MagicMock
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=basic_config, loop=mock_loop
        )
        limiter = service.get_limiter("POST", "/another/path")  # Rate 1, Bucket 1
        assert limiter.rate == 1
        assert limiter.bucket_size == 1

        start_time = time.monotonic()
        await limiter.acquire()  # First token should be available immediately
        duration1 = time.monotonic() - start_time
        assert duration1 < 0.1  # Should be very fast

        start_time_2 = time.monotonic()
        # Second acquire should block for approx 1 second (1/rate)
        # because bucket is 1 and it was just consumed.
        await limiter.acquire()
        duration2 = time.monotonic() - start_time_2
        # Allow for some timing inaccuracies
        assert 0.9 < duration2 < 1.2  # Check it waited roughly 1 sec
