import copy
import time
from typing import Any

import pytest
from pydantic import ValidationError
from pytest import LogCaptureFixture

from cyberdelta.apis.connectivity.rate_limiter_service import (
    SERVICE_FALLBACK_BUCKET_SIZE,
    SERVICE_FALLBACK_RATE,
    RateLimiterService,
)
from cyberdelta.apis.rate_limiter import TokenBucketRateLimiterRuntime


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
    @pytest.fixture
    def basic_config(self) -> dict[str, Any]:
        return {
            "rate_limits": {
                "default_rate": 10,
                "default_bucket_size": 10,
                "endpoints": {
                    "GET:/path1": {"rate": 5, "bucket_size": 5},
                    "POST:/path2": {"rate": 1, "bucket_size": 2},
                    "GET:/specific/path": {"rate": 5, "bucket_size": 5},
                    "PUT:/path/only": {"rate": 2, "bucket_size": 2},
                    "POST:/another/path": {"rate": 1, "bucket_size": 1},
                },
            }
        }

    def test_initialization(self, basic_config: dict[str, Any], caplog: LogCaptureFixture) -> None:
        """Test basic initialization and limiter creation."""
        service = RateLimiterService(exchange_name="test_exchange", config=basic_config)
        assert service.default_limiter.rate == 10
        assert service.default_limiter.bucket_size == 10
        assert "GET:/path1" in service.endpoint_limiters
        assert service.endpoint_limiters["GET:/path1"].rate == 5
        assert "POST:/path2" in service.endpoint_limiters
        assert service.endpoint_limiters["POST:/path2"].tokens == 2.0
        assert not caplog.text  # No errors expected

    def test_initialization_with_basic_config(self, basic_config: dict[str, Any]) -> None:
        service = RateLimiterService(exchange_name="test_exchange", config=basic_config)
        assert isinstance(service.default_limiter, TokenBucketRateLimiterRuntime)
        assert service.default_limiter.rate == 10
        assert service.default_limiter.bucket_size == 10
        assert len(service.endpoint_limiters) == 5
        assert "GET:/specific/path" in service.endpoint_limiters
        assert service.endpoint_limiters["GET:/specific/path"].rate == 5
        assert "PUT:/path/only" in service.endpoint_limiters
        assert service.endpoint_limiters["PUT:/path/only"].rate == 2
        assert "POST:/another/path" in service.endpoint_limiters
        # Check model_config
        assert service.model_config.get("frozen") is True
        assert service.model_config.get("extra") == "forbid"
        assert service.model_config.get("arbitrary_types_allowed") is True

    def test_initialization_no_endpoints(self, config_no_endpoints: dict[str, Any]) -> None:
        service = RateLimiterService(exchange_name="test_exchange", config=config_no_endpoints)
        assert service.default_limiter.rate == 20
        assert service.default_limiter.bucket_size == 20
        assert len(service.endpoint_limiters) == 0

    def test_initialization_invalid_types_uses_defaults(
        self, config_invalid_types: dict[str, Any], caplog: LogCaptureFixture
    ) -> None:
        service = RateLimiterService(exchange_name="test_exchange", config=config_invalid_types)
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert not service.endpoint_limiters
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )
        assert "default_rate" in caplog.text and "invalid_float" in caplog.text
        assert "default_bucket_size" in caplog.text and "invalid_int" in caplog.text
        assert "endpoints.GET:/specific/path.rate" in caplog.text and "bad_rate" in caplog.text
        assert (
            "endpoints.GET:/specific/path.bucket_size" in caplog.text
            and "bad_bucket" in caplog.text
        )

    def test_initialization_missing_rate_limits_key_uses_defaults(
        self, config_missing_rate_limits_key: dict[str, Any]
    ) -> None:
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_missing_rate_limits_key
        )
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0

    def test_initialization_invalid_rate_limits_structure(self, caplog: LogCaptureFixture) -> None:
        config_bad_structure = {"rate_limits": "not_a_dict"}
        service = RateLimiterService(exchange_name="test_exchange", config=config_bad_structure)
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0
        assert "'rate_limits' in config is not a dictionary" in caplog.text
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    def test_initialization_invalid_endpoints_structure(self, caplog: LogCaptureFixture) -> None:
        config_bad_endpoints = {"rate_limits": {"endpoints": "not_a_dict"}}
        service = RateLimiterService(exchange_name="test_exchange", config=config_bad_endpoints)
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert "Input should be a valid dictionary" in caplog.text and "endpoints" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    def test_initialization_invalid_endpoint_config_item_type(
        self, caplog: LogCaptureFixture
    ) -> None:
        config_bad_item = {"rate_limits": {"endpoints": {"GET:/foo": "not_a_dict"}}}
        service = RateLimiterService(exchange_name="test_exchange", config=config_bad_item)
        assert service.default_limiter.rate == SERVICE_FALLBACK_RATE
        assert service.default_limiter.bucket_size == SERVICE_FALLBACK_BUCKET_SIZE
        assert len(service.endpoint_limiters) == 0
        assert "Failed to validate 'rate_limits' config" in caplog.text
        assert "endpoints.GET:/foo" in caplog.text
        assert "Input should be a valid dictionary or instance of EndpointRateConfig" in caplog.text
        assert (
            f"Using service fallbacks: rate={SERVICE_FALLBACK_RATE}, "
            f"bucket={SERVICE_FALLBACK_BUCKET_SIZE}" in caplog.text
        )

    @pytest.mark.parametrize(
        "method, path, expected_rate, expected_bucket_size",
        [
            ("GET", "/specific/path", 5, 5),  # Specific method and path
            ("gEt", "/specific/path", 5, 5),  # Case-insensitive method match
            ("PUT", "/path/only", 2, 2),  # Path-only match
            ("DELETE", "/unknown/path", 10, 10),  # Default fallback
            ("POST", "/another/path", 1, 1),  # Another specific method and path
        ],
    )
    def test_get_limiter(
        self,
        basic_config: dict[str, Any],
        method: str,
        path: str,
        expected_rate: float,
        expected_bucket_size: int,
    ) -> None:
        service = RateLimiterService(exchange_name="test_exchange", config=basic_config)
        limiter = service.get_limiter(method, path)
        assert limiter.rate == expected_rate
        assert limiter.bucket_size == expected_bucket_size

    @pytest.mark.asyncio
    async def test_limiter_acquires_token(self, basic_config: dict[str, Any]) -> None:
        service = RateLimiterService(exchange_name="test_exchange", config=basic_config)
        limiter = service.get_limiter("POST", "/another/path")  # Rate 1, Bucket 1
        assert limiter.rate == 1
        assert limiter.bucket_size == 1

        start_time = time.monotonic()
        await limiter.acquire()  # First token should be available immediately
        duration1 = time.monotonic() - start_time
        assert duration1 < 0.1  # Should be very fast

        start_time_2 = time.monotonic()
        await limiter.acquire()  # Second acquire should block for approx 1 second (1/rate)
        duration2 = time.monotonic() - start_time_2
        # Allow for some scheduling leeway
        assert 0.9 < duration2 < 1.2  # Bucket was 1, rate 1.0/s

    def test_frozen_behavior(self, basic_config: dict[str, Any]) -> None:
        """Test that the RateLimiterService model is frozen."""
        service = RateLimiterService(exchange_name="test_exchange", config=basic_config)
        with pytest.raises(ValidationError) as exc_info:  # Pydantic v2 raises ValidationError
            service.default_limiter = TokenBucketRateLimiterRuntime(1, 1)
        assert "frozen" in str(exc_info.value).lower()

    def test_init_signature_prevents_extra_kwargs(
        self, basic_config: dict[str, Any], caplog: LogCaptureFixture
    ) -> None:
        """Test that extra kwargs in config for RateLimiterConfig are logged as errors."""
        config_with_extra = copy.deepcopy(basic_config)
        # Add an unknown param that RateLimiterConfig will reject
        config_with_extra["rate_limits"]["unknown_param_for_config"] = "should_be_logged_as_error"

        # RateLimiterService construction should succeed by falling back to defaults
        service = RateLimiterService(exchange_name="test_extra_log", config=config_with_extra)
        assert service is not None  # Service initializes with defaults

        # Check that a Pydantic validation error for RateLimiterConfig was logged
        assert len(caplog.records) >= 1
        found_log = False
        for record in caplog.records:
            if (
                record.levelname == "ERROR"
                and "Failed to validate 'rate_limits' config" in record.message
                and "unknown_param_for_config" in record.message
                and "Extra inputs are not permitted" in record.message
            ):
                found_log = True
                break
        assert found_log, "Expected Pydantic ValidationError for extra field not logged."

    def test_initialization_with_endpoint_config_none(self, caplog: LogCaptureFixture) -> None:
        """Test initialization when an endpoint config is explicitly None."""
        config_with_none_endpoint = {
            "rate_limits": {
                "default_rate": 10,
                "default_bucket_size": 10,
                "endpoints": {"GET:/path_with_none_config": None},
            }
        }
        service = RateLimiterService(
            exchange_name="test_exchange", config=config_with_none_endpoint
        )
        # Assert that the endpoint with None config is NOT in endpoint_limiters
        assert "GET:/path_with_none_config" not in service.endpoint_limiters
        # Assert that the warning log was generated
        assert len(caplog.records) == 1
        assert "Failed to validate 'rate_limits' config" in caplog.records[0].message
        assert "GET:/path_with_none_config" in caplog.records[0].message
        assert (
            "Input should be a valid dictionary or instance of EndpointRateConfig"
            in caplog.records[0].message
        )
