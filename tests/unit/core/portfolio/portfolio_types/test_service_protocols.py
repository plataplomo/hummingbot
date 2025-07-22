"""Tests for service protocol types using Pydantic dataclasses."""

import math

import pytest
from pydantic import ValidationError

from cyberdelta.core.portfolio.portfolio_types.service_protocols import (
    ServiceCacheStats,
    ServiceResilienceStatus,
    ServiceStateData,
    ServiceSymbolMetadata,
    ServiceValidationResult,
    ServiceValidationStats,
)


class TestServiceValidationResult:
    """Test ServiceValidationResult dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid validation result."""
        result = ServiceValidationResult(is_valid=True, errors=[], warnings=["Minor issue"])
        assert result.is_valid is True
        assert result.errors == []
        assert result.warnings == ["Minor issue"]

    def test_defaults(self) -> None:
        """Test default values."""
        result = ServiceValidationResult(is_valid=False)
        assert result.is_valid is False
        assert result.errors == []
        assert result.warnings == []


class TestServiceValidationStats:
    """Test ServiceValidationStats dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid validation stats."""
        stats = ServiceValidationStats(
            total_validations=100,
            successful_validations=90,
            failed_validations=10,
            validation_errors=5,
        )
        assert stats.total_validations == 100
        assert stats.successful_validations == 90
        assert stats.failed_validations == 10
        assert stats.validation_errors == 5

    def test_invalid_counts(self) -> None:
        """Test validation of counts."""
        # Successful validations exceeding total
        with pytest.raises(ValidationError) as exc_info:
            ServiceValidationStats(total_validations=10, successful_validations=15)
        assert "cannot exceed total_validations" in str(exc_info.value)

        # Failed validations exceeding total
        with pytest.raises(ValidationError) as exc_info:
            ServiceValidationStats(total_validations=10, failed_validations=15)
        assert "cannot exceed total_validations" in str(exc_info.value)

    def test_negative_values(self) -> None:
        """Test negative value validation."""
        with pytest.raises(ValidationError):
            ServiceValidationStats(total_validations=-1)


class TestServiceResilienceStatus:
    """Test ServiceResilienceStatus dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid resilience status."""
        status = ServiceResilienceStatus(
            circuit_breaker_state="open",
            retry_count=3,
            last_failure_time=1234567890.0,
            is_healthy=False,
        )
        assert status.circuit_breaker_state == "open"
        assert status.retry_count == 3
        assert status.last_failure_time == 1234567890.0
        assert status.is_healthy is False

    def test_invalid_state(self) -> None:
        """Test invalid circuit breaker state."""
        with pytest.raises(ValidationError) as exc_info:
            ServiceResilienceStatus(circuit_breaker_state="invalid")
        assert "Invalid circuit breaker state" in str(exc_info.value)

    def test_state_normalization(self) -> None:
        """Test state is normalized to lowercase."""
        status = ServiceResilienceStatus(circuit_breaker_state="OPEN")
        assert status.circuit_breaker_state == "open"

    def test_negative_retry_count(self) -> None:
        """Test negative retry count validation."""
        with pytest.raises(ValidationError):
            ServiceResilienceStatus(retry_count=-1)


class TestServiceCacheStats:
    """Test ServiceCacheStats dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid cache stats."""
        stats = ServiceCacheStats(
            hits=100,
            misses=50,
            hit_rate=0.667,  # 100 / (100 + 50)
            total_size=1000,
            ttl_expirations=10,
        )
        assert stats.hits == 100
        assert stats.misses == 50
        assert abs(stats.hit_rate - 0.667) < 0.001
        assert stats.total_size == 1000
        assert stats.ttl_expirations == 10

    def test_invalid_hit_rate(self) -> None:
        """Test hit rate validation."""
        # Hit rate doesn't match calculated rate
        with pytest.raises(ValidationError) as exc_info:
            ServiceCacheStats(
                hits=100,
                misses=50,
                hit_rate=0.5,  # Should be 0.667
            )
        assert "doesn't match calculated rate" in str(exc_info.value)

    def test_hit_rate_bounds(self) -> None:
        """Test hit rate must be between 0 and 1."""
        with pytest.raises(ValidationError):
            ServiceCacheStats(hit_rate=1.5)

        with pytest.raises(ValidationError):
            ServiceCacheStats(hit_rate=-0.1)

    def test_zero_total_requests(self) -> None:
        """Test when there are no requests."""
        stats = ServiceCacheStats(hits=0, misses=0, hit_rate=0.0)
        assert stats.hit_rate == 0.0


class TestServiceStateData:
    """Test ServiceStateData dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid state data."""
        state = ServiceStateData(
            timestamp=1234567890.0,
            version="1.0.0",
            data={
                "key1": "value1",
                "key2": 42,
                "key3": math.pi,
                "key4": True,
                "key5": ["a", "b", "c"],
                "key6": {"nested": "value"},
            },
        )
        assert state.timestamp == 1234567890.0
        assert state.version == "1.0.0"
        assert state.data["key1"] == "value1"
        assert state.data["key2"] == 42

    def test_invalid_timestamp(self) -> None:
        """Test timestamp validation."""
        with pytest.raises(ValidationError) as exc_info:
            ServiceStateData(timestamp=-1.0, version="1.0.0")
        assert "timestamp must be positive" in str(exc_info.value).lower()

    def test_empty_version(self) -> None:
        """Test version validation."""
        with pytest.raises(ValidationError) as exc_info:
            ServiceStateData(timestamp=1234567890.0, version="")
        assert "at least 1 character" in str(exc_info.value).lower()


class TestServiceSymbolMetadata:
    """Test ServiceSymbolMetadata dataclass."""

    def test_valid_creation(self) -> None:
        """Test creating valid symbol metadata."""
        metadata = ServiceSymbolMetadata(
            base_asset="btc",
            quote_asset="usdt",
            exchange_type="perpetual",
            is_perpetual=True,
            contract_size=1.0,
        )
        assert metadata.base_asset == "BTC"  # Normalized to uppercase
        assert metadata.quote_asset == "USDT"  # Normalized to uppercase
        assert metadata.exchange_type == "PERPETUAL"  # Normalized to uppercase
        assert metadata.is_perpetual is True
        assert metadata.contract_size == 1.0

    def test_string_normalization(self) -> None:
        """Test string fields are normalized."""
        metadata = ServiceSymbolMetadata(
            base_asset="  BtC  ", quote_asset="  uSdT  ", exchange_type="  SpOt  "
        )
        assert metadata.base_asset == "BTC"
        assert metadata.quote_asset == "USDT"
        assert metadata.exchange_type == "SPOT"

    def test_empty_strings(self) -> None:
        """Test empty string validation."""
        with pytest.raises(ValidationError) as exc_info:
            ServiceSymbolMetadata(base_asset="", quote_asset="USDT", exchange_type="SPOT")
        assert "Asset and exchange type strings cannot be empty" in str(exc_info.value)

    def test_invalid_contract_size(self) -> None:
        """Test contract size validation for perpetuals."""
        # Negative contract size for perpetual
        with pytest.raises(ValidationError) as exc_info:
            ServiceSymbolMetadata(
                base_asset="BTC",
                quote_asset="USDT",
                exchange_type="PERPETUAL",
                is_perpetual=True,
                contract_size=-1.0,
            )
        assert "Contract size must be positive for perpetual contracts" in str(exc_info.value)

    def test_spot_contract_size(self) -> None:
        """Test contract size can be None for spot."""
        metadata = ServiceSymbolMetadata(
            base_asset="BTC",
            quote_asset="USDT",
            exchange_type="SPOT",
            is_perpetual=False,
            contract_size=None,
        )
        assert metadata.contract_size is None
