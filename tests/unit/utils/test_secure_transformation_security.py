"""Security tests for secure transformation utilities.

This module tests security scenarios for the secure_transform function,
ensuring that it properly validates data and prevents validation bypass attacks.
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel, Field

from cyberdelta.utils.secure_transformation import (
    TransformationError,
    secure_transform,
    secure_transform_with_audit,
)


# Test models for validation
class TestBalance(BaseModel):
    """Test model for balance validation."""

    asset: str = Field(..., min_length=1, max_length=10)
    quantity: Decimal = Field(..., ge=0)  # Must be non-negative
    timestamp: datetime
    exchange: str


class TestOrder(BaseModel):
    """Test model for order validation."""

    symbol: str
    side: str = Field(..., pattern="^(buy|sell)$")
    quantity: Decimal = Field(..., gt=0)  # Must be positive
    price: Decimal = Field(..., gt=0)
    order_type: str = Field(..., pattern="^(limit|market)$")


class TestPosition(BaseModel):
    """Test model for position validation."""

    symbol: str
    size: Decimal  # Can be negative (short position)
    entry_price: Decimal = Field(..., gt=0)
    mark_price: Decimal = Field(..., gt=0)
    pnl: Decimal


class TestSecureTransformSecurity:
    """Test security scenarios for secure_transform."""

    def test_negative_value_attack_prevention(self) -> None:
        """Test that negative values are caught when constraints exist."""
        malicious_data = {
            "asset": "BTC",
            "quantity": "-100.5",  # Negative attack
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
        }

        with pytest.raises(TransformationError) as exc_info:
            secure_transform(
                data=malicious_data,
                model_class=TestBalance,
                context="balance_update",
                source_exchange="backpack",
            )

        assert "Validation failed" in str(exc_info.value)
        assert "balance_update" in str(exc_info.value)
        # The actual validation error should contain details
        assert "validation errors" in str(exc_info.value)

    def test_validation_bypass_prevention(self) -> None:
        """Test that direct instantiation bypass is prevented."""
        # This is what mappers were doing wrong (direct instantiation)
        # balance = TestBalance(asset="BTC", quantity=-100, ...)  # Would bypass!

        # secure_transform prevents this
        data = {
            "asset": "BTC",
            "quantity": "-100",
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "malicious",
        }

        with pytest.raises(TransformationError):
            secure_transform(
                data=data,
                model_class=TestBalance,
                context="validation_bypass_test",
                source_exchange="test",
            )

    def test_type_coercion_attacks(self) -> None:
        """Test that type coercion attacks are handled."""
        # String that looks like a number but has malicious content
        malicious_data = {
            "symbol": "BTC-USD",
            "side": "buy",
            "quantity": "10.5; DROP TABLE orders;--",  # SQL injection attempt
            "price": "50000",
            "order_type": "limit",
        }

        with pytest.raises(TransformationError) as exc_info:
            secure_transform(
                data=malicious_data,
                model_class=TestOrder,
                context="order_creation",
                source_exchange="backpack",
            )

        # Pydantic should fail to parse the malicious quantity
        assert "Validation failed" in str(exc_info.value)

    def test_field_injection_attacks(self) -> None:
        """Test that extra fields don't cause security issues."""
        data_with_extra_fields = {
            "asset": "BTC",
            "quantity": "100.5",
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
            # Attempt to inject extra fields
            "__class__": "malicious",
            "__init__": "attack",
            "_internal": "should_be_ignored",
            "isAdmin": True,
        }

        # Should succeed but ignore extra fields
        result = secure_transform(
            data=data_with_extra_fields,
            model_class=TestBalance,
            context="field_injection_test",
            source_exchange="backpack",
        )

        assert result.asset == "BTC"
        assert result.quantity == Decimal("100.5")
        assert not hasattr(result, "__class__")
        assert not hasattr(result, "isAdmin")

    def test_constraint_validation_enforcement(self) -> None:
        """Test that all model constraints are enforced."""
        test_cases: list[dict[str, Any]] = [
            # Invalid side
            {
                "data": {
                    "symbol": "BTC-USD",
                    "side": "invalid_side",
                    "quantity": "10",
                    "price": "50000",
                    "order_type": "limit",
                },
                "error_contains": "side",
            },
            # Invalid order type
            {
                "data": {
                    "symbol": "BTC-USD",
                    "side": "buy",
                    "quantity": "10",
                    "price": "50000",
                    "order_type": "stop_loss",  # Not in allowed pattern
                },
                "error_contains": "order_type",
            },
            # Zero quantity (must be positive)
            {
                "data": {
                    "symbol": "BTC-USD",
                    "side": "buy",
                    "quantity": "0",
                    "price": "50000",
                    "order_type": "limit",
                },
                "error_contains": "quantity",
            },
            # Negative price
            {
                "data": {
                    "symbol": "BTC-USD",
                    "side": "buy",
                    "quantity": "10",
                    "price": "-50000",
                    "order_type": "limit",
                },
                "error_contains": "price",
            },
        ]

        for test_case in test_cases:
            with pytest.raises(TransformationError) as exc_info:
                secure_transform(
                    data=test_case["data"],
                    model_class=TestOrder,
                    context="constraint_test",
                    source_exchange="test",
                )

            assert "Validation failed" in str(exc_info.value)
            assert "validation errors" in str(exc_info.value)

    def test_decimal_precision_attacks(self) -> None:
        """Test handling of decimal precision attacks."""
        # Extremely precise decimal that could cause issues
        precision_attack_data = {
            "asset": "BTC",
            "quantity": "100.123456789012345678901234567890",  # 30 decimal places
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
        }

        # Should handle gracefully
        result = secure_transform(
            data=precision_attack_data,
            model_class=TestBalance,
            context="precision_test",
            source_exchange="backpack",
        )

        assert isinstance(result.quantity, Decimal)
        # Decimal should preserve precision
        assert str(result.quantity) == "100.123456789012345678901234567890"

    def test_string_length_attacks(self) -> None:
        """Test validation of string length constraints."""
        # Asset name too long
        long_asset_data = {
            "asset": "A" * 100,  # Exceeds max_length=10
            "quantity": "100",
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
        }

        with pytest.raises(TransformationError) as exc_info:
            secure_transform(
                data=long_asset_data,
                model_class=TestBalance,
                context="string_length_test",
                source_exchange="backpack",
            )

        assert "Validation failed" in str(exc_info.value)

    @patch("cyberdelta.utils.secure_transformation.logger")
    def test_security_logging(self, mock_logger: MagicMock) -> None:
        """Test that security events are properly logged."""
        # Test successful transformation logging
        valid_data = {
            "asset": "BTC",
            "quantity": "100",
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
        }

        # Verify successful transformation occurs
        _ = secure_transform(
            data=valid_data,
            model_class=TestBalance,
            context="security_log_test",
            source_exchange="backpack",
        )

        # Check debug log for successful transformation
        mock_logger.debug.assert_called()
        debug_call = str(mock_logger.debug.call_args)
        assert "Successful transformation" in debug_call
        assert "TestBalance" in debug_call

        # Test failed transformation logging
        mock_logger.reset_mock()
        invalid_data = {
            "asset": "BTC",
            "quantity": "-100",  # Invalid
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
        }

        with pytest.raises(TransformationError):
            secure_transform(
                data=invalid_data,
                model_class=TestBalance,
                context="security_fail_test",
                source_exchange="backpack",
            )

        # Check error log
        mock_logger.error.assert_called()
        error_call = str(mock_logger.error.call_args)
        assert "Transformation validation failed" in error_call
        assert "TestBalance" in error_call

    def test_secure_transform_with_audit_compliance(self) -> None:
        """Test audit logging for compliance requirements."""
        position_data = {
            "symbol": "BTC-USD",
            "size": "10.5",
            "entry_price": "50000",
            "mark_price": "51000",
            "pnl": "10500",
        }

        with patch("cyberdelta.utils.secure_transformation.logger") as mock_logger:
            result = secure_transform_with_audit(
                data=position_data,
                model_class=TestPosition,
                context="position_update",
                source_exchange="hyperliquid",
            )

            # Verify audit log was called
            mock_logger.info.assert_called()
            info_calls = [str(call) for call in mock_logger.info.call_args_list]

            # Should have audit log
            audit_log_found = any("AUDIT" in call for call in info_calls)
            assert audit_log_found

            # Verify result is correct
            assert result.symbol == "BTC-USD"
            assert result.size == Decimal("10.5")
            assert result.pnl == Decimal("10500")

    def test_concurrent_transformation_safety(self) -> None:
        """Test thread safety of secure_transform."""
        import threading

        results = []
        errors = []

        def transform_concurrently(data: dict[str, Any], should_fail: bool) -> None:
            try:
                result = secure_transform(
                    data=data,
                    model_class=TestBalance,
                    context=f"thread_{threading.current_thread().name}",
                    source_exchange="test",
                )
                results.append(result)
            except TransformationError as e:
                errors.append(e)

        threads = []

        # Create mix of valid and invalid data
        for i in range(20):
            if i % 2 == 0:
                # Valid data
                data = {
                    "asset": f"BTC{i}",
                    "quantity": str(i * 10),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "exchange": "test",
                }
                thread = threading.Thread(target=transform_concurrently, args=(data, False))
            else:
                # Invalid data (negative quantity)
                data = {
                    "asset": f"ETH{i}",
                    "quantity": str(-i * 10),
                    "timestamp": datetime.now(UTC).isoformat(),
                    "exchange": "test",
                }
                thread = threading.Thread(target=transform_concurrently, args=(data, True))

            threads.append(thread)

        # Run all threads
        for thread in threads:
            thread.start()

        for thread in threads:
            thread.join()

        # Verify results
        assert len(results) == 10  # 10 successful transformations
        assert len(errors) == 10  # 10 failed transformations

        # Verify each successful result
        for result in results:
            assert isinstance(result, TestBalance)
            assert result.quantity >= 0

    def test_memory_safety_large_objects(self) -> None:
        """Test that large objects don't cause memory issues."""
        # Create a large but valid data structure
        large_data = {
            "asset": "BTC",
            "quantity": "100",
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "test",
            # Add many extra fields that will be ignored
            **{f"extra_field_{i}": f"value_{i}" for i in range(10000)},
        }

        # Should handle without memory issues
        result = secure_transform(
            data=large_data, model_class=TestBalance, context="memory_test", source_exchange="test"
        )

        assert result.asset == "BTC"
        assert result.quantity == Decimal("100")

    def test_error_message_safety(self) -> None:
        """Test that error messages don't leak sensitive information."""
        sensitive_data = {
            "asset": "BTC",
            "quantity": "-100",  # Will fail validation
            "timestamp": datetime.now(UTC).isoformat(),
            "exchange": "backpack",
            "api_key": "secret_key_12345",  # Sensitive field
            "user_id": "user_123",
        }

        with pytest.raises(TransformationError) as exc_info:
            secure_transform(
                data=sensitive_data,
                model_class=TestBalance,
                context="sensitive_test",
                source_exchange="backpack",
            )

        error_message = str(exc_info.value)

        # Should not contain sensitive data
        assert "secret_key_12345" not in error_message
        assert "user_123" not in error_message

        # Should contain generic error info
        assert "Validation failed" in error_message
        assert "sensitive_test" in error_message


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
