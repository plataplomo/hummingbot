"""Template for unit tests following mandatory test patterns.

This template ensures every test file includes SUCCESS, EDGE, and FAILURE cases.
"""

from collections.abc import Mapping
from typing import Any
from unittest.mock import Mock

import pytest


class ServiceError(Exception):
    """Custom service error for template."""


# Dummy class for template demonstration
class MyClass:
    """Example class for template purposes."""

    def __init__(self, dependency: Mock | None = None) -> None:
        """Initialize with optional dependency."""
        self.dependency = dependency
        self._internal_state: str = "valid"

    @staticmethod
    def process(data: Mapping[str, Any]) -> str:
        """Process data and return result.

        Args:
            data: Input data mapping to process

        Returns:
            str: Processed result string

        Raises:
            KeyError: If 'key' is missing from data
            ValueError: If 'key' is None or 'number' is negative
            TypeError: If 'number' is not an integer
        """
        if "key" not in data:
            raise KeyError("key")
        if data.get("key") is None:
            raise ValueError("key cannot be None")
        number = data.get("number")
        if not isinstance(number, int):
            raise TypeError("number must be int")
        if number < 0:
            raise ValueError("number must be positive")
        return "expected output"

    @staticmethod
    def process_single(value: str | float) -> str | int | float:
        """Process single value.

        Args:
            value: Value to process

        Returns:
            str | int | float: The processed value

        Raises:
            ValueError: If value is the string 'error'
        """
        if isinstance(value, str) and value == "error":
            raise ValueError("Invalid value")
        return value

    def process_with_dependency(self, _data: Mapping[str, Any]) -> dict[str, Any]:
        """Process using dependency.

        Args:
            _data: Input data (unused in this template example)

        Returns:
            dict[str, Any]: Result from dependency fetch operation

        Raises:
            ServiceError: If no dependency is configured or dependency is unavailable
        """
        if self.dependency is None:
            raise ServiceError("No dependency configured")
        try:
            result: dict[str, Any] = self.dependency.fetch()
        except ConnectionError:
            raise ServiceError("Dependency unavailable") from None
        else:
            return result

    def process_instance(self, data: Mapping[str, Any]) -> str:
        """Instance method that checks internal state.

        Args:
            data: Input data mapping to process

        Returns:
            str: Processed result string

        Raises:
            RuntimeError: If internal state is invalid
        """
        if self._internal_state != "valid":
            raise RuntimeError("Invalid state")
        return self.process(data)


class TestMyComponent:
    """Test suite for MyComponent with success, edge, and failure cases."""

    # ==================== SUCCESS CASES ====================

    def test_component_success_typical_values(self) -> None:
        """Test successful execution with typical valid inputs."""
        # Arrange
        input_data = {"key": "value", "number": 42}
        expected_result = "expected output"

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result == expected_result
        assert isinstance(result, str)

    def test_component_success_alternate_path(self) -> None:
        """Test successful execution with alternate valid inputs."""
        # Arrange
        input_data = {"key": "alternate", "number": 100}

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result is not None
        assert len(result) > 0

    def test_component_success_with_optional_params(self) -> None:
        """Test successful execution with optional parameters included."""
        # Arrange
        input_data = {"key": "value", "number": 42, "optional": "extra"}

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result == "expected output"

    # ==================== EDGE CASES ====================

    def test_component_edge_zero_values(self) -> None:
        """Test behavior with zero values."""
        # Arrange
        input_data = {"key": "value", "number": 0}

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result == "expected output"

    def test_component_edge_boundary_values(self) -> None:
        """Test behavior at min/max boundaries."""
        # Arrange
        min_input = {"key": "min", "number": 1}
        max_input = {"key": "max", "number": 999999}

        # Act
        min_result = MyClass.process(min_input)
        max_result = MyClass.process(max_input)

        # Assert
        assert min_result is not None
        assert max_result is not None

    def test_component_edge_empty_collections(self) -> None:
        """Test behavior with empty lists/dicts."""
        # Arrange
        input_data: dict[str, Any] = {
            "key": "",
            "number": 0,
            "list": [],
            "dict": {},
        }

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result == "expected output"

    def test_component_edge_unicode_special_chars(self) -> None:
        """Test behavior with unicode and special characters."""
        # Arrange
        input_data = {"key": "测试🚀", "number": 42}

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result == "expected output"

    def test_component_edge_very_large_inputs(self) -> None:
        """Test behavior with very large inputs."""
        # Arrange
        input_data = {"key": "x" * 10000, "number": 10**18}

        # Act
        result = MyClass.process(input_data)

        # Assert
        assert result is not None

    # ==================== FAILURE CASES ====================

    def test_component_failure_none_required_field(self) -> None:
        """Test handling of None for required fields."""
        # Arrange
        input_data = {"key": None, "number": 42}

        # Act & Assert
        with pytest.raises(ValueError, match="key cannot be None"):
            MyClass.process(input_data)

    def test_component_failure_invalid_type(self) -> None:
        """Test handling of wrong type inputs."""
        # Arrange
        input_data = {"key": "value", "number": "not_a_number"}

        # Act & Assert
        with pytest.raises(TypeError, match="number must be int"):
            MyClass.process(input_data)

    def test_component_failure_missing_required_field(self) -> None:
        """Test handling of missing required fields."""
        # Arrange
        input_data = {"number": 42}  # Missing 'key'

        # Act & Assert
        with pytest.raises(KeyError, match="key"):
            MyClass.process(input_data)

    def test_component_failure_invalid_state(self) -> None:
        """Test handling of error conditions through public API."""
        # This test demonstrates testing error conditions through public interface only
        # We test dependency failures which can be triggered through public methods

        # Arrange - create instance without dependency to test error handling
        instance = MyClass(dependency=None)

        # Act & Assert
        # Test the public interface behavior when dependency is missing
        with pytest.raises(ServiceError, match="No dependency configured"):
            # Call public method that requires dependency
            instance.process_with_dependency({"key": "value", "number": 42})

    def test_component_failure_constraint_violation(self) -> None:
        """Test handling of business rule violations."""
        # Arrange
        input_data = {"key": "value", "number": -1}  # Negative not allowed

        # Act & Assert
        with pytest.raises(ValueError, match="number must be positive"):
            MyClass.process(input_data)


# ==================== PARAMETRIZED TESTS ====================


@pytest.mark.parametrize(
    ("input_value", "expected", "description"),
    [
        # Success cases
        ("valid", "valid", "typical valid input"),
        ("VALID", "VALID", "uppercase variant"),
        # Edge cases
        ("", "", "empty string"),
        ("a" * 100, "a" * 100, "long string"),
        # Failure cases - use 'error' string to trigger exception
        ("error", ValueError, "error input triggers exception"),
    ],
)
def test_component_parametrized_scenarios(
    input_value: str | float | None,
    expected: str | float | type[Exception],
    description: str,
) -> None:
    """Test multiple scenarios with parametrization."""
    # Description is used for test documentation and debugging
    assert description, "Test description should not be empty"
    if isinstance(expected, type):
        # This is a failure case
        with pytest.raises(expected):
            # Type assertion for mypy - we know it won't be None in the test cases
            assert input_value is not None
            MyClass.process_single(input_value)
    else:
        # This is a success or edge case
        # Type assertion for mypy - we know it won't be None in the test cases
        assert input_value is not None
        result = MyClass.process_single(input_value)
        assert result == expected


# ==================== INTEGRATION TESTS (if applicable) ====================


class TestMyComponentIntegration:
    """Integration tests with dependencies."""

    @pytest.fixture
    def mock_dependency(self) -> Mock:
        """Mock external dependency.

        Returns:
            Mock: Mock object for external dependency
        """
        return Mock()

    def test_integration_with_dependency_success(self, mock_dependency: Mock) -> None:
        """Test successful integration with mocked dependency."""
        # Arrange
        mock_dependency.fetch.return_value = {"status": "ok"}
        instance = MyClass(dependency=mock_dependency)

        # Act
        result = instance.process_with_dependency({"key": "value"})

        # Assert
        assert result["status"] == "ok"
        mock_dependency.fetch.assert_called_once()

    def test_integration_with_dependency_failure(self, mock_dependency: Mock) -> None:
        """Test handling of dependency failures."""
        # Arrange
        mock_dependency.fetch.side_effect = ConnectionError("Network error")
        instance = MyClass(dependency=mock_dependency)

        # Act & Assert
        with pytest.raises(ServiceError, match="Dependency unavailable"):
            instance.process_with_dependency({"key": "value"})
