"""Centralized time control fixtures for CyberDeltaEngine tests.

This module provides standardized time mocking and control fixtures for use across
all test types. It centralizes time-related testing utilities to ensure consistency
and reduce code duplication.

Fixtures:
    freezer: The pytest-freezer fixture (implicitly provided by pytest-freezer plugin)
    frozen_time: High-level fixture for freezing time at a specific point
    mock_time_factory: Factory for creating time mocks with standard patterns
"""

from collections.abc import Callable, Generator
from datetime import UTC, datetime
from typing import Any, Protocol
from unittest.mock import MagicMock, patch

import pytest


class FreezerProtocol(Protocol):
    """Protocol for pytest-freezer fixture.

    This protocol defines the interface for the freezer fixture provided by
    the pytest-freezer plugin. It allows type-safe usage of the fixture
    throughout the test suite.
    """

    def move_to(self, target: datetime | str) -> None:
        """Move the frozen time to the target datetime.

        Args:
            target: The datetime to freeze time at, or a string representation
        """
        ...


@pytest.fixture
def frozen_time(freezer: FreezerProtocol) -> Generator[FreezerProtocol]:
    """Provides frozen time control for deterministic tests.

    This fixture wraps the pytest-freezer fixture to provide additional
    functionality and a consistent interface for time control in tests.

    Example:
        def test_with_frozen_time(frozen_time):
            frozen_time.move_to("2024-01-01 12:00:00")
            assert datetime.now(UTC).hour == 12

    Yields:
        FreezerProtocol: The freezer instance for time control
    """
    # Start at a deterministic time for consistency
    freezer.move_to("2024-01-01 00:00:00+00:00")
    yield freezer


@pytest.fixture
def mock_time_factory() -> Generator[Callable[..., Any]]:
    """Factory for creating time mocks with standard patterns.

    This fixture provides a factory function that creates properly configured
    time mocks for different testing scenarios. It helps standardize time
    mocking patterns across the test suite.

    Example:
        def test_with_time_mock(mock_time_factory):
            mock_dt = mock_time_factory(
                module_path="cyberdelta.core.models.trade_signal.datetime",
                fixed_time=datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
            )
            with mock_dt:
                # Code under test uses mocked time
                pass

    Yields:
        Callable: Factory function for creating time mocks
    """

    def create_time_mock(
        module_path: str,
        fixed_time: datetime | None = None,
        side_effect: Callable[[], datetime] | None = None,
    ) -> Any:
        """Create a time mock for the specified module.

        Args:
            module_path: Full path to the datetime module to mock
            fixed_time: Fixed datetime to return (mutually exclusive with side_effect)
            side_effect: Function to call for dynamic time values

        Returns:
            Mock context manager

        Raises:
            ValueError: If both fixed_time and side_effect are provided
        """
        if fixed_time and side_effect:
            raise ValueError("Cannot specify both fixed_time and side_effect")

        if fixed_time:
            mock_datetime = MagicMock()
            mock_datetime.now = MagicMock(return_value=fixed_time)
            mock_datetime.utcnow = MagicMock(return_value=fixed_time.replace(tzinfo=None))
            return patch(module_path, mock_datetime)
        elif side_effect:
            mock_datetime = MagicMock()
            mock_datetime.now = MagicMock(side_effect=side_effect)
            return patch(module_path, mock_datetime)
        else:
            # Default to current time
            now = datetime.now(UTC)
            mock_datetime = MagicMock()
            mock_datetime.now = MagicMock(return_value=now)
            mock_datetime.utcnow = MagicMock(return_value=now.replace(tzinfo=None))
            return patch(module_path, mock_datetime)

    yield create_time_mock


@pytest.fixture
def mock_time_patch() -> Generator[MagicMock]:
    """Mock time.time() for auth and rate limiting tests.

    This fixture provides a standardized way to mock time.time() calls,
    commonly used in authentication signatures and rate limiting logic.

    Example:
        def test_auth_signature(mock_time_patch):
            mock_time_patch.return_value = 1678886400.0
            # Test auth signature generation

    Yields:
        MagicMock: The time.time mock
    """
    with patch("time.time", return_value=1678886400.0) as mock_time:
        yield mock_time


@pytest.fixture
def market_time_simulation(freezer: FreezerProtocol) -> Generator[Callable[..., None]]:
    """Fixture for simulating market hours and timing.

    Provides utilities for simulating different market conditions and
    time-based scenarios in trading tests.

    Example:
        def test_market_hours(market_time_simulation):
            market_time_simulation(market="NYSE", hour=9, minute=30)  # Market open
            # Test market open behavior

    Yields:
        Callable: Function to set market time
    """

    def set_market_time(
        market: str = "24/7",  # Default to crypto markets
        year: int = 2024,
        month: int = 1,
        day: int = 1,
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
        timezone: str = "UTC",
    ) -> None:
        """Set the current time for market simulation.

        Args:
            market: Market identifier (e.g., "NYSE", "24/7")
            year, month, day, hour, minute, second: Time components
            timezone: Timezone string
        """
        from zoneinfo import ZoneInfo

        target_time = datetime(year, month, day, hour, minute, second, tzinfo=ZoneInfo(timezone))
        freezer.move_to(target_time)

    yield set_market_time


@pytest.fixture
def rate_limit_timer(freezer: FreezerProtocol) -> Generator[Callable[..., None]]:
    """Fixture for rate limiting tests with precise timing control.

    Provides utilities for testing rate limiting behavior by controlling
    time progression in tests.

    Example:
        def test_rate_limit(rate_limit_timer):
            # Make request
            rate_limit_timer(advance_seconds=0.1)  # Advance 100ms
            # Make another request

    Yields:
        Callable: Function to advance time
    """

    def advance_time(
        advance_seconds: float = 0,
        advance_minutes: float = 0,
        advance_hours: float = 0,
    ) -> None:
        """Advance the frozen time by the specified amount.

        Args:
            advance_seconds: Seconds to advance
            advance_minutes: Minutes to advance
            advance_hours: Hours to advance
        """
        from datetime import timedelta

        # Get current frozen time
        current = datetime.now(UTC)

        # Calculate total advance
        total_advance = timedelta(
            seconds=advance_seconds,
            minutes=advance_minutes,
            hours=advance_hours,
        )

        # Move to new time
        freezer.move_to(current + total_advance)

    yield advance_time
