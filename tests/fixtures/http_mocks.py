"""HTTP mocking utilities for testing API clients.

This module provides mock classes for aiohttp ClientSession and Response
to enable testing of HTTP-based API clients without making real network requests.
"""

from __future__ import annotations

from collections.abc import Callable
from types import TracebackType
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest


class MockResponse:
    """Mock aiohttp response for testing API clients."""

    def __init__(
        self,
        data: object,  # Test data can be any JSON-serializable object
        status: int = 200,
        headers: dict[str, str] | None = None,
        content_type: str = "application/json",
        text_data: str | None = None,  # Added for direct initialization
    ) -> None:
        """Initialize mock response with test data and status.

        Args:
            data: JSON-serializable test data
            status: HTTP status code (default: 200)
            headers: HTTP response headers
            content_type: Response content type
            text_data: Raw text data for response

        """
        self._data = data
        self.status = status
        self.headers = headers if headers is not None else {}  # Ensure headers is a dict
        self.content_type = content_type
        self._raise_for_status_called = False

        # Attributes for mocking
        effective_text_data = text_data if text_data is not None else str(self._data)
        self.text: AsyncMock = AsyncMock(return_value=effective_text_data)
        self.raise_for_status: MagicMock = MagicMock()
        if self.status >= 400:
            # aiohttp's ClientResponseError headers expect a MultiMapping or None.
            # For simplicity in mock, we pass our dict; aiohttp might handle basic dicts.
            # Or, pass `None` if type issues persist: `headers=None`
            minimal_request_info = MagicMock()
            minimal_request_info.url = "mock://url"
            minimal_request_info.method = "GET"
            minimal_request_info.headers = self.headers
            minimal_request_info.real_url = "mock://real_url"

            self.raise_for_status.side_effect = aiohttp.ClientResponseError(
                request_info=minimal_request_info,
                history=(),
                status=self.status,
                message="Mock ResponseError",
                headers=cast("Any", self.headers),
            )

    async def json(self) -> object:  # JSON data can be any serializable object
        """Return JSON data from response.

        Returns:
            object: The JSON-serializable data stored in this mock response.
        """
        return self._data

    async def __aenter__(self) -> MockResponse:
        """Enter async context manager.

        Returns:
            MockResponse: This mock response instance.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager."""


class MockClientSession:
    """Mock aiohttp ClientSession for testing HTTP clients."""

    def __init__(
        self,
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> None:
        """Initialize mock session with predefined responses.

        Args:
            responses: Mapping of (method, url) tuples to mock responses

        """
        self.responses = responses or {}
        self.requests: list[dict[str, Any]] = []  # Flexible for test requests
        self.closed = False

    async def __aenter__(self) -> MockClientSession:
        """Enter async context manager.

        Returns:
            MockClientSession: This mock session instance.
        """
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager."""

    async def close(self) -> None:
        """Close the session."""
        self.closed = True

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: dict[str, Any],
    ) -> MockResponse:  # Accepts any kwargs
        """Internal method to handle HTTP requests.

        Returns:
            MockResponse: Mock response matching the request or a 404 response.
        """
        self.requests.append({"method": method, "url": url, "kwargs": kwargs})

        # Find match in responses
        for pattern, response in self.responses.items():
            if (method, url) == pattern or (
                (method, pattern[1]) == pattern and url.startswith(pattern[1])
            ):
                return response

        # Default response if no match
        return MockResponse({}, status=404)

    async def get(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute GET request.

        Returns:
            MockResponse: Mock response for the GET request.
        """
        return await self._request("GET", url, **kwargs)

    async def post(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute POST request.

        Returns:
            MockResponse: Mock response for the POST request.
        """
        return await self._request("POST", url, **kwargs)

    async def put(self, url: str, **kwargs: dict[str, Any]) -> MockResponse:  # Accepts any kwargs
        """Execute PUT request.

        Returns:
            MockResponse: Mock response for the PUT request.
        """
        return await self._request("PUT", url, **kwargs)

    async def delete(
        self,
        url: str,
        **kwargs: dict[str, Any],
    ) -> MockResponse:  # Accepts any kwargs
        """Execute DELETE request.

        Returns:
            MockResponse: Mock response for the DELETE request.
        """
        return await self._request("DELETE", url, **kwargs)


# --- Pytest Fixtures ---


@pytest.fixture
def mock_client_session() -> Callable[
    [dict[tuple[str, str], MockResponse] | None],
    MockClientSession,
]:
    """Fixture to provide a mock aiohttp ClientSession.

    Returns:
        Callable: Factory function that creates MockClientSession instances.
    """

    def create_session(
        responses: dict[tuple[str, str], MockResponse] | None = None,
    ) -> MockClientSession:
        """Create session for testing.

        Returns:
            MockClientSession: New mock session with predefined responses.
        """
        return MockClientSession(responses)

    return create_session


# --- Helper Functions ---


def create_mock_response(
    status: int = 200,
    json_data: object | None = None,  # JSON data can be any serializable object
    text_data: str | None = None,
    headers: dict[str, str] | None = None,
) -> MockResponse:
    """Create mock response for testing.

    Returns:
        MockResponse: Mock response configured with the provided parameters.
    """
    mock_resp = MockResponse(
        json_data,
        status,
        headers,
        "application/json",
        text_data=text_data if text_data is not None else str(json_data),
    )
    # Attributes are now set in MockResponse.__init__
    if status >= 400 and mock_resp.raise_for_status.side_effect is None:
        minimal_request_info = MagicMock()
        minimal_request_info.url = "mock://url"
        minimal_request_info.method = "GET"
        minimal_request_info.headers = headers
        minimal_request_info.real_url = "mock://real_url"
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            minimal_request_info,
            (),
            status=status,
            message="Mock Response Error",
            headers=cast("Any", headers),
        )
    return mock_resp


def mock_request(
    method: str,
    url: str,
    *,
    params: dict[str, Any] | None = None,
    data: object | None = None,  # Request data can be any serializable object
    json: object | None = None,  # JSON data can be any serializable object
    headers: dict[str, Any] | None = None,
    status_code: int = 200,
    **kwargs: object,  # Additional kwargs for flexibility
) -> MockResponse:
    """Create mock HTTP request for testing.

    Returns:
        MockResponse: Mock response representing the HTTP request result.
    """
    text_data = str(json) if json else ""
    actual_headers = headers or {}
    mock_resp = MockResponse(
        json,
        status_code,
        actual_headers,
        "application/json",
        text_data=text_data,
    )
    # Attributes are now set in MockResponse.__init__
    if status_code >= 400 and mock_resp.raise_for_status.side_effect is None:
        minimal_request_info = MagicMock()
        minimal_request_info.url = "mock://url"
        minimal_request_info.method = "GET"
        minimal_request_info.headers = actual_headers
        minimal_request_info.real_url = "mock://real_url"
        mock_resp.raise_for_status.side_effect = aiohttp.ClientResponseError(
            minimal_request_info,
            (),
            status=status_code,
            message="Mock Response Error",
            headers=cast("Any", actual_headers),
        )
    return mock_resp
