from abc import ABC, abstractmethod
from typing import Any, TypedDict


class AuthenticatedRequestComponents(TypedDict):
    """Data structure for components of an authenticated request."""

    headers: dict[str, str]
    params: dict[str, Any] | None
    data: dict[str, Any] | None


class IAuthenticator(ABC):
    """
    Interface for request authentication strategies.
    """

    @abstractmethod
    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: dict[str, Any] | None,  # Added headers for context, e.g. content-type
    ) -> AuthenticatedRequestComponents:
        """
        Prepares and signs an API request.

        Args:
            method: The HTTP method (e.g., 'GET', 'POST').
            path: The API endpoint path.
            params: Optional dictionary of query parameters.
            data: Optional dictionary of request body data (for POST/PUT).
            headers: Optional dictionary of existing headers to be included or modified.

        Returns:
            An AuthenticatedRequestComponents TypedDict containing the necessary
            headers, params, and data for the authenticated request.
        """
        pass
