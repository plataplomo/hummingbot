"""Exchange Authentication Interface.

This module defines the abstract interface for exchange-specific authentication
implementations across different trading platforms.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict


class AuthenticatedRequestComponents(BaseModel):
    """Data structure for components of an authenticated request."""

    headers: Mapping[str, str]
    params: dict[str, Any] | None = None
    data: dict[str, Any] | None = None
    model_config = ConfigDict(extra="forbid", frozen=True)


class IAuthenticator(ABC):
    """Interface for request authentication strategies."""

    @abstractmethod
    async def prepare_request(
        self,
        method: str,
        path: str,
        params: dict[str, Any] | None,
        data: dict[str, Any] | None,
        headers: Mapping[str, Any] | None,
    ) -> AuthenticatedRequestComponents:
        """Prepares and signs an API request.

        Args:
            method: The HTTP method (e.g., 'GET', 'POST').
            path: The API endpoint path (relative path).
            params: Optional dictionary of query parameters.
            data: Optional dictionary of request body data (for POST/PUT).
            headers: Optional mapping of existing headers to be included or modified.

        Returns:
            An AuthenticatedRequestComponents Pydantic model containing the necessary
            headers, params, and data for the authenticated request.

        """
