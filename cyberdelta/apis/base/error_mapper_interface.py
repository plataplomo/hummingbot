"""
Defines the interface for mapping exchange-specific errors to standardized APIError instances.
"""

from abc import ABC, abstractmethod
from typing import Any

from cyberdelta.apis.models.api_error import APIError


class IErrorMapper(ABC):
    """
    Interface for mapping exchange-specific error responses to a common APIError format.
    """

    @abstractmethod
    def map_exchange_error(
        self,
        status_code: int,
        error_body: str,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
    ) -> APIError:
        """
        Maps raw exchange error details to a standardized APIError.

        Args:
            status_code: The HTTP status code received from the exchange.
            error_body: The raw error response body as a string.
            error_data: The parsed error response body as a dictionary, if parsing was successful.
            request_path: The specific API endpoint path that was called, if available.

        Returns:
            An APIError instance representing the mapped error.
        """
        pass
