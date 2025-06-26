"""Defines the interface for mapping exchange-specific errors to standardized APIError instances."""

from abc import ABC, abstractmethod
from typing import Any

from cyberdelta.apis.models.api_error import APIError


class IErrorMapper(ABC):
    """Interface for mapping exchange-specific error responses to a common APIError format."""

    @abstractmethod
    def map_exchange_error(
        self,
        status_code: int,
        error_body: str | None,
        error_data: dict[str, Any] | None,
        request_path: str | None = None,
        original_exception: Exception | None = None,
    ) -> APIError:
        """Maps a raw exchange error to a standardized APIError.

        Converts HTTP status, body, or parsed data from exchange responses.

        Implementations should handle specifics of their exchange's error reporting.
        This can also be used to map errors derived from other exceptions.
        """

    @abstractmethod
    def map_string_error(self, error_message: str, http_status: int | None = None) -> APIError:
        """Maps a raw error string from an exchange to a standardized APIError.

        Useful when the error is not from a typical HTTP error response but embedded in data.
        """
