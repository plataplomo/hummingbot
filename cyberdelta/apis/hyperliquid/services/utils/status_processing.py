"""Status processing utilities for Hyperliquid exchange responses.

This module provides utilities for processing various status formats from Hyperliquid,
extracted from the monolithic trading service to improve maintainability and reusability.
"""

from typing import Any, cast

from cyberdelta.apis.common import APIErrorCode
from cyberdelta.apis.exceptions import OrderError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.config.structlog_config import get_logger


logger = get_logger(__name__)


def process_exchange_status(
    status_raw: object,
    action_description: str,
) -> dict[str, Any]:
    """Process raw exchange status into a standardized format.

    Handles different status formats from Hyperliquid API responses,
    including Pydantic models, dictionaries, and strings.

    Args:
        status_raw: Raw status object from exchange response
        action_description: Description of the action for error context

    Returns:
        Dictionary containing standardized status information

    Raises:
        OrderError: If status format is unrecognized or invalid
    """
    # Handle Pydantic model status
    if isinstance(status_raw, HyperliquidRawExchangeStatusObject):
        return _process_pydantic_status(status_raw, action_description)

    # Handle dict status (for backwards compatibility)
    if isinstance(status_raw, dict):
        status_dict = cast("dict[str, Any]", status_raw)
        result = _process_dict_status(status_dict, action_description)
        if result:  # If we found a recognized status
            return result

    # Handle string status
    elif isinstance(status_raw, str):
        return _process_string_status(status_raw, action_description)

    # Unknown status type
    raise OrderError(
        message=f"Unknown status structure for {action_description}: {status_raw!r}",
        code=APIErrorCode.INVALID_RESPONSE.value,
    )


def check_error_response(
    raw_exchange_response: HyperliquidRawExchangeResponse,
    http_status: int,
    error_mapper: HyperliquidErrorMapper,
) -> None:
    """Check if the response is an error and raise appropriate exception.

    Args:
        raw_exchange_response: Raw response from Hyperliquid
        http_status: HTTP status code
        error_mapper: Error mapper for converting Hyperliquid errors

    Raises:
        APIError: If response contains an error
    """
    if (raw_exchange_response.status == "err" and raw_exchange_response.response) and (
        isinstance(raw_exchange_response.response, str)
    ):
        # Use the error mapper to get the specific error code for this message
        mapped_error = error_mapper.map_string_error(
            raw_exchange_response.response,
            http_status=http_status,
        )
        raise mapped_error


def validate_batch_response_counts(
    expected_count: int,
    response_count: int,
    context: str,
) -> None:
    """Validate that batch response count matches expected count.

    Args:
        expected_count: Expected number of responses
        response_count: Actual number of responses
        context: Context description for error messages

    Raises:
        ValueError: If counts don't match
    """
    if response_count != expected_count:
        error_msg = (
            f"Response count mismatch in {context}: expected {expected_count}, got {response_count}"
        )
        logger.error(
            "batch_response_count_mismatch",
            context=context,
            expected=expected_count,
            actual=response_count,
        )
        raise ValueError(error_msg)


def _process_pydantic_status(
    status_raw: HyperliquidRawExchangeStatusObject,
    action_description: str,
) -> dict[str, Any]:
    """Process Pydantic model status.

    Args:
        status_raw: Pydantic status object
        action_description: Description for error context

    Returns:
        Dictionary containing processed status information

    Raises:
        OrderError: If status structure is unrecognized
    """
    if status_raw.resting:
        return {"resting": status_raw.resting}
    if status_raw.filled:
        return {"filled": status_raw.filled}
    if status_raw.error:
        return {"error": status_raw.error}

    raise OrderError(
        message=f"Unknown status structure for {action_description}",
        code=APIErrorCode.INVALID_RESPONSE.value,
    )


def _process_dict_status(
    status_raw: dict[str, Any],
    action_description: str,
) -> dict[str, Any]:
    """Process dict status (for backwards compatibility).

    Args:
        status_raw: Status dictionary
        action_description: Description for error context

    Returns:
        Dictionary containing processed status, empty if no recognized status
    """
    # Check for resting status
    if "resting" in status_raw:
        return _process_dict_resting_status(status_raw, action_description)

    # Check for filled status
    if "filled" in status_raw:
        return _process_dict_filled_status(status_raw, action_description)

    # Check for canceled status
    if "canceled" in status_raw:
        return _process_dict_canceled_status(status_raw, action_description)

    # Check for error status
    if "error" in status_raw:
        return {"error": status_raw["error"]}

    # No recognized status found
    return {}


def _process_dict_resting_status(
    status_raw: dict[str, Any],
    action_description: str,
) -> dict[str, Any]:
    """Process dict resting status.

    Args:
        status_raw: Status dictionary with resting key
        action_description: Description for error context

    Returns:
        Dictionary containing processed resting status

    Raises:
        OrderError: If oid is invalid or missing
    """
    oid = status_raw["resting"].get("oid")
    if not isinstance(oid, int):
        raise OrderError(
            message=f"Invalid or missing 'oid' in resting status for {action_description}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )
    return {"resting": HyperliquidRawExchangeStatusResting(oid=oid)}


def _process_dict_filled_status(
    status_raw: dict[str, Any],
    action_description: str,
) -> dict[str, Any]:
    """Process dict filled status.

    Args:
        status_raw: Status dictionary with filled key
        action_description: Description for error context

    Returns:
        Dictionary containing processed filled status

    Raises:
        OrderError: If filled status data is invalid
    """
    filled_details = status_raw["filled"]
    oid = filled_details.get("oid")
    total_sz = filled_details.get("totalSz")
    avg_px = filled_details.get("avgPx")

    if not isinstance(oid, int):
        raise OrderError(
            message=f"Invalid or missing 'oid' in filled status for {action_description}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    if not isinstance(total_sz, str) or not isinstance(avg_px, str):
        raise OrderError(
            message=f"Invalid filled status data for {action_description}",
            code=APIErrorCode.INVALID_RESPONSE.value,
        )

    return {
        "filled": HyperliquidRawExchangeStatusFilled(
            oid=oid,
            totalSz=total_sz,
            avgPx=avg_px,
        ),
    }


def _process_dict_canceled_status(
    status_raw: dict[str, Any],
    action_description: str,
) -> dict[str, Any]:
    """Process dict canceled status.

    Args:
        status_raw: Status dictionary with canceled key
        action_description: Description for error context

    Returns:
        Dictionary containing processed canceled status
    """
    # For canceled status, we just need to confirm it exists
    return {"canceled": {"type": "dict"}}


def _process_string_status(status_raw: str, action_description: str) -> dict[str, Any]:
    """Process string status.

    Args:
        status_raw: Status string
        action_description: Description for error context

    Returns:
        Dictionary containing processed string status
    """
    status_lower = status_raw.lower()

    # Handle success statuses
    if status_lower in {"success", "ok", "accepted"}:
        return {"success": status_raw}

    # Handle canceled status
    if status_lower == "canceled":
        return {"canceled": {"type": "string"}}

    # Any other string is treated as an error
    logger.warning(
        "direct_string_status_error",
        message="Encountered direct string status for %s: '%s'. Treating as error.",
        message_args=(action_description, status_raw),
    )
    return {"error": status_raw}
