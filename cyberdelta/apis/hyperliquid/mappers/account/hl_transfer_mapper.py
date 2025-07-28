"""Hyperliquid Transfer Mapper - Maps raw transfer responses to internal Transfer models.

This mapper follows the exact same pattern as BackpackTransferMapper, transforming
Hyperliquid's raw transfer API responses into CyberDeltaEngine's internal Transfer model.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from cyberdelta.apis.hyperliquid.models.hl_raw_usd_transfer_response import (
    HyperliquidRawUsdTransferResponse,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import InternalTransferStatus
from cyberdelta.core.models.operations import HyperliquidTransferDetails, Transfer
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class HyperliquidTransferMapper:
    """Maps Hyperliquid raw transfer responses to internal Transfer models."""

    @staticmethod
    def transform_raw_transfer_to_internal(
        raw_response: HyperliquidRawUsdTransferResponse,
        exchange_name: str,
        asset: str,
        quantity: Decimal,
        from_account_type_raw: str,
        to_account_type_raw: str,
        client_transfer_id: str | None,
    ) -> Transfer:
        """Transform raw Hyperliquid transfer response to internal Transfer model.

        Args:
            raw_response: Validated HyperliquidRawUsdTransferResponse from /exchange endpoint
            exchange_name: Exchange name for logging and tracking
            asset: Asset symbol that was transferred
            quantity: Amount that was transferred
            from_account_type_raw: Source account type (spot/perp)
            to_account_type_raw: Destination account type (spot/perp)
            client_transfer_id: Optional client-provided transfer ID

        Returns:
            Internal Transfer model with Hyperliquid-specific details
        """
        logger.debug(
            "hyperliquid_transfer_mapper_transform_start",
            exchange=exchange_name,
            asset=asset,
            quantity=str(quantity),
            from_account=from_account_type_raw,
            to_account=to_account_type_raw,
            client_id=client_transfer_id,
            response_status=raw_response.status,
            has_response_data=raw_response.response is not None,
            message=(
                f"[{exchange_name}] Starting transformation of raw transfer response "
                f"to internal model"
            ),
        )

        # Extract transfer ID from response (generate one since transfers don't return IDs)
        transfer_id = f"transfer_{exchange_name}_{asset}_{quantity}"

        # Map transfer status to internal enum
        internal_status = HyperliquidTransferMapper._map_status_to_internal(raw_response.status)

        # Use current time for timestamp (transfers don't return timestamps)
        timestamp = HyperliquidTransferMapper._get_current_timestamp()

        # Extract response message for debugging
        response_message = None
        if raw_response.status == "err" and isinstance(raw_response.response, str):
            response_message = raw_response.response
        elif raw_response.status == "ok":
            response_message = "Transfer completed successfully"

        # Create Hyperliquid-specific details
        hl_details = HyperliquidTransferDetails(
            from_user=None,  # Internal transfers don't have from_user
            to_user=None,  # Internal transfers don't have to_user
        )

        # Use secure_transform for type-safe model creation
        transfer_data = {
            "id": str(transfer_id),
            "exchange": exchange_name,
            "status": internal_status,
            "asset": asset,
            "quantity": quantity,
            "timestamp": timestamp,
            "response_message": response_message,
            "hl_details": hl_details,
        }

        # Use secure_transform for type-safe model creation
        transfer = secure_transform(
            data=transfer_data,
            model_class=Transfer,
            context=f"Hyperliquid transfer transformation for {asset}",
        )

        logger.info(
            "hyperliquid_transfer_mapper_transform_complete",
            exchange=exchange_name,
            transfer_id=transfer.id,
            status=transfer.status.value,
            asset=transfer.asset,
            quantity=str(transfer.quantity),
            timestamp=transfer.timestamp.isoformat(),
            message=(
                f"[{exchange_name}] Successfully transformed transfer response to internal model"
            ),
        )

        return transfer

    @staticmethod
    def _get_current_timestamp() -> datetime:
        """Get current timestamp for transfers that don't return timestamps.
        
        Returns:
            datetime: Current UTC datetime.
        """
        return datetime.now(UTC)

    @staticmethod
    def _map_status_to_internal(raw_status: str) -> InternalTransferStatus:
        """Map Hyperliquid transfer status to internal enum.

        Args:
            raw_status: Raw status string from Hyperliquid API

        Returns:
            Corresponding internal transfer status enum
        """
        status_lower = raw_status.lower() if raw_status else ""

        # Hyperliquid status mapping for transfer responses
        if status_lower == "ok":
            return InternalTransferStatus.COMPLETED
        if status_lower == "err":
            return InternalTransferStatus.FAILED
        logger.warning(
            "hyperliquid_transfer_unknown_status",
            raw_status=raw_status,
            mapped_to="UNKNOWN",
            message=f"Unknown Hyperliquid transfer status: {raw_status}, mapping to UNKNOWN",
        )
        return InternalTransferStatus.UNKNOWN

    @staticmethod
    def _parse_timestamp(raw_response: dict[str, object]) -> datetime:
        """Parse timestamp from Hyperliquid response.

        Args:
            raw_response: Raw JSON response containing timestamp

        Returns:
            Parsed datetime object, defaults to current time if parsing fails
        """
        timestamp_ms = raw_response.get("timestamp") or raw_response.get("time")

        if not isinstance(timestamp_ms, (int, float)):
            timestamp_ms = None

        if timestamp_ms:
            try:
                # Hyperliquid typically returns timestamps in milliseconds
                # Define timestamp threshold constant
                ms_threshold = 1_000_000_000_000  # 1e12 in milliseconds
                if timestamp_ms > ms_threshold:
                    # Convert milliseconds to seconds
                    timestamp_seconds = timestamp_ms / 1000
                    return datetime.fromtimestamp(timestamp_seconds, tz=UTC).replace(tzinfo=None)
                # Already in seconds
                return datetime.fromtimestamp(timestamp_ms, tz=UTC).replace(tzinfo=None)
            except (ValueError, OSError) as e:
                logger.warning(
                    "hyperliquid_transfer_timestamp_parse_error",
                    timestamp_value=timestamp_ms,
                    error=str(e),
                    fallback="current_time",
                    message=(
                        f"Failed to parse Hyperliquid timestamp {timestamp_ms}, using current time"
                    ),
                )

        # Fallback to current time
        return datetime.now(UTC)
