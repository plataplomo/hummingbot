"""Backpack Transfer Mapper.

This mapper handles transformations for transfer and withdrawal data from the Backpack exchange,
extracted from the monolithic account data mapper to improve maintainability and testability.

Focused on:
- Transfer transformations from raw transfer responses
- Withdrawal transformations from raw withdrawal responses
- Transfer and withdrawal status mapping
- Transfer-specific data validation and error handling
"""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.backpack.mappers.utils.common_mappers import BackpackCommonMappers
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.backpack.protocols.mapper_protocols import TransferMapperProtocol
from cyberdelta.apis.exceptions.data_transformation import (
    DataTransformationError,
    InvalidMappingError,
    MissingRequiredFieldError,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import InternalTransferStatus, InternalWithdrawalStatus
from cyberdelta.core.models import BackpackTransferDetails, BackpackWithdrawalDetails
from cyberdelta.core.models.operations import Transfer, Withdrawal
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform
from cyberdelta.utils.typing import ParsedJsonResponse


# Type alias for raw JSON response from HTTP client
type RawJsonResponse = ParsedJsonResponse


logger = get_logger(__name__)


class BackpackTransferMapper(TransferMapperProtocol):
    """Focused mapper for Backpack transfer and withdrawal data transformations.

    This class contains static methods for transforming validated Backpack Raw transfer models
    into CyberDeltaEngine Internal Domain Models for transfers and withdrawals.
    """

    @staticmethod
    def _ensure_response_is_dict(raw_response: dict[str, Any] | list[Any] | str) -> dict[str, Any]:
        """Ensure raw response is a dictionary.

        Args:
            raw_response: Raw response data to validate

        Returns:
            dict[str, Any]: The validated dict for type narrowing

        Raises:
            InvalidMappingError: If response is not a dictionary
        """
        if not isinstance(raw_response, dict):
            raise InvalidMappingError(
                field_name="raw_response",
                source_value=raw_response,
                reason=f"Expected dict, got {type(raw_response).__name__}",
                expected_format="dict",
            )
        return raw_response

    @staticmethod
    def _ensure_transfer_id_not_empty(
        transfer_id: str | None,
        raw_response: dict[str, Any],
    ) -> None:
        """Ensure transfer ID is not empty after extraction.

        Args:
            transfer_id: Transfer ID to validate
            raw_response: Raw response data for context

        Raises:
            MissingRequiredFieldError: If transfer ID is missing or empty
        """
        if not transfer_id:
            raise MissingRequiredFieldError(
                field_names="id",
                context="transfer_response",
                source_data=raw_response,
            )

    @staticmethod
    def _map_transfer_status_to_internal(raw_status: str | None) -> InternalTransferStatus:
        """Map a Backpack transfer status string to internal InternalTransferStatus enum.

        Args:
            raw_status: Raw status string from Backpack

        Returns:
            InternalTransferStatus: InternalTransferStatus enum value corresponding to the
                raw status
        """
        if raw_status is None:
            return InternalTransferStatus.UNKNOWN
        status_lower = raw_status.lower()
        if status_lower in {"success", "completed", "processed"}:
            return InternalTransferStatus.COMPLETED
        if status_lower in {"pending", "processing"}:
            return InternalTransferStatus.PENDING
        if status_lower in {"failed", "failure", "rejected"}:
            return InternalTransferStatus.FAILED
        if status_lower in {"cancelled", "canceled"}:
            return InternalTransferStatus.REJECTED  # Map canceled to REJECTED

        logger.warning(
            "unknown_transfer_status",
            raw_status=raw_status,
            mapped_to="UNKNOWN",
            message="Unknown Backpack transfer status encountered, defaulting to UNKNOWN",
        )
        return InternalTransferStatus.UNKNOWN

    @staticmethod
    def _map_withdrawal_status_to_internal(raw_status: str | None) -> InternalWithdrawalStatus:
        """Map a Backpack withdrawal status string to internal InternalWithdrawalStatus enum.

        Args:
            raw_status: Raw status string from Backpack

        Returns:
            InternalWithdrawalStatus: InternalWithdrawalStatus enum value corresponding to the
                raw status
        """
        if raw_status is None:
            return InternalWithdrawalStatus.UNKNOWN
        status_upper = raw_status.upper()
        if status_upper in {"COMPLETED", "SUCCESS", "PROCESSED", "CONFIRMED"}:
            return InternalWithdrawalStatus.COMPLETED
        if status_upper == "PENDING":
            return InternalWithdrawalStatus.PENDING
        if status_upper in {"FAILED", "FAILURE", "REJECTED"}:
            return InternalWithdrawalStatus.FAILED
        if status_upper == "CANCELLED":
            return InternalWithdrawalStatus.CANCELED
        logger.warning(
            "unknown_withdrawal_status",
            raw_status=raw_status,
            mapped_to="UNKNOWN",
            message="Unknown Backpack withdrawal status encountered, defaulting to UNKNOWN",
        )
        return InternalWithdrawalStatus.UNKNOWN

    @staticmethod
    def transform_raw_transfer_to_internal(
        raw_response: RawJsonResponse,
        exchange_name: str,
        asset: str,
        quantity: Decimal,
        from_account_type_raw: str,
        to_account_type_raw: str,
        client_transfer_id: str | None,
    ) -> Transfer:
        """Transform a raw Backpack transfer JSON response into an internal Transfer model.

        Converts transfer response data from Backpack into an internal Transfer domain model.

        Args:
            raw_response: The raw JSON dictionary from the transfer API call
            exchange_name: The name of the exchange to embed in the internal model
            asset: The symbol of the asset transferred
            quantity: The amount of the asset transferred
            from_account_type_raw: The raw string for the source account type
            to_account_type_raw: The raw string for the destination account type
            client_transfer_id: Optional client-provided ID for the transfer

        Returns:
            Transfer: The internal domain model representing the transfer

        Raises:
            DataTransformationError: If essential fields are missing or transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_transfer",
                asset=asset,
                quantity=str(quantity),
                from_account_type=from_account_type_raw,
                to_account_type=to_account_type_raw,
                client_transfer_id=client_transfer_id,
                message="Transforming raw transfer response to Transfer",
            )

            response_dict = BackpackTransferMapper._ensure_response_is_dict(raw_response)

            transfer_id = response_dict.get("id")
            raw_status_val = response_dict.get("status")
            message = response_dict.get("message")
            timestamp_ms_str = response_dict.get("timestamp")

            BackpackTransferMapper._ensure_transfer_id_not_empty(transfer_id, response_dict)

            # Ensure raw_status is str or None
            raw_status_str: str | None = None
            if raw_status_val is None:
                raw_status_str = None
            elif isinstance(raw_status_val, str):
                raw_status_str = raw_status_val
            else:
                logger.warning(
                    "unexpected_status_type",
                    status_type=type(raw_status_val).__name__,
                    status_value=raw_status_val,
                    message="Unexpected type for raw transfer status, treating as None",
                )
                raw_status_str = None

            internal_status = BackpackTransferMapper._map_transfer_status_to_internal(
                raw_status_str,
            )

            # Parse timestamp using common mapper utility
            timestamp: datetime
            if timestamp_ms_str and isinstance(timestamp_ms_str, str | int):
                try:
                    timestamp_ms = int(timestamp_ms_str)
                    parsed_timestamp = BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
                    timestamp = parsed_timestamp or datetime.now(UTC)
                except ValueError:
                    logger.warning(
                        "invalid_timestamp_format",
                        timestamp_ms_str=timestamp_ms_str,
                        transfer_id=transfer_id,
                        message="Invalid timestamp format for transfer, using current time",
                    )
                    timestamp = datetime.now(UTC)
            else:
                timestamp = datetime.now(UTC)

            # Create BP-specific details
            bp_details = BackpackTransferDetails(
                client_id=client_transfer_id,
                from_account_type=from_account_type_raw,
                to_account_type=to_account_type_raw,
            )

            # Use secure_transform for type-safe model creation
            transfer_data = {
                "id": str(transfer_id),
                "exchange": exchange_name,
                "asset": asset,
                "quantity": str(quantity),
                "status": internal_status.value,
                "timestamp": timestamp.isoformat(),
                "response_message": str(message) if message is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            transfer = secure_transform(
                data=transfer_data,
                model_class=Transfer,
                context=f"backpack_transfer_transform_{asset}",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_transfer_transformed",
                transfer_id=str(transfer_id),
                asset=asset,
                quantity=str(quantity),
                status=internal_status.value,
                from_account_type=from_account_type_raw,
                to_account_type=to_account_type_raw,
                message="Successfully transformed raw transfer response to Transfer",
            )

        except Exception as e:
            logger.exception(
                "raw_transfer_transform_failed",
                asset=asset,
                quantity=str(quantity) if quantity else None,
                from_account_type=from_account_type_raw,
                to_account_type=to_account_type_raw,
                raw_response=raw_response,
                error=str(e),
                message="Failed to transform raw transfer response to Transfer",
            )
            raise DataTransformationError(
                source_model="raw_transfer_response",
                target_model="Transfer",
                reason=str(e),
                original_error=e,
                source_data=raw_response,
            ) from e
        else:
            return transfer

    @staticmethod
    def transform_raw_withdrawal_response_to_internal(
        raw_response: BackpackRawWithdrawalResponse,
        asset: str,
        quantity: Decimal,
        address: str,
        network: str | None,
        client_withdrawal_id: str | None,
        tag: str | None,
    ) -> Withdrawal:
        """Transform a raw Backpack withdrawal response into an internal Withdrawal model.

        Converts withdrawal response data from Backpack into an internal Withdrawal domain model.

        Args:
            raw_response: The validated BackpackRawWithdrawalResponse Pydantic model
            asset: The asset symbol being withdrawn
            quantity: The amount of the asset withdrawn
            address: The destination address
            network: The blockchain network used
            client_withdrawal_id: Client-provided ID for the withdrawal
            tag: Destination tag/memo, if provided

        Returns:
            Withdrawal: The corresponding internal Withdrawal model

        Raises:
            DataTransformationError: If transformation fails
        """
        try:
            logger.debug(
                "transforming_raw_withdrawal_response",
                withdrawal_id=raw_response.id,
                asset=asset,
                quantity=str(quantity),
                address=address,
                network=network,
                status=raw_response.status,
                message="Transforming BackpackRawWithdrawalResponse to Withdrawal",
            )

            withdrawal_id = raw_response.id
            raw_status = raw_response.status
            timestamp_str = raw_response.created_at
            fee_str = raw_response.fee
            tx_hash_str = raw_response.transaction_hash

            internal_status = BackpackTransferMapper._map_withdrawal_status_to_internal(raw_status)

            # Parse timestamp
            timestamp_value: datetime
            if timestamp_str:
                try:
                    parsed_dt = parse_datetime_utc(timestamp_str, field_name="created_at")
                    timestamp_value = datetime.now(UTC) if parsed_dt is None else parsed_dt
                except ValueError:
                    logger.warning(
                        "timestamp_parse_failed",
                        timestamp_str=timestamp_str,
                        withdrawal_id=withdrawal_id,
                        message="Failed to parse withdrawal timestamp, using current time",
                    )
                    timestamp_value = datetime.now(UTC)
            else:
                timestamp_value = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(fee_str, allow_none=True, field_name="fee")

            # Create BP-specific details
            bp_details = BackpackWithdrawalDetails(
                blockchain=network or raw_response.blockchain,
                is_internal=raw_response.is_internal,
                client_id=client_withdrawal_id or raw_response.client_id,
                identifier=raw_response.identifier,
                fiat_fee=parse_decimal_value(
                    raw_response.fiat_fee,
                    allow_none=True,
                    field_name="fiat_fee",
                )
                if raw_response.fiat_fee is not None
                else None,
                fiat_state=raw_response.fiat_state,
                fiat_symbol=raw_response.fiat_symbol,
                provider_id=raw_response.provider_id,
                subaccount_id=raw_response.subaccount_id,
                bank_name=raw_response.bank_name,
                bank_identifier=raw_response.bank_identifier,
                account_identifier=raw_response.account_identifier,
            )

            # Use secure_transform for type-safe model creation
            withdrawal_data = {
                "id": str(withdrawal_id),
                "exchange": ExchangeName.BACKPACK.value,
                "status": internal_status.value,
                "asset": asset,
                "quantity": str(quantity),
                "address": address,
                "timestamp": timestamp_value.isoformat(),
                "fee": str(fee) if fee is not None else None,
                "tx_hash": tx_hash_str,
                "response_message": None,
                "bp_details": bp_details.model_dump() if bp_details else None,
            }

            withdrawal = secure_transform(
                data=withdrawal_data,
                model_class=Withdrawal,
                context="backpack_withdrawal_transform",
                source_exchange="backpack",
            )

            logger.debug(
                "raw_withdrawal_response_transformed",
                withdrawal_id=str(withdrawal_id),
                asset=asset,
                quantity=str(quantity),
                address=address,
                status=internal_status.value,
                fee=str(fee) if fee else None,
                tx_hash=tx_hash_str,
                blockchain=network or raw_response.blockchain,
                message="Successfully transformed BackpackRawWithdrawalResponse to Withdrawal",
            )

        except Exception as e:
            logger.exception(
                "raw_withdrawal_response_transform_failed",
                withdrawal_id=getattr(raw_response, "id", None),
                asset=asset,
                quantity=str(quantity) if quantity else None,
                address=address,
                network=network,
                raw_response=raw_response.model_dump() if raw_response else None,
                error=str(e),
                message="Failed to transform BackpackRawWithdrawalResponse to Withdrawal",
            )
            raise DataTransformationError(
                source_model="BackpackRawWithdrawalResponse",
                target_model="Withdrawal",
                reason=str(e),
                original_error=e,
                source_data=raw_response.model_dump() if raw_response else None,
            ) from e
        else:
            return withdrawal

    # MapperProtocol implementation - delegate to common utilities
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None,
        default: Decimal = Decimal(0),
    ) -> Decimal:
        """Safely parse decimal values with fallback.

        Args:
            value: Value to parse as Decimal (string, float, Decimal, or None).
            default: Default value to return if parsing fails.

        Returns:
            Parsed Decimal value or default if parsing fails.
        """
        return BackpackCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to UTC datetime.

        Args:
            timestamp_ms: Timestamp in milliseconds (float or None).

        Returns:
            UTC datetime object if timestamp is provided, None otherwise.
        """
        return BackpackCommonMappers.timestamp_ms_to_datetime(timestamp_ms)
