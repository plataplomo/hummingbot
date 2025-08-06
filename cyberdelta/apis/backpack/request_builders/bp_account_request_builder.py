"""Backpack Account Request Builder.

This module handles the construction of request payloads for account operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- Balance and position queries
- Account information requests
- Transfer and withdrawal requests
- Account settings updates
- Financial operations (dust conversion, borrowing/lending)
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Literal

from pydantic import ValidationError

from cyberdelta.apis.backpack.models.bp_raw_api_request_payloads import (
    BackpackRawAccountConvertDustRequest,
    BackpackRawAccountWithdrawalRequest,
    BackpackRawBorrowLendExecuteRequest,
    BackpackRawInternalTransferRequest,
    BackpackRawUpdateAccountSettingsRequest,
)
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralQueryParams
from cyberdelta.apis.backpack.models.bp_raw_query_params import (
    BackpackRawGetAccountInfoParams,
    BackpackRawGetBalancesParams,
    BackpackRawGetPositionsParams,
    BackpackRawMaxBorrowQuantityParams,
    BackpackRawMaxOrderQuantityParams,
    BackpackRawMaxWithdrawalQuantityParams,
)
from cyberdelta.apis.backpack.protocols.builder_protocols import AccountRequestBuilderProtocol
from cyberdelta.apis.base.trading_execution_domain import AccountSettings
from cyberdelta.apis.exceptions import (
    MissingRequiredParameterError,
)
from cyberdelta.apis.exceptions.request_validation import InvalidParameterTypeError
from cyberdelta.apis.models.service_args.internal import (
    GetMaxBorrowQuantityArgs,
    GetMaxOrderQuantityArgs,
    GetMaxWithdrawalQuantityArgs,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.enums import OrderSide
from cyberdelta.symbols.models import Symbol


logger = get_logger(__name__)


class BackpackAccountRequestBuilder(AccountRequestBuilderProtocol):
    """Focused request builder for Backpack account operations.

    This class contains static methods for constructing validated request payloads
    for all account related API endpoints.
    """

    def __init__(self) -> None:
        """Initialize the account request builder."""
        logger.debug("Initializing Backpack account request builder")

    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Generic request builder dispatch method.

        This method serves as the entry point for the registry system
        and dispatches to the appropriate specific builder method based on context.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments including 'operation' to specify the request type

        Raises:
            NotImplementedError: If operation is not supported or parameters are insufficient
        """
        operation = kwargs.get("operation")
        if not operation:
            raise NotImplementedError(
                "Account request builder requires 'operation' parameter for generic build_request",
            )

        # Note: Account request builders require specific parameters for each operation
        # which are not available in the generic build_request interface.
        # This dispatcher is implemented for protocol consistency but most operations
        # will require direct method calls with proper parameters.

        if operation in {
            "get_balance",
            "get_positions",
            "get_transfers",
            "get_deposits",
            "withdraw",
            "transfer",
            "get_account_info",
            "get_collateral",
        }:
            # Account operations require specific parameters not available in generic interface
            raise NotImplementedError(
                f"Account operation '{operation}' requires specific parameters not available "
                f"in generic build_request interface. Use specific builder methods directly.",
            )
        raise NotImplementedError(
            f"Account operation '{operation}' not supported by registry dispatch",
        )

    @staticmethod
    def build_get_balances_params() -> BackpackRawGetBalancesParams:
        """Build query parameters for fetching account balances.

        Returns:
            BackpackRawGetBalancesParams: Empty params object (no parameters needed)
        """
        logger.debug("building_get_balances_params")
        return BackpackRawGetBalancesParams()

    @staticmethod
    def build_get_positions_params(symbol: Symbol | None) -> BackpackRawGetPositionsParams:
        """Build query parameters for fetching account positions.

        Args:
            symbol: Optional symbol to filter positions

        Returns:
            BackpackRawGetPositionsParams: Validated query parameters
        """
        logger.debug(
            "building_get_positions_params",
            symbol=symbol,
        )

        params_dict: dict[str, Any] = {}

        return BackpackRawGetPositionsParams(**params_dict)

    @staticmethod
    def build_get_account_info_params() -> BackpackRawGetAccountInfoParams:
        """Build query parameters for fetching account information.

        Returns:
            BackpackRawGetAccountInfoParams: Empty params object (no parameters needed)
        """
        logger.debug("building_get_account_info_params")
        return BackpackRawGetAccountInfoParams()

    @staticmethod
    def build_withdraw_payload(
        asset_symbol: Symbol,
        network: str,
        address: str,
        amount: Decimal,
        transaction_priority: str | None = None,
        tag: str | None = None,
        client_withdraw_id: str | None = None,
    ) -> BackpackRawAccountWithdrawalRequest:
        """Build the request payload for withdrawing assets.

        Args:
            asset_symbol: Asset Symbol object to withdraw
            network: Blockchain network
            address: Destination address
            amount: Amount to withdraw
            transaction_priority: Optional transaction priority
            tag: Optional address tag (for certain networks)
            client_withdraw_id: Optional client withdrawal ID

        Returns:
            BackpackRawAccountWithdrawalRequest: Validated request payload
        """
        logger.debug(
            "building_withdraw_payload",
            asset=asset_symbol.value,
            network=network,
            address=address,
            amount=str(amount),
        )

        request_dict: dict[str, Any] = {
            "symbol": asset_symbol.value,
            "address": address,
            "blockchain": network,
            "quantity": str(amount),
        }

        if tag is not None:
            request_dict["addressTag"] = tag
        if transaction_priority is not None:
            request_dict["transactionPriority"] = transaction_priority
        if client_withdraw_id is not None:
            request_dict["clientId"] = client_withdraw_id

        return BackpackRawAccountWithdrawalRequest(**request_dict)

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: Symbol,
        from_wallet: str,
        to_wallet: str,
        amount: Decimal,
        sub_account_id: str | None = None,
    ) -> BackpackRawInternalTransferRequest:
        """Build the request payload for internal transfers.

        Args:
            asset_symbol: Asset Symbol object to transfer
            from_wallet: Source wallet type
            to_wallet: Destination wallet type
            amount: Amount to transfer
            sub_account_id: Optional sub-account ID

        Returns:
            BackpackRawInternalTransferRequest: Validated request payload

        Raises:
            InvalidParameterTypeError: If wallet types are invalid
        """
        # Validate wallet types
        valid_wallets = {"SPOT", "MARGIN", "FUTURES"}
        if from_wallet not in valid_wallets:
            raise InvalidParameterTypeError(
                parameter_name="from_wallet",
                expected_type="one of: SPOT, MARGIN, FUTURES",
                actual_type="invalid wallet type",
                value=from_wallet,
            )
        if to_wallet not in valid_wallets:
            raise InvalidParameterTypeError(
                parameter_name="to_wallet",
                expected_type="one of: SPOT, MARGIN, FUTURES",
                actual_type="invalid wallet type",
                value=to_wallet,
            )

        logger.debug(
            "building_internal_transfer_payload",
            asset=asset_symbol.value,
            from_wallet=from_wallet,
            to_wallet=to_wallet,
            amount=str(amount),
            sub_account_id=sub_account_id,
        )

        request_dict: dict[str, Any] = {
            "symbol": asset_symbol.value,
            "fromAccount": from_wallet,
            "toAccount": to_wallet,
            "quantity": str(amount),
        }

        if sub_account_id is not None:
            request_dict["clientId"] = sub_account_id

        return BackpackRawInternalTransferRequest(**request_dict)

    @staticmethod
    def build_convert_dust_payload(asset_symbol: Symbol) -> BackpackRawAccountConvertDustRequest:
        """Build the request payload for converting dust to USDC.

        Args:
            asset_symbol: Asset Symbol object to convert

        Returns:
            BackpackRawAccountConvertDustRequest: Validated request payload

        Raises:
            InvalidParameterTypeError: If asset_symbol is not supported for dust conversion
        """
        logger.debug(
            "building_convert_dust_payload",
            asset=asset_symbol.value,
        )

        # Validate the asset symbol before creating the model
        # This ensures type safety while maintaining runtime validation
        try:
            # The Pydantic model will validate the symbol at runtime
            # mypy cannot know this validation happens, but it does
            return BackpackRawAccountConvertDustRequest.model_validate({
                "symbol": asset_symbol.value,
            })
        except ValidationError as e:
            raise InvalidParameterTypeError(
                parameter_name="asset_symbol",
                expected_type="supported asset symbol",
                actual_type="unsupported asset symbol",
                value=asset_symbol.value,
            ) from e

    @staticmethod
    def build_borrow_lend_payload(
        operation: Literal["BORROW", "REPAY", "LEND", "REDEEM"],
        asset_symbol: Symbol,
        amount: Decimal,
    ) -> BackpackRawBorrowLendExecuteRequest:
        """Build the request payload for borrowing/lending operations.

        Args:
            operation: Type of operation
            asset_symbol: Asset Symbol object to operate on
            amount: Amount for the operation

        Returns:
            BackpackRawBorrowLendExecuteRequest: Validated request payload

        Raises:
            InvalidParameterTypeError: If parameters are invalid or asset is not supported
        """
        logger.debug(
            "building_borrow_lend_payload",
            operation=operation,
            asset=asset_symbol.value,
            amount=str(amount),
        )

        # Map operation strings to API strings
        operation_mapping = {
            "BORROW": "Borrow",
            "REPAY": "Repay",
            "LEND": "Lend",
            "REDEEM": "Redeem",
        }

        # Validate inputs before creating the model
        try:
            # Use model_validate to handle runtime validation
            return BackpackRawBorrowLendExecuteRequest.model_validate({
                "side": operation_mapping[operation],
                "symbol": asset_symbol.value,
                "quantity": str(amount),
            })
        except ValidationError as e:
            raise InvalidParameterTypeError(
                parameter_name="operation_parameters",
                expected_type="valid borrow/lend parameters",
                actual_type="invalid parameters",
                value=f"operation={operation}, asset={asset_symbol.value}, amount={amount}",
            ) from e

    @staticmethod
    def build_update_account_settings_payload(
        leverage: int | None = None,
        account_settings: AccountSettings | None = None,
        margin_account_type: Literal["STANDARD", "PORTFOLIO"] | None = None,
    ) -> BackpackRawUpdateAccountSettingsRequest:
        """Build the request payload for updating account settings.

        Args:
            leverage: Optional leverage setting (1-50)
            account_settings: Optional account settings with validated policies
            margin_account_type: Optional margin account type

        Returns:
            BackpackRawUpdateAccountSettingsRequest: Validated request payload

        Raises:
            MissingRequiredParameterError: If no settings are provided
        """
        if leverage is None and account_settings is None and margin_account_type is None:
            raise MissingRequiredParameterError(
                parameter_name="at least one setting",
                operation="update account settings",
            )

        logger.debug(
            "building_update_account_settings_payload",
            leverage=leverage,
            account_settings=account_settings.automation_policy.value if account_settings else None,
            margin_account_type=margin_account_type,
        )

        request_dict: dict[str, Any] = {}

        if leverage is not None:
            request_dict["leverage"] = leverage
        if account_settings is not None:
            # Convert AccountSettings domain object to API format
            api_fields = account_settings.to_api_fields()
            request_dict["autoLend"] = api_fields["autoLend"]
        if margin_account_type is not None:
            request_dict["marginAccountType"] = margin_account_type

        return BackpackRawUpdateAccountSettingsRequest(**request_dict)

    @staticmethod
    def build_collateral_query_params(
        sub_account_id: str | None = None,
    ) -> BackpackRawCollateralQueryParams:
        """Build query parameters for fetching collateral information.

        Args:
            sub_account_id: Optional sub-account ID

        Returns:
            BackpackRawCollateralQueryParams: Validated query parameters
        """
        logger.debug(
            "building_collateral_query_params",
            sub_account_id=sub_account_id,
        )

        params_dict: dict[str, Any] = {}

        if sub_account_id is not None:
            params_dict["subAccountId"] = sub_account_id

        return BackpackRawCollateralQueryParams(**params_dict)

    @staticmethod
    def build_max_borrow_quantity_params(
        args: GetMaxBorrowQuantityArgs,
    ) -> BackpackRawMaxBorrowQuantityParams:
        """Build query parameters for fetching maximum borrow quantity.

        Args:
            args: Validated GetMaxBorrowQuantityArgs

        Returns:
            BackpackRawMaxBorrowQuantityParams: Validated query parameters
        """
        logger.debug(
            "building_max_borrow_quantity_params",
            symbol=args.symbol,
        )

        return BackpackRawMaxBorrowQuantityParams(symbol=str(args.symbol))

    @staticmethod
    def build_max_order_quantity_params(
        args: GetMaxOrderQuantityArgs,
    ) -> BackpackRawMaxOrderQuantityParams:
        """Build query parameters for fetching maximum order quantity.

        Args:
            args: Validated GetMaxOrderQuantityArgs

        Returns:
            BackpackRawMaxOrderQuantityParams: Validated query parameters
        """
        logger.debug(
            "building_max_order_quantity_params",
            symbol=args.symbol,
            side=args.side.value,
        )

        # Map side enum to API string
        side_str = "Bid" if args.side == OrderSide.BUY else "Ask"

        return BackpackRawMaxOrderQuantityParams(
            symbol=str(args.symbol),
            side=side_str,
            price=str(args.price) if args.price is not None else None,
            reduceOnly=args.reduce_only,
            autoBorrow=args.auto_borrow,
            autoBorrowRepay=args.auto_borrow_repay,
        )

    @staticmethod
    def build_max_withdrawal_quantity_params(
        args: GetMaxWithdrawalQuantityArgs,
    ) -> BackpackRawMaxWithdrawalQuantityParams:
        """Build query parameters for fetching maximum withdrawal quantity.

        Args:
            args: Validated GetMaxWithdrawalQuantityArgs

        Returns:
            BackpackRawMaxWithdrawalQuantityParams: Validated query parameters
        """
        logger.debug(
            "building_max_withdrawal_quantity_params",
            symbol=args.symbol,
        )

        return BackpackRawMaxWithdrawalQuantityParams(
            symbol=str(args.symbol),
            autoBorrow=args.auto_borrow,
            autoLendRedeem=args.auto_lend_redeem,
        )
