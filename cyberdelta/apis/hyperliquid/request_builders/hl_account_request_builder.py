"""Hyperliquid Account Request Builder.

This module handles the construction of request payloads for account operations,
extracted from the monolithic request builder to improve maintainability and testability.

Focused on:
- User state and account summary requests
- Balance and position queries
- Transfer and withdrawal requests
- Leverage and margin updates
- Account settings and configurations
"""

from __future__ import annotations

import time
from decimal import Decimal

from eth_typing import ChecksumAddress, HexAddress, HexStr

from cyberdelta.apis.exceptions.request_validation import InvalidEnumValueError
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
    HyperliquidApiTokenWithdrawalRequest,
    HyperliquidApiUpdateLeverageRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
    HyperliquidRawUpdateLeverageAction,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
    HyperliquidRawHistoricalOrdersRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawInternalUsdTransferPayload,
    HyperliquidRawL2UsdTransferPayload,
    HyperliquidRawWithdrawalToL1ActionPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import (
    HyperliquidRawUserFillsRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import (
    HyperliquidRawUserStateRequestPayload,
)
from cyberdelta.apis.hyperliquid.protocols.builder_protocols import AccountRequestBuilderProtocol
from cyberdelta.apis.hyperliquid.request_builders.hl_request_builder_base import (
    HyperliquidRequestBuilderBase,
)
from cyberdelta.apis.models.service_args.hyperliquid import (
    HyperliquidGetUserStateArgs,
    HyperliquidTransferL2UsdArgs,
    HyperliquidUpdateLeverageArgs,
    HyperliquidWithdrawL1Args,
)
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.symbols.models import Symbol


logger = get_logger(__name__)

# Constants
PRECISION_TOLERANCE = 1e-12  # Tolerance for floating point precision checks


class HyperliquidAccountRequestBuilder(
    HyperliquidRequestBuilderBase,
    AccountRequestBuilderProtocol,
):
    """Focused request builder for Hyperliquid account operations.

    This class contains static methods for constructing validated request payloads
    for all account related API endpoints. Implements AccountRequestBuilderProtocol
    for type safety and consistency.
    """

    # Base protocol method implementation
    def build_request(self, *args: object, **kwargs: object) -> dict[str, object]:
        """Build request payload per base protocol.

        Args:
            *args: Positional arguments for request building
            **kwargs: Keyword arguments for request building

        Returns:
            Dictionary containing the request payload
        """
        # This is a generic method required by the protocol
        # In practice, specific builder methods are used
        request_type = kwargs.get("request_type", "")

        if request_type == "user_state":
            user = kwargs.get("user", "")
            return self.build_get_user_state_params(
                ChecksumAddress(HexAddress(HexStr(str(user)))),
            ).model_dump(mode="json", by_alias=True)
        if request_type in {"open_orders", "frontend_open_orders"}:
            user = kwargs.get("user", "")
            return self.build_get_open_orders_params(
                ChecksumAddress(HexAddress(HexStr(str(user)))),
            ).model_dump(mode="json", by_alias=True)
        if request_type == "order_status":
            # No specific order status method available, return empty dict
            return {}
        if request_type == "user_fills":
            user = kwargs.get("user", "")
            return self.build_get_user_fills_params(
                ChecksumAddress(HexAddress(HexStr(str(user)))),
            ).model_dump(mode="json", by_alias=True)
        if request_type == "historical_orders":
            user = kwargs.get("user", "")
            return self.build_historical_orders_payload(str(user)).model_dump(
                mode="json",
                by_alias=True,
            )
        # Default to empty request
        return {}

    @staticmethod
    def build_user_state_payload(
        args: HyperliquidGetUserStateArgs,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Build the Pydantic model for fetching user state information.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidGetUserStateArgs containing wallet address

        Returns:
            HyperliquidRawUserStateRequestPayload: Validated Raw API model

        Assumes all business validation has been done by the service layer.
        """
        logger.debug(
            "building_user_state_payload",
            wallet_address=args.wallet_address,
            message="Building user state request payload",
        )

        # The RawLaxEthereumAddressStrHL in the model will handle format validation
        # Explicitly provide 'type' to satisfy Pydantic, even if model has a default Field value.
        return HyperliquidRawUserStateRequestPayload(
            type="clearinghouseState",
            user=args.wallet_address,
        )

    def build_l2_usd_transfer_payload(
        self,
        args: HyperliquidTransferL2UsdArgs,
    ) -> HyperliquidApiL2UsdTransferRequest:
        """Build the Pydantic model for an L2 USD transfer request.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidTransferL2UsdArgs containing transfer parameters

        Returns:
            HyperliquidApiL2UsdTransferRequest: Validated Raw API model

        Assumes all business validation has been done by the service layer.
        """
        logger.debug(
            "building_l2_usd_transfer_payload",
            destination=args.destination_address,
            amount=str(args.amount),
            message="Building L2 USD transfer request payload",
        )

        # Convert amount to wire format for precision
        amount_wire = self._decimal_to_wire_format(args.amount)

        transfer_payload_model = HyperliquidRawL2UsdTransferPayload(
            destination=args.destination_address,
            token="USDC",  # noqa: S106
            amount=amount_wire,
        )
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=transfer_payload_model,
        )
        return HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    @staticmethod
    def build_internal_transfer_payload(
        asset_symbol: Symbol,
        from_account_type: str,
        to_account_type: str,
        amount: Decimal,
    ) -> HyperliquidRawInternalUsdTransferPayload:
        """Build payload for internal USD transfers between spot and perp accounts.

        Following exact Backpack pattern: Static method that takes validated parameters
        and returns Raw Pydantic model for internal transfers within same wallet.

        Args:
            asset_symbol: Asset Symbol object to transfer (must be "USDC" for Hyperliquid)
            from_account_type: Source account type ("spot" or "perp")
            to_account_type: Destination account type ("spot" or "perp")
            amount: Transfer amount (positive decimal)

        Returns:
            HyperliquidRawInternalUsdTransferPayload: Validated top-level API request model

        Raises:
            InvalidEnumValueError: If account types are invalid or asset is not USDC
        """
        logger.debug(
            "building_internal_transfer_payload",
            asset=asset_symbol.value,
            from_account=from_account_type,
            to_account=to_account_type,
            amount=str(amount),
            message="Building internal USD transfer request payload",
        )

        # Validate asset - Hyperliquid internal transfers only support USDC
        if asset_symbol.value.upper() != "USDC":
            raise InvalidEnumValueError(
                parameter_name="asset_symbol",
                value=asset_symbol.value,
                valid_values=["USDC"],
                enum_type="supported_assets",
            )

        # Validate account types
        valid_accounts = {"spot", "perp"}
        if from_account_type.lower() not in valid_accounts:
            raise InvalidEnumValueError(
                parameter_name="from_account_type",
                value=from_account_type,
                valid_values=list(valid_accounts),
                enum_type="account_types",
            )
        if to_account_type.lower() not in valid_accounts:
            raise InvalidEnumValueError(
                parameter_name="to_account_type",
                value=to_account_type,
                valid_values=list(valid_accounts),
                enum_type="account_types",
            )
        if from_account_type.lower() == to_account_type.lower():
            raise InvalidEnumValueError(
                parameter_name="account_type_combination",
                value=f"{from_account_type}->{to_account_type}",
                valid_values=["spot->perp", "perp->spot"],
                enum_type="transfer_directions",
            )

        # Map account types to toPerp boolean
        # toPerp=True means spot→perp, toPerp=False means perp→spot
        if from_account_type.lower() == "spot" and to_account_type.lower() == "perp":
            to_perp = True
        elif from_account_type.lower() == "perp" and to_account_type.lower() == "spot":
            to_perp = False
        else:
            raise InvalidEnumValueError(
                parameter_name="account_combination",
                value=f"{from_account_type}→{to_account_type}",
                valid_values=["spot→perp", "perp→spot"],
                enum_type="transfer_combinations",
            )

        # Convert amount to wire format string using class static method
        amount_wire = HyperliquidAccountRequestBuilder._decimal_to_wire_format(amount)

        # Create timestamp/nonce for the action
        nonce = int(time.time() * 1000)  # Millisecond timestamp

        return HyperliquidRawInternalUsdTransferPayload(
            type="usdClassTransfer",
            amount=amount_wire,
            toPerp=to_perp,
            nonce=nonce,
        )

    def build_withdrawal_payload(
        self,
        args: HyperliquidWithdrawL1Args,
    ) -> HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
        """Build the Pydantic model for a withdrawal to L1 request.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidWithdrawL1Args containing withdrawal parameters

        Returns:
            HyperliquidApiEthWithdrawalRequest | HyperliquidApiTokenWithdrawalRequest:
                Validated Raw API model

        Returns a specific model based on whether the asset is ETH or another token.
        Assumes all business validation has been done by the service layer.
        """
        logger.debug(
            "building_withdrawal_payload",
            asset=args.asset,
            amount=str(args.amount),
            destination=args.destination_address,
            message="Building withdrawal request payload",
        )

        # Convert amount to wire format for precision
        amount_wire = self._decimal_to_wire_format(args.amount)

        if args.asset.upper() == "ETH":
            eth_withdrawal_model = HyperliquidRawEthWithdrawalActionPayload(
                amount=amount_wire,
                destination=args.destination_address,
            )
            return HyperliquidApiEthWithdrawalRequest(
                type="withdrawEth",
                action=eth_withdrawal_model,
            )

        withdrawal_payload_model = HyperliquidRawWithdrawalToL1ActionPayload(
            token=args.asset,
            amount=amount_wire,
            destination=args.destination_address,
        )
        return HyperliquidApiTokenWithdrawalRequest(
            type="withdraw",
            action=withdrawal_payload_model,
        )

    @staticmethod
    def build_update_leverage_request(
        args: HyperliquidUpdateLeverageArgs,
    ) -> HyperliquidApiUpdateLeverageRequest:
        """Build the request payload for updating leverage on a specific asset.

        Following proper Request Builder Pattern: Takes internal Args model and
        returns Raw Pydantic models.

        Args:
            args: Validated HyperliquidUpdateLeverageArgs containing leverage parameters

        Returns:
            HyperliquidApiUpdateLeverageRequest: The validated request payload model.

        """
        logger.debug(
            "building_update_leverage_request",
            asset_index=args.asset_index,
            leverage=args.leverage,
            is_cross=args.is_cross,
            message="Building update leverage request payload",
        )

        return HyperliquidApiUpdateLeverageRequest(
            type="updateLeverage",
            action=HyperliquidRawUpdateLeverageAction(
                asset=args.asset_index,
                isCross=args.is_cross,
                leverage=args.leverage,
            ),
        )

    @staticmethod
    def build_historical_orders_payload(
        wallet_address: str,
    ) -> HyperliquidRawHistoricalOrdersRequestPayload:
        """Build the Pydantic model for fetching historical orders.

        Uses 'historicalOrders' endpoint which returns all historical orders.
        Time filtering must be done after fetching the results.

        Args:
            wallet_address: User's wallet address

        Returns:
            HyperliquidRawHistoricalOrdersRequestPayload: Validated Raw API model
        """
        logger.debug(
            "building_historical_orders_payload",
            wallet_address=wallet_address,
            message="Building historical orders request payload",
        )

        return HyperliquidRawHistoricalOrdersRequestPayload(
            type="historicalOrders",
            user=wallet_address,
        )

    # Protocol implementation methods
    @staticmethod
    def build_get_user_state_params(user: ChecksumAddress) -> HyperliquidRawUserStateRequestPayload:
        """Build parameters for user state retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        logger.debug(
            "building_get_user_state_params",
            user=user,
            message="Building user state request parameters",
        )

        return HyperliquidRawUserStateRequestPayload(
            type="clearinghouseState",
            user=user,
        )

    @staticmethod
    def build_get_clearinghouse_state_params(
        user: ChecksumAddress,
    ) -> HyperliquidRawUserStateRequestPayload:
        """Build parameters for clearinghouse state retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        logger.debug(
            "building_get_clearinghouse_state_params",
            user=user,
            message="Building clearinghouse state request parameters",
        )

        return HyperliquidRawUserStateRequestPayload(
            type="clearinghouseState",
            user=user,
        )

    @staticmethod
    def build_get_open_orders_params(
        user: ChecksumAddress,
    ) -> HyperliquidRawOpenOrdersRequestPayload:
        """Build parameters for open orders retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        logger.debug(
            "building_get_open_orders_params",
            user=user,
            message="Building open orders request parameters",
        )

        return HyperliquidRawOpenOrdersRequestPayload(
            type="openOrders",
            user=user,
        )

    @staticmethod
    def build_get_user_fills_params(user: ChecksumAddress) -> HyperliquidRawUserFillsRequestPayload:
        """Build parameters for user fills retrieval.

        Args:
            user: The user address to query

        Returns:
            Validated Pydantic model containing request parameters
        """
        logger.debug(
            "building_get_user_fills_params",
            user=user,
            message="Building user fills request parameters",
        )

        return HyperliquidRawUserFillsRequestPayload(
            type="userFills",
            user=user,
        )
