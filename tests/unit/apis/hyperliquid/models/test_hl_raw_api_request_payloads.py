from typing import Any  # Added for casting

import pytest
from pydantic import ValidationError

from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiEthWithdrawalRequest,
    HyperliquidApiL2UsdTransferRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_actions import (
    HyperliquidRawEthWithdrawalActionPayload,
    HyperliquidRawL2UsdTransferActionDetails,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_transfer_withdrawal import (
    HyperliquidRawL2UsdTransferPayload,
)


class TestHyperliquidApiL2UsdTransferRequest:
    def test_valid_l2_usd_transfer(self) -> None:
        """Test valid l2 usd transfer."""
        payload_data: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345670",  # ETH-like
            "token": "USDC",  # Added token
            "amount": "100.50",
            # "time": 1678886400000, # Removed time based on linter error
        }
        payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=payload_model,
        )

        req = HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)
        assert req.type == "usdTransfer"
        assert req.action.chain == "L2"
        assert req.action.payload.destination == "0x1234567890abcdef1234567890abcdef12345670"
        assert req.action.payload.amount == "100.50"
        assert req.action.payload.token == "USDC"
        # assert req.action.payload.time == 1678886400000 # Removed time assertion

    def test_l2_usd_transfer_invalid_payload_values(self) -> None:
        """Test l2 usd transfer invalid payload values."""
        payload_data: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345670",
            "token": "USDC",
            "amount": "-100.50",  # Invalid amount (RawPositiveFiniteDecimalStr)
        }
        with pytest.raises(ValidationError):
            payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
            action_details_model = HyperliquidRawL2UsdTransferActionDetails(
                chain="L2",
                payload=payload_model,
            )
            HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    def test_l2_usd_transfer_missing_amount_in_payload(self) -> None:  # Renamed test for clarity
        """Test l2 usd transfer missing amount in payload."""
        payload_data: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345670",
            "token": "USDC",
            # "amount" is missing
        }
        with pytest.raises(ValidationError):
            # Pydantic should raise when HyperliquidRawL2UsdTransferPayload is instantiated
            payload_model = HyperliquidRawL2UsdTransferPayload(**payload_data)
            action_details_model = HyperliquidRawL2UsdTransferActionDetails(
                chain="L2",
                payload=payload_model,
            )
            HyperliquidApiL2UsdTransferRequest(type="usdTransfer", action=action_details_model)

    def test_l2_usd_transfer_explicit_type_provided_direct_unpack(self) -> None:
        """Test l2 usd transfer explicit type provided direct unpack."""
        # For direct unpacking, ensure inner models are instantiated correctly
        # or the dict is precise
        payload_dict: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345670",
            "token": "USDC",
            "amount": "100.50",
        }
        # Instantiate inner models explicitly if **request_data is problematic
        payload_model = HyperliquidRawL2UsdTransferPayload(**payload_dict)
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=payload_model,
        )

        request_data_for_unpack: dict[str, Any] = {
            "type": "usdTransfer",
            "action": action_details_model,  # Pass the model instance
        }
        req = HyperliquidApiL2UsdTransferRequest(**request_data_for_unpack)
        assert req.type == "usdTransfer"
        assert req.action.chain == "L2"
        assert req.action.payload.destination == "0x1234567890abcdef1234567890abcdef12345670"
        assert req.action.payload.amount == "100.50"
        assert req.action.payload.token == "USDC"

    def test_l2_usd_transfer_incorrect_outer_type(self) -> None:
        """Test l2 usd transfer incorrect outer type."""
        payload_dict: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345670",
            "token": "USDC",
            "amount": "100.50",
        }
        payload_model = HyperliquidRawL2UsdTransferPayload(**payload_dict)
        action_details_model = HyperliquidRawL2UsdTransferActionDetails(
            chain="L2",
            payload=payload_model,
        )

        request_data_for_unpack: dict[str, Any] = {
            "type": "wrongUsdTransferType",  # Incorrect type
            "action": action_details_model,
        }
        with pytest.raises(ValidationError):
            HyperliquidApiL2UsdTransferRequest(**request_data_for_unpack)


class TestHyperliquidApiEthWithdrawalRequest:
    def test_valid_eth_withdrawal(self) -> None:
        """Test valid eth withdrawal."""
        action_payload_data: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345678",
            "amount": "1.234",
        }
        action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
        req = HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)
        assert req.type == "withdrawEth"
        assert req.action.destination == "0x1234567890abcdef1234567890abcdef12345678"
        assert req.action.amount == "1.234"

    def test_eth_withdrawal_invalid_address(self) -> None:
        """Test eth withdrawal invalid address."""
        action_payload_data: dict[str, Any] = {
            "destination": "invalid-eth-address",  # Expected to fail RawStrictEthereumAddressStrHL
            "amount": "1.234",
        }
        with pytest.raises(ValidationError):
            action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
            HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)

    def test_eth_withdrawal_invalid_amount(self) -> None:
        """Test eth withdrawal invalid amount."""
        action_payload_data: dict[str, Any] = {
            "destination": "0x1234567890abcdef1234567890abcdef12345678",
            "amount": "not-a-number",  # Expected to fail RawFiniteDecimalStr
        }
        with pytest.raises(ValidationError):
            action_model = HyperliquidRawEthWithdrawalActionPayload(**action_payload_data)
            HyperliquidApiEthWithdrawalRequest(type="withdrawEth", action=action_model)

    def test_eth_withdrawal_explicit_type_provided_direct_unpack(self) -> None:
        """Test eth withdrawal explicit type provided direct unpack."""
        action_payload_model = HyperliquidRawEthWithdrawalActionPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678",
            amount="1.234",
        )
        request_data_for_unpack: dict[str, Any] = {
            "type": "withdrawEth",
            "action": action_payload_model,  # Pass the model instance
        }
        req = HyperliquidApiEthWithdrawalRequest(**request_data_for_unpack)
        assert req.type == "withdrawEth"
        assert req.action.destination == "0x1234567890abcdef1234567890abcdef12345678"
        assert req.action.amount == "1.234"

    def test_eth_withdrawal_incorrect_outer_type(self) -> None:
        """Test eth withdrawal incorrect outer type."""
        action_payload_model = HyperliquidRawEthWithdrawalActionPayload(
            destination="0x1234567890abcdef1234567890abcdef12345678",
            amount="1.234",
        )
        request_data_for_unpack: dict[str, Any] = {
            "type": "wrongWithdrawType",  # Incorrect type
            "action": action_payload_model,
        }
        with pytest.raises(ValidationError):
            HyperliquidApiEthWithdrawalRequest(**request_data_for_unpack)


# If there are other request payload types like HyperliquidApiCancelOrderRequest,
# HyperliquidApiPlaceOrderRequest, HyperliquidApiTokenWithdrawalRequest,
# HyperliquidApiUsdTransferRequest, HyperliquidApiUpdateLeverageRequest,
# HyperliquidApiUpdateIsolatedMarginRequest, HyperliquidApiAgentKeyRequest,
# they should have similar test classes.

# For example:
# from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
#     HyperliquidApiPlaceOrderRequest,
# )
# from cyberdelta.apis.hyperliquid.models.hl_raw_order import (
#     HyperliquidRawPlaceOrderAction,
#     HyperliquidRawOrderType, # Assuming this contains LimitOrderTypeDetails etc.
#     HyperliquidRawLimitOrderTypeDetails,
# )

# class TestHyperliquidApiPlaceOrderRequest:
#     def test_valid_place_order(self) -> None:
#         # Assuming HyperliquidRawPlaceOrderAction uses Python field names like is_buy, limit_px
#         # or has populate_by_name=True and aliases.
#         # For this example, assume field names are direct:
#         action_payload_data = {
#             "asset": 0,
#             "is_buy": True,
#             "limit_px": "30000.0",
#             "sz": "0.001",
#             "reduce_only": False,
#             "order_type": {"limit": {"tif": "Gtc"}}, # Parsed to HyperliquidRawOrderType
#             "cloid": None,
#         }
#         action_model = HyperliquidRawPlaceOrderAction(**action_payload_data)
#         req = HyperliquidApiPlaceOrderRequest(
#             type="order", # Assuming "order" is the correct type literal
#             action=action_model
#         )
#         assert req.type == "order"
#         assert req.action.asset == 0
#         assert req.action.is_buy is True
#         assert req.action.limit_px == "30000.0"
#         assert req.action.sz == "0.001" # Assuming 'sz' is the field name
#         assert req.action.order_type.limit.tif == "Gtc"
#
#     # ... more tests for place order ...
