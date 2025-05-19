"""
CyberDeltaEngine: Hyperliquid Account Service
----------------------------------------------

This service encapsulates the logic for fetching and managing account-specific
information from the Hyperliquid Exchange. It uses relevant HttpClient (via requester),
HyperliquidRequestBuilder, HyperliquidResponseHandler, and Mappers to interact
with the API and returns Internal Domain Models.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime # Added back for order history
from decimal import Decimal
# from typing import TYPE_CHECKING, Any # Any no longer used
from typing import TYPE_CHECKING, cast # Import cast

from pydantic import ValidationError

# Mappers
from cyberdelta.apis.hyperliquid.hl_mapper import (
    HyperliquidMapper,
    HyperliquidOrderMapper,
    HyperliquidUserFillMapper,
)
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import (
    HyperliquidResponseHandler,
    RawJsonResponse, # Import RawJsonResponse
)
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.models.api_error import APIError
from cyberdelta.apis.models.api_error_codes import APIErrorCode

# Core Domain Models
from cyberdelta.core.models import (
    DerivativePosition,
    MarginAccountSummary,
    Order, # For order history
    SpotBalance,
    Trade, # For trade history
)
from cyberdelta.core.models.operations import Transfer, Withdrawal # If HL supports these
from cyberdelta.utils.logging_config import get_logger

# Import HyperliquidRawClearinghouseState
from cyberdelta.apis.hyperliquid.models.hl_raw_user_state import HyperliquidRawClearinghouseState
# Import HyperliquidRawUserFill (used as type hint for raw_fill in get_trade_history)
# and HyperliquidRawUserFillsResponse (returned by handler in get_trade_history)
from cyberdelta.apis.hyperliquid.models.hl_raw_user_fills import HyperliquidRawUserFill, HyperliquidRawUserFillsResponse
# Import for queryOrderHistory: HyperliquidRawHistoricalOrder for mapper, HyperliquidRawHistoricalOrderResponse for handler return type
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import HyperliquidRawHistoricalOrder, HyperliquidRawHistoricalOrderResponse

if TYPE_CHECKING:
    from cyberdelta.apis.base.authenticator_interface import IAuthenticator

logger = get_logger(__name__)

HttpClientRequesterSig = Callable[
    ..., Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]]
]

class HyperliquidAccountService:
    """
    Service class for Hyperliquid account management operations.
    Returns Internal Domain Models.
    """

    _http_client_requester: HttpClientRequesterSig
    _request_builder: HyperliquidRequestBuilder
    _response_handler: HyperliquidResponseHandler
    _mapper: HyperliquidMapper # Primary mapper
    _order_mapper: HyperliquidOrderMapper # For order/trade specific mappings
    _authenticator: IAuthenticator | None
    _exchange_name: str
    _info_url: str # Specific to Hyperliquid for some requests
    _wallet_address: str | None

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        info_url: str,
        wallet_address: str | None,
    ) -> None:
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._info_url = info_url
        self._wallet_address = wallet_address
        self._mapper = HyperliquidMapper()
        self._order_mapper = HyperliquidOrderMapper()

    async def _get_raw_clearinghouse_state(self) -> HyperliquidRawClearinghouseState:
        """Helper to fetch and validate the raw HyperliquidClearinghouseState."""
        if not self._wallet_address:
            logger.error(f"[{self._exchange_name}] Wallet address not set. Cannot fetch clearinghouse state.")
            raise APIError(
                message="Wallet address is required to fetch clearinghouse state for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"
        payload_model = self._request_builder.build_user_state_payload(self._wallet_address)
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting user state for clearinghouse_state from {endpoint_path} with payload: {payload_dict}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=payload_dict,
                is_info_endpoint=True,
                is_signed=False,
            )
            logger.debug(
                f"[{self._exchange_name}] Raw user state response for clearinghouse_state: {raw_data!r} (Status: {status_code})"
            )

            if raw_data is None:
                raise APIError(
                    message=f"No data received for user state (for clearinghouse_state), status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            if not isinstance(raw_data, list) or not raw_data:
                 raise APIError(
                    message=f"Unexpected raw user state response format, expected non-empty list, got {type(raw_data)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            state_data_dict = raw_data[0]
            if not isinstance(state_data_dict, dict):
                raise APIError(
                    message=f"Unexpected item format in user state response, expected dict, got {type(state_data_dict)}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )

            return self._response_handler.handle_info_user_state_response(
                raw_response_content=state_data_dict,
                user_address=self._wallet_address 
            )
        except APIError: # Re-raise APIErrors directly
            raise
        except (ValidationError, ValueError) as e_val: # Catch Pydantic/parsing errors
            logger.error(
                f"Validation/map error for HL clearinghouse_state: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing HL clearinghouse_state data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled: # Catch any other unexpected errors
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for HL clearinghouse_state: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for HL clearinghouse_state: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_balances(self) -> dict[str, SpotBalance]:
        """Retrieves all account balances (spot balances derived from user state)."""
        # Error logging and specific APIError for balances context handled within helper
        raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
        internal_balances = self._mapper.map_raw_clearinghouse_state_to_spot_balances(raw_clearinghouse_state)
        logger.debug(f"[{self._exchange_name}] Mapped internal balances: {internal_balances}")
        return internal_balances

    async def get_positions(self, symbol: str | None = None) -> list[DerivativePosition]:
        """Retrieves derivative positions, optionally filtered by symbol."""
        raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
        # Assuming mapper returns Dict[str, DerivativePosition] where key is symbol
        all_positions_dict = self._mapper.map_raw_clearinghouse_state_to_derivative_positions(raw_clearinghouse_state)

        if symbol:
            position = all_positions_dict.get(symbol)
            if position:
                logger.debug(f"[{self._exchange_name}] Filtered position for symbol '{symbol}': {position}")
                return [position]
            logger.debug(f"[{self._exchange_name}] No position found for symbol '{symbol}'. Positions: {list(all_positions_dict.keys())}")
            return []
        
        all_positions_list = list(all_positions_dict.values())
        logger.debug(f"[{self._exchange_name}] Mapped all internal positions: {all_positions_list}")
        return all_positions_list

    async def get_account_summary(self) -> MarginAccountSummary | None:
        """Retrieves general account information or summary from the clearinghouse state."""
        try:
            raw_clearinghouse_state = await self._get_raw_clearinghouse_state()
            internal_summary = self._mapper.map_raw_clearinghouse_state_to_margin_summary(raw_clearinghouse_state)
            logger.debug(f"[{self._exchange_name}] Mapped internal account summary: {internal_summary}")
            return internal_summary
        except APIError: # Re-raise APIErrors directly
            raise
        except (ValidationError, ValueError) as e_val: # Catch Pydantic/parsing errors
            logger.error(
                f"Validation/map error for HL account summary: {e_val}."
            )
            raise APIError(
                message=f"Processing HL account summary data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                # http_status may not be directly available here if error is from mapper or state fetch helper
            ) from e_val
        except Exception as e_unhandled: # Catch any other unexpected errors
            logger.error(
                f"Unhandled error for HL account summary: {e_unhandled}.",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for HL account summary: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
            ) from e_unhandled

    async def get_order_history(
        self,
        symbol: str | None = None, # queryOrderHistory does not take symbol
        start_time: datetime | None = None, 
        end_time: datetime | None = None,
        # limit is not a direct param for HL queryOrderHistory
    ) -> list[Order]:
        """
        Retrieves historical order data using the 'queryOrderHistory' endpoint.
        Requires start_time and end_time.
        Filtering by symbol (if provided) is done client-side.
        """
        if not self._wallet_address:
            logger.error(f"[{self._exchange_name}] Wallet address not set. Cannot fetch order history.")
            raise APIError(
                message="Wallet address is required to fetch order history for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )
        if not start_time or not end_time:
            logger.error(f"[{self._exchange_name}] start_time and end_time are required for order history.")
            raise APIError(
                message="start_time and end_time are required to fetch order history for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info" 
        start_time_ms = int(start_time.timestamp() * 1000)
        end_time_ms = int(end_time.timestamp() * 1000)
        
        payload_model = self._request_builder.build_order_history_payload(
            wallet_address=self._wallet_address, 
            start_time_ms=start_time_ms, 
            end_time_ms=end_time_ms
        )
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting order history from {endpoint_path} with payload: {payload_dict}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=payload_dict,
                is_info_endpoint=True,
                is_signed=False, # queryOrderHistory via /info is typically not signed
            )
            logger.debug(
                f"[{self._exchange_name}] Raw order history response: {raw_data!r} (Status: {status_code})"
            )

            if raw_data is None:
                raise APIError(
                    message=f"No data received for order history, status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # handle_query_order_history_response expects the raw list and returns list[HyperliquidRawHistoricalOrderResponse]
            raw_historical_order_responses: list[HyperliquidRawHistoricalOrderResponse] = self._response_handler.handle_query_order_history_response(
                raw_response_content=raw_data, # Pass the raw list directly
                user_address=self._wallet_address
            )
            
            # Extract the actual HyperliquidRawHistoricalOrder from each response object
            actual_raw_historical_orders: list[HyperliquidRawHistoricalOrder] = [
                resp.order for resp in raw_historical_order_responses
            ]

            # The mapper method transform_raw_historical_order_to_internal needs to be defined in HyperliquidOrderMapper
            internal_orders: list[Order] = []
            if actual_raw_historical_orders:
                for raw_hist_order in actual_raw_historical_orders:
                    try:
                        mapped_order = self._order_mapper.transform_raw_historical_order_to_internal(
                            raw_historical_order=raw_hist_order,
                            trigger=None # Assuming no separate trigger info for historical orders here
                        )
                        if mapped_order:
                            # Client-side symbol filtering
                            if symbol is None or mapped_order.symbol == symbol:
                                internal_orders.append(mapped_order)
                    except (ValidationError, ValueError) as e_map_item:
                        logger.warning(
                            f"[{self._exchange_name}] Skipping historical order in mapping error: {e_map_item}. Raw: {raw_hist_order.model_dump_json() if hasattr(raw_hist_order, 'model_dump_json') else raw_hist_order!r}"
                        )

            if symbol:
                # Assuming Order object has an instrument_symbol attribute after mapping
                # which would correspond to raw_hist_order.asset
                filtered_orders = [o for o in internal_orders if o.symbol == symbol]
                logger.debug(f"[{self._exchange_name}] Filtered order history for symbol '{symbol}': {len(filtered_orders)} orders")
                return filtered_orders
            
            logger.debug(f"[{self._exchange_name}] Mapped internal order history: {len(internal_orders)} orders")
            return internal_orders
        except APIError:
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for HL order history: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing HL order history data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for HL order history: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for HL order history: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def get_trade_history(
        self,
        symbol: str | None = None, # HL userFills doesn't filter by symbol at request time
        # start_time, end_time, limit are not typical for HL userFills endpoint
        # Filtering will be client-side if needed.
    ) -> list[Trade]:
        """
        Retrieves historical trade data (fills).
        This uses the same userFills endpoint as get_order_history but maps to Trade objects.
        """
        if not self._wallet_address:
            logger.error(f"[{self._exchange_name}] Wallet address not set. Cannot fetch trade history.")
            raise APIError(
                message="Wallet address is required to fetch trade history for Hyperliquid.",
                code=APIErrorCode.INVALID_REQUEST.value,
            )

        endpoint_path = "/info"
        payload_model = self._request_builder.build_user_fills_request_payload(self._wallet_address)
        payload_dict = payload_model.model_dump()

        logger.debug(
            f"[{self._exchange_name}] Requesting user fills for trade history from {endpoint_path} with payload: {payload_dict}"
        )
        raw_data: ParsedJsonResponse | None = None
        status_code: int = 0
        try:
            raw_data, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint_path=endpoint_path,
                data=payload_dict,
                is_info_endpoint=True,
                is_signed=False, # User fills via /info is typically not signed
            )
            logger.debug(
                f"[{self._exchange_name}] Raw user fills response for trade history: {raw_data!r} (Status: {status_code})"
            )

            if raw_data is None:
                raise APIError(
                    message=f"No data received for user fills (for trade history), status: {status_code}",
                    code=APIErrorCode.INVALID_RESPONSE.value,
                    http_status=status_code,
                )
            
            # The handler returns HyperliquidRawUserFillsResponse, which has a root list of fills
            raw_fills_response_model: HyperliquidRawUserFillsResponse = self._response_handler.handle_info_user_fills_response(
                raw_response_content=cast(RawJsonResponse, raw_data), # Cast to satisfy handler
                user_address=self._wallet_address,
            )

            internal_trades: list[Trade] = []
            if raw_fills_response_model.root:
                raw_fill: HyperliquidRawUserFill # Add type hint for loop variable
                for raw_fill in raw_fills_response_model.root:
                    # Use HyperliquidUserFillMapper.map for individual fill transformation
                    trade = HyperliquidUserFillMapper.map(raw_fill)
                    if trade:
                        # Client-side symbol filtering
                        if symbol is None or trade.symbol == symbol:
                            internal_trades.append(trade)
            
            logger.debug(f"[{self._exchange_name}] Mapped {len(internal_trades)} trades for history. Symbol: {symbol or 'all'}")
            return internal_trades
        except APIError: # Re-raise APIErrors directly
            raise
        except (ValidationError, ValueError) as e_val:
            logger.error(
                f"Validation/map error for HL trade history: {e_val}. Raw: {raw_data!r}, Status: {status_code}"
            )
            raise APIError(
                message=f"Processing HL trade history data failed: {e_val}",
                code=APIErrorCode.INVALID_RESPONSE.value,
                original_exception=e_val,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_val
        except Exception as e_unhandled:
            raw_info_for_log = (
                f"Raw: {raw_data!r}" if raw_data is not None else "Raw data unavailable"
            )
            logger.error(
                f"Unhandled error for HL trade history: {e_unhandled}. {raw_info_for_log}, Status: {status_code}",
                exc_info=True,
            )
            raise APIError(
                message=f"Unexpected error for HL trade history: {e_unhandled}",
                code=APIErrorCode.UNKNOWN.value,
                original_exception=e_unhandled,
                http_status=status_code,
                exchange_message=str(raw_data),
            ) from e_unhandled

    async def transfer(
        self,
        # Define params based on HL capabilities (e.g. L1 to L1, subaccount transfers?)
        # For now, placeholder based on generic Transfer model
        asset: str, 
        amount: Decimal,
        from_account: str, # Might be L1 address
        to_account: str,   # Might be L1 address or subaccount ID
        # ... other params as needed
    ) -> Transfer:
        """Performs an internal transfer. Details depend on HL capabilities."""
        logger.warning(f"[{self._exchange_name}] transfer functionality may be limited or different for Hyperliquid.")
        raise NotImplementedError("transfer not yet implemented in HyperliquidAccountService")

    async def withdraw(
        self,
        # Define params based on HL capabilities
        asset: str, # Usually implied by L1 token being withdrawn
        amount: Decimal,
        destination_address: str, # L1 address
        # ... other params as needed (e.g. signature of L1 transaction for withdrawal intent)
    ) -> Withdrawal:
        """Initiates a withdrawal of funds. Details depend on HL (L1 interaction)."""
        logger.warning(f"[{self._exchange_name}] withdraw functionality is complex for Hyperliquid (L1 interaction)." )
        raise NotImplementedError("withdraw not yet implemented in HyperliquidAccountService")

