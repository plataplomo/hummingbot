"""CyberDeltaEngine: Hyperliquid Trading Service.

----------------------------------------------

This service encapsulates the logic for trading operations on the Hyperliquid Exchange.
It uses the HttpClient (via a requester callable), HyperliquidRequestBuilder,
and HyperliquidResponseHandler to interact with the API.
This version of the service returns Internal Domain Models by using the HyperliquidOrderMapper.
"""

import inspect
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from typing import Any, cast

from pydantic import ValidationError

from cyberdelta.apis.base.authenticator_interface import IAuthenticator
from cyberdelta.apis.connectivity.http_client import ParsedJsonResponse
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.hl_request_builder import HyperliquidRequestBuilder
from cyberdelta.apis.hyperliquid.hl_response_handler import HyperliquidResponseHandler

# Preprocessing is now handled by Pydantic model validators
# Internal Domain Models & Mappers
from cyberdelta.apis.hyperliquid.mappers.hl_trading_data_mapper import HyperliquidTradingDataMapper
from cyberdelta.apis.hyperliquid.models.hl_raw_api_request_payloads import (
    HyperliquidApiCancelOrderRequest,
    HyperliquidApiPlaceOrderRequest,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeResponse,
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusObject,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_historical_order import (
    HyperliquidRawHistoricalOrder,
    HyperliquidRawHistoricalOrderResponse,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_open_orders import (
    HyperliquidRawOpenOrdersResponse,
    HyperliquidRawSimpleOpenOrder,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_order_status import (
    HyperliquidRawOrderStatusRequestPayload,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_orderbook import HyperliquidRawL2Book
from cyberdelta.apis.models.api_error import APIError, TransformationError
from cyberdelta.apis.models.api_error_codes import APIErrorCode
from cyberdelta.apis.models.service_args_models import (
    CancelOrderArgs,
    GetAllOpenOrdersArgs,
    GetL2BookArgs,
    GetOpenOrdersArgs,
    GetOrderArgs,
    HyperliquidGetOrderStatusArgs,
    PlaceOrderArgs,
)
from cyberdelta.apis.utils.response_validation import (
    ensure_dict_response,
    ensure_list_response,
)
from cyberdelta.config.logging_config import get_logger
from cyberdelta.core.models import Order
from cyberdelta.core.models.enums import (
    CancelOrderResultStatus,
    OrderSide,
    OrderStatus,
    OrderType,
    TimeInForce,
)
from cyberdelta.core.models.market.order import CancelOrderResult
from cyberdelta.core.models.market.order_book import OrderBook
from cyberdelta.utils.parsing import parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform
from cyberdelta.utils.typing import is_dict_response


logger = get_logger(__name__)


HttpClientRequesterSig = Callable[
    ...,
    Awaitable[tuple[ParsedJsonResponse | None, int, Mapping[str, str]]],
]


class HyperliquidTradingService:
    """Service class for Hyperliquid trading operations. Returns Internal Domain Models."""

    def __init__(
        self,
        http_client_requester: HttpClientRequesterSig,
        request_builder: HyperliquidRequestBuilder,
        response_handler: HyperliquidResponseHandler,
        authenticator: IAuthenticator | None,
        exchange_name: str,
        wallet_address: str | None,
        get_asset_index_callable: Callable[[str], Awaitable[int | None]],
        trading_mapper: HyperliquidTradingDataMapper,
        error_mapper: HyperliquidErrorMapper,
    ) -> None:
        """Initialize the Hyperliquid trading service with required dependencies.

        Args:
            http_client_requester: HTTP client function for making API requests
            request_builder: Builder for constructing Hyperliquid API requests
            response_handler: Handler for processing Hyperliquid API responses
            authenticator: Authentication interface for signing requests (optional)
            exchange_name: Name identifier for this exchange instance
            wallet_address: Wallet address for authenticated operations (optional)
            get_asset_index_callable: Function to retrieve asset index for symbols
            trading_mapper: Mapper for converting raw data to internal domain models
            error_mapper: Mapper for handling and transforming API errors

        """
        self._http_client_requester = http_client_requester
        self._request_builder = request_builder
        self._response_handler = response_handler
        self._authenticator = authenticator
        self._exchange_name = exchange_name
        self._wallet_address = wallet_address
        self._get_asset_index_callable = get_asset_index_callable
        self._trading_mapper = trading_mapper
        self._error_mapper = error_mapper
        self._action_endpoint = "/exchange"
        self._info_endpoint = "/info"

    def _validate_authentication(self, action: str) -> None:
        """Validate authentication requirements for actions that need signing."""
        if not self._authenticator:
            raise APIError(
                "HL authenticator not initialized (e.g., missing/invalid private key).",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )
        self._validate_wallet_address(action)

    def _validate_wallet_address(self, action: str) -> None:
        """Validate wallet address is available for the given action."""
        if not self._wallet_address:
            raise APIError(
                f"Wallet address is required to {action}.",
                code=APIErrorCode.AUTHENTICATION_FAILED.value,
            )

    async def _execute_exchange_action(
        self,
        request_payload_model: HyperliquidApiPlaceOrderRequest | HyperliquidApiCancelOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Execute an exchange action request and return the response."""
        # Pass model directly - the ExchangeAPI handles proper serialization via strategy
        raw_content, http_status, _ = await self._http_client_requester(
            method="POST",
            endpoint=self._action_endpoint,
            data=request_payload_model,
            is_signed=True,
            serialize_none_as_null=True,
        )
        if not is_dict_response(raw_content):
            _error_msg_no_content = (
                f"Exchange action ({request_payload_model.type}) returned invalid content."
            )
            raise APIError(_error_msg_no_content, APIErrorCode.INVALID_RESPONSE.value)

        exchange_response = self._response_handler.handle_exchange_response(
            raw_content,
            action_type=request_payload_model.type,
            status_code=http_status,
        )
        return exchange_response, http_status

    def _handle_service_error(
        self,
        error: Exception,
        current_method: str,
        context: str,
        status_code: int = 0,
        raw_response_content: str | None = None,
    ) -> APIError:
        """Handle service errors in a standardized way."""
        if isinstance(error, TransformationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for {context}: {error}",
                exc_info=True,
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )
        elif isinstance(error, ValidationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for {context}: {error}",
                exc_info=True,
            )
            return APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )
        elif isinstance(error, ValueError | TypeError):
            # Distinguish input validation from internal errors
            error_msg = str(error)
            if current_method in error_msg:
                # Re-raise input validation errors
                raise error
            else:
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for {context}: {error}",
                    exc_info=True,
                )
                return APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=error,
                    http_status=status_code if status_code != 0 else None,
                    exchange_message=raw_response_content,
                )
        else:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for {context}: {error}",
                exc_info=True,
            )
            return APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            )

    async def _place_order_raw(
        self,
        place_order_payload: HyperliquidApiPlaceOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Private method to place an order, using the API request payload model.

        The payload is already built by the request builder with the correct format.
        Returns the raw exchange response Pydantic model and HTTP status code.
        """
        # Early authentication checks - fail fast if auth requirements not met
        self._validate_authentication("placing an order")

        # Use the payload directly - it's already in the correct format
        request_payload_model = place_order_payload

        try:
            return await self._execute_exchange_action(request_payload_model)
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error placing order raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"[{self._exchange_name}] Unexpected error placing order raw: {e}")
            _error_msg_unexpected = f"Unexpected error placing order raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _cancel_order_raw(
        self,
        cancel_request_payload: HyperliquidApiCancelOrderRequest,
    ) -> tuple[HyperliquidRawExchangeResponse, int]:
        """Private method to cancel an order, using the full request payload.

        Returns the raw exchange response Pydantic model and HTTP status code.
        """
        # Early authentication checks - fail fast if auth requirements not met
        self._validate_authentication("cancelling an order")

        try:
            return await self._execute_exchange_action(cancel_request_payload)
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error cancelling order raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(f"[{self._exchange_name}] Unexpected error cancelling order raw: {e}")
            _error_msg_unexpected = f"Unexpected error cancelling order raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _get_open_orders_raw(self) -> list[HyperliquidRawSimpleOpenOrder]:
        """Private method to fetch raw open orders.

        Returns a list of HyperliquidRawSimpleOpenOrder Pydantic models.
        """
        self._validate_wallet_address("fetch open orders")
        # _validate_wallet_address guarantees wallet_address is not None
        if self._wallet_address is None:
            raise RuntimeError(
                "Wallet address validation failed unexpectedly. "
                "This should not happen after _validate_wallet_address."
            )

        request_payload_model = self._request_builder.build_open_orders_payload(
            GetOpenOrdersArgs(wallet_address=self._wallet_address)
        )

        try:
            raw_response_content, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=False,  # openOrders is a public endpoint in Hyperliquid
            )
            # Use centralized validation
            validated_raw_data = ensure_list_response(
                raw_response_content, "open orders", status_code
            )

            validated_response: HyperliquidRawOpenOrdersResponse = (
                self._response_handler.handle_info_open_orders_response(
                    validated_raw_data,
                    user_address=self._wallet_address,  # Already asserted not None above
                    status_code=status_code,
                )
            )
            return validated_response.items
        except APIError as e:
            logger.error(f"[{self._exchange_name}] API error fetching open orders raw: {e.message}")
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching open orders raw: {e}",
            )
            _error_msg_unexpected = f"Unexpected error fetching open orders raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def _get_order_status_raw(
        self,
        order_id: int,
    ) -> HyperliquidRawHistoricalOrder | None:
        """Private method to fetch raw order status.

        Returns a HyperliquidRawHistoricalOrder Pydantic model or None if not found.
        The actual response from HL for orderStatus is a HyperliquidRawHistoricalOrderResponse,
        which contains the HyperliquidRawHistoricalOrder.
        """
        self._validate_wallet_address("fetch order status")
        # _validate_wallet_address guarantees wallet_address is not None
        if self._wallet_address is None:
            raise RuntimeError(
                "Wallet address validation failed unexpectedly. "
                "This should not happen after _validate_wallet_address."
            )

        request_payload_model: HyperliquidRawOrderStatusRequestPayload = (
            self._request_builder.build_order_status_payload(
                HyperliquidGetOrderStatusArgs(
                    wallet_address=self._wallet_address,
                    order_id=order_id,
                )
            )
        )

        # Initialize raw_response_content before try block
        raw_response_content: ParsedJsonResponse | None = None

        try:
            raw_response_content, status_code, _ = await self._http_client_requester(
                method="POST",
                endpoint=self._info_endpoint,
                data=request_payload_model.model_dump(by_alias=True),
                is_signed=False,  # orderStatus is a public endpoint in Hyperliquid
            )
            # Use centralized validation
            validated_raw_data = ensure_dict_response(
                raw_response_content, f"order status for OID {order_id}", status_code
            )

            # Pass the raw response directly - model validator will handle preprocessing
            historical_order_response: HyperliquidRawHistoricalOrderResponse = (
                self._response_handler.handle_info_order_status_response(
                    validated_raw_data,
                    user_address=self._wallet_address,  # Already asserted not None above
                    order_id=order_id,
                )
            )
            if historical_order_response and historical_order_response.order:
                # Construct HyperliquidRawHistoricalOrder from response components
                order_data = historical_order_response.order
                return HyperliquidRawHistoricalOrder(
                    **order_data.model_dump(by_alias=True),
                    status=historical_order_response.status,
                    statusTimestamp=historical_order_response.status_timestamp,
                )
            return None

        except APIError as e:
            # Check if this is a validation error that indicates "unknownOid" response
            if e.code == APIErrorCode.INVALID_RESPONSE.value:
                # Extract the original validation error if available
                if isinstance(e.original_exception, ValidationError):
                    # Check if the raw response has "unknownOid" status
                    if (
                        isinstance(raw_response_content, dict)
                        and raw_response_content.get("status") == "unknownOid"
                    ):
                        # This is a known "order not found" response from Hyperliquid
                        # Raise a proper ORDER_NOT_FOUND error
                        raise APIError(
                            message=f"Order {order_id} not found",
                            code=APIErrorCode.ORDER_NOT_FOUND.value,
                            exchange_message="unknownOid",
                            http_status=200,  # Hyperliquid returns 200 for unknownOid
                        ) from e

            # For other errors, log and re-raise
            logger.error(
                f"[{self._exchange_name}] API error fetching order status raw for OID {order_id}: "
                f"{e.message}",
            )
            raise
        except Exception as e:
            logger.exception(
                f"[{self._exchange_name}] Unexpected error fetching order status raw for OID "
                f"{order_id}: {e}",
            )
            _error_msg_unexpected = f"Unexpected error fetching order status raw: {e}"
            raise APIError(_error_msg_unexpected, APIErrorCode.UNKNOWN.value) from e

    async def get_order(self, args: GetOrderArgs) -> Order | None:
        """Retrieve a specific order by ID for a given symbol.

        Args:
            args: Arguments containing order_id and symbol for order retrieval

        Returns:
            Order object if found, None otherwise

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If order_id is not a valid integer

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_order"

        # args.order_id is guaranteed to be non-empty by the GetOrderArgs validator
        # Hyperliquid doesn't require symbol for orderStatus endpoint, so we don't validate it

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            # DEFENSIVE CHECK: Convert order_id to int with proper error handling.
            # args.order_id is guaranteed to be str by Pydantic validator.
            try:
                order_id_int = int(args.order_id)
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"[{current_method}] 'order_id' must be a valid integer, got '{args.order_id}'",
                ) from e

            raw_historical_order = await self._get_order_status_raw(order_id_int)
            if raw_historical_order is None:
                return None

            # Use trading mapper to convert to internal order
            internal_order = self._trading_mapper.transform_raw_historical_order_to_internal(
                raw_historical_order=raw_historical_order,
                trigger=None,
            )
            return internal_order

        except APIError:
            # Re-raise APIErrors from _get_order_status_raw, ResponseHandler, etc.
            raise
        except TransformationError as e_transform:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data for order {args.order_id}: {e_transform}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=e_transform,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_transform
        except ValidationError as e_val:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed for order {args.order_id}: {e_val}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=e_val,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_val
        except (ValueError, TypeError) as e_service_logic:
            # Distinguish input validation from internal errors per ERROR_HANDLING.md
            error_msg = str(e_service_logic)
            if current_method in error_msg and any(
                param in error_msg for param in ["order_id", "symbol"]
            ):
                # Re-raise input validation errors
                raise
            else:
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error "
                    f"for order {args.order_id}: {e_service_logic}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=e_service_logic,
                ) from e_service_logic
        except Exception as e_unexpected:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure "
                f"for order {args.order_id}: {e_unexpected}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=e_unexpected,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from e_unexpected

    async def place_order(self, args: PlaceOrderArgs) -> Order:
        """Places an order and maps the raw response to an internal Order model."""
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "place_order"

        # Validate order parameters
        self._validate_place_order_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            asset_index = await self._get_asset_index_callable(args.symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {args.symbol} not found.",
                    APIErrorCode.INVALID_SYMBOL.value,
                )

            # Handle post_only mapping to ALO time-in-force
            effective_tif = args.time_in_force
            if args.post_only and args.time_in_force != TimeInForce.ALO:
                effective_tif = TimeInForce.ALO

            # Map time in force to Hyperliquid format
            tif_str = None
            if args.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT]:
                tif_str = self._map_time_in_force_to_hyperliquid(effective_tif)

            # ⚠️ WARNING: THIN MARKET ORDER IMPLEMENTATION - MISSING RISK CONTROLS
            # This is a backwards compatibility hack that bypasses our sophisticated
            # MarketOrder business logic. Use cyberdelta.core.execution.orders.MarketOrder
            # for proper slippage protection, liquidity validation, and risk management.
            if args.order_type == OrderType.MARKET:
                return await self._execute_thin_market_order(args)

            # Use the request builder to create the proper payload format
            place_order_payload = self._request_builder.build_place_order_payload(
                args=args,
                asset_index=asset_index,
                tif_str=tif_str,
            )

            raw_exchange_response, http_status = await self._place_order_raw(place_order_payload)
            status_code = http_status

            # Process the response and return the order
            return await self._process_place_order_response(
                raw_exchange_response, http_status, args
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            raise self._handle_service_error(
                e, current_method, args.symbol, status_code, raw_response_content
            ) from e

    def _validate_place_order_params(self, args: PlaceOrderArgs, current_method: str) -> None:
        """Validate order parameters for Hyperliquid exchange.

        Moves business validation logic from request builder to service layer.
        """
        # Validate order type is supported by Hyperliquid
        supported_order_types = [
            OrderType.LIMIT,
            OrderType.MARKET,
            OrderType.STOP_MARKET,
            OrderType.STOP_LIMIT,
        ]
        if args.order_type not in supported_order_types:
            raise ValueError(
                f"[{current_method}] Order type {args.order_type.value} is not supported "
                f"by Hyperliquid. Supported types: {[ot.value for ot in supported_order_types]}"
            )

        # Validate time in force
        if args.time_in_force == TimeInForce.FOK:
            raise ValueError(
                f"[{current_method}] TimeInForce FOK is not supported by Hyperliquid. "
                f"Supported values: GTC, IOC, ALO"
            )

        # Validate price for limit orders
        if args.order_type in [OrderType.LIMIT, OrderType.STOP_LIMIT] and args.price is None:
            raise ValueError(
                f"[{current_method}] Price is required for {args.order_type.value} orders"
            )

        # Validate stop price for stop orders
        if (
            args.order_type in [OrderType.STOP_MARKET, OrderType.STOP_LIMIT]
            and args.stop_price is None
        ):
            raise ValueError(
                f"[{current_method}] Stop price is required for {args.order_type.value} orders"
            )

    @staticmethod
    def _map_time_in_force_to_hyperliquid(tif: TimeInForce) -> str:
        """Map internal TimeInForce enum values to Hyperliquid-specific format.

        Moved from request builder to service layer.
        """
        mapping = {
            TimeInForce.GTC: "Gtc",
            TimeInForce.IOC: "Ioc",
            TimeInForce.ALO: "Alo",
        }

        if tif not in mapping:
            raise ValueError(
                f"TimeInForce {tif.value} is not supported by Hyperliquid. "
                f"Supported values: {list(mapping.keys())}"
            )

        return mapping[tif]

    def _process_pydantic_status(
        self, status_raw: HyperliquidRawExchangeStatusObject, action_description: str
    ) -> dict[str, Any]:
        """Process Pydantic model status."""
        if status_raw.resting:
            return {"resting": status_raw.resting}
        elif status_raw.filled:
            return {"filled": status_raw.filled}
        elif status_raw.error:
            return {"error": status_raw.error}
        else:
            raise APIError(
                f"Unknown status structure for {action_description}",
                APIErrorCode.INVALID_RESPONSE.value,
            )

    def _process_dict_resting_status(
        self, status_raw: dict[str, Any], action_description: str
    ) -> dict[str, Any]:
        """Process dict resting status."""
        oid = status_raw["resting"].get("oid")
        if not isinstance(oid, int):
            raise APIError(
                f"Invalid or missing 'oid' in resting status for {action_description}",
                APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": status_raw},
            )
        return {"resting": HyperliquidRawExchangeStatusResting(oid=oid)}

    def _process_dict_filled_status(
        self, status_raw: dict[str, Any], action_description: str
    ) -> dict[str, Any]:
        """Process dict filled status."""
        filled_details = status_raw["filled"]
        oid = filled_details.get("oid")
        total_sz = filled_details.get("totalSz")
        avg_px = filled_details.get("avgPx")

        if not isinstance(oid, int):
            raise APIError(
                f"Invalid or missing 'oid' in filled status for {action_description}",
                APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": status_raw},
            )

        if not isinstance(total_sz, str) or not isinstance(avg_px, str):
            raise APIError(
                f"Invalid filled status data for {action_description}",
                APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": status_raw},
            )

        return {
            "filled": HyperliquidRawExchangeStatusFilled(
                oid=oid,
                totalSz=total_sz,
                avgPx=avg_px,
            )
        }

    def _process_dict_canceled_status(
        self, status_raw: dict[str, Any], action_description: str
    ) -> dict[str, Any]:
        """Process dict canceled status."""
        oid = status_raw["canceled"].get("oid")
        if not isinstance(oid, int):
            raise APIError(
                f"Invalid or missing 'oid' in canceled status for {action_description}",
                APIErrorCode.INVALID_RESPONSE.value,
                metadata={"raw_status": status_raw},
            )
        return {"canceled": {"oid": oid}}

    def _process_dict_status(
        self, status_raw: dict[str, Any], action_description: str
    ) -> dict[str, Any]:
        """Process dict status for backwards compatibility."""
        # Check for resting status
        if "resting" in status_raw and isinstance(status_raw["resting"], dict):
            return self._process_dict_resting_status(status_raw, action_description)

        # Check for filled status
        if "filled" in status_raw and isinstance(status_raw["filled"], dict):
            return self._process_dict_filled_status(status_raw, action_description)

        # Check for canceled status
        if "canceled" in status_raw and isinstance(status_raw["canceled"], dict):
            return self._process_dict_canceled_status(status_raw, action_description)

        # Check for error status
        if "error" in status_raw and isinstance(status_raw["error"], str):
            return {"error": status_raw["error"]}

        # No recognized status found
        return {}

    def _process_string_status(self, status_raw: str, action_description: str) -> dict[str, Any]:
        """Process string status."""
        status_lower = status_raw.lower()

        # Handle success statuses
        if status_lower in ["success", "ok", "accepted"]:
            return {"success": status_raw}

        # Handle canceled status
        if status_lower == "canceled":
            return {"canceled": {"type": "string"}}

        # Any other string is treated as an error
        logger.warning(
            f"Encountered direct string status for {action_description}: '{status_raw}'. "
            f"Treating as error."
        )
        return {"error": status_raw}

    def _process_exchange_status(
        self,
        status_raw: object,
        action_description: str,
    ) -> dict[str, Any]:
        """Process raw exchange status into a standardized format."""
        # Handle Pydantic model status
        if isinstance(status_raw, HyperliquidRawExchangeStatusObject):
            return self._process_pydantic_status(status_raw, action_description)

        # Handle dict status (for backwards compatibility)
        elif isinstance(status_raw, dict):
            # Type assertion for pyright - we know it's a dict after isinstance check
            status_dict = cast(dict[str, Any], status_raw)
            result = self._process_dict_status(status_dict, action_description)
            if result:  # If we found a recognized status
                return result

        # Handle string status
        elif isinstance(status_raw, str):
            return self._process_string_status(status_raw, action_description)

        # Unknown status type
        raise APIError(
            f"Unknown status structure for {action_description}: {status_raw!r}",
            APIErrorCode.INVALID_RESPONSE.value,
        )

    def _check_error_response(
        self, raw_exchange_response: HyperliquidRawExchangeResponse, http_status: int
    ) -> None:
        """Check if the response is an error and raise appropriate exception."""
        if raw_exchange_response.status == "err" and raw_exchange_response.response:
            if isinstance(raw_exchange_response.response, str):
                # Use the error mapper to get the specific error code for this message
                mapped_error = self._error_mapper.map_string_error(
                    raw_exchange_response.response,
                    http_status=http_status,
                )
                raise mapped_error

    async def _process_place_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        args: PlaceOrderArgs,
    ) -> Order:
        """Process the place order response and return the internal Order."""
        # Check if this is an error response
        self._check_error_response(raw_exchange_response, http_status)

        # Process successful response using the normalized property
        response_data = raw_exchange_response.response_data

        if response_data and response_data.statuses:
            first_status = response_data.statuses[0]

            # Process the status - moved business logic from ResponseHandler
            processed_status = self._process_exchange_status(first_status, "place_order")

            if "error" in processed_status:
                # Use the error mapper for error messages
                mapped_error = self._error_mapper.map_string_error(
                    processed_status["error"],
                    http_status=http_status,
                )
                raise mapped_error

            # Handle successful statuses
            if "resting" in processed_status:
                return await self._handle_resting_order(processed_status["resting"], args)
            elif "filled" in processed_status:
                return await self._handle_filled_order(processed_status["filled"], args)
            elif "canceled" in processed_status:
                raise APIError(
                    f"Order was canceled unexpectedly: {processed_status}",
                    APIErrorCode.UNKNOWN.value,
                )

        raise APIError("Failed to place order or parse response.", APIErrorCode.UNKNOWN.value)

    async def _handle_resting_order(
        self, resting_info: HyperliquidRawExchangeStatusResting, args: PlaceOrderArgs
    ) -> Order:
        """Handle a resting (open) order response."""
        new_oid = resting_info.oid
        logger.info(
            f"Order placed with OID: {new_oid}. Re-fetching for full details.",
        )

        try:
            internal_order = await self.get_order(
                GetOrderArgs(symbol=args.symbol, order_id=str(new_oid)),
            )
            if internal_order:
                return internal_order
        except APIError as e:
            # If order status fetch fails, create a minimal Order object
            logger.warning(
                f"Failed to fetch full order details for OID {new_oid}: {e}. "
                "Creating minimal order object."
            )

        # Create a minimal order object with the information we have
        from datetime import UTC, datetime

        from cyberdelta.core.models.market.order import Order

        # Generate a client order ID if none provided
        client_order_id = (
            args.client_order_id or f"HL_{new_oid}_{int(datetime.now(UTC).timestamp())}"
        )

        # Use secure_transform to ensure validation
        order_data = {
            "exchange": "hyperliquid",
            "exchange_order_id": str(new_oid),
            "symbol": args.symbol,
            "side": args.side.value,
            "order_type": args.order_type.value,
            "quantity_requested": str(args.quantity),
            "price": str(args.price) if args.price else None,
            "time_in_force": args.time_in_force.value,
            "status": OrderStatus.OPEN.value,  # We know it's resting/open
            "created_at": datetime.now(UTC).isoformat(),
            "updated_at": datetime.now(UTC).isoformat(),
            "triggered_at": None,
            "strategy_name": None,
            "signal_id": None,
            "reduce_only": args.reduce_only,
            "post_only": args.post_only,
            "client_order_id": client_order_id,
        }

        return secure_transform(
            data=order_data,
            model_class=Order,
            context=f"place_order_resting_{args.symbol}",
            source_exchange="hyperliquid",
        )

    async def _handle_filled_order(
        self, filled_info: HyperliquidRawExchangeStatusFilled, args: PlaceOrderArgs
    ) -> Order:
        """Handle a filled order response."""
        filled_oid = filled_info.oid
        logger.info(
            f"Order OID {filled_oid} filled immediately. Re-fetching for full details.",
        )
        internal_order = await self.get_order(
            GetOrderArgs(symbol=args.symbol, order_id=str(filled_oid)),
        )
        if internal_order:
            internal_order.status = OrderStatus.FILLED
            internal_order.quantity_filled = (
                parse_decimal_value(
                    filled_info.total_sz,
                    allow_none=False,
                    field_name="totalSz",
                )
                or args.quantity
            )
            internal_order.average_fill_price = parse_decimal_value(
                filled_info.avg_px,
                allow_none=False,
                field_name="avgPx",
            )
            return internal_order
        else:
            raise APIError(
                f"Order filled (OID {filled_oid}) but failed to re-fetch details.",
                APIErrorCode.UNKNOWN.value,
            )

    async def get_open_orders(self, symbol: str | None = None) -> list[Order]:
        """Retrieve all open orders, optionally filtered by symbol.

        Maps the raw orders to a list of internal Order models.

        Args:
            symbol: Optional symbol filter for orders (case-insensitive)

        Returns:
            List of Order objects representing open orders

        Raises:
            APIError: If API request fails or data transformation fails
            ValueError: If symbol is provided but empty

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_open_orders"

        if symbol is not None and not symbol:
            raise ValueError(
                f"[{current_method}] 'symbol' must be a non-empty string when provided.",
            )

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            raw_open_orders = await self._get_open_orders_raw()
            internal_orders = self._process_raw_orders_to_internal(raw_open_orders, symbol)

            logger.info(
                f"[{self._exchange_name}] Retrieved {len(internal_orders)} open orders "
                f"(symbol filter: {symbol})",
            )
            return internal_orders

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            return self._handle_get_open_orders_error(
                e, current_method, status_code, raw_response_content
            )

    def _process_raw_orders_to_internal(
        self, raw_open_orders: list[HyperliquidRawSimpleOpenOrder], symbol: str | None
    ) -> list[Order]:
        """Process raw open orders and convert to internal Order objects."""
        internal_orders: list[Order] = []

        for raw_simple_order in raw_open_orders:
            # Convert symbol filter to use 'coin' field from simple order
            if symbol is None or raw_simple_order.coin.upper() == symbol.upper():
                try:
                    mapped_order = self._trading_mapper.transform_raw_simple_open_order_to_internal(
                        raw_simple_order=raw_simple_order
                    )
                    internal_orders.append(mapped_order)
                except Exception as e:
                    logger.error(
                        f"[{self._exchange_name}] Error mapping raw simple open order to "
                        f"internal: {e}. Raw order: {raw_simple_order.model_dump_json()}",
                    )
                    # Continue processing other orders

        return internal_orders

    def _handle_get_open_orders_error(
        self,
        error: Exception,
        current_method: str,
        status_code: int,
        raw_response_content: str | None,
    ) -> list[Order]:
        """Handle errors during get_open_orders processing."""
        if isinstance(error, TransformationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Failed to transform exchange "
                f"data: {error}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Failed to process/transform exchange data.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from error
        elif isinstance(error, ValidationError):
            logger.error(
                f"[{self._exchange_name}] {current_method}: Internal data validation "
                f"failed: {error}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.INVALID_RESPONSE.value,
                message="Internal data validation failed.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from error
        elif isinstance(error, ValueError | TypeError):
            # Distinguish input validation from internal errors per ERROR_HANDLING.md
            error_msg = str(error)
            if current_method in error_msg and any(
                param in error_msg for param in ["order_id", "symbol"]
            ):
                # Re-raise input validation errors
                raise
            else:
                logger.error(
                    f"[{self._exchange_name}] {current_method}: Service internal logic error: "
                    f"{error}",
                    exc_info=True,
                )
                raise APIError(
                    code=APIErrorCode.UNKNOWN.value,
                    message="Service internal logic error.",
                    original_exception=error,
                ) from error
        else:
            logger.error(
                f"[{self._exchange_name}] {current_method}: Unexpected service failure: {error}",
                exc_info=True,
            )
            raise APIError(
                code=APIErrorCode.UNKNOWN.value,
                message="Unexpected service failure.",
                original_exception=error,
                http_status=status_code if status_code != 0 else None,
                exchange_message=raw_response_content,
            ) from error

    async def cancel_order(self, args: CancelOrderArgs) -> bool:
        """Cancel a specific order and return True if successful.

        Args:
            args: Arguments containing order_id and symbol for order cancellation

        Returns:
            True if order was successfully cancelled

        Raises:
            APIError: If API request fails or order cancellation fails
            ValueError: If order_id is not a valid positive integer or symbol is missing

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_order"

        # Validate and prepare cancellation parameters
        symbol, order_id_int = self._validate_and_prepare_cancel_order_params(args, current_method)

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic - need asset index for cancellation
            asset_index = await self._get_asset_index_callable(symbol)
            if asset_index is None:
                raise APIError(
                    f"Asset index for {args.symbol} not found for cancellation.",
                    APIErrorCode.INVALID_SYMBOL.value,
                )

            # Use the request builder to create the proper payload format
            cancel_request_payload = self._request_builder.build_cancel_order_payload(
                args=args,
                asset_index=asset_index,
                order_id=order_id_int,
            )
            # Pass the full request payload to the raw method
            raw_exchange_response, http_status = await self._cancel_order_raw(
                cancel_request_payload
            )
            status_code = http_status

            # Process the cancellation response
            return self._process_cancel_order_response(
                raw_exchange_response, http_status, order_id_int
            )

        except APIError:
            # Re-raise APIErrors from _requester, ResponseHandler, etc.
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            raise self._handle_service_error(
                e, current_method, f"order {order_id_int}", status_code, raw_response_content
            ) from e

    def _validate_and_prepare_cancel_order_params(
        self, args: CancelOrderArgs, current_method: str
    ) -> tuple[str, int]:
        """Validate and prepare parameters for order cancellation."""
        # Extract validated fields from Pydantic model
        symbol = args.symbol
        order_id = args.order_id

        # For Hyperliquid, symbol is required
        if symbol is None:
            raise ValueError(f"[{current_method}] 'symbol' parameter is required.")

        # Convert order_id to int (it's always a string from Pydantic model)
        try:
            order_id_int = int(order_id)
        except ValueError as e:
            raise ValueError(
                f"[{current_method}] 'order_id' must be a valid integer: {order_id}",
            ) from e

        if order_id_int <= 0:
            raise ValueError(f"[{current_method}] 'order_id' must be positive.")

        return symbol, order_id_int

    def _process_cancel_order_response(
        self,
        raw_exchange_response: HyperliquidRawExchangeResponse,
        http_status: int,
        order_id_int: int,
    ) -> bool:
        """Process the cancel order response and return success status."""
        # Debug logging to understand the response
        logger.debug(
            f"[{self._exchange_name}] Cancel order response - "
            f"status: {raw_exchange_response.status}, "
            f"response: {raw_exchange_response.response}, data: {raw_exchange_response.data}"
        )

        # Check if this is an error response
        self._check_error_response(raw_exchange_response, http_status)

        # Process successful response using the normalized property
        response_data = raw_exchange_response.response_data

        if response_data and response_data.statuses:
            first_status = response_data.statuses[0]

            # Process the status - moved business logic from ResponseHandler
            processed_status = self._process_exchange_status(first_status, "cancel_order")

            if "error" in processed_status:
                # Use the error mapper for error messages
                mapped_error = self._error_mapper.map_string_error(
                    processed_status["error"],
                    http_status=http_status,
                )
                raise mapped_error

            # Any non-error status means successful cancellation
            logger.info(
                f"[{self._exchange_name}] Successfully cancelled order OID {order_id_int}",
            )
            return True

        # If we reach here, something unexpected happened
        raise APIError("Failed to cancel order or parse response.", APIErrorCode.UNKNOWN.value)

    async def cancel_all_orders(self, symbol: str | None = None) -> list[CancelOrderResult]:
        """Cancel all open orders, optionally filtered by symbol.

        Returns a list of CancelOrderResult for each attempted cancellation.

        Args:
            symbol: Optional symbol filter for orders to cancel (case-insensitive)

        Returns:
            List of CancelOrderResult objects with cancellation status for each order

        Raises:
            APIError: If API request fails or authentication is missing

        """
        # Service Input Parameter Validation
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "cancel_all_orders"

        # No specific input validation needed for this method

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic
            self._validate_cancel_all_orders_prerequisites()

            open_orders_internal = await self.get_open_orders(symbol=symbol)
            if not open_orders_internal:
                logger.info(
                    f"[{self._exchange_name}] No open orders found matching symbol "
                    f"'{symbol if symbol else 'any'}' to cancel.",
                )
                return []

            # Process cancellations for all orders
            return await self._process_all_order_cancellations(open_orders_internal)

        except APIError:
            # Re-raise APIErrors from get_open_orders, cancel_order, etc.
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            raise self._handle_service_error(
                e, current_method, "cancel all orders", status_code, raw_response_content
            ) from e

    def _validate_cancel_all_orders_prerequisites(self) -> None:
        """Validate prerequisites for cancelling all orders."""
        self._validate_wallet_address("cancel all orders")

    async def _process_all_order_cancellations(
        self, open_orders_internal: list[Order]
    ) -> list[CancelOrderResult]:
        """Process cancellations for all open orders."""
        results: list[CancelOrderResult] = []
        active_cancels_count = 0

        for order_to_cancel in open_orders_internal:
            if order_to_cancel.exchange_order_id is None:
                results.append(self._create_failed_cancel_result_no_id(order_to_cancel))
                continue

            cancel_result = await self._attempt_single_order_cancellation(order_to_cancel)
            results.append(cancel_result)

            if cancel_result.success:
                active_cancels_count += 1

        if active_cancels_count == 0 and len(open_orders_internal) > 0:
            logger.info(
                f"[{self._exchange_name}] Found {len(open_orders_internal)} orders but none "
                f"had valid exchange_order_id for cancellation attempt.",
            )

        return results

    def _create_failed_cancel_result_no_id(self, order_to_cancel: Order) -> CancelOrderResult:
        """Create a failed cancel result for orders without exchange_order_id."""
        logger.warning(
            f"[{self._exchange_name}] Skipping cancellation for order without "
            f"exchange_order_id: ClientOID {order_to_cancel.client_order_id}, "
            f"Symbol {order_to_cancel.symbol}",
        )
        return CancelOrderResult(
            symbol=order_to_cancel.symbol,
            order_id=None,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message="Order has no exchange_order_id, cannot be canceled by ID.",
            status=CancelOrderResultStatus.FAILED,
        )

    def _validate_exchange_order_id(self, order_to_cancel: Order) -> str:
        """Validate and return exchange order ID."""
        order_id_to_cancel_str = order_to_cancel.exchange_order_id
        if order_id_to_cancel_str is None:
            raise ValueError("exchange_order_id is None")
        return order_id_to_cancel_str

    async def _execute_cancel_request(
        self, order_id_int: int, order_symbol_for_cancel: str
    ) -> bool:
        """Execute the cancellation request and return success status."""
        logger.debug(
            f"Attempting to cancel order {order_id_int} for symbol {order_symbol_for_cancel}",
        )

        cancel_args = CancelOrderArgs(
            order_id=str(order_id_int),
            symbol=order_symbol_for_cancel,
        )
        return await self.cancel_order(args=cancel_args)

    def _create_success_result(
        self,
        order_to_cancel: Order,
        order_id_int: int,
        order_symbol_for_cancel: str,
        success_flag: bool,
    ) -> CancelOrderResult:
        """Create a successful cancellation result."""
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=str(order_id_int),
            client_order_id=order_to_cancel.client_order_id,
            success=success_flag,
            message="Successfully canceled." if success_flag else "Failed to cancel via API.",
            status=CancelOrderResultStatus.SUCCESS
            if success_flag
            else CancelOrderResultStatus.FAILED,
        )

    async def _attempt_single_order_cancellation(self, order_to_cancel: Order) -> CancelOrderResult:
        """Attempt to cancel a single order and return the result."""
        # Initialize variables for use in exception handlers
        order_symbol_for_cancel = order_to_cancel.symbol
        order_id_to_cancel_str = order_to_cancel.exchange_order_id or "unknown"

        try:
            order_id_to_cancel_str = self._validate_exchange_order_id(order_to_cancel)

            order_id_int = int(order_id_to_cancel_str)
            success_flag = await self._execute_cancel_request(order_id_int, order_symbol_for_cancel)

            return self._create_success_result(
                order_to_cancel, order_id_int, order_symbol_for_cancel, success_flag
            )

        except ValueError:
            return self._create_invalid_order_id_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel
            )
        except APIError as e_api:
            return self._create_api_error_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel, e_api
            )
        except Exception as e_generic:
            return self._create_generic_error_result(
                order_to_cancel, order_id_to_cancel_str, order_symbol_for_cancel, e_generic
            )

    def _create_invalid_order_id_result(
        self, order_to_cancel: Order, order_id_to_cancel_str: str, order_symbol_for_cancel: str
    ) -> CancelOrderResult:
        """Create result for invalid order ID format."""
        logger.error(
            f"[{self._exchange_name}] Invalid order_id format "
            f"'{order_id_to_cancel_str}' for cancellation.",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=f"Invalid order_id format: {order_id_to_cancel_str}.",
            status=CancelOrderResultStatus.FAILED,
        )

    def _create_api_error_result(
        self,
        order_to_cancel: Order,
        order_id_to_cancel_str: str,
        order_symbol_for_cancel: str,
        e_api: APIError,
    ) -> CancelOrderResult:
        """Create result for API errors during cancellation."""
        logger.error(
            f"[{self._exchange_name}] APIError cancelling order "
            f"{order_id_to_cancel_str} for {order_symbol_for_cancel}: {e_api.message}",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=e_api.message,
            status=CancelOrderResultStatus.FAILED,
            raw_response={
                "error_code": e_api.code,
                "http_status": e_api.http_status,
            },
        )

    def _create_generic_error_result(
        self,
        order_to_cancel: Order,
        order_id_to_cancel_str: str,
        order_symbol_for_cancel: str,
        e_generic: Exception,
    ) -> CancelOrderResult:
        """Create result for generic errors during cancellation."""
        logger.exception(
            f"[{self._exchange_name}] Unexpected error cancelling order "
            f"{order_id_to_cancel_str} for {order_symbol_for_cancel}: {e_generic}",
        )
        return CancelOrderResult(
            symbol=order_symbol_for_cancel,
            order_id=order_id_to_cancel_str,
            client_order_id=order_to_cancel.client_order_id,
            success=False,
            message=str(e_generic),
            status=CancelOrderResultStatus.UNKNOWN,
        )

    async def _execute_thin_market_order(self, args: PlaceOrderArgs) -> Order:
        """Execute thin market order implementation - WARNING: MISSING RISK CONTROLS.

        This is a backwards compatibility hack that converts market orders to aggressive
        IOC limit orders. It bypasses sophisticated risk management like slippage protection,
        liquidity validation, and price deviation checks.

        For proper market order execution with full risk controls, use:
        cyberdelta.core.execution.orders.MarketOrder

        Args:
            args: Market order arguments to convert

        Returns:
            Executed order result

        Raises:
            APIError: If order book fetch or order execution fails
        """
        # Get order book using existing infrastructure
        order_book = await self._get_order_book_for_thin_market_order(args.symbol)

        # Extract aggressive price - use multiple levels if needed to ensure fill
        # WARNING: This uses up to 3 price levels to ensure IOC orders fill

        if args.side == OrderSide.BUY:
            if not order_book.asks:
                raise APIError(
                    f"No ask levels available for market buy of {args.symbol}",
                    APIErrorCode.ORDER_REJECTED.value,
                )
            # Use the 3rd ask level (or best available) to ensure aggressive fill
            ask_index = min(2, len(order_book.asks) - 1)  # Index 2 = 3rd level
            aggressive_price = order_book.asks[ask_index][0]
        else:  # SELL
            if not order_book.bids:
                raise APIError(
                    f"No bid levels available for market sell of {args.symbol}",
                    APIErrorCode.ORDER_REJECTED.value,
                )
            # Use the 3rd bid level (or best available) to ensure aggressive fill
            bid_index = min(2, len(order_book.bids) - 1)  # Index 2 = 3rd level
            aggressive_price = order_book.bids[bid_index][0]

        # Convert to IOC limit order
        limit_args = PlaceOrderArgs(
            symbol=args.symbol,
            side=args.side,
            order_type=OrderType.LIMIT,  # Convert to limit
            quantity=args.quantity,
            price=aggressive_price,  # Aggressive market-taking price
            time_in_force=TimeInForce.IOC,  # Immediate or cancel
            client_order_id=args.client_order_id,
            post_only=False,  # Ensure market-taking behavior
        )

        # Recursive call with limit order (no circular dependency)
        return await self.place_order(limit_args)

    async def _get_order_book_for_thin_market_order(self, symbol: str) -> OrderBook:
        """Get order book for thin market order using existing infrastructure.

        Reuses all existing request building, HTTP client, response handling,
        and mapping infrastructure to avoid code duplication.

        Args:
            symbol: Trading symbol to get order book for

        Returns:
            OrderBook with current bid/ask levels

        Raises:
            APIError: If order book fetch fails
        """
        # Reuse existing request builder
        request_payload = self._request_builder.build_l2_book_request_payload(
            GetL2BookArgs(symbol=symbol)
        )

        # Reuse existing HTTP client
        raw_response_content_parsed, status_code, headers = await self._http_client_requester(
            method="POST",
            endpoint=self._info_endpoint,
            data=request_payload.model_dump(by_alias=True, exclude_none=True),
            is_signed=False,
        )

        # Reuse existing response handler
        if raw_response_content_parsed is None:
            raise APIError(
                message=f"No data received for L2 book request ({symbol})",
                code=APIErrorCode.INVALID_RESPONSE.value,
                http_status=status_code,
            )
        validated_response = self._response_handler.handle_info_l2_book_response(
            raw_response_content_parsed, symbol, status_code, headers
        )

        # We need access to market data mapper to transform the response
        # This is the only missing piece - trading service doesn't have market data mapper
        # For now, let's create a minimal transformation
        return self._minimal_transform_to_order_book(validated_response)

    def _minimal_transform_to_order_book(self, raw_book: HyperliquidRawL2Book) -> OrderBook:
        """Minimal transformation to OrderBook - WARNING: Simplified implementation.

        This is a simplified transformation that bypasses the full market data mapper.
        Use proper market data service for complete transformation logic.
        """
        # Convert to OrderBook format
        from datetime import UTC, datetime

        bids: list[tuple[Decimal, Decimal]] = []
        asks: list[tuple[Decimal, Decimal]] = []

        if raw_book.levels and len(raw_book.levels) >= 2:
            # Hyperliquid format: levels[0] = bids, levels[1] = asks
            for bid_level in raw_book.levels[0]:
                price = parse_decimal_value(bid_level.px)
                size = parse_decimal_value(bid_level.sz)
                if price is not None and size is not None:
                    bids.append((price, size))

            for ask_level in raw_book.levels[1]:
                price = parse_decimal_value(ask_level.px)
                size = parse_decimal_value(ask_level.sz)
                if price is not None and size is not None:
                    asks.append((price, size))

        # Convert timestamp from milliseconds to datetime
        timestamp = datetime.fromtimestamp(raw_book.time / 1000, tz=UTC)

        return OrderBook(symbol=raw_book.coin, bids=bids, asks=asks, timestamp=timestamp)

    async def get_all_open_orders(self, args: GetAllOpenOrdersArgs) -> list[Order]:
        """Fetch all open orders, optionally filtering by symbol.

        Args:
            args: Parameters for filtering open orders including optional symbol.

        """
        # Service Input Parameter Validation is now handled by GetAllOpenOrdersArgs Pydantic model
        frame = inspect.currentframe()
        current_method = frame.f_code.co_name if frame is not None else "get_all_open_orders"

        # Initialize context for error handling
        status_code: int = 0
        raw_response_content: str | None = None

        try:
            # Core operational logic - delegate to get_open_orders with validated symbol
            return await self.get_open_orders(symbol=args.symbol)

        except APIError:
            # Re-raise APIErrors from get_open_orders method
            raise
        except (TransformationError, ValidationError, ValueError, TypeError, Exception) as e:
            if isinstance(e, APIError):
                raise
            raise self._handle_service_error(
                e, current_method, "get all open orders", status_code, raw_response_content
            ) from e
