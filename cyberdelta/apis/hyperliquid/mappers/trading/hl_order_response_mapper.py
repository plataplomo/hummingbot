"""Hyperliquid Order Response Mapper.

This mapper handles order response transformations from the Hyperliquid exchange,
extracted from the monolithic trading data mapper to improve maintainability and testability.

Focused on:
- Place order response mapping
- Resting order transformations
- Filled order transformations
- Order response status processing
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from cyberdelta.apis.common import TransformationError
from cyberdelta.apis.exceptions import OrderError
from cyberdelta.apis.hyperliquid.hl_errors_mapper import HyperliquidErrorMapper
from cyberdelta.apis.hyperliquid.mappers.utils.hyperliquid_common_mappers import (
    HyperliquidCommonMappers,
)
from cyberdelta.apis.hyperliquid.models.hl_raw_exchange_response import (
    HyperliquidRawExchangeStatusFilled,
    HyperliquidRawExchangeStatusResting,
)
from cyberdelta.apis.hyperliquid.protocols.mapper_protocols import (
    OrderResponseMapperProtocol,
)
from cyberdelta.apis.models.service_args import PlaceOrderArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.enums import OrderStatus
from cyberdelta.core.models import Order
from cyberdelta.enums.exchange_names import ExchangeName


logger = get_logger(__name__)


class HyperliquidOrderResponseMapper(OrderResponseMapperProtocol):
    """Focused mapper for Hyperliquid order response transformations.

    Handles transformations of order placement responses and status updates
    from Hyperliquid into the internal Order model.
    """

    # Protocol method implementations - delegate to common utilities
    @staticmethod
    def parse_decimal_safely(
        value: str | float | Decimal | None, default: Decimal = Decimal(0)
    ) -> Decimal:
        """Parse decimal values safely with default fallback."""
        return HyperliquidCommonMappers.parse_decimal_safely(value, default)

    @staticmethod
    def normalize_symbol(symbol: str) -> str:
        """Normalize symbol to internal format."""
        return HyperliquidCommonMappers.normalize_symbol(symbol)

    @staticmethod
    def denormalize_symbol(symbol: str) -> str:
        """Denormalize symbol to exchange format."""
        return HyperliquidCommonMappers.denormalize_symbol(symbol)

    @staticmethod
    def timestamp_ms_to_datetime(timestamp_ms: float | None) -> datetime | None:
        """Convert millisecond timestamp to datetime."""
        return HyperliquidCommonMappers.timestamp_ms_to_datetime(timestamp_ms)

    @staticmethod
    async def map_place_order_response_to_order(
        processed_status: dict[str, Any],
        order_args: PlaceOrderArgs,
        timestamp: datetime,
    ) -> Order:
        """Map place order response status to internal Order model.

        Args:
            processed_status: Processed status response from order placement
            order_args: Original order placement arguments
            timestamp: Order placement timestamp

        Returns:
            Order: Internal domain model

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Handle resting orders
            if "resting" in processed_status:
                resting_data = processed_status["resting"]
                return HyperliquidOrderResponseMapper.transform_resting_order_to_internal(
                    resting_data, order_args
                )

            # Handle filled orders
            if "filled" in processed_status:
                filled_data = processed_status["filled"]
                return HyperliquidOrderResponseMapper.transform_filled_order_to_internal(
                    filled_data, order_args
                )

            # Handle error statuses
            if "error" in processed_status:
                error_message = processed_status["error"]
                HyperliquidOrderResponseMapper._raise_order_error(error_message)

            # Handle other statuses - create minimal order from args
            return HyperliquidOrderResponseMapper._create_order_from_args(
                order_args, processed_status, timestamp
            )

        except Exception as e:
            # Re-raise OrderError as it's a legitimate business logic error
            if isinstance(e, OrderError):
                raise
            raise TransformationError(
                message="Failed to map place order response to Order",
                source_data={
                    "processed_status": processed_status,
                    "error": str(e),
                },
                original_exception=e,
            ) from e

    @staticmethod
    def transform_resting_order_to_internal(
        resting_data: HyperliquidRawExchangeStatusResting,
        order_args: PlaceOrderArgs,
    ) -> Order:
        """Transform resting order data to internal Order model.

        Args:
            resting_data: Resting order data from Hyperliquid
            order_args: Original order placement arguments

        Returns:
            Order: Internal domain model
        """
        # Extract order ID from resting data (resting_data is a Pydantic model)
        exchange_order_id = str(resting_data.oid)
        # Extract client order ID from resting data
        client_order_id = resting_data.cloid

        # Fall back to order args if no cloid in response
        # client_order_id is always a field in PlaceOrderArgs (can be None)
        if not client_order_id and order_args.client_order_id:
            client_order_id = order_args.client_order_id

        # For resting orders, we know they are OPEN
        status = OrderStatus.OPEN

        # Create order from args with resting order specifics
        if client_order_id:
            return Order(
                exchange=ExchangeName.HYPERLIQUID.value,
                symbol=order_args.symbol,
                side=order_args.side,
                order_type=order_args.order_type,
                quantity_requested=order_args.quantity,
                price=order_args.price,
                time_in_force=order_args.time_in_force,
                status=status,
                triggered_at=None,
                strategy_name=getattr(order_args, "strategy_name", None),
                signal_id=getattr(order_args, "signal_id", None),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                reduce_only=getattr(order_args, "reduce_only", False),
                post_only=getattr(order_args, "post_only", False),
            )

        # Let Order model handle client_order_id with default_factory
        return Order(
            exchange=ExchangeName.HYPERLIQUID.value,
            symbol=order_args.symbol,
            side=order_args.side,
            order_type=order_args.order_type,
            quantity_requested=order_args.quantity,
            price=order_args.price,
            time_in_force=order_args.time_in_force,
            status=status,
            triggered_at=None,
            strategy_name=getattr(order_args, "strategy_name", None),
            signal_id=getattr(order_args, "signal_id", None),
            exchange_order_id=exchange_order_id,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            reduce_only=getattr(order_args, "reduce_only", False),
            post_only=getattr(order_args, "post_only", False),
        )

    @staticmethod
    def _raise_order_error(error_message: str) -> None:
        """Raise an OrderError with the appropriate error code.

        Args:
            error_message: Error message from the exchange
        """
        mapper = HyperliquidErrorMapper()
        mapped_error = mapper.map_string_error(error_message)
        raise OrderError(
            message=f"Order placement failed: {error_message}",
            code=mapped_error.code,
        )

    @staticmethod
    def transform_filled_order_to_internal(
        filled_data: HyperliquidRawExchangeStatusFilled,
        order_args: PlaceOrderArgs,
    ) -> Order:
        """Transform filled order data to internal Order model.

        Args:
            filled_data: Filled order data from Hyperliquid (Pydantic model)
            order_args: Original order placement arguments

        Returns:
            Order: Internal domain model
        """
        # Extract order ID from filled data (filled_data is a Pydantic model)
        exchange_order_id = str(filled_data.oid)
        # Extract client order ID from filled data
        client_order_id = filled_data.cloid
        # For filled orders, extract fill information
        total_sz = str(filled_data.total_sz)
        avg_px = str(filled_data.avg_px)

        # Fall back to order args if no cloid in response
        # client_order_id is always a field in PlaceOrderArgs (can be None)
        if not client_order_id and order_args.client_order_id:
            client_order_id = order_args.client_order_id

        # For filled orders, we know they are FILLED
        status = OrderStatus.FILLED

        # Create order from args with filled order specifics
        if client_order_id:
            return Order(
                exchange=ExchangeName.HYPERLIQUID.value,
                symbol=order_args.symbol,
                side=order_args.side,
                order_type=order_args.order_type,
                quantity_requested=order_args.quantity,
                quantity_filled=Decimal(total_sz),  # Filled orders have full quantity filled
                price=order_args.price,
                average_fill_price=Decimal(avg_px) if avg_px else None,
                time_in_force=order_args.time_in_force,
                status=status,
                triggered_at=None,
                strategy_name=getattr(order_args, "strategy_name", None),
                signal_id=getattr(order_args, "signal_id", None),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                created_at=datetime.now(UTC),
                updated_at=datetime.now(UTC),
                reduce_only=getattr(order_args, "reduce_only", False),
                post_only=getattr(order_args, "post_only", False),
            )

        # Let Order model handle client_order_id with default_factory
        return Order(
            exchange=ExchangeName.HYPERLIQUID.value,
            symbol=order_args.symbol,
            side=order_args.side,
            order_type=order_args.order_type,
            quantity_requested=order_args.quantity,
            quantity_filled=Decimal(total_sz),  # Filled orders have full quantity filled
            price=order_args.price,
            average_fill_price=Decimal(avg_px) if avg_px else None,
            time_in_force=order_args.time_in_force,
            status=status,
            triggered_at=None,
            strategy_name=getattr(order_args, "strategy_name", None),
            signal_id=getattr(order_args, "signal_id", None),
            exchange_order_id=exchange_order_id,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
            reduce_only=getattr(order_args, "reduce_only", False),
            post_only=getattr(order_args, "post_only", False),
        )

    @staticmethod
    def _create_order_from_args(
        order_args: PlaceOrderArgs,
        processed_status: dict[str, Any],
        timestamp: datetime,
    ) -> Order:
        """Create minimal Order from order arguments when status is unclear.

        Args:
            order_args: Original order placement arguments
            processed_status: Processed status response
            timestamp: Order placement timestamp

        Returns:
            Order: Internal domain model with minimal information
        """
        # Try to extract order ID if available
        exchange_order_id = ""
        # Look for order ID in various possible locations
        exchange_order_id = str(
            processed_status.get(
                "oid", processed_status.get("orderId", processed_status.get("id", ""))
            )
        )

        # Default to NEW status for unclear statuses
        status = OrderStatus.NEW

        # Log warning about unclear status
        logger.warning(
            "unclear_order_status_creating_minimal_order",
            component="HyperliquidOrderResponseMapper",
            action="_create_order_from_args",
            processed_status=processed_status,
            message="Creating minimal order due to unclear status response",
        )

        # Extract client_order_id from order_args if present
        client_order_id = getattr(order_args, "client_order_id", None)

        # Create order with conditional client_order_id
        if client_order_id:
            return Order(
                exchange=ExchangeName.HYPERLIQUID.value,
                symbol=order_args.symbol,
                side=order_args.side,
                order_type=order_args.order_type,
                quantity_requested=order_args.quantity,
                price=order_args.price,
                time_in_force=order_args.time_in_force,
                status=status,
                triggered_at=None,
                strategy_name=getattr(order_args, "strategy_name", None),
                signal_id=getattr(order_args, "signal_id", None),
                exchange_order_id=exchange_order_id,
                client_order_id=client_order_id,
                created_at=timestamp,
                updated_at=timestamp,
                reduce_only=getattr(order_args, "reduce_only", False),
                post_only=getattr(order_args, "post_only", False),
            )

        # Let Order model handle client_order_id with default_factory
        return Order(
            exchange=ExchangeName.HYPERLIQUID.value,
            symbol=order_args.symbol,
            side=order_args.side,
            order_type=order_args.order_type,
            quantity_requested=order_args.quantity,
            price=order_args.price,
            time_in_force=order_args.time_in_force,
            status=status,
            triggered_at=None,
            strategy_name=getattr(order_args, "strategy_name", None),
            signal_id=getattr(order_args, "signal_id", None),
            exchange_order_id=exchange_order_id,
            created_at=timestamp,
            updated_at=timestamp,
            reduce_only=getattr(order_args, "reduce_only", False),
            post_only=getattr(order_args, "post_only", False),
        )
