"""Data transformation exceptions for CyberDelta.

These exceptions handle errors that occur during data transformation
in mapper classes. They extend TransformationError (Layer 3).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cyberdelta.apis.common.api_error import TransformationError


if TYPE_CHECKING:
    from cyberdelta.enums import ExchangeName


class UnknownEnumError(TransformationError):
    """Raised when an unknown enum value is encountered during mapping."""

    def __init__(
        self,
        enum_type: str,
        value: str | object,
        valid_values: list[str] | None = None,
    ) -> None:
        """Initialize unknown enum error.

        Args:
            enum_type: The enum type being mapped
            value: The unknown value
            valid_values: List of valid enum values
        """
        if valid_values:
            message = f"Unknown {enum_type}: '{value}' (valid values: {', '.join(valid_values)})"
        else:
            message = f"Unknown {enum_type}: '{value}'"

        super().__init__(message)
        self.enum_type = enum_type
        self.value = value
        self.valid_values = valid_values
        self.source_value = value
        self.target_type = enum_type
        self.details = {"valid_values": valid_values} if valid_values else {}


class MissingRequiredFieldError(TransformationError):
    """Raised when required fields are missing during transformation."""

    def __init__(
        self,
        field_names: str | list[str] | None = None,
        context: str | None = None,
        source_data: dict[str, object] | None = None,
        *,
        field: str | None = None,
        exchange: ExchangeName | None = None,
        operation: str | None = None,
        reason: str | None = None,
        **kwargs: object,
    ) -> None:
        """Initialize missing required field error.

        Args:
            field_names: Name(s) of missing field(s)
            context: Context where fields are required
            source_data: The source data being transformed
            field: Single field name (alias for field_names)
            exchange: Exchange enum value
            operation: Operation being performed
            reason: Additional reason
            **kwargs: Additional context
        """
        # Use field if provided and field_names is not
        if field_names is None and field:
            field_names = field
        elif field_names is None:
            field_names = "unknown field"

        if isinstance(field_names, list):
            fields_str = ", ".join(field_names)
            message = f"{fields_str} are required"
        else:
            message = f"{field_names} is required"

        # Build context from multiple sources
        context_parts: list[str] = []
        if operation:
            context_parts.append(operation)
        if exchange:
            context_parts.append(f"on {exchange.value}")
        if context:
            context_parts.append(context)

        if context_parts:
            message = f"{message} for {' '.join(context_parts)}"

        if reason:
            message = f"{message} ({reason})"

        super().__init__(
            message=message,
            field_name=field_names if isinstance(field_names, str) else None,
            source_value=source_data,
        )
        self.field_names = field_names
        self.context = context
        self.field = field
        self.exchange = exchange
        self.operation = operation
        self.reason = reason
        self.details = {
            "missing_fields": field_names if isinstance(field_names, list) else [field_names],
            "context": context,
            "exchange": exchange,
            "operation": operation,
            "reason": reason,
            **kwargs,
        }


class DataTransformationError(TransformationError):
    """Raised when data transformation fails."""

    def __init__(
        self,
        source_model: str,
        target_model: str,
        reason: str,
        original_error: Exception | None = None,
        source_data: object = None,
    ) -> None:
        """Initialize data transformation error.

        Args:
            source_model: Source model type
            target_model: Target model type
            reason: Reason for transformation failure
            original_error: The original exception
            source_data: The source data that failed
        """
        message = f"Failed to transform {source_model} to {target_model}: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.source_model = source_model
        self.target_model = target_model
        self.reason = reason
        self.source_type = source_model
        self.target_type = target_model


class InvalidMappingError(TransformationError):
    """Raised when a mapping is invalid or impossible."""

    def __init__(
        self,
        field_name: str,
        source_value: object,
        reason: str,
        expected_format: str | None = None,
    ) -> None:
        """Initialize invalid mapping error.

        Args:
            field_name: Field that has invalid mapping
            source_value: The invalid value
            reason: Reason why mapping is invalid
            expected_format: Expected format description
        """
        message = f"Invalid mapping for {field_name}: {reason}"

        super().__init__(
            message=message,
            field_name=field_name,
            source_value=source_value,
        )
        self.reason = reason
        self.expected_format = expected_format
        self.details = {
            "reason": reason,
            "expected_format": expected_format,
        }


class CollateralTransformationError(TransformationError):
    """Raised when collateral data transformation fails."""

    def __init__(
        self,
        collateral_type: str,
        reason: str,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize collateral transformation error.

        Args:
            collateral_type: Type of collateral being transformed
            reason: Reason for transformation failure
            source_data: The source collateral data
        """
        message = f"Failed to transform {collateral_type} collateral: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
        )
        self.collateral_type = collateral_type
        self.reason = reason
        self.source_type = f"{collateral_type}_collateral"
        self.target_type = "CollateralInfo"
        self.details = {"collateral_type": collateral_type}


class OrderTransformationError(TransformationError):
    """Raised when order data transformation fails."""

    def __init__(
        self,
        order_id: str | None,
        reason: str,
        order_data: dict[str, object] | None = None,
        original_error: Exception | None = None,
    ) -> None:
        """Initialize order transformation error.

        Args:
            order_id: Order ID if available
            reason: Reason for transformation failure
            order_data: The source order data
            original_error: The original exception
        """
        if order_id:
            message = f"Failed to transform order {order_id}: {reason}"
        else:
            message = f"Failed to transform order: {reason}"

        super().__init__(
            message=message,
            source_value=order_data,
            original_exception=original_error,
        )
        self.order_id = order_id
        self.reason = reason
        self.source_type = "BackpackRawOrder"
        self.target_type = "Order"
        self.details = {"order_id": order_id} if order_id else {}


class TickerTransformationError(TransformationError):
    """Raised when ticker data transformation fails."""

    def __init__(
        self,
        ticker_source: str,
        reason: str,
        symbol: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize ticker transformation error.

        Args:
            ticker_source: Source of ticker data (e.g., 'BackpackRawTicker')
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            original_error: The original exception
            source_data: The source ticker data
        """
        if symbol:
            message = f"Failed to transform {ticker_source} to Ticker for {symbol}: {reason}"
        else:
            message = f"Failed to transform {ticker_source} to Ticker: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.ticker_source = ticker_source
        self.symbol = symbol
        self.reason = reason
        self.source_type = ticker_source
        self.target_type = "Ticker"
        self.details = {"symbol": symbol} if symbol else {}


class MarketTransformationError(TransformationError):
    """Raised when market data transformation fails."""

    def __init__(
        self,
        reason: str,
        symbol: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
        source_type: str = "RawMarket",
    ) -> None:
        """Initialize market transformation error.

        Args:
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            original_error: The original exception
            source_data: The source market data
            source_type: Type of source data (default: RawMarket)
        """
        if symbol:
            message = f"Failed to transform {source_type} to Market for {symbol}: {reason}"
        else:
            message = f"Failed to transform {source_type} to Market: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.symbol = symbol
        self.reason = reason
        self.source_type = source_type
        self.target_type = "Market"
        self.details = {"symbol": symbol} if symbol else {}


class OrderBookTransformationError(TransformationError):
    """Raised when order book data transformation fails."""

    def __init__(
        self,
        source_type: str,
        reason: str,
        symbol: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize order book transformation error.

        Args:
            source_type: Source type (e.g., 'BackpackRawOrderBook')
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            original_error: The original exception
            source_data: The source order book data
        """
        if symbol:
            message = f"Failed to transform {source_type} to OrderBook for {symbol}: {reason}"
        else:
            message = f"Failed to transform {source_type} to OrderBook: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.symbol = symbol
        self.reason = reason
        self.source_type = source_type
        self.target_type = "OrderBook"
        self.details = {"symbol": symbol} if symbol else {}


class TradeTransformationError(TransformationError):
    """Raised when trade data transformation fails."""

    def __init__(
        self,
        trade_source: str,
        reason: str,
        symbol: str | None = None,
        trade_id: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize trade transformation error.

        Args:
            trade_source: Source of trade data (e.g., 'BackpackRawPublicTrade')
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            trade_id: Trade ID if available
            original_error: The original exception
            source_data: The source trade data
        """
        if symbol and trade_id:
            message = (
                f"Failed to transform {trade_source} to Trade for {symbol} "
                f"(ID: {trade_id}): {reason}"
            )
        elif symbol:
            message = f"Failed to transform {trade_source} to Trade for {symbol}: {reason}"
        elif trade_id:
            message = f"Failed to transform {trade_source} to Trade (ID: {trade_id}): {reason}"
        else:
            message = f"Failed to transform {trade_source} to Trade: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.trade_source = trade_source
        self.symbol = symbol
        self.trade_id = trade_id
        self.reason = reason
        self.source_type = trade_source
        self.target_type = "Trade"
        self.details = {
            "symbol": symbol,
            "trade_id": trade_id,
        }


class FillTransformationError(TransformationError):
    """Raised when fill data transformation fails."""

    def __init__(
        self,
        fill_source: str,
        reason: str,
        symbol: str | None = None,
        fill_id: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize fill transformation error.

        Args:
            fill_source: Source of fill data (e.g., 'BackpackRawFillResponse')
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            fill_id: Fill ID if available
            original_error: The original exception
            source_data: The source fill data
        """
        if symbol and fill_id:
            message = (
                f"Failed to transform {fill_source} to Fill for {symbol} (ID: {fill_id}): {reason}"
            )
        elif symbol:
            message = f"Failed to transform {fill_source} to Fill for {symbol}: {reason}"
        elif fill_id:
            message = f"Failed to transform {fill_source} to Fill (ID: {fill_id}): {reason}"
        else:
            message = f"Failed to transform {fill_source} to Fill: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.fill_source = fill_source
        self.symbol = symbol
        self.fill_id = fill_id
        self.reason = reason
        self.source_type = fill_source
        self.target_type = "Fill"
        self.details = {
            "symbol": symbol,
            "fill_id": fill_id,
        }


class FundingRateTransformationError(TransformationError):
    """Raised when funding rate data transformation fails."""

    def __init__(
        self,
        source_type: str,
        reason: str,
        symbol: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize funding rate transformation error.

        Args:
            source_type: Source type (e.g., 'BackpackRawFundingRate')
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            original_error: The original exception
            source_data: The source funding rate data
        """
        if symbol:
            message = f"Failed to transform {source_type} to FundingRate for {symbol}: {reason}"
        else:
            message = f"Failed to transform {source_type} to FundingRate: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.symbol = symbol
        self.reason = reason
        self.source_type = source_type
        self.target_type = "FundingRate"
        self.details = {"symbol": symbol} if symbol else {}


class CandleTransformationError(TransformationError):
    """Raised when candle/kline data transformation fails."""

    def __init__(
        self,
        reason: str,
        symbol: str | None = None,
        interval: str | None = None,
        original_error: Exception | None = None,
        source_data: dict[str, object] | None = None,
    ) -> None:
        """Initialize candle transformation error.

        Args:
            reason: Reason for transformation failure
            symbol: Symbol being transformed if available
            interval: Interval being transformed if available
            original_error: The original exception
            source_data: The source kline data
        """
        if symbol and interval:
            message = (
                f"Failed to transform BackpackRawKline to Candle for {symbol} "
                f"({interval}): {reason}"
            )
        elif symbol:
            message = f"Failed to transform BackpackRawKline to Candle for {symbol}: {reason}"
        else:
            message = f"Failed to transform BackpackRawKline to Candle: {reason}"

        super().__init__(
            message=message,
            source_value=source_data,
            original_exception=original_error,
        )
        self.symbol = symbol
        self.interval = interval
        self.reason = reason
        self.source_type = "BackpackRawKline"
        self.target_type = "Candle"
        self.details = {
            "symbol": symbol,
            "interval": interval,
        }
