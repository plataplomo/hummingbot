"""
CyberDeltaEngine: Backpack Account Data Mapper
---------------------------------------------

This module provides the BackpackAccountDataMapper class for transforming
Backpack Raw Account Data models into Internal Domain Models.

Responsibilities:
- Transform Raw Balances to Internal SpotBalance models
- Transform Raw Positions to Internal DerivativePosition models
- Transform Raw Account Summaries to Internal MarginAccountSummary models
- Transform Raw User Fills to Internal Trade models
- Transform WebSocket Account Data events to Internal models

All transformation methods follow the standard pattern:
- Take a validated Raw Pydantic Model as primary input
- Return fully populated Internal Domain Model with Details slots
- Handle type conversions, enum mapping, and error cases
- Raise TransformationError for unmappable data
"""

import logging
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import RawJsonResponse
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPosition,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawFill, BackpackRawTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.exchange_names import ExchangeName
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.core.models import (
    BackpackMarginDetails,
    BackpackOrderDetails,
    BackpackPositionDetails,
    BackpackSpotBalanceDetails,
    BackpackTransferDetails,
    BackpackWithdrawalDetails,
    DerivativePosition,
    MarginAccountSummary,
    Order,
    SpotBalance,
    Trade,
)
from cyberdelta.core.models.enums import (
    OrderExpiryReason,
    OrderSide,
    OrderStatus,
    OrderType,
    OrderUpdateOrigin,
    SelfTradePrevention,
    TimeInForce,
    TriggerType,
)
from cyberdelta.core.models.market.trade import BackpackTradeDetails
from cyberdelta.core.models.operations import (
    InternalTransferStatus,
    InternalWithdrawalStatus,
    Transfer,
    Withdrawal,
)
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value

logger = logging.getLogger(__name__)


class BackpackAccountDataMapper:
    """
    Domain-focused mapper for Backpack account data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to account data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """
        Maps a Backpack order side string to internal OrderSide enum.

        Args:
            bp_side: Raw side string from Backpack ("Buy", "Sell", "Bid", "Ask")

        Returns:
            OrderSide: Mapped internal enum value

        Raises:
            TransformationError: If side cannot be mapped
        """
        side_lower = bp_side.lower() if bp_side else ""
        if side_lower in ("buy", "bid"):
            return OrderSide.BUY
        elif side_lower in ("sell", "ask"):
            return OrderSide.SELL

        raise TransformationError(f"Unknown Backpack order side: '{bp_side}'")

    @staticmethod
    def _map_status_to_internal(bp_status: str) -> OrderStatus:
        """Maps a Backpack order status string to internal OrderStatus enum."""
        status_lower = bp_status.lower() if bp_status else ""
        if status_lower == "new":
            return OrderStatus.NEW
        elif status_lower in ("open", "pending"):
            return OrderStatus.OPEN
        elif status_lower in ("filled", "executed"):
            return OrderStatus.FILLED
        elif status_lower in ("cancelled", "canceled"):
            return OrderStatus.CANCELED
        elif status_lower in ("partially_filled", "partiallyfilled", "partial"):
            return OrderStatus.PARTIALLY_FILLED
        elif status_lower in ("rejected", "failed"):
            return OrderStatus.REJECTED
        elif status_lower == "expired":
            return OrderStatus.EXPIRED
        else:
            logger.warning(f"Unknown Backpack order status: '{bp_status}', mapping to UNKNOWN")
            return OrderStatus.UNKNOWN

    @staticmethod
    def _map_type_to_internal(bp_type: str) -> OrderType:
        """Maps a Backpack order type string to internal OrderType enum."""
        type_lower = bp_type.lower() if bp_type else ""
        if type_lower in ("limit", "limit_order"):
            return OrderType.LIMIT
        elif type_lower in ("market", "market_order"):
            return OrderType.MARKET
        elif type_lower in ("stop", "stop_loss", "stoploss", "stop_market"):
            return OrderType.STOP_MARKET
        elif type_lower in ("take_profit", "takeprofit", "take_profit_market"):
            return OrderType.TAKE_PROFIT_MARKET
        elif type_lower in ("stop_limit", "stop_loss_limit"):
            return OrderType.STOP_LIMIT
        elif type_lower in ("take_profit_limit", "takeprofit_limit"):
            return OrderType.TAKE_PROFIT_LIMIT
        else:
            logger.warning(f"Unknown Backpack order type: '{bp_type}', mapping to LIMIT")
            return OrderType.LIMIT  # Default to LIMIT instead of UNKNOWN

    @staticmethod
    def _map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        """Maps a Backpack time in force string to internal TimeInForce enum."""
        if bp_tif is None:
            return TimeInForce.GTC  # Default to GTC
        tif_lower = bp_tif.lower()
        if tif_lower == "gtc":
            return TimeInForce.GTC
        elif tif_lower == "ioc":
            return TimeInForce.IOC
        elif tif_lower == "fok":
            return TimeInForce.FOK
        else:
            logger.warning(f"Unknown Backpack TIF: '{bp_tif}', mapping to GTC")
            return TimeInForce.GTC  # Default to GTC instead of UNKNOWN

    @staticmethod
    def _map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        """Maps a Backpack trigger_by string to internal TriggerType enum."""
        if trigger_by is None:
            return None
        trigger_lower = trigger_by.lower()
        if trigger_lower in ("mark", "mark_price"):
            return TriggerType.MARK_PRICE
        elif trigger_lower in ("last", "last_price"):
            return TriggerType.LAST_PRICE
        elif trigger_lower in ("index", "index_price"):
            return TriggerType.INDEX_PRICE
        else:
            logger.warning(f"Unknown Backpack trigger_by: '{trigger_by}', returning None")
            return None

    @staticmethod
    def _map_transfer_status_to_internal(raw_status: str | None) -> InternalTransferStatus:
        """Maps a Backpack transfer status string to internal InternalTransferStatus enum."""
        if raw_status is None:
            return InternalTransferStatus.UNKNOWN
        status_lower = raw_status.lower()
        if status_lower in ("success", "completed", "processed"):
            return InternalTransferStatus.COMPLETED
        elif status_lower in ("pending", "processing"):
            return InternalTransferStatus.PENDING
        elif status_lower in ("failed", "failure", "rejected"):
            return InternalTransferStatus.FAILED
        elif status_lower in ("cancelled", "canceled"):
            return InternalTransferStatus.REJECTED  # Map canceled to REJECTED
        else:
            logger.warning(f"Unknown Backpack transfer status: '{raw_status}', mapping to UNKNOWN")
            return InternalTransferStatus.UNKNOWN

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: BackpackRawFill) -> Trade | None:
        """
        Transforms a BackpackRawFill to an Internal Trade model.

        Args:
            raw_fill: Validated raw fill from Backpack

        Returns:
            Trade | None: Internal domain model with BP details populated, or None if
                         price or quantity is zero

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Map side
            side = BackpackAccountDataMapper._map_side_to_internal(raw_fill.side)

            # Parse price and quantity
            price = parse_decimal_value(raw_fill.price, allow_none=False, field_name="price")
            quantity = parse_decimal_value(
                raw_fill.quantity, allow_none=False, field_name="quantity"
            )

            if price is None or quantity is None:
                raise TransformationError("Price and quantity are required for trade")

            # Check if price or quantity is zero - Trade model requires positive values
            if price <= Decimal("0") or quantity <= Decimal("0"):
                logger.warning(
                    f"Skipping trade {raw_fill.trade_id} with zero price ({price}) "
                    f"or quantity ({quantity})"
                )
                return None

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(raw_fill.fee, allow_none=True, field_name="fee") or Decimal(
                "0"
            )

            # Create BP-specific details
            details = BackpackTradeDetails(
                system_order_type=None  # Not available in fill data
            )

            return Trade(
                id=str(raw_fill.trade_id),
                symbol=raw_fill.symbol,
                executed_at=executed_at,
                side=side,
                order_id=raw_fill.order_id,
                exchange=ExchangeName.BACKPACK.value,
                client_order_id=raw_fill.client_id,
                price=price,
                quantity=quantity,
                fee=fee,
                fee_asset=raw_fill.fee_symbol,
                is_maker=raw_fill.is_maker,
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawFill to Trade: {e}") from e

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str, total_balance: str, available_balance: str
    ) -> SpotBalance:
        """
        Transforms balance data to an Internal SpotBalance model.

        Args:
            asset: Asset symbol
            total_balance: Total balance as string
            available_balance: Available balance as string

        Returns:
            SpotBalance: Internal domain model with BP details populated

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse balances
            total = parse_decimal_value(total_balance, allow_none=False, field_name="total_balance")
            available = parse_decimal_value(
                available_balance, allow_none=False, field_name="available_balance"
            )

            if total is None or available is None:
                raise TransformationError("Total and available balances are required")

            # Create BP-specific details
            details = BackpackSpotBalanceDetails()

            return SpotBalance(
                asset=asset,
                exchange=ExchangeName.BACKPACK.value,
                total_quantity=total,
                available_quantity=available,
                timestamp=datetime.now(UTC),
                bp_details=details,
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform balance data to SpotBalance: {e}"
            ) from e

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str, raw: BackpackRawBalance
    ) -> SpotBalance:
        """
        Transforms a validated `BackpackRawBalance` object for a specific asset into an
        internal `SpotBalance` domain model.

        Args:
            asset_symbol: The symbol of the asset (e.g., 'USDC', 'SOL').
            raw: The validated raw balance data for the asset.

        Returns:
            SpotBalance: The corresponding internal `SpotBalance` object.

        Raises:
            TransformationError: If essential numeric fields are missing or invalid.
        """
        try:
            # Defensive parsing of numeric strings
            parsed_total = parse_decimal_value(
                raw.total, allow_none=False, field_name=f"{asset_symbol}_total"
            )
            parsed_available = parse_decimal_value(
                raw.available, allow_none=False, field_name=f"{asset_symbol}_available"
            )

            if parsed_total is None:
                raise TransformationError(
                    f"Total quantity missing/invalid for {asset_symbol} in BackpackRawBalance"
                )
            if parsed_available is None:
                raise TransformationError(
                    f"Available quantity missing/invalid for {asset_symbol} in BackpackRawBalance"
                )

            bp_details = BackpackSpotBalanceDetails()

            return SpotBalance(
                exchange=ExchangeName.BACKPACK,
                asset=asset_symbol.upper(),
                timestamp=datetime.now(UTC),
                total_quantity=parsed_total,
                available_quantity=parsed_available,
                bp_details=bp_details,
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw balance to internal: {e}") from e

    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPosition) -> DerivativePosition:
        """
        Transforms a validated `BackpackRawPosition` object into an internal
        `DerivativePosition` domain model.

        Args:
            raw: The validated raw position data from Backpack.

        Returns:
            DerivativePosition: The corresponding internal `DerivativePosition` object.

        Raises:
            TransformationError: If essential numeric fields are missing or invalid.
        """
        try:
            # Parse core numeric fields defensively
            size_dec = parse_decimal_value(
                raw.net_quantity, allow_none=False, field_name="net_quantity"
            )
            if size_dec is None:
                raise TransformationError("net_quantity missing/invalid in BackpackRawPosition")

            entry_price_dec = parse_decimal_value(raw.entry_price)
            mark_price_dec = parse_decimal_value(raw.mark_price)
            liq_price_dec = parse_decimal_value(raw.est_liquidation_price)
            unrealized_pnl_dec = parse_decimal_value(raw.pnl_unrealized)
            realized_pnl_dec = parse_decimal_value(raw.pnl_realized)

            # Determine side
            side = OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL
            if size_dec == Decimal("0"):
                entry_price_dec = None

            timestamp = datetime.now(UTC)

            # Create BackpackPositionDetails with available data
            imf_base_dec = parse_decimal_value(raw.imf_function.base, allow_none=True)
            imf_factor_dec = parse_decimal_value(raw.imf_function.factor, allow_none=True)
            mmf_base_dec = parse_decimal_value(raw.mmf_function.base, allow_none=True)
            mmf_factor_dec = parse_decimal_value(raw.mmf_function.factor, allow_none=True)
            cumulative_funding_dec = parse_decimal_value(
                raw.cumulative_funding_payment, allow_none=True
            )

            bp_details = BackpackPositionDetails(
                imf_base=imf_base_dec,
                imf_factor=imf_factor_dec,
                mmf_base=mmf_base_dec,
                mmf_factor=mmf_factor_dec,
                cumulative_funding=cumulative_funding_dec,
            )

            return DerivativePosition(
                exchange=ExchangeName.BACKPACK,
                symbol=raw.symbol,
                timestamp=timestamp,
                side=side,
                size=size_dec,
                entry_price=entry_price_dec,
                mark_price=mark_price_dec,
                liquidation_price=liq_price_dec,
                unrealized_pnl=unrealized_pnl_dec,
                realized_pnl=realized_pnl_dec,
                bp_details=bp_details,
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw position to internal: {e}") from e

    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummary,
        spot_balances_raw: dict[str, BackpackRawBalance],
        derivative_positions_raw: list[BackpackRawPosition],
    ) -> MarginAccountSummary:
        """
        Transforms raw Backpack account settings, along with separately fetched raw balances
        and positions, into an internal `MarginAccountSummary` model.

        Args:
            raw_settings: The validated `BackpackRawAccountSummary` Pydantic model.
            spot_balances_raw: A dictionary of validated raw spot balances.
            derivative_positions_raw: A list of validated raw derivative positions.

        Returns:
            MarginAccountSummary: The corresponding internal `MarginAccountSummary` model.

        Raises:
            TransformationError: If critical numeric fields cannot be parsed.
        """
        try:
            internal_spot_balances = [
                BackpackAccountDataMapper.transform_raw_balance_to_internal(symbol, bal_raw)
                for symbol, bal_raw in spot_balances_raw.items()
            ]
            internal_derivative_positions = [
                BackpackAccountDataMapper.transform_raw_position_to_internal(pos_raw)
                for pos_raw in derivative_positions_raw
            ]

            calculated_total_equity = Decimal("0.0")
            calculated_available_equity = Decimal("0.0")
            calculated_total_position_notional = Decimal("0.0")
            calculated_total_unrealized_pnl = Decimal("0.0")
            calculated_assets_value_spot = Decimal("0.0")

            for sb in internal_spot_balances:
                if sb.asset.upper() in ["USD", "USDC", "USDT"]:
                    calculated_total_equity += sb.total_quantity
                    calculated_available_equity += sb.available_quantity
                    calculated_assets_value_spot += sb.total_quantity

            for dp in internal_derivative_positions:
                if dp.unrealized_pnl is not None:
                    calculated_total_unrealized_pnl += dp.unrealized_pnl
                if dp.entry_price is not None:
                    if dp.size.is_finite() and dp.entry_price.is_finite():
                        calculated_total_position_notional += abs(dp.size * dp.entry_price)

            calculated_total_equity += calculated_total_unrealized_pnl

            bp_details = BackpackMarginDetails(
                assets_value=calculated_assets_value_spot,
                borrow_liability=None,
                liabilities_value=None,
                locked_equity=None,
                margin_fraction=None,
                imf_raw=str(raw_settings.leverage_limit),
                mmf_raw=None,
            )

            return MarginAccountSummary(
                exchange=ExchangeName.BACKPACK.value,
                timestamp=datetime.now(UTC),
                total_equity=calculated_total_equity,
                available_equity=calculated_available_equity,
                total_initial_margin_required=None,
                total_maintenance_margin_required=None,
                total_position_notional=calculated_total_position_notional
                if internal_derivative_positions
                else Decimal("0.0"),
                total_unrealized_pnl=calculated_total_unrealized_pnl
                if internal_derivative_positions
                else Decimal("0.0"),
                bp_details=bp_details,
                hl_details=None,
            )
        except (ValidationError, TypeError, AttributeError, KeyError) as e:
            logger.error(
                f"[BackpackAccountDataMapper] Error transforming raw account summary: {e}",
                exc_info=True,
            )
            raise TransformationError(f"Error transforming raw account summary: {e}") from e

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
        """
        Transforms a raw Backpack transfer JSON response into an internal `Transfer` model.

        Args:
            raw_response: The raw JSON dictionary from the transfer API call.
            exchange_name: The name of the exchange to embed in the internal model.
            asset: The symbol of the asset transferred.
            quantity: The amount of the asset transferred.
            from_account_type_raw: The raw string for the source account type.
            to_account_type_raw: The raw string for the destination account type.
            client_transfer_id: Optional client-provided ID for the transfer.

        Returns:
            Transfer: The internal domain model representing the transfer.

        Raises:
            TransformationError: If essential fields are missing or transformation fails.
        """
        try:
            logger.debug(
                f"[BackpackAccountDataMapper] Transforming raw transfer: "
                f"{raw_response!r} for {asset}"
            )

            if not isinstance(raw_response, dict):
                raise TransformationError(
                    f"Raw transfer response is not a dict: {type(raw_response)}"
                )

            transfer_id = raw_response.get("id")
            raw_status_val = raw_response.get("status")
            message = raw_response.get("message")
            timestamp_ms_str = raw_response.get("timestamp")

            if not transfer_id:
                raise TransformationError("Missing 'id' in raw transfer response")

            # Ensure raw_status is str or None
            raw_status_str: str | None = None
            if raw_status_val is None:
                raw_status_str = None
            elif isinstance(raw_status_val, str):
                raw_status_str = raw_status_val
            else:
                logger.warning(f"Unexpected type for raw transfer status: {type(raw_status_val)}")
                raw_status_str = None

            internal_status = BackpackAccountDataMapper._map_transfer_status_to_internal(
                raw_status_str
            )

            timestamp: datetime
            if timestamp_ms_str and isinstance(timestamp_ms_str, str | int):
                try:
                    timestamp_ms = int(timestamp_ms_str)
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
                except ValueError:
                    logger.warning(
                        f"Invalid timestamp format '{timestamp_ms_str}' "
                        f"for transfer '{transfer_id}'"
                    )
                    timestamp = datetime.now(UTC)
            else:
                timestamp = datetime.now(UTC)

            bp_details = BackpackTransferDetails(
                client_id=client_transfer_id,
                from_account_type=from_account_type_raw,
                to_account_type=to_account_type_raw,
            )

            return Transfer(
                id=str(transfer_id),
                exchange=exchange_name,
                asset=asset,
                quantity=quantity,
                status=internal_status,
                timestamp=timestamp,
                response_message=str(message) if message is not None else None,
                bp_details=bp_details,
                hl_details=None,
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw transfer to internal: {e}") from e

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
        """
        Transforms a raw Backpack withdrawal response into an internal `Withdrawal` model.

        Args:
            raw_response: The validated `BackpackRawWithdrawalResponse` Pydantic model.
            asset: The asset symbol being withdrawn.
            quantity: The amount of the asset withdrawn.
            address: The destination address.
            network: The blockchain network used.
            client_withdrawal_id: Client-provided ID for the withdrawal.
            tag: Destination tag/memo, if provided.

        Returns:
            Withdrawal: The corresponding internal `Withdrawal` model.

        Raises:
            TransformationError: If transformation fails.
        """
        try:
            withdrawal_id = raw_response.id
            raw_status = raw_response.status
            timestamp_str = raw_response.created_at
            fee_str = raw_response.fee
            tx_hash_str = raw_response.transaction_hash

            internal_status = InternalWithdrawalStatus.UNKNOWN
            if raw_status:
                status_upper = raw_status.upper()
                if status_upper in ["COMPLETED", "SUCCESS", "PROCESSED", "CONFIRMED"]:
                    internal_status = InternalWithdrawalStatus.COMPLETED
                elif status_upper == "PENDING":
                    internal_status = InternalWithdrawalStatus.PENDING
                elif status_upper in ["FAILED", "FAILURE", "REJECTED"]:
                    internal_status = InternalWithdrawalStatus.FAILED
                elif status_upper == "CANCELLED":
                    internal_status = InternalWithdrawalStatus.CANCELED
                else:
                    logger.warning(f"Unknown Backpack withdrawal status: {raw_status}")

            timestamp_value: datetime
            if timestamp_str:
                try:
                    parsed_dt = parse_datetime_utc(timestamp_str, field_name="created_at")
                    if parsed_dt is None:
                        timestamp_value = datetime.now(UTC)
                    else:
                        timestamp_value = parsed_dt
                except ValueError:
                    logger.warning(f"Could not parse withdrawal timestamp: {timestamp_str}")
                    timestamp_value = datetime.now(UTC)
            else:
                timestamp_value = datetime.now(UTC)

            fee = parse_decimal_value(fee_str, field_name="fee", allow_none=True)

            bp_details = BackpackWithdrawalDetails(
                blockchain=network or raw_response.blockchain,
                is_internal=raw_response.is_internal,
                client_id=client_withdrawal_id or raw_response.client_id,
                identifier=raw_response.identifier,
                fiat_fee=parse_decimal_value(
                    raw_response.fiat_fee, field_name="fiat_fee", allow_none=True
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

            return Withdrawal(
                id=str(withdrawal_id),
                exchange=ExchangeName.BACKPACK.value,
                status=internal_status,
                asset=asset,
                quantity=quantity,
                address=address,
                timestamp=timestamp_value,
                fee=fee,
                tx_hash=tx_hash_str,
                response_message=None,
                bp_details=bp_details,
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw withdrawal to internal: {e}") from e

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        """
        Transforms a validated `BackpackRawOrder` object into an internal `Order` domain model.

        Args:
            raw: The validated raw order data from Backpack.

        Returns:
            Order: The corresponding internal `Order` object.

        Raises:
            TransformationError: If essential fields are missing or cannot be parsed.
        """
        try:
            # Defensive: ensure required fields are present and valid
            parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)
            if parsed_quantity is None:
                raise TransformationError("quantity missing/invalid in BackpackRawOrder")
            parsed_created_at = parse_datetime_utc(raw.createdAt)
            if parsed_created_at is None:
                raise TransformationError("createdAt missing/invalid in BackpackRawOrder")

            # Optional fields
            parsed_quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")
            parsed_price = parse_decimal_value(raw.price)
            parsed_stop_price = parse_decimal_value(raw.triggerPrice)
            parsed_avg_fill_price = parse_decimal_value(raw.avgFillPrice)

            # Create BackpackOrderDetails with available data
            executed_quote_quantity = parse_decimal_value(
                raw.executedQuoteQuantity, allow_none=True
            )

            # Map self trade prevention string to enum if available
            stp_enum = None
            if raw.selfTradePrevention:
                stp_str = raw.selfTradePrevention.upper()
                if stp_str == "REJECT_TAKER" or stp_str == "REJECTTAKER":
                    stp_enum = SelfTradePrevention.REJECT_TAKER
                elif stp_str == "REJECT_MAKER" or stp_str == "REJECTMAKER":
                    stp_enum = SelfTradePrevention.REJECT_MAKER
                elif stp_str == "REJECT_BOTH" or stp_str == "REJECTBOTH":
                    stp_enum = SelfTradePrevention.REJECT_BOTH
                elif stp_str == "NONE":
                    stp_enum = SelfTradePrevention.NONE

            # Map expiry reason string to enum if available
            expiry_enum = None
            if raw.expiryReason:
                expiry_str = raw.expiryReason.upper()
                if expiry_str == "USER_CANCELLED" or expiry_str == "CANCELLED":
                    expiry_enum = OrderExpiryReason.USER_CANCELLED
                elif expiry_str == "LIQUIDATION":
                    expiry_enum = OrderExpiryReason.LIQUIDATION
                elif expiry_str == "INSUFFICIENT_FUNDS":
                    expiry_enum = OrderExpiryReason.INSUFFICIENT_FUNDS
                elif expiry_str == "SELF_TRADE_PREVENTION":
                    expiry_enum = OrderExpiryReason.SELF_TRADE_PREVENTION
                elif expiry_str == "POST_ONLY_TAKER":
                    expiry_enum = OrderExpiryReason.POST_ONLY_TAKER
                elif expiry_str == "FILL_OR_KILL":
                    expiry_enum = OrderExpiryReason.FILL_OR_KILL
                elif expiry_str == "IMMEDIATE_OR_CANCEL":
                    expiry_enum = OrderExpiryReason.IMMEDIATE_OR_CANCEL
                else:
                    expiry_enum = OrderExpiryReason.UNKNOWN

            # Map origin string to enum if available
            origin_enum = None
            if raw.origin:
                origin_str = raw.origin.upper()
                if origin_str == "USER":
                    origin_enum = OrderUpdateOrigin.USER
                elif origin_str == "LIQUIDATION_AUTOCLOSE":
                    origin_enum = OrderUpdateOrigin.LIQUIDATION_AUTOCLOSE
                elif origin_str == "ADL_AUTOCLOSE":
                    origin_enum = OrderUpdateOrigin.ADL_AUTOCLOSE
                elif origin_str == "COLLATERAL_CONVERSION":
                    origin_enum = OrderUpdateOrigin.COLLATERAL_CONVERSION
                elif origin_str == "SETTLEMENT_AUTOCLOSE":
                    origin_enum = OrderUpdateOrigin.SETTLEMENT_AUTOCLOSE
                elif origin_str == "BACKSTOP_LIQUIDITY_PROVIDER":
                    origin_enum = OrderUpdateOrigin.BACKSTOP_LIQUIDITY_PROVIDER
                else:
                    origin_enum = OrderUpdateOrigin.UNKNOWN

            bp_details = BackpackOrderDetails(
                executed_quote_quantity=executed_quote_quantity,
                self_trade_prevention=stp_enum,
                expiry_reason=expiry_enum,
                origin=origin_enum,
                # Note: Other fields like sl_trigger_price, tp_trigger_price etc. are not
                # available in the basic BackpackRawOrder model and would need to come from
                # other API endpoints
            )

            return Order(
                client_order_id=raw.clientId or str(uuid.uuid4()),
                exchange_order_id=raw.id,
                related_order_id=raw.relatedOrderId,
                exchange=ExchangeName.BACKPACK,
                symbol=raw.symbol,
                side=BackpackAccountDataMapper._map_side_to_internal(raw.side),
                order_type=BackpackAccountDataMapper._map_type_to_internal(raw.orderType),
                status=BackpackAccountDataMapper._map_status_to_internal(raw.status),
                quantity_requested=parsed_quantity,
                quantity_filled=parsed_quantity_filled,
                price=parsed_price,
                stop_price=parsed_stop_price,
                average_fill_price=parsed_avg_fill_price,
                trigger_by=BackpackAccountDataMapper._map_trigger_by_to_internal(raw.triggerBy),
                time_in_force=BackpackAccountDataMapper._map_tif_to_internal(raw.timeInForce),
                reduce_only=raw.reduceOnly or False,
                post_only=raw.postOnly or False,
                created_at=parsed_created_at,
                updated_at=parse_datetime_utc(raw.updatedAt),
                triggered_at=parse_datetime_utc(raw.triggeredAt),
                strategy_name=None,
                signal_id=None,
                trades=[],
                bp_details=bp_details,
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw order to internal: {e}") from e

    @staticmethod
    def transform_raw_trade_to_internal(raw: BackpackRawTrade) -> Trade | None:
        """
        Transforms a validated `BackpackRawTrade` object into an internal `Trade` domain model.

        Note: Backpack REST API for trades typically lacks side information.
        Returns None if essential information cannot be determined.

        Args:
            raw: The validated raw trade data from Backpack.

        Returns:
            Trade | None: The corresponding internal `Trade` object, or None if essential
                         information (like side) cannot be determined.

        Raises:
            TransformationError: If essential fields are missing or cannot be parsed.
        """
        try:
            price_dec = parse_decimal_value(raw.price, allow_none=False, field_name="price")
            quantity_dec = parse_decimal_value(
                raw.quantity, allow_none=False, field_name="quantity"
            )
            timestamp = parse_datetime_utc(raw.time, field_name="time")

            if price_dec is None:
                raise TransformationError("price missing/invalid in BackpackRawTrade")
            if quantity_dec is None:
                raise TransformationError("quantity missing/invalid in BackpackRawTrade")
            if timestamp is None:
                raise TransformationError("time missing/invalid in BackpackRawTrade")

            # Backpack REST API for recent trades doesn't provide side
            logger.warning(
                f"Cannot determine trade side for raw trade {raw.id} from REST API. Skipping."
            )
            return None
        except Exception as e:
            raise TransformationError(f"Failed to transform raw trade to internal: {e}") from e

    @staticmethod
    def transform_ws_fill_event_to_internal_trade(raw_fill: BackpackRawFill) -> Trade | None:
        """
        Transforms a WebSocket fill event (BackpackRawFill) to an Internal Trade model.

        This is an alias for transform_raw_fill_to_internal for consistency with WebSocket naming.

        Args:
            raw_fill: Validated raw fill event from Backpack WebSocket

        Returns:
            Trade | None: Internal domain model with BP details populated, or None if
                         price or quantity is zero

        Raises:
            TransformationError: If transformation fails
        """
        return BackpackAccountDataMapper.transform_raw_fill_to_internal(raw_fill)

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> DerivativePosition:
        """
        Transforms a BackpackRawPositionUpdate (WebSocket position update event) to an
        Internal DerivativePosition model.

        Args:
            raw_position_update: Validated raw position update event data from Backpack WebSocket

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails
        """
        try:
            # Parse core numeric fields defensively
            if raw_position_update.net_quantity:
                size_dec = parse_decimal_value(
                    raw_position_update.net_quantity, allow_none=False, field_name="net_quantity"
                )
                # DEFENSIVE CHECK: Ensure size_dec is not None after parsing.
                # Mypy=[unreachable] Ruff=[unreachable]
                if size_dec is None:
                    size_dec = Decimal("0")
            else:
                size_dec = Decimal("0")

            # Parse optional fields
            entry_price_dec = None
            if raw_position_update.entry_price:
                entry_price_dec = parse_decimal_value(raw_position_update.entry_price)

            mark_price_dec = None
            if raw_position_update.mark_price:
                mark_price_dec = parse_decimal_value(raw_position_update.mark_price)

            liq_price_dec = None
            if raw_position_update.liquidation_price:
                liq_price_dec = parse_decimal_value(raw_position_update.liquidation_price)

            # Determine side
            side = OrderSide.BUY if size_dec > Decimal("0") else OrderSide.SELL
            if size_dec == Decimal("0"):
                entry_price_dec = None

            # Parse timestamp from event_time
            timestamp = datetime.now(UTC)
            if raw_position_update.event_time:
                event_timestamp = parse_datetime_utc(
                    raw_position_update.event_time, field_name="event_time"
                )
                if event_timestamp is not None:
                    timestamp = event_timestamp

            # Create BackpackPositionDetails with available data
            imf_dec = None
            if raw_position_update.initial_margin_fraction:
                imf_dec = parse_decimal_value(
                    raw_position_update.initial_margin_fraction, allow_none=True
                )

            mmf_dec = None
            if raw_position_update.maintenance_margin_fraction:
                mmf_dec = parse_decimal_value(
                    raw_position_update.maintenance_margin_fraction, allow_none=True
                )

            bp_details = BackpackPositionDetails(
                imf_base=imf_dec,
                imf_factor=None,  # Not available in position update
                mmf_base=mmf_dec,
                mmf_factor=None,  # Not available in position update
                cumulative_funding=None,  # Not available in position update
            )

            return DerivativePosition(
                exchange=ExchangeName.BACKPACK,
                symbol=raw_position_update.symbol,
                timestamp=timestamp,
                side=side,
                size=size_dec,
                entry_price=entry_price_dec,
                mark_price=mark_price_dec,
                liquidation_price=liq_price_dec,
                unrealized_pnl=None,  # Not available in position update
                realized_pnl=None,  # Not available in position update
                bp_details=bp_details,
            )
        except Exception as e:
            raise TransformationError(
                f"Failed to transform WebSocket position update to internal: {e}"
            ) from e
