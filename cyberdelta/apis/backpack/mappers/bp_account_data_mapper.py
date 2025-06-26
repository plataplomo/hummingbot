"""CyberDeltaEngine: Backpack Account Data Mapper.

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

import uuid
from datetime import UTC, datetime
from decimal import Decimal
from itertools import starmap
from typing import Any

from pydantic import ValidationError

from cyberdelta.apis.backpack.bp_response_handler import RawJsonResponse
from cyberdelta.apis.backpack.models.bp_raw_account import BackpackRawBalance
from cyberdelta.apis.backpack.models.bp_raw_account_summary import BackpackRawAccountSummary
from cyberdelta.apis.backpack.models.bp_raw_collateral import BackpackRawCollateralResponse
from cyberdelta.apis.backpack.models.bp_raw_fills import BackpackRawFill
from cyberdelta.apis.backpack.models.bp_raw_order import BackpackRawOrder
from cyberdelta.apis.backpack.models.bp_raw_position import (
    BackpackRawPosition,
    BackpackRawPositionUpdate,
)
from cyberdelta.apis.backpack.models.bp_raw_trade import BackpackRawPublicTrade
from cyberdelta.apis.backpack.models.bp_raw_withdrawal import BackpackRawWithdrawalResponse
from cyberdelta.apis.models.api_error import TransformationError
from cyberdelta.apis.models.service_args_models import UpdateAccountSettingsArgs
from cyberdelta.config.structlog_config import get_logger
from cyberdelta.core.models import (
    AccountSettings,
    BackpackAccountSettingsDetails,
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
    InternalTransferStatus,
    InternalWithdrawalStatus,
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
    Transfer,
    Withdrawal,
)
from cyberdelta.enums.exchange_names import ExchangeName
from cyberdelta.utils.parsing import parse_datetime_utc, parse_decimal_value
from cyberdelta.utils.secure_transformation import secure_transform


logger = get_logger(__name__)


class BackpackAccountDataMapper:
    """Domain-focused mapper for Backpack account data transformations.

    This class contains static methods for transforming validated Backpack Raw models
    related to account data into CyberDeltaEngine Internal Domain Models.
    """

    @staticmethod
    def _map_side_to_internal(bp_side: str) -> OrderSide:
        """Map a Backpack order side string to internal OrderSide enum.

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
        if side_lower in ("sell", "ask"):
            return OrderSide.SELL

        raise TransformationError(f"Unknown Backpack order side: '{bp_side}'")

    @staticmethod
    def _map_status_to_internal(bp_status: str) -> OrderStatus:
        """Map a Backpack order status string to internal OrderStatus enum."""
        status_lower = bp_status.lower() if bp_status else ""
        if status_lower == "new":
            return OrderStatus.NEW
        if status_lower in ("open", "pending"):
            return OrderStatus.OPEN
        if status_lower in ("filled", "executed"):
            return OrderStatus.FILLED
        if status_lower in ("cancelled", "canceled"):
            return OrderStatus.CANCELED
        if status_lower in ("partially_filled", "partiallyfilled", "partial"):
            return OrderStatus.PARTIALLY_FILLED
        if status_lower in ("rejected", "failed"):
            return OrderStatus.REJECTED
        if status_lower == "expired":
            return OrderStatus.EXPIRED
        logger.warning(
            "bp_account_mapper_unknown_order_status",
            action="map_status_to_internal",
            message="Unknown Backpack order status encountered",
            bp_status=bp_status,
            mapped_to="UNKNOWN",
        )
        return OrderStatus.UNKNOWN

    @staticmethod
    def _map_type_to_internal(bp_type: str) -> OrderType:
        """Map a Backpack order type string to internal OrderType enum."""
        type_lower = bp_type.lower() if bp_type else ""
        if type_lower in ("limit", "limit_order"):
            return OrderType.LIMIT
        if type_lower in ("market", "market_order"):
            return OrderType.MARKET
        if type_lower in ("stop", "stop_loss", "stoploss", "stop_market"):
            return OrderType.STOP_MARKET
        if type_lower in ("take_profit", "takeprofit", "take_profit_market"):
            return OrderType.TAKE_PROFIT_MARKET
        if type_lower in ("stop_limit", "stop_loss_limit"):
            return OrderType.STOP_LIMIT
        if type_lower in ("take_profit_limit", "takeprofit_limit"):
            return OrderType.TAKE_PROFIT_LIMIT
        logger.warning(
            "bp_account_mapper_unknown_order_type",
            action="map_type_to_internal",
            message="Unknown Backpack order type encountered",
            bp_type=bp_type,
            mapped_to="LIMIT",
        )
        return OrderType.LIMIT  # Default to LIMIT instead of UNKNOWN

    @staticmethod
    def _map_tif_to_internal(bp_tif: str | None) -> TimeInForce:
        """Map a Backpack time in force string to internal TimeInForce enum."""
        if bp_tif is None:
            return TimeInForce.GTC  # Default to GTC
        tif_lower = bp_tif.lower()
        if tif_lower == "gtc":
            return TimeInForce.GTC
        if tif_lower == "ioc":
            return TimeInForce.IOC
        if tif_lower == "fok":
            return TimeInForce.FOK
        logger.warning(
            "bp_account_mapper_unknown_tif",
            action="map_tif_to_internal",
            message="Unknown Backpack time-in-force encountered",
            bp_tif=bp_tif,
            mapped_to="GTC",
        )
        return TimeInForce.GTC  # Default to GTC instead of UNKNOWN

    @staticmethod
    def _map_trigger_by_to_internal(trigger_by: str | None) -> TriggerType | None:
        """Map a Backpack trigger_by string to internal TriggerType enum."""
        if trigger_by is None:
            return None
        trigger_lower = trigger_by.lower()
        if trigger_lower in ("mark", "mark_price"):
            return TriggerType.MARK_PRICE
        if trigger_lower in ("last", "last_price"):
            return TriggerType.LAST_PRICE
        if trigger_lower in ("index", "index_price"):
            return TriggerType.INDEX_PRICE
        logger.warning(
            "bp_account_mapper_unknown_trigger_by",
            action="map_trigger_by_to_internal",
            message="Unknown Backpack trigger_by value encountered",
            trigger_by=trigger_by,
            returning="None",
        )
        return None

    @staticmethod
    def _map_transfer_status_to_internal(raw_status: str | None) -> InternalTransferStatus:
        """Map a Backpack transfer status string to internal InternalTransferStatus enum."""
        if raw_status is None:
            return InternalTransferStatus.UNKNOWN
        status_lower = raw_status.lower()
        if status_lower in ("success", "completed", "processed"):
            return InternalTransferStatus.COMPLETED
        if status_lower in ("pending", "processing"):
            return InternalTransferStatus.PENDING
        if status_lower in ("failed", "failure", "rejected"):
            return InternalTransferStatus.FAILED
        if status_lower in ("cancelled", "canceled"):
            return InternalTransferStatus.REJECTED  # Map canceled to REJECTED
        logger.warning(
            "bp_account_mapper_unknown_transfer_status",
            action="map_transfer_status_to_internal",
            message="Unknown Backpack transfer status encountered",
            raw_status=raw_status,
            mapped_to="UNKNOWN",
        )
        return InternalTransferStatus.UNKNOWN

    @staticmethod
    def transform_raw_fill_to_internal(raw_fill: BackpackRawFill) -> Trade | None:
        """Transform a BackpackRawFill to an Internal Trade model.

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
                raw_fill.quantity,
                allow_none=False,
                field_name="quantity",
            )

            if price is None or quantity is None:
                raise TransformationError("Price and quantity are required for trade")

            # Check if price or quantity is zero - Trade model requires positive values
            if price <= Decimal(0) or quantity <= Decimal(0):
                logger.warning(
                    f"Skipping trade {raw_fill.trade_id} with zero price ({price}) "
                    f"or quantity ({quantity})",
                )
                return None

            # Parse timestamp
            executed_at = parse_datetime_utc(raw_fill.timestamp, field_name="timestamp")
            if executed_at is None:
                executed_at = datetime.now(UTC)

            # Parse fee
            fee = parse_decimal_value(raw_fill.fee, allow_none=True, field_name="fee") or Decimal(
                0,
            )

            # Create BP-specific details
            details = BackpackTradeDetails(
                system_order_type=None,  # Not available in fill data
            )

            # Use secure_transform for type-safe model creation
            trade_data = {
                "id": str(raw_fill.trade_id),
                "symbol": raw_fill.symbol,
                "executed_at": executed_at.isoformat(),
                "side": side.value,
                "order_id": raw_fill.order_id,
                "exchange": ExchangeName.BACKPACK.value,
                "client_order_id": raw_fill.client_id,
                "price": str(price),
                "quantity": str(quantity),
                "fee": str(fee),
                "fee_asset": raw_fill.fee_symbol,
                "is_maker": raw_fill.is_maker,
                "bp_details": details.model_dump() if details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=trade_data,
                model_class=Trade,
                context="backpack_fill_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(f"Failed to transform BackpackRawFill to Trade: {e}") from e

    @staticmethod
    def transform_balance_data_to_spot_balance(
        asset: str,
        total_balance: str,
        available_balance: str,
    ) -> SpotBalance:
        """Transform balance data to an Internal SpotBalance model.

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
                available_balance,
                allow_none=False,
                field_name="available_balance",
            )

            if total is None or available is None:
                raise TransformationError("Total and available balances are required")

            # Create BP-specific details
            details = BackpackSpotBalanceDetails()

            # Use secure_transform for type-safe model creation
            balance_data = {
                "asset": asset,
                "exchange": ExchangeName.BACKPACK.value,
                "total_quantity": str(total),
                "available_quantity": str(available),
                "timestamp": datetime.now(UTC).isoformat(),
                "bp_details": details.model_dump() if details else None,
            }

            return secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context="backpack_balance_from_dict",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform balance data to SpotBalance: {e}",
            ) from e

    @staticmethod
    def transform_raw_balance_to_internal(
        asset_symbol: str,
        raw: BackpackRawBalance,
    ) -> SpotBalance:
        """Transform a validated BackpackRawBalance object for a specific asset.

        Converts the raw balance data into an internal SpotBalance domain model.

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
            parsed_available = parse_decimal_value(
                raw.available,
                allow_none=False,
                field_name=f"{asset_symbol}_available",
            )
            parsed_locked = parse_decimal_value(
                raw.locked,
                allow_none=False,
                field_name=f"{asset_symbol}_locked",
            )
            parsed_staked = parse_decimal_value(
                raw.staked,
                allow_none=False,
                field_name=f"{asset_symbol}_staked",
            )

            if parsed_available is None:
                raise TransformationError(
                    f"Available quantity missing/invalid for {asset_symbol} in BackpackRawBalance",
                )
            if parsed_locked is None:
                raise TransformationError(
                    f"Locked quantity missing/invalid for {asset_symbol} in BackpackRawBalance",
                )
            if parsed_staked is None:
                raise TransformationError(
                    f"Staked quantity missing/invalid for {asset_symbol} in BackpackRawBalance",
                )

            # Calculate total = available + locked + staked
            parsed_total = parsed_available + parsed_locked + parsed_staked

            bp_details = BackpackSpotBalanceDetails()

            # Use secure_transform for type-safe model creation
            balance_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "asset": asset_symbol.upper(),
                "timestamp": datetime.now(UTC).isoformat(),
                "total_quantity": str(parsed_total),
                "available_quantity": str(parsed_available),
                "bp_details": bp_details.model_dump() if bp_details else None,
            }

            return secure_transform(
                data=balance_data,
                model_class=SpotBalance,
                context=f"backpack_raw_balance_transform_{asset_symbol}",
                source_exchange="backpack",
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw balance to internal: {e}") from e

    @staticmethod
    def transform_raw_position_to_internal(raw: BackpackRawPosition) -> DerivativePosition:
        """Transform a validated `BackpackRawPosition` object into an internal model.

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
                raw.net_quantity,
                allow_none=False,
                field_name="net_quantity",
            )
            if size_dec is None:
                raise TransformationError("net_quantity missing/invalid in BackpackRawPosition")

            entry_price_dec = parse_decimal_value(raw.entry_price)
            # Note: Entry prices are often calculated averages that may have higher precision
            # than the market's tick size. We preserve the full precision as reported by the
            # exchange since this represents the actual average entry price across multiple fills.
            mark_price_dec = parse_decimal_value(raw.mark_price)
            liq_price_dec = parse_decimal_value(raw.est_liquidation_price)
            unrealized_pnl_dec = parse_decimal_value(raw.pnl_unrealized)
            realized_pnl_dec = parse_decimal_value(raw.pnl_realized)

            # Determine side
            side = OrderSide.BUY if size_dec > Decimal(0) else OrderSide.SELL
            if size_dec == Decimal(0):
                entry_price_dec = None

            timestamp = datetime.now(UTC)

            # Create BackpackPositionDetails with available data
            imf_base_dec = parse_decimal_value(raw.imf_function.base, allow_none=True)
            imf_factor_dec = parse_decimal_value(raw.imf_function.factor, allow_none=True)
            mmf_base_dec = parse_decimal_value(raw.mmf_function.base, allow_none=True)
            mmf_factor_dec = parse_decimal_value(raw.mmf_function.factor, allow_none=True)
            cumulative_funding_dec = parse_decimal_value(
                raw.cumulative_funding_payment,
                allow_none=True,
            )

            bp_details = BackpackPositionDetails(
                imf_base=imf_base_dec,
                imf_factor=imf_factor_dec,
                mmf_base=mmf_base_dec,
                mmf_factor=mmf_factor_dec,
                cumulative_funding=cumulative_funding_dec,
            )

            # Use secure_transform for type-safe model creation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw.symbol,
                "timestamp": timestamp.isoformat(),
                "side": side.value,
                "size": str(size_dec),
                "entry_price": str(entry_price_dec) if entry_price_dec is not None else None,
                "mark_price": str(mark_price_dec) if mark_price_dec is not None else None,
                "liquidation_price": str(liq_price_dec) if liq_price_dec is not None else None,
                "unrealized_pnl": str(unrealized_pnl_dec)
                if unrealized_pnl_dec is not None
                else None,
                "realized_pnl": str(realized_pnl_dec) if realized_pnl_dec is not None else None,
                "bp_details": bp_details.model_dump() if bp_details else None,
            }

            return secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context=f"backpack_position_transform_{raw.symbol}",
                source_exchange="backpack",
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw position to internal: {e}") from e

    @staticmethod
    def transform_raw_account_summary_to_internal(
        raw_settings: BackpackRawAccountSummary,
        spot_balances_raw: dict[str, BackpackRawBalance],
        derivative_positions_raw: list[BackpackRawPosition],
    ) -> MarginAccountSummary:
        """Transform raw Backpack account data into an internal `MarginAccountSummary`.

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
            internal_spot_balances = list(starmap(BackpackAccountDataMapper.transform_raw_balance_to_internal, spot_balances_raw.items()))
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
                imf_raw=None,  # IMF should come from actual margin data, not leverage limit
                mmf_raw=None,
                leverage_limit=raw_settings.leverage_limit,
            )

            # Use secure_transform for type-safe model creation
            margin_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "timestamp": datetime.now(UTC).isoformat(),
                "total_equity": str(calculated_total_equity),
                "available_equity": str(calculated_available_equity),
                "total_initial_margin_required": None,
                "total_maintenance_margin_required": None,
                "total_position_notional": str(
                    calculated_total_position_notional
                    if internal_derivative_positions
                    else Decimal("0.0"),
                ),
                "total_unrealized_pnl": str(
                    calculated_total_unrealized_pnl
                    if internal_derivative_positions
                    else Decimal("0.0"),
                ),
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="backpack_account_summary_transform",
                source_exchange="backpack",
            )
        except (ValidationError, TypeError, AttributeError, KeyError) as e:
            logger.error(
                f"[BackpackAccountDataMapper] Error transforming raw account summary: {e}",
                exc_info=True,
            )
            raise TransformationError(f"Error transforming raw account summary: {e}") from e

    @staticmethod
    def transform_enhanced_account_data_to_margin_summary(
        raw_collateral: BackpackRawCollateralResponse,
        raw_settings: BackpackRawAccountSummary,
        raw_positions: list[BackpackRawPosition],
    ) -> MarginAccountSummary:
        """Transform enhanced collateral data into internal MarginAccountSummary.

        Uses comprehensive margin data from /api/v1/capital/collateral endpoint
        to create a detailed MarginAccountSummary with enhanced bp_details.

        Args:
            raw_collateral: Validated collateral response from Backpack API
            raw_settings: Validated account settings from Backpack API
            raw_positions: List of validated derivative positions

        Returns:
            MarginAccountSummary: Enhanced account summary with collateral data

        Raises:
            TransformationError: If critical data cannot be parsed
        """
        try:
            # Parse core equity fields from collateral response
            total_equity = parse_decimal_value(
                raw_collateral.net_equity,
                allow_none=False,
                field_name="net_equity",
            )
            if total_equity is None:
                raise TransformationError("net_equity missing/invalid in collateral response")

            available_equity = parse_decimal_value(
                raw_collateral.net_equity_available,
                allow_none=False,
                field_name="net_equity_available",
            )
            if available_equity is None:
                raise TransformationError(
                    "net_equity_available missing/invalid in collateral response",
                )

            # Parse detailed collateral fields
            assets_value = parse_decimal_value(raw_collateral.assets_value, allow_none=True)
            liabilities_value = parse_decimal_value(
                raw_collateral.liabilities_value,
                allow_none=True,
            )
            locked_equity = parse_decimal_value(raw_collateral.net_equity_locked, allow_none=True)
            borrow_liability = parse_decimal_value(raw_collateral.borrow_liability, allow_none=True)
            unsettled_equity = parse_decimal_value(raw_collateral.unsettled_equity, allow_none=True)
            margin_fraction = parse_decimal_value(raw_collateral.margin_fraction, allow_none=True)
            net_exposure_futures = parse_decimal_value(
                raw_collateral.net_exposure_futures,
                allow_none=True,
            )

            # Parse margin factors
            imf_value = parse_decimal_value(raw_collateral.imf, allow_none=True)
            mmf_value = parse_decimal_value(raw_collateral.mmf, allow_none=True)

            # Parse unrealized PnL
            total_unrealized_pnl = parse_decimal_value(
                raw_collateral.pnl_unrealized,
                allow_none=True,
            )

            # Transform derivative positions
            internal_derivative_positions = [
                BackpackAccountDataMapper.transform_raw_position_to_internal(pos_raw)
                for pos_raw in raw_positions
            ]

            # Calculate position notional from derivative positions
            calculated_total_position_notional = Decimal("0.0")
            calculated_total_unrealized_pnl_positions = Decimal("0.0")

            for dp in internal_derivative_positions:
                if dp.unrealized_pnl is not None:
                    calculated_total_unrealized_pnl_positions += dp.unrealized_pnl
                if (
                    dp.entry_price is not None
                    and dp.size.is_finite()
                    and dp.entry_price.is_finite()
                ):
                    calculated_total_position_notional += abs(dp.size * dp.entry_price)

            # Prepare collateral assets data for bp_details
            collateral_assets_data: list[dict[str, str]] = []
            for asset in raw_collateral.collateral:
                collateral_assets_data.append({
                    "symbol": asset.symbol,
                    "total_quantity": str(asset.total_quantity),
                    "collateral_value": str(asset.collateral_value),
                    "collateral_weight": str(asset.collateral_weight),
                    "asset_mark_price": str(asset.asset_mark_price),
                })

            # Create enhanced BackpackMarginDetails
            bp_details = BackpackMarginDetails(
                assets_value=assets_value,
                liabilities_value=liabilities_value,
                locked_equity=locked_equity,
                borrow_liability=borrow_liability,
                unsettled_equity=unsettled_equity,
                margin_fraction=margin_fraction,
                net_exposure_futures=net_exposure_futures,
                imf_raw=str(raw_collateral.imf) if raw_collateral.imf else None,
                mmf_raw=str(raw_collateral.mmf) if raw_collateral.mmf else None,
                leverage_limit=raw_settings.leverage_limit,
                subaccount_id=None,  # Set if subaccount was used in request
                collateral_assets=collateral_assets_data,
                source_endpoint="collateral",
            )

            # Use secure_transform for type-safe model creation
            margin_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "timestamp": datetime.now(UTC).isoformat(),
                "total_equity": str(total_equity),
                "available_equity": str(available_equity),
                "total_initial_margin_required": str(imf_value) if imf_value is not None else None,
                "total_maintenance_margin_required": str(mmf_value)
                if mmf_value is not None
                else None,
                "total_position_notional": str(calculated_total_position_notional),
                "total_unrealized_pnl": (
                    str(total_unrealized_pnl) if total_unrealized_pnl is not None else None
                ),
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=margin_data,
                model_class=MarginAccountSummary,
                context="backpack_enhanced_account_transform",
                source_exchange="backpack",
            )

        except (ValidationError, TypeError, AttributeError, KeyError) as e:
            logger.error(
                f"[BackpackAccountDataMapper] Error transforming collateral data: {e}",
                exc_info=True,
            )
            raise TransformationError(f"Error transforming collateral data: {e}") from e

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
        """Transform a raw Backpack transfer JSON response into an internal `Transfer` model.

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
                f"{raw_response!r} for {asset}",
            )

            if not isinstance(raw_response, dict):
                raise TransformationError(
                    f"Raw transfer response is not a dict: {type(raw_response)}",
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
                logger.warning(
                    "bp_account_mapper_unexpected_status_type",
                    action="transform_raw_transfer_to_internal",
                    message="Unexpected type for raw transfer status",
                    status_type=str(type(raw_status_val)),
                )
                raw_status_str = None

            internal_status = BackpackAccountDataMapper._map_transfer_status_to_internal(
                raw_status_str,
            )

            timestamp: datetime
            if timestamp_ms_str and isinstance(timestamp_ms_str, str | int):
                try:
                    timestamp_ms = int(timestamp_ms_str)
                    timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC)
                except ValueError:
                    logger.warning(
                        f"Invalid timestamp format '{timestamp_ms_str}' "
                        f"for transfer '{transfer_id}'",
                    )
                    timestamp = datetime.now(UTC)
            else:
                timestamp = datetime.now(UTC)

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

            return secure_transform(
                data=transfer_data,
                model_class=Transfer,
                context=f"backpack_transfer_transform_{asset}",
                source_exchange="backpack",
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
        """Transform a raw Backpack withdrawal response into an internal `Withdrawal` model.

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
                    logger.warning(
                        "bp_account_mapper_unknown_withdrawal_status",
                        action="transform_raw_withdrawal_response_to_internal",
                        message="Unknown Backpack withdrawal status encountered",
                        raw_status=raw_status,
                    )

            timestamp_value: datetime
            if timestamp_str:
                try:
                    parsed_dt = parse_datetime_utc(timestamp_str, field_name="created_at")
                    if parsed_dt is None:
                        timestamp_value = datetime.now(UTC)
                    else:
                        timestamp_value = parsed_dt
                except ValueError:
                    logger.warning(
                        "bp_account_mapper_timestamp_parse_failed",
                        action="transform_raw_withdrawal_response_to_internal",
                        message="Failed to parse withdrawal timestamp",
                        timestamp_str=timestamp_str,
                    )
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
                    raw_response.fiat_fee,
                    field_name="fiat_fee",
                    allow_none=True,
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

            # SECURITY FIX: Use secure_transform instead of direct instantiation
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

            return secure_transform(
                data=withdrawal_data,
                model_class=Withdrawal,
                context="backpack_withdrawal_transform",
                source_exchange="backpack",
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw withdrawal to internal: {e}") from e

    @staticmethod
    def _map_self_trade_prevention(raw_stp: str | None) -> SelfTradePrevention | None:
        """Map self trade prevention string to enum."""
        if not raw_stp:
            return None

        stp_str = raw_stp.upper()
        stp_mapping = {
            "REJECT_TAKER": SelfTradePrevention.REJECT_TAKER,
            "REJECTTAKER": SelfTradePrevention.REJECT_TAKER,
            "REJECT_MAKER": SelfTradePrevention.REJECT_MAKER,
            "REJECTMAKER": SelfTradePrevention.REJECT_MAKER,
            "REJECT_BOTH": SelfTradePrevention.REJECT_BOTH,
            "REJECTBOTH": SelfTradePrevention.REJECT_BOTH,
            "NONE": SelfTradePrevention.NONE,
        }
        return stp_mapping.get(stp_str)

    @staticmethod
    def _map_expiry_reason(raw_expiry: str | None) -> OrderExpiryReason | None:
        """Map expiry reason string to enum."""
        if not raw_expiry:
            return None

        expiry_str = raw_expiry.upper()
        expiry_mapping = {
            "USER_CANCELLED": OrderExpiryReason.USER_CANCELLED,
            "CANCELLED": OrderExpiryReason.USER_CANCELLED,
            "LIQUIDATION": OrderExpiryReason.LIQUIDATION,
            "INSUFFICIENT_FUNDS": OrderExpiryReason.INSUFFICIENT_FUNDS,
            "SELF_TRADE_PREVENTION": OrderExpiryReason.SELF_TRADE_PREVENTION,
            "POST_ONLY_TAKER": OrderExpiryReason.POST_ONLY_TAKER,
            "FILL_OR_KILL": OrderExpiryReason.FILL_OR_KILL,
            "IMMEDIATE_OR_CANCEL": OrderExpiryReason.IMMEDIATE_OR_CANCEL,
        }
        return expiry_mapping.get(expiry_str, OrderExpiryReason.UNKNOWN)

    @staticmethod
    def _map_order_origin(raw_origin: str | None) -> OrderUpdateOrigin | None:
        """Map origin string to enum."""
        if not raw_origin:
            return None

        origin_str = raw_origin.upper()
        origin_mapping = {
            "USER": OrderUpdateOrigin.USER,
            "LIQUIDATION_AUTOCLOSE": OrderUpdateOrigin.LIQUIDATION_AUTOCLOSE,
            "ADL_AUTOCLOSE": OrderUpdateOrigin.ADL_AUTOCLOSE,
            "COLLATERAL_CONVERSION": OrderUpdateOrigin.COLLATERAL_CONVERSION,
            "SETTLEMENT_AUTOCLOSE": OrderUpdateOrigin.SETTLEMENT_AUTOCLOSE,
            "BACKSTOP_LIQUIDITY_PROVIDER": OrderUpdateOrigin.BACKSTOP_LIQUIDITY_PROVIDER,
        }
        return origin_mapping.get(origin_str, OrderUpdateOrigin.UNKNOWN)

    @staticmethod
    def _parse_required_order_fields(raw: BackpackRawOrder) -> tuple[Decimal, datetime]:
        """Parse and validate required order fields."""
        parsed_quantity = parse_decimal_value(raw.quantity, allow_none=False)
        if parsed_quantity is None:
            raise TransformationError("quantity missing/invalid in BackpackRawOrder")

        parsed_created_at = parse_datetime_utc(raw.createdAt)
        if parsed_created_at is None:
            raise TransformationError("createdAt missing/invalid in BackpackRawOrder")

        return parsed_quantity, parsed_created_at

    @staticmethod
    def _parse_optional_order_fields(raw: BackpackRawOrder) -> dict[str, Any]:
        """Parse optional order fields."""
        quantity_filled = parse_decimal_value(raw.executedQuantity) or Decimal("0.0")

        # Calculate average fill price if not provided but order has fills
        avg_fill_price = parse_decimal_value(raw.avgFillPrice)
        if avg_fill_price is None and quantity_filled > 0 and raw.executedQuoteQuantity:
            executed_quote = parse_decimal_value(raw.executedQuoteQuantity, allow_none=True)
            if executed_quote is not None and executed_quote > 0:
                avg_fill_price = executed_quote / quantity_filled

        return {
            "quantity_filled": quantity_filled,
            "price": parse_decimal_value(raw.price),
            "stop_price": parse_decimal_value(raw.triggerPrice),
            "avg_fill_price": avg_fill_price,
            "executed_quote_quantity": parse_decimal_value(
                raw.executedQuoteQuantity,
                allow_none=True,
            ),
        }

    @staticmethod
    def transform_raw_order_to_internal(raw: BackpackRawOrder) -> Order:
        """Transform a validated `BackpackRawOrder` object into an internal `Order` domain model.

        Args:
            raw: The validated raw order data from Backpack.

        Returns:
            Order: The corresponding internal `Order` object.

        Raises:
            TransformationError: If essential fields are missing or cannot be parsed.
        """
        try:
            # Parse required fields
            parsed_quantity, parsed_created_at = (
                BackpackAccountDataMapper._parse_required_order_fields(raw)
            )

            # Parse optional fields
            optional_fields = BackpackAccountDataMapper._parse_optional_order_fields(raw)

            # Create BackpackOrderDetails with mapped enums
            bp_details = BackpackOrderDetails(
                executed_quote_quantity=optional_fields["executed_quote_quantity"],
                self_trade_prevention=BackpackAccountDataMapper._map_self_trade_prevention(
                    raw.selfTradePrevention,
                ),
                expiry_reason=BackpackAccountDataMapper._map_expiry_reason(raw.expiryReason),
                origin=BackpackAccountDataMapper._map_order_origin(raw.origin),
            )

            # Parse optional complex fields that can be None
            trigger_by_result = BackpackAccountDataMapper._map_trigger_by_to_internal(raw.triggerBy)
            trigger_by_value = trigger_by_result.value if trigger_by_result is not None else None

            updated_dt = parse_datetime_utc(raw.updatedAt) if raw.updatedAt else None
            updated_at_value = updated_dt.isoformat() if updated_dt is not None else None

            triggered_dt = parse_datetime_utc(raw.triggeredAt) if raw.triggeredAt else None
            triggered_at_value = triggered_dt.isoformat() if triggered_dt is not None else None

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            order_data: dict[str, Any] = {
                "client_order_id": raw.clientId or str(uuid.uuid4()),
                "exchange_order_id": raw.id,
                "related_order_id": raw.relatedOrderId,
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw.symbol,
                "side": BackpackAccountDataMapper._map_side_to_internal(raw.side).value,
                "order_type": BackpackAccountDataMapper._map_type_to_internal(raw.orderType).value,
                "status": BackpackAccountDataMapper._map_status_to_internal(raw.status).value,
                "quantity_requested": str(parsed_quantity),
                "quantity_filled": str(optional_fields["quantity_filled"]),
                "price": str(optional_fields["price"])
                if optional_fields["price"] is not None
                else None,
                "stop_price": str(optional_fields["stop_price"])
                if optional_fields["stop_price"] is not None
                else None,
                "average_fill_price": str(optional_fields["avg_fill_price"])
                if optional_fields["avg_fill_price"] is not None
                else None,
                "trigger_by": trigger_by_value,
                "time_in_force": BackpackAccountDataMapper._map_tif_to_internal(
                    raw.timeInForce,
                ).value,
                "reduce_only": raw.reduceOnly or False,
                "post_only": raw.postOnly or False,
                "created_at": parsed_created_at.isoformat(),
                "updated_at": updated_at_value,
                "triggered_at": triggered_at_value,
                "strategy_name": None,
                "signal_id": None,
                "trades": [],
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=order_data,
                model_class=Order,
                context="backpack_order_transform",
                source_exchange="backpack",
            )
        except Exception as e:
            raise TransformationError(f"Failed to transform raw order to internal: {e}") from e

    @staticmethod
    def transform_raw_trade_to_internal(raw: BackpackRawPublicTrade) -> Trade | None:
        """Transform a validated `BackpackRawPublicTrade` into an internal `Trade` model.

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
                raw.quantity,
                allow_none=False,
                field_name="quantity",
            )
            timestamp = parse_datetime_utc(raw.time, field_name="time")

            if price_dec is None:
                raise TransformationError("price missing/invalid in BackpackRawPublicTrade")
            if quantity_dec is None:
                raise TransformationError("quantity missing/invalid in BackpackRawPublicTrade")
            if timestamp is None:
                raise TransformationError("time missing/invalid in BackpackRawPublicTrade")

            # Backpack REST API for recent trades doesn't provide side
            logger.warning(
                f"Cannot determine trade side for raw trade {raw.id} from REST API. Skipping.",
            )
            return None
        except Exception as e:
            raise TransformationError(f"Failed to transform raw trade to internal: {e}") from e

    @staticmethod
    def transform_ws_fill_event_to_internal_trade(raw_fill: BackpackRawFill) -> Trade | None:
        """Transform a WebSocket fill event (BackpackRawFill) to an Internal Trade model.

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
    def _parse_position_update_size(raw_position_update: BackpackRawPositionUpdate) -> Decimal:
        """Parse and validate size from position update."""
        if raw_position_update.net_quantity:
            size_dec = parse_decimal_value(
                raw_position_update.net_quantity,
                allow_none=False,
                field_name="net_quantity",
            )
            # DEFENSIVE CHECK: Ensure size_dec is not None after parsing.
            # Mypy=[unreachable] Ruff=[unreachable]
            if size_dec is None:
                size_dec = Decimal(0)
        else:
            size_dec = Decimal(0)
        return size_dec

    @staticmethod
    def _parse_position_update_prices(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> dict[str, Decimal | None]:
        """Parse optional price fields from position update."""
        entry_price_dec = None
        if raw_position_update.entry_price:
            entry_price_dec = parse_decimal_value(raw_position_update.entry_price)

        mark_price_dec = None
        if raw_position_update.mark_price:
            mark_price_dec = parse_decimal_value(raw_position_update.mark_price)

        liq_price_dec = None
        if raw_position_update.liquidation_price:
            liq_price_dec = parse_decimal_value(raw_position_update.liquidation_price)

        return {
            "entry_price": entry_price_dec,
            "mark_price": mark_price_dec,
            "liquidation_price": liq_price_dec,
        }

    @staticmethod
    def _parse_position_update_timestamp(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> datetime:
        """Parse timestamp from position update event."""
        timestamp = datetime.now(UTC)
        if raw_position_update.event_time:
            event_timestamp = parse_datetime_utc(
                raw_position_update.event_time,
                field_name="event_time",
            )
            if event_timestamp is not None:
                timestamp = event_timestamp
        return timestamp

    @staticmethod
    def _parse_position_update_margin_details(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> BackpackPositionDetails:
        """Parse margin details from position update."""
        imf_dec = None
        if raw_position_update.initial_margin_fraction:
            imf_dec = parse_decimal_value(
                raw_position_update.initial_margin_fraction,
                allow_none=True,
            )

        mmf_dec = None
        if raw_position_update.maintenance_margin_fraction:
            mmf_dec = parse_decimal_value(
                raw_position_update.maintenance_margin_fraction,
                allow_none=True,
            )

        return BackpackPositionDetails(
            imf_base=imf_dec,
            imf_factor=None,  # Not available in position update
            mmf_base=mmf_dec,
            mmf_factor=None,  # Not available in position update
            cumulative_funding=None,  # Not available in position update
        )

    @staticmethod
    def transform_ws_position_update_to_internal_position(
        raw_position_update: BackpackRawPositionUpdate,
    ) -> DerivativePosition:
        """Transform a BackpackRawPositionUpdate to an Internal DerivativePosition.

        Args:
            raw_position_update: Validated raw position update event data from Backpack WebSocket

        Returns:
            DerivativePosition: Internal domain model with populated fields

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Parse core fields using helper methods
            size_dec = BackpackAccountDataMapper._parse_position_update_size(raw_position_update)
            prices = BackpackAccountDataMapper._parse_position_update_prices(raw_position_update)
            timestamp = BackpackAccountDataMapper._parse_position_update_timestamp(
                raw_position_update,
            )
            bp_details = BackpackAccountDataMapper._parse_position_update_margin_details(
                raw_position_update,
            )

            # Determine side
            side = OrderSide.BUY if size_dec > Decimal(0) else OrderSide.SELL
            entry_price_dec = prices["entry_price"]
            if size_dec == Decimal(0):
                entry_price_dec = None

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            position_data = {
                "exchange": ExchangeName.BACKPACK.value,
                "symbol": raw_position_update.symbol,
                "timestamp": timestamp.isoformat(),
                "side": side.value,
                "size": str(size_dec),
                "entry_price": str(entry_price_dec) if entry_price_dec is not None else None,
                "mark_price": str(prices["mark_price"])
                if prices["mark_price"] is not None
                else None,
                "liquidation_price": str(prices["liquidation_price"])
                if prices["liquidation_price"] is not None
                else None,
                "unrealized_pnl": None,  # Not available in position update
                "realized_pnl": None,  # Not available in position update
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,
            }

            return secure_transform(
                data=position_data,
                model_class=DerivativePosition,
                context="backpack_ws_position_update_transform",
                source_exchange="backpack",
            )
        except Exception as e:
            raise TransformationError(
                f"Failed to transform WebSocket position update to internal: {e}",
            ) from e

    @staticmethod
    def transform_account_settings_update_to_internal(
        args: UpdateAccountSettingsArgs,
        exchange_name: str,
    ) -> AccountSettings:
        """Transform updated account settings args to internal AccountSettings model.

        Args:
            args: The account settings update arguments that were applied
            exchange_name: Name of the exchange

        Returns:
            AccountSettings: Internal model representing the updated settings

        Raises:
            TransformationError: If transformation fails

        """
        try:
            # Create Backpack-specific details
            bp_details = BackpackAccountSettingsDetails(
                leverage_limit_raw=str(args.leverage_limit) if args.leverage_limit else None,
                source_endpoint="/api/v1/account",
            )

            # SECURITY FIX: Use secure_transform instead of direct instantiation
            settings_data = {
                "exchange": exchange_name,
                "timestamp": datetime.now(UTC).isoformat(),
                "leverage_limit": args.leverage_limit,
                "auto_borrow_settlements": args.auto_borrow_settlements,
                "auto_lend": args.auto_lend,
                "auto_realize_pnl": args.auto_realize_pnl,
                "auto_repay_borrows": args.auto_repay_borrows,
                "bp_details": bp_details.model_dump() if bp_details else None,
                "hl_details": None,  # Not applicable for Backpack
            }

            return secure_transform(
                data=settings_data,
                model_class=AccountSettings,
                context="backpack_account_settings_transform",
                source_exchange="backpack",
            )

        except Exception as e:
            raise TransformationError(
                f"Failed to transform account settings update to internal: {e}",
            ) from e
