"""DataFrame processing utilities for historical market data.

This module provides functionality to process pandas DataFrames containing
historical market data and convert them to Candle objects for backtesting.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

import pandas as pd
import structlog

# Import Candle directly - circular import is resolved by module structure
from cyberdelta.core.models.market.candle import Candle
from cyberdelta.core.symbols import Symbol
from cyberdelta.enums.exchange_names import ExchangeName


if TYPE_CHECKING:
    pass


logger = structlog.get_logger(__name__)


class DataFrameProcessingError(ValueError):
    """Error processing DataFrame with required columns."""

    def __init__(self, missing_columns: list[str]) -> None:
        """Initialize with missing column names.

        Args:
            missing_columns: List of column names that are missing from the DataFrame.
        """
        self.missing_columns = missing_columns
        super().__init__(f"DataFrame missing required columns: {missing_columns}")


async def process_dataframe(
    df: pd.DataFrame,
    symbol: Symbol,
    process_market_data: Callable[[Candle], Awaitable[None]],
) -> None:
    """Process a pandas DataFrame of historical/batch market data.

    Expects columns: timestamp (int/str), open/high/low/close/volume (float/str/Decimal).
    Converts rows to Candle objects and feeds them to the provided market data processor.

    Args:
        df: DataFrame with market data (must have timestamp, open, high, low, close, volume).
        symbol: Symbol object this data represents.
        process_market_data: Async function to process each Candle.

    Raises:
        DataFrameProcessingError: If DataFrame is missing required columns.
    """
    required_cols = ["timestamp", "open", "high", "low", "close", "volume"]
    missing = [col for col in required_cols if col not in df.columns]

    if missing:
        # Use logger for errors
        logger.error(
            "dataframe_processing_failed",
            symbol=symbol.value,
            missing_columns=missing,
            message="DataFrame processing failed: Missing required columns",
        )
        raise DataFrameProcessingError(missing)

    logger.info(
        "dataframe_processing_started",
        symbol=symbol,
        rows_count=len(df),
        action="processing_dataframe",
        message=f"Processing DataFrame for {symbol} with {len(df)} rows.",
    )

    # Process each row in the DataFrame
    for idx, row in df.iterrows():
        # Cast to ensure proper typing for pandas operations
        # Note: pandas iterrows returns (index, Series[Unknown]) due to dynamic nature
        idx_typed: int = idx  # type: ignore
        # DEFENSIVE CHECK: Handle pandas Series dynamic typing
        # Pyright=[reportUnknownVariableType] - pandas Series typing is inherently dynamic
        row_typed: pd.Series[Any] = row

        # Extract timestamp and convert to datetime
        # DEFENSIVE CHECK: Handle pandas Series.get dynamic return type
        # Pyright=[reportUnknownMemberType, reportUnknownArgumentType] - pandas Series.get has
        # complex overloads
        timestamp_raw: Any = row_typed.get("timestamp")
        if timestamp_raw is None:
            logger.warning(
                "dataframe_row_missing_timestamp",
                row_index=idx_typed,
                symbol=symbol,
                action="skipping_row",
                message=f"Row {idx_typed}: Missing timestamp, skipping",
            )
            continue

        # Convert timestamp to datetime
        try:
            # Use pandas to_datetime for robust conversion
            # DEFENSIVE CHECK: Handle pandas to_datetime complex overloads
            # Pyright=[reportUnknownMemberType, reportUnknownArgumentType] - pd.to_datetime has
            # many overloads
            pd_timestamp_result: pd.Timestamp = pd.to_datetime(timestamp_raw, utc=True)
            # Convert to standard datetime if it's a pandas Timestamp
            if hasattr(pd_timestamp_result, "to_pydatetime"):
                timestamp = pd_timestamp_result.to_pydatetime()
            else:
                timestamp: datetime = pd_timestamp_result  # type: ignore
        except (ValueError, TypeError, OverflowError) as e:
            logger.warning(
                "dataframe_row_invalid_timestamp",
                row_index=idx_typed,
                symbol=symbol,
                timestamp_raw=timestamp_raw,
                error=str(e),
                action="skipping_row",
                message=f"Row {idx_typed}: Invalid timestamp {timestamp_raw}, skipping: {e}",
            )
            continue

        # Convert row to dict for Candle creation
        row_dict: dict[str, Any] | None = None
        try:
            # Cast the to_dict result to ensure proper typing
            # DEFENSIVE CHECK: Handle pandas Series.to_dict complex overloads
            # Pyright=[reportUnknownMemberType] - pandas Series.to_dict has complex overloads
            row_dict_result: dict[str, Any] = row_typed.to_dict()
            row_dict = row_dict_result

            # Ensure conversion from string for precision
            open_p = Decimal(str(row_dict["open"]))
            high_p = Decimal(str(row_dict["high"]))
            low_p = Decimal(str(row_dict["low"]))
            close_p = Decimal(str(row_dict["close"]))
            volume_p = Decimal(str(row_dict["volume"]))

            # Create Candle instance
            candle = Candle(
                symbol=symbol,
                interval="1m",  # TODO: Use actual interval if available
                open_time=timestamp,
                open=open_p,
                high=high_p,
                low=low_p,
                close=close_p,
                volume=volume_p,
            )

            # Delegate processing to the provided function
            await process_market_data(candle)
        except (InvalidOperation, TypeError, ValueError) as e:
            logger.exception(
                "dataframe_row_conversion_error",
                row_index=idx_typed,
                symbol=symbol,
                error=str(e),
                message="Error converting DataFrame row to MarketData types",
            )
            continue  # Skip this row if conversion fails
    logger.info(
        "dataframe_processing_completed",
        symbol=symbol,
        action="processing_completed",
        message=f"Finished processing DataFrame for {symbol}.",
    )
